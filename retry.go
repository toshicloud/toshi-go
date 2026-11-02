package toshi

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"math"
	mathrand "math/rand/v2"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"go.toshicloud.io/api/toshipb"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"
)

// retryClass says what one procedure's retries are.
type retryClass int

const (
	retryNone retryClass = iota
	retryRead
	retryKeyed
)

// A call's retry class comes from its generated descriptor, so a call a new
// API version adds has one without an edit here:
//   - a stream is never retried here (a watch reconnects on its own);
//   - a unary call whose request has a request_key field is retryKeyed: the
//     interceptor fills the key and keeps it for every try, so the server
//     answers a retry with the first result;
//   - a unary call whose toshi.permission level is read is retryRead;
//   - every other call is retryNone.
//
// The API has no option that marks a call whose retry would fail after the
// first try worked, so such a call is listed in retryExceptions.

// retryExceptions holds the calls the rule above gets wrong. A test fails
// when the rule gives one of them its listed class, so the list only holds
// real exceptions.
var retryExceptions = map[string]retryClass{
	// A retry returns the session the first call opened, with replayed set
	// and no token, and opens nothing, so the caller cannot use it; its
	// request takes a request_key.
	workspacev1connect.DisplayServiceOpenDisplaySessionProcedure: retryNone,
}

// retryClassFor is the retry class of the call spec describes: an
// exception's, else the one its descriptor gives it. A call whose
// descriptor is unknown is not retried.
func retryClassFor(spec connect.Spec) retryClass {
	if class, ok := retryExceptions[spec.Procedure]; ok {
		return class
	}
	method, ok := spec.Schema.(protoreflect.MethodDescriptor)
	if !ok {
		method = methodByProcedure(spec.Procedure)
	}
	if method == nil {
		return retryNone
	}
	return derivedRetryClass(method)
}

// derivedRetryClass is the class method's descriptor gives it, before the
// exceptions.
func derivedRetryClass(method protoreflect.MethodDescriptor) retryClass {
	if method.IsStreamingClient() || method.IsStreamingServer() {
		return retryNone
	}
	if method.Input().Fields().ByName("request_key") != nil {
		return retryKeyed
	}
	if options := method.Options(); options != nil {
		level, _ := proto.GetExtension(options, toshipb.E_Permission).(string)
		if strings.HasSuffix(level, ".read") {
			return retryRead
		}
	}
	return retryNone
}

// methodByProcedure finds a procedure's method among the registered
// descriptors, for a call made without a schema; nil when none matches.
func methodByProcedure(procedure string) protoreflect.MethodDescriptor {
	name := strings.ReplaceAll(strings.TrimPrefix(procedure, "/"), "/", ".")
	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil
	}
	method, _ := desc.(protoreflect.MethodDescriptor)
	return method
}

// normalizeRetry fills zero backoff values with the defaults and leaves
// MaxAttempts as given, so a value of 1 or less still disables retries.
func normalizeRetry(policy RetryPolicy) RetryPolicy {
	defaults := DefaultRetryPolicy()
	if policy.InitialBackoff <= 0 {
		policy.InitialBackoff = defaults.InitialBackoff
	}
	if policy.MaxBackoff <= 0 {
		policy.MaxBackoff = defaults.MaxBackoff
	}
	return policy
}

// retryInterceptor retries a unary call when its procedure's class allows
// it, under the caller's context and with full-jitter backoff.
type retryInterceptor struct {
	policy RetryPolicy
}

func (r retryInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		class := retryClassFor(req.Spec())
		if class == retryNone {
			return next(ctx, req)
		}
		if class == retryKeyed {
			fillRequestKey(requestMessage(req))
		}
		attempts := r.policy.MaxAttempts
		if attempts < 1 {
			attempts = 1
		}
		var last error
		for attempt := 0; attempt < attempts; attempt++ {
			tooMany := &tooManyRequests{}
			resp, err := next(context.WithValue(ctx, tooManyKey{}, tooMany), req)
			if err == nil {
				return resp, nil
			}
			last = err
			wait, waited := serverWait(err, tooMany)
			if attempt == attempts-1 || (!retryable(err) && !waited) {
				return nil, err
			}
			if err := sleepBackoff(ctx, r.policy, attempt, wait); err != nil {
				return nil, err
			}
		}
		return nil, last
	}
}

func (r retryInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (r retryInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// retryable reports whether err is a dropped connection or an unavailable
// server.
func retryable(err error) bool {
	return connect.CodeOf(err) == connect.CodeUnavailable || errors.Is(err, io.ErrUnexpectedEOF)
}

// sleepBackoff waits a full-jitter delay for attempt, or the server's wait
// when that is longer, never more than MaxBackoff. It returns the context's
// error when ctx ends first.
func sleepBackoff(ctx context.Context, policy RetryPolicy, attempt int, wait time.Duration) error {
	delay := min(max(backoff(policy, attempt), wait), policy.MaxBackoff)
	if delay <= 0 {
		return nil
	}
	return retrySleep(ctx, delay)
}

// retrySleep waits d, or returns the context's error when ctx ends first.
// Tests replace it so that a wait costs no time.
var retrySleep = func(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctxError(ctx)
	case <-timer.C:
		return nil
	}
}

// serverWait returns how long the server asked the client to wait before a
// retry, and whether it asked. A RESOURCE_EXHAUSTED refusal names the wait
// in a google.rpc.RetryInfo detail and in its Retry-After metadata, the
// detail winning when both are there; the API's rate limit sends both, with
// the same whole seconds. Such a refusal is retried only when it names a
// wait: one without, such as a plan's limit, does not pass on a retry. An
// HTTP 429, which connect-go reports as CodeUnavailable without its headers,
// names it in the Retry-After the HTTP client recorded in tooMany. Every
// other error names none.
func serverWait(err error, tooMany *tooManyRequests) (time.Duration, bool) {
	var cerr *connect.Error
	if !errors.As(err, &cerr) {
		return 0, false
	}
	switch cerr.Code() {
	case connect.CodeResourceExhausted:
		for _, d := range cerr.Details() {
			if d.Type() != "google.rpc.RetryInfo" {
				continue
			}
			if wait, ok := decodeRetryInfo(d.Bytes()); ok {
				return wait, true
			}
		}
		return parseRetryAfter(cerr.Meta().Get("Retry-After"))
	case connect.CodeUnavailable:
		if tooMany != nil && tooMany.seen {
			return parseRetryAfter(tooMany.retryAfter)
		}
	}
	return 0, false
}

// parseRetryAfter reads a Retry-After value of whole seconds, the form the
// API sends. A date or anything else names no wait.
func parseRetryAfter(v string) (time.Duration, bool) {
	secs, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || secs < 0 || secs > int64(math.MaxInt64/time.Second) {
		return 0, false
	}
	return time.Duration(secs) * time.Second, true
}

// decodeRetryInfo reads google.rpc.RetryInfo's retry_delay (1), a
// google.protobuf.Duration of seconds (1) and nanos (2).
func decodeRetryInfo(b []byte) (time.Duration, bool) {
	var delay []byte
	found := false
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, false
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return 0, false
			}
			delay, found = v, true
			b = b[n:]
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return 0, false
		}
		b = b[n:]
	}
	if !found {
		return 0, false
	}
	var secs, nanos int64
	for len(delay) > 0 {
		num, typ, n := protowire.ConsumeTag(delay)
		if n < 0 {
			return 0, false
		}
		delay = delay[n:]
		if typ == protowire.VarintType && (num == 1 || num == 2) {
			v, n := protowire.ConsumeVarint(delay)
			if n < 0 {
				return 0, false
			}
			if num == 1 {
				secs = int64(v)
			} else {
				nanos = int64(int32(v))
			}
			delay = delay[n:]
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, delay)
		if n < 0 {
			return 0, false
		}
		delay = delay[n:]
	}
	if secs < 0 || nanos < 0 || secs > int64(math.MaxInt64/time.Second)-1 {
		return 0, false
	}
	return time.Duration(secs)*time.Second + time.Duration(nanos), true
}

// tooManyRequests records an HTTP 429 answer to one try of a unary call
// and its Retry-After. The retry interceptor puts one in each try's context
// under tooManyKey, and headerClient fills it.
type tooManyRequests struct {
	seen       bool
	retryAfter string
}

type tooManyKey struct{}

// backoff returns a random delay in [0, min(MaxBackoff, InitialBackoff times
// two to the attempt)].
func backoff(policy RetryPolicy, attempt int) time.Duration {
	limit := policy.InitialBackoff
	for i := 0; i < attempt; i++ {
		if limit >= policy.MaxBackoff {
			break
		}
		limit *= 2
		if limit <= 0 {
			limit = policy.MaxBackoff
			break
		}
	}
	if limit > policy.MaxBackoff || limit <= 0 {
		limit = policy.MaxBackoff
	}
	if limit <= 0 {
		return 0
	}
	return time.Duration(mathrand.Int64N(int64(limit) + 1))
}

// ctxError returns the context's error as a Connect error with its code.
func ctxError(ctx context.Context) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}
	code := connect.CodeCanceled
	if errors.Is(err, context.DeadlineExceeded) {
		code = connect.CodeDeadlineExceeded
	}
	return connect.NewError(code, err)
}

// fillRequestKey sets an empty request_key string field in place.
func fillRequestKey(msg proto.Message) {
	if msg == nil {
		return
	}
	m := msg.ProtoReflect()
	if !m.IsValid() {
		return
	}
	fd := m.Descriptor().Fields().ByName("request_key")
	if !isString(fd) || m.Get(fd).String() != "" {
		return
	}
	m.Set(fd, protoreflect.ValueOfString(randomKey()))
}

// maxReconnects is how many times a watch reconnects after a drop.
func (c *Client) maxReconnects() int {
	if c.retry.MaxAttempts <= 1 {
		return 0
	}
	return c.retry.MaxAttempts - 1
}

// randomKey returns 128 random bits as 32 hexadecimal characters.
func randomKey() string {
	var b [16]byte
	_, _ = cryptorand.Read(b[:])
	return hex.EncodeToString(b[:])
}
