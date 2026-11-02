package toshi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
)

// deprecationWarned holds the procedures already warned about in this
// process.
var deprecationWarned sync.Map

// deprecationInterceptor warns once per process about a call the server
// marks deprecated: an answer with a Deprecation header (RFC 9745,
// "@<unix seconds>") or a Sunset header (RFC 8594, an HTTP-date), on success
// or error, and on a stream once its response headers arrive. An answer
// without either header changes nothing.
type deprecationInterceptor struct {
	warn func(message string)
}

func (d deprecationInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		res, err := next(ctx, req)
		if res != nil {
			d.notice(req.Spec().Procedure, res.Header())
		}
		var connectErr *connect.Error
		if errors.As(err, &connectErr) {
			d.notice(req.Spec().Procedure, connectErr.Meta())
		}
		return res, err
	}
}

func (d deprecationInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		return &deprecationClientConn{StreamingClientConn: next(ctx, spec), interceptor: d}
	}
}

func (deprecationInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// deprecationClientConn looks at a stream's response headers after its
// first Receive, when they have arrived.
type deprecationClientConn struct {
	connect.StreamingClientConn
	interceptor deprecationInterceptor
	once        sync.Once
}

func (c *deprecationClientConn) Receive(msg any) error {
	err := c.StreamingClientConn.Receive(msg)
	c.once.Do(func() {
		c.interceptor.notice(c.Spec().Procedure, c.ResponseHeader())
		var connectErr *connect.Error
		if errors.As(err, &connectErr) {
			c.interceptor.notice(c.Spec().Procedure, connectErr.Meta())
		}
	})
	return err
}

// notice warns about procedure when header marks it deprecated and nothing
// has warned about it in this process yet.
func (d deprecationInterceptor) notice(procedure string, header http.Header) {
	deprecation, sunset := header.Get("Deprecation"), header.Get("Sunset")
	if deprecation == "" && sunset == "" {
		return
	}
	if _, warned := deprecationWarned.LoadOrStore(procedure, true); warned {
		return
	}
	message := deprecationMessage(procedure, deprecation, sunset, header.Values("Link"))
	if d.warn != nil {
		d.warn(message)
		return
	}
	fmt.Fprintln(os.Stderr, message)
}

// deprecationMessage is the warning for procedure, from its Deprecation,
// Sunset and Link header values. A value it cannot read is left out.
func deprecationMessage(procedure, deprecation, sunset string, links []string) string {
	message := "toshi: " + procedure + " is deprecated"
	if since, ok := deprecationDate(deprecation); ok {
		message += " since " + since.Format(time.DateOnly)
	}
	if until, err := http.ParseTime(strings.TrimSpace(sunset)); sunset != "" && err == nil {
		message += " and is removed after " + until.UTC().Format(time.DateOnly)
	}
	if docs, ok := deprecationLink(links); ok {
		message += "; see " + docs
	}
	return message
}

// deprecationDate reads RFC 9745's structured-field date, "@" and unix
// seconds.
func deprecationDate(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "@") {
		return time.Time{}, false
	}
	seconds, err := strconv.ParseInt(value[1:], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(seconds, 0).UTC(), true
}

var (
	linkValue = regexp.MustCompile(`<([^>]*)>([^,<]*)`)
	linkRel   = regexp.MustCompile(`(?i);\s*rel\s*=\s*(?:"([^"]*)"|([^\s;,]+))`)
)

// deprecationLink is the target of the first link whose rel includes
// "deprecation".
func deprecationLink(values []string) (string, bool) {
	for _, value := range values {
		for _, link := range linkValue.FindAllStringSubmatch(value, -1) {
			rel := linkRel.FindStringSubmatch(link[2])
			if rel == nil {
				continue
			}
			for _, name := range strings.Fields(rel[1] + " " + rel[2]) {
				if strings.EqualFold(name, "deprecation") {
					return link[1], true
				}
			}
		}
	}
	return "", false
}
