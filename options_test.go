package toshi_test

import (
	"context"
	"errors"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi"
	"go.toshicloud.io/toshi/toshitest"
)

// callerInterceptor counts the calls and streams it sees and keeps the
// request headers it was handed.
type callerInterceptor struct {
	unary   atomic.Int32
	streams atomic.Int32
	mu      sync.Mutex
	headers []http.Header
}

func (c *callerInterceptor) keep(header http.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headers = append(c.headers, header)
}

func (c *callerInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		c.unary.Add(1)
		c.keep(req.Header())
		return next(ctx, req)
	}
}

func (c *callerInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		c.streams.Add(1)
		conn := next(ctx, spec)
		c.keep(conn.RequestHeader())
		return conn
	}
}

func (c *callerInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// noAuthorization fails the test when a header the interceptor kept holds
// Authorization. The headers are read after the calls end, so a header set
// later on the same request would show.
func (c *callerInterceptor) noAuthorization(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, header := range c.headers {
		if got := header.Get("Authorization"); got != "" {
			t.Fatalf("the interceptor saw Authorization %q", got)
		}
	}
}

func TestInterceptorSeesOneCallAcrossRetries(t *testing.T) {
	var tries atomic.Int32
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceGetJobProcedure,
		func(_ context.Context, _ *workspacev1.GetJobRequest) (*workspacev1.GetJobResponse, error) {
			if tries.Add(1) < 3 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
			}
			return &workspacev1.GetJobResponse{Job: &workspacev1.Job{Id: "job-1"}}, nil
		})
	caller := &callerInterceptor{}
	client := tt.Client(toshi.WithRetryPolicy(fastPolicy()), toshi.WithInterceptors(caller))
	if _, err := client.Workload.Jobs.Get(context.Background(), &workspacev1.GetJobRequest{Id: "job-1"}); err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if tries.Load() != 3 || caller.unary.Load() != 1 {
		t.Fatalf("the server saw %d tries and the interceptor %d calls, want 3 and 1", tries.Load(), caller.unary.Load())
	}
	caller.noAuthorization(t)
}

func TestInterceptorRunsForStreams(t *testing.T) {
	tt := toshitest.New(t)
	tt.Job("job-1").ExitCode(0)
	caller := &callerInterceptor{}
	client := tt.Client(toshi.WithInterceptors(caller))
	watch, err := client.Workload.Jobs.WatchOutput(context.Background(), &workspacev1.WatchOutputRequest{Id: "job-1"})
	if err != nil {
		t.Fatalf("WatchOutput: %v", err)
	}
	if !watch.Receive() {
		t.Fatalf("WatchOutput ended: %v", watch.Err())
	}
	if caller.streams.Load() != 1 {
		t.Fatalf("the interceptor saw %d streams, want 1", caller.streams.Load())
	}
	if got := tt.Calls()[0].Header.Get("Authorization"); got != "Bearer "+toshitest.Token {
		t.Fatalf("the server got Authorization %q", got)
	}
	caller.noAuthorization(t)
}

func TestUserAgent(t *testing.T) {
	base := "toshi-go/" + toshi.Version + " connect-go/" + connect.Version + " (" + runtime.Version() + ")"
	for _, tc := range []struct {
		name string
		opts []toshi.Option
		want string
	}{
		{"default", nil, base},
		{"product", []toshi.Option{toshi.WithUserAgent("myapp/1.2")}, "myapp/1.2 " + base},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := toshitest.New(t)
			tt.Job("job-1")
			if _, err := tt.Client(tc.opts...).Workload.Jobs.Get(context.Background(), &workspacev1.GetJobRequest{Id: "job-1"}); err != nil {
				t.Fatalf("GetJob: %v", err)
			}
			if got := tt.Calls()[0].Header.Get("User-Agent"); got != tc.want {
				t.Fatalf("User-Agent %q, want %q", got, tc.want)
			}
		})
	}
}

// TestConnectOptionsApply proves an option reaches the service clients: the
// fake sets no size limit, so RESOURCE_EXHAUSTED comes from the client's.
func TestConnectOptionsApply(t *testing.T) {
	tt := toshitest.New(t)
	client := tt.Client(toshi.WithConnectOptions(connect.WithSendMaxBytes(16)))
	_, err := client.Workload.Jobs.Get(context.Background(), &workspacev1.GetJobRequest{Id: strings.Repeat("x", 64)})
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("GetJob over the send limit: err %v, want RESOURCE_EXHAUSTED", err)
	}
}
