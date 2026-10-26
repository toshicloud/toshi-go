package toshi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"
)

// fakeWaitClock is the Wait loops' clock under the test's control: time moves
// only when a pause asks for it, and a pause ends at once unless it is held.
type fakeWaitClock struct {
	mu     sync.Mutex
	now    time.Time
	starts []time.Time
	hold   bool
	began  chan struct{}
	once   sync.Once
}

// newFakeWaitClock makes clock the Wait loops' clock for one test.
func newFakeWaitClock(t *testing.T) *fakeWaitClock {
	t.Helper()
	c := &fakeWaitClock{
		now:   time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		began: make(chan struct{}),
	}
	now, after := waitNow, waitAfter
	waitNow, waitAfter = c.nowTime, c.after
	t.Cleanup(func() { waitNow, waitAfter = now, after })
	return c
}

// nowTime returns the fake time.
func (c *fakeWaitClock) nowTime() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// attempt records a Wait call that began at the fake time.
func (c *fakeWaitClock) attempt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.starts = append(c.starts, c.now)
}

// attemptStarts returns the fake times the Wait calls began at.
func (c *fakeWaitClock) attemptStarts() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.starts...)
}

// after advances the fake time by d and returns a channel that fired then, or
// one that never fires while the clock is held.
func (c *fakeWaitClock) after(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	hold := c.hold
	c.mu.Unlock()
	c.once.Do(func() { close(c.began) })
	fired := make(chan time.Time, 1)
	if !hold {
		fired <- now
	}
	return fired
}

// waitServer answers the four Wait procedures from one answer function.
type waitServer struct {
	workspacev1connect.UnimplementedWorkloadServiceHandler
	answer func() bool
}

func (s *waitServer) WaitJob(context.Context, *connect.Request[workspacev1.WaitJobRequest]) (*connect.Response[workspacev1.WaitJobResponse], error) {
	return connect.NewResponse(&workspacev1.WaitJobResponse{Job: &workspacev1.Job{}, Reached: s.answer()}), nil
}

func (s *waitServer) WaitSandbox(context.Context, *connect.Request[workspacev1.WaitSandboxRequest]) (*connect.Response[workspacev1.WaitSandboxResponse], error) {
	return connect.NewResponse(&workspacev1.WaitSandboxResponse{Sandbox: &workspacev1.Sandbox{}, Reached: s.answer()}), nil
}

func (s *waitServer) WaitService(context.Context, *connect.Request[workspacev1.WaitServiceRequest]) (*connect.Response[workspacev1.WaitServiceResponse], error) {
	return connect.NewResponse(&workspacev1.WaitServiceResponse{Service: &workspacev1.Service{}, Reached: s.answer()}), nil
}

func (s *waitServer) WaitOperation(context.Context, *connect.Request[workspacev1.WaitOperationRequest]) (*connect.Response[workspacev1.WaitOperationResponse], error) {
	return connect.NewResponse(&workspacev1.WaitOperationResponse{Operation: &workspacev1.Operation{Done: s.answer()}}), nil
}

// newWaitTestServer serves the four Wait procedures over TLS with HTTP/2.
func newWaitTestServer(t *testing.T, s *waitServer) *testServer {
	t.Helper()
	mux := http.NewServeMux()
	path, handler := workspacev1connect.NewWorkloadServiceHandler(s)
	mux.Handle(path, handler)
	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	return &testServer{Server: server}
}

// TestWaitPausesBetweenAttempts proves that every Wait loop lets a second pass
// between the starts of two calls: the server answers reached=false twice and
// then reached, and the fake clock moves by the pause each call asks for.
func TestWaitPausesBetweenAttempts(t *testing.T) {
	loops := []struct {
		name string
		wait func(context.Context, *Client) error
	}{
		{"jobs", func(ctx context.Context, c *Client) error {
			_, err := c.Workload.Jobs.Wait(ctx, &workspacev1.WaitJobRequest{})
			return err
		}},
		{"sandboxes", func(ctx context.Context, c *Client) error {
			_, err := c.Workload.Sandboxes.Wait(ctx, &workspacev1.WaitSandboxRequest{})
			return err
		}},
		{"services", func(ctx context.Context, c *Client) error {
			_, err := c.Workload.Services.Wait(ctx, &workspacev1.WaitServiceRequest{})
			return err
		}},
		{"operation", func(ctx context.Context, c *Client) error {
			_, err := c.Workload.WaitOperation(ctx, &workspacev1.WaitOperationRequest{})
			return err
		}},
	}
	for _, loop := range loops {
		t.Run(loop.name, func(t *testing.T) {
			clock := newFakeWaitClock(t)
			var calls atomic.Int32
			server := &waitServer{answer: func() bool {
				clock.attempt()
				return calls.Add(1) >= 3
			}}
			client := newTestClient(t, newWaitTestServer(t, server))
			if err := loop.wait(context.Background(), client); err != nil {
				t.Fatalf("Wait: %v", err)
			}
			starts := clock.attemptStarts()
			if len(starts) != 3 {
				t.Fatalf("%d calls, want 3", len(starts))
			}
			for i := 1; i < len(starts); i++ {
				gap := starts[i].Sub(starts[i-1])
				if gap < waitMinInterval || gap > waitMinInterval+waitMaxJitter {
					t.Errorf("call %d began %v after call %d, want between %v and %v",
						i+1, gap, i, waitMinInterval, waitMinInterval+waitMaxJitter)
				}
			}
		})
	}
}

// TestWaitContextEndsThePause proves that the context ends a paused Wait
// before it asks again, with a pause that never ends on its own.
func TestWaitContextEndsThePause(t *testing.T) {
	clock := newFakeWaitClock(t)
	clock.hold = true
	var calls atomic.Int32
	server := &waitServer{answer: func() bool {
		clock.attempt()
		calls.Add(1)
		return false
	}}
	client := newTestClient(t, newWaitTestServer(t, server))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-clock.began
		cancel()
	}()

	_, err := client.Workload.Jobs.Wait(ctx, &workspacev1.WaitJobRequest{})
	if err == nil {
		t.Fatal("Wait returned no error after its context ended")
	}
	if got := connect.CodeOf(err); got != connect.CodeCanceled {
		t.Fatalf("code %v, want %v (err %v)", got, connect.CodeCanceled, err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want one that matches context.Canceled", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("%d calls, want 1: the context ended the pause before the next call", got)
	}
}
