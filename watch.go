package toshi

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

// WorkloadWatch streams a workspace's workload changes. Receive reports each
// message, and the watch resumes from its last cursor after a dropped
// connection.
type WorkloadWatch struct {
	client *Client
	ctx    context.Context
	cancel context.CancelFunc
	req    *workspacev1.WatchWorkloadsRequest

	mu       sync.Mutex
	stream   *connect.ServerStreamForClient[workspacev1.WatchWorkloadsResponse]
	msg      *workspacev1.WatchWorkloadsResponse
	cursor   string
	snapshot []*workspacev1.Workload
	err      error
	closed   atomic.Bool
	attempt  int
}

// WatchWorkloads starts a workload watch. With an empty cursor it lists the
// workloads first, page by page, and watches from the list's cursor; the
// listed workloads are available from Snapshot. The stream opens on the first
// Receive, and an error opening it is returned by Err. A refused cursor ends
// the watch with OUT_OF_RANGE.
func (c *Client) WatchWorkloads(ctx context.Context, req *workspacev1.WatchWorkloadsRequest) (*WorkloadWatch, error) {
	if req == nil {
		req = &workspacev1.WatchWorkloadsRequest{}
	}
	watchCtx, cancel := context.WithCancel(ctx)
	w := &WorkloadWatch{
		client: c,
		ctx:    watchCtx,
		cancel: cancel,
		req:    proto.Clone(req).(*workspacev1.WatchWorkloadsRequest),
		cursor: req.GetCursor(),
	}
	if req.GetCursor() == "" {
		if err := w.list(watchCtx); err != nil {
			cancel()
			return nil, err
		}
	}
	w.req.Cursor = w.cursor
	w.req.Keepalive = true
	return w, nil
}

// list pages ListWorkloads with the watch's project, environment and kinds,
// and leaves the list's cursor in cursor.
func (w *WorkloadWatch) list(ctx context.Context) error {
	list := &workspacev1.ListWorkloadsRequest{
		Project:     w.req.GetProject(),
		Environment: w.req.GetEnvironment(),
		Kinds:       w.req.GetKinds(),
	}
	for {
		resp, err := w.client.Workspace.ListWorkloads(ctx, connect.NewRequest(list))
		if err != nil {
			return err
		}
		w.snapshot = append(w.snapshot, resp.Msg.GetWorkloads()...)
		w.cursor = resp.Msg.GetCursor()
		next := resp.Msg.GetNextPageToken()
		if next == "" {
			return nil
		}
		list.PageToken = next
	}
}

// open starts the stream, retrying an unavailable server or a dropped
// connection within the retry policy.
func (w *WorkloadWatch) open() error {
	w.req.Cursor = w.cursor
	for {
		stream, err := w.client.Workspace.WatchWorkloads(w.ctx, connect.NewRequest(w.req))
		if err == nil {
			w.mu.Lock()
			w.stream = stream
			w.mu.Unlock()
			return nil
		}
		if retryErr := w.retry(err); retryErr != nil {
			return retryErr
		}
	}
}

// retry waits before one more try. It returns the cause when no try remains
// or the error is not retryable.
func (w *WorkloadWatch) retry(cause error) error {
	if !retryable(cause) {
		return cause
	}
	if w.attempt >= w.client.maxReconnects() {
		return cause
	}
	if err := sleepBackoff(w.ctx, w.client.retry, w.attempt); err != nil {
		return err
	}
	w.attempt++
	return nil
}

// Receive waits for the next change. It returns false when the watch ended,
// and Err then says why. A keepalive is not reported as a change.
func (w *WorkloadWatch) Receive() bool {
	w.mu.Lock()
	w.msg = nil
	w.mu.Unlock()
	if w.closed.Load() {
		return false
	}
	if w.err != nil {
		return false
	}
	for {
		w.mu.Lock()
		stream := w.stream
		w.mu.Unlock()
		if stream == nil {
			if err := w.open(); err != nil {
				w.fail(err)
				return false
			}
			continue
		}
		if !stream.Receive() {
			err := stream.Err()
			w.mu.Lock()
			w.stream = nil
			w.mu.Unlock()
			if w.closed.Load() {
				return false
			}
			if err == nil {
				if w.ctx.Err() != nil {
					w.fail(ctxError(w.ctx))
					return false
				}
				err = errStreamEnded
			}
			if retryErr := w.retry(err); retryErr != nil {
				w.fail(retryErr)
				return false
			}
			continue
		}
		msg := stream.Msg()
		w.attempt = 0
		if keepalive := msg.GetKeepalive(); keepalive != nil {
			w.mu.Lock()
			if keepalive.GetCursor() != "" {
				w.cursor = keepalive.GetCursor()
			}
			w.mu.Unlock()
			continue
		}
		w.mu.Lock()
		w.msg = msg
		if msg.GetCursor() != "" {
			w.cursor = msg.GetCursor()
		}
		w.mu.Unlock()
		return true
	}
}

// fail records the watch's terminal error.
func (w *WorkloadWatch) fail(err error) {
	w.mu.Lock()
	w.err = err
	w.mu.Unlock()
}

// Msg returns the last change Receive reported, or nil.
func (w *WorkloadWatch) Msg() *workspacev1.WatchWorkloadsResponse {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.msg
}

// Err returns why the watch ended, or nil while it has not.
func (w *WorkloadWatch) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// Cursor returns the position to resume from: the last change's or
// keepalive's cursor, or the position the watch started at.
func (w *WorkloadWatch) Cursor() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cursor
}

// Snapshot returns the workloads listed before the watch started, or nil when
// the request named a cursor.
func (w *WorkloadWatch) Snapshot() []*workspacev1.Workload {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snapshot
}

// Close ends the watch and releases its stream.
func (w *WorkloadWatch) Close() error {
	w.closed.Store(true)
	w.mu.Lock()
	stream := w.stream
	w.stream = nil
	w.mu.Unlock()
	if stream != nil {
		_ = stream.Close()
	}
	w.cancel()
	return nil
}

// EventWatch streams a workspace's events. With an empty cursor it lists
// events one page at a time first, then watches from the last cursor.
type EventWatch struct {
	client *Client
	ctx    context.Context
	cancel context.CancelFunc
	req    *workspacev1.WatchEventsRequest
	list   *workspacev1.ListEventsRequest

	mu        sync.Mutex
	stream    *connect.ServerStreamForClient[workspacev1.WatchEventsResponse]
	event     *workspacev1.Event
	page      []*workspacev1.Event
	pageIndex int
	listing   bool
	cursor    string
	resume    string
	err       error
	closed    atomic.Bool
	attempt   int
}

// WatchEvents starts an event watch. With an empty cursor it pages
// ListEvents, yields each listed event, and then watches from the last
// page's cursor. The stream opens during Receive, and an error opening it is
// returned by Err. A refused cursor ends the watch with OUT_OF_RANGE.
func (c *Client) WatchEvents(ctx context.Context, req *workspacev1.WatchEventsRequest) (*EventWatch, error) {
	if req == nil {
		req = &workspacev1.WatchEventsRequest{}
	}
	watchCtx, cancel := context.WithCancel(ctx)
	w := &EventWatch{
		client:  c,
		ctx:     watchCtx,
		cancel:  cancel,
		req:     proto.Clone(req).(*workspacev1.WatchEventsRequest),
		cursor:  req.GetCursor(),
		resume:  req.GetCursor(),
		listing: req.GetCursor() == "",
	}
	if w.listing {
		w.list = &workspacev1.ListEventsRequest{
			Project:     w.req.GetProject(),
			Environment: w.req.GetEnvironment(),
			WorkloadId:  w.req.GetWorkloadId(),
		}
	}
	return w, nil
}

// open starts the watch stream from resume.
func (w *EventWatch) open() error {
	w.req.Cursor = w.resume
	w.req.Keepalive = true
	for {
		stream, err := w.client.Workspace.WatchEvents(w.ctx, connect.NewRequest(w.req))
		if err == nil {
			w.mu.Lock()
			w.stream = stream
			w.mu.Unlock()
			return nil
		}
		if retryErr := w.retry(err); retryErr != nil {
			return retryErr
		}
	}
}

// retry waits before one more try, or returns the cause when the watch must
// end.
func (w *EventWatch) retry(cause error) error {
	if !retryable(cause) {
		return cause
	}
	if w.attempt >= w.client.maxReconnects() {
		return cause
	}
	if err := sleepBackoff(w.ctx, w.client.retry, w.attempt); err != nil {
		return err
	}
	w.attempt++
	return nil
}

// Receive waits for the next event. It returns false when the watch ended,
// and Err then says why. A keepalive is not reported.
func (w *EventWatch) Receive() bool {
	w.mu.Lock()
	w.event = nil
	w.mu.Unlock()
	if w.closed.Load() {
		return false
	}
	if w.err != nil {
		return false
	}
	for {
		if w.listing {
			if w.pageIndex < len(w.page) {
				event := w.page[w.pageIndex]
				w.pageIndex++
				w.attempt = 0
				w.mu.Lock()
				w.event = event
				if event.GetCursor() != "" {
					w.cursor = event.GetCursor()
					w.resume = event.GetCursor()
				}
				w.mu.Unlock()
				return true
			}
			resp, err := w.client.Workspace.ListEvents(w.ctx, connect.NewRequest(w.list))
			if err != nil {
				if w.closed.Load() {
					return false
				}
				w.fail(err)
				return false
			}
			w.page = resp.Msg.GetEvents()
			w.pageIndex = 0
			if cursor := resp.Msg.GetCursor(); cursor != "" {
				w.list.Cursor = cursor
			}
			if len(w.page) > 0 {
				continue
			}
			if resp.Msg.GetCursor() != "" {
				w.resume = resp.Msg.GetCursor()
			}
			w.listing = false
			if err := w.open(); err != nil {
				w.fail(err)
				return false
			}
			continue
		}

		w.mu.Lock()
		stream := w.stream
		w.mu.Unlock()
		if stream == nil {
			if err := w.open(); err != nil {
				w.fail(err)
				return false
			}
			continue
		}
		if !stream.Receive() {
			err := stream.Err()
			w.mu.Lock()
			w.stream = nil
			w.mu.Unlock()
			if w.closed.Load() {
				return false
			}
			if err == nil {
				if w.ctx.Err() != nil {
					w.fail(ctxError(w.ctx))
					return false
				}
				err = errStreamEnded
			}
			if retryErr := w.retry(err); retryErr != nil {
				w.fail(retryErr)
				return false
			}
			continue
		}
		msg := stream.Msg()
		w.attempt = 0
		if keepalive := msg.GetKeepalive(); keepalive != nil {
			w.mu.Lock()
			if w.pageIndex >= len(w.page) && keepalive.GetCursor() != "" {
				w.cursor = keepalive.GetCursor()
				w.resume = keepalive.GetCursor()
			}
			w.mu.Unlock()
			continue
		}
		event := msg.GetEvent()
		if event == nil {
			continue
		}
		w.mu.Lock()
		w.event = event
		if event.GetCursor() != "" {
			w.cursor = event.GetCursor()
			w.resume = event.GetCursor()
		}
		w.mu.Unlock()
		return true
	}
}

// fail records the watch's terminal error.
func (w *EventWatch) fail(err error) {
	w.mu.Lock()
	w.err = err
	w.mu.Unlock()
}

// Event returns the last event Receive reported, or nil.
func (w *EventWatch) Event() *workspacev1.Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.event
}

// Err returns why the watch ended, or nil while it has not.
func (w *EventWatch) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// Cursor returns the position to resume from: the last event's cursor, then
// a keepalive's, or the position the watch started at.
func (w *EventWatch) Cursor() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cursor
}

// Close ends the watch and releases its stream.
func (w *EventWatch) Close() error {
	w.closed.Store(true)
	w.mu.Lock()
	stream := w.stream
	w.stream = nil
	w.mu.Unlock()
	if stream != nil {
		_ = stream.Close()
	}
	w.cancel()
	return nil
}

// errStreamEnded reports a stream that ended without an error while its
// context was still alive. It is retryable because it wraps
// io.ErrUnexpectedEOF.
var errStreamEnded = fmt.Errorf("%w: toshi: stream ended without an error", io.ErrUnexpectedEOF)
