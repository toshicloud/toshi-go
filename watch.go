package toshi

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"
)

// watch is a server stream that resumes from its last cursor after a dropped
// connection. A type's watch embeds one and adds its snapshot.
type watch[T any] struct {
	client *Client
	ctx    context.Context
	cancel context.CancelFunc
	// open starts the stream at a cursor.
	open func(ctx context.Context, cursor string) (*connect.ServerStreamForClient[T], error)
	// place returns a message's cursor and whether the message is a
	// keepalive, which is not reported as a change.
	place func(*T) (string, bool)

	mu      sync.Mutex
	stream  *connect.ServerStreamForClient[T]
	msg     *T
	cursor  string
	err     error
	closed  atomic.Bool
	attempt int
}

// newWatch starts a watch at cursor. The stream opens on the first Receive,
// and an error opening it is returned by Err.
func newWatch[T any](ctx context.Context, client *Client, cursor string, open func(context.Context, string) (*connect.ServerStreamForClient[T], error), place func(*T) (string, bool)) *watch[T] {
	watchCtx, cancel := context.WithCancel(ctx)
	return &watch[T]{
		client: client,
		ctx:    watchCtx,
		cancel: cancel,
		cursor: cursor,
		open:   open,
		place:  place,
	}
}

// Receive waits for the next change. It returns false when the watch ended,
// and Err then says why. A keepalive is not reported as a change.
func (w *watch[T]) Receive() bool {
	w.mu.Lock()
	w.msg = nil
	done := w.err != nil
	w.mu.Unlock()
	if w.closed.Load() || done {
		return false
	}
	for {
		w.mu.Lock()
		stream := w.stream
		w.mu.Unlock()
		if stream == nil {
			if err := w.reopen(); err != nil {
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
		cursor, keepalive := w.place(msg)
		w.mu.Lock()
		if cursor != "" {
			w.cursor = cursor
		}
		if keepalive {
			w.mu.Unlock()
			continue
		}
		w.msg = msg
		w.mu.Unlock()
		return true
	}
}

// reopen starts the stream, retrying an unavailable server or a dropped
// connection within the retry policy.
func (w *watch[T]) reopen() error {
	for {
		w.mu.Lock()
		cursor := w.cursor
		w.mu.Unlock()
		stream, err := w.open(w.ctx, cursor)
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
func (w *watch[T]) retry(cause error) error {
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

// fail records the watch's terminal error.
func (w *watch[T]) fail(err error) {
	w.mu.Lock()
	w.err = err
	w.mu.Unlock()
}

// Msg returns the last change Receive reported, or nil.
func (w *watch[T]) Msg() *T {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.msg
}

// Err returns why the watch ended, or nil while it has not.
func (w *watch[T]) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// Cursor returns the position to resume from: the last change's or
// keepalive's cursor, or the position the watch started at.
func (w *watch[T]) Cursor() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cursor
}

// setMsg reports msg as the last one Receive handed back.
func (w *watch[T]) setMsg(msg *T) {
	w.mu.Lock()
	w.msg = msg
	w.mu.Unlock()
}

// setCursor moves the position to resume from.
func (w *watch[T]) setCursor(cursor string) {
	w.mu.Lock()
	w.cursor = cursor
	w.mu.Unlock()
}

// Close ends the watch and releases its stream.
func (w *watch[T]) Close() error {
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

// JobWatch streams a workspace's job changes. Receive reports each message,
// and the watch resumes from its last cursor after a dropped connection.
type JobWatch struct {
	core     *watch[workspacev1.WatchJobsResponse]
	snapshot []*workspacev1.Job
}

// Receive waits for the next change. It returns false when the watch ended,
// and Err then says why. A keepalive is not reported as a change.
func (w *JobWatch) Receive() bool { return w.core.Receive() }

// Msg returns the last change Receive reported, or nil.
func (w *JobWatch) Msg() *workspacev1.WatchJobsResponse { return w.core.Msg() }

// Err returns why the watch ended, or nil while it has not.
func (w *JobWatch) Err() error { return w.core.Err() }

// Cursor returns the position to resume from.
func (w *JobWatch) Cursor() string { return w.core.Cursor() }

// Close ends the watch and releases its stream.
func (w *JobWatch) Close() error { return w.core.Close() }

// Watch starts a job watch. With an empty cursor it lists the jobs first,
// page by page, and watches from the list's cursor; the listed jobs are
// available from Snapshot.
func (j JobsClient) Watch(ctx context.Context, req *workspacev1.WatchJobsRequest) (*JobWatch, error) {
	if req == nil {
		req = &workspacev1.WatchJobsRequest{}
	}
	base := proto.Clone(req).(*workspacev1.WatchJobsRequest)
	w := &JobWatch{}
	cursor := base.GetCursor()
	if cursor == "" {
		jobs, from, err := listJobs(ctx, j.call.svc, base)
		if err != nil {
			return nil, err
		}
		w.snapshot, cursor = jobs, from
	}
	w.core = newWatch(ctx, j.call.client, cursor, func(ctx context.Context, cursor string) (*connect.ServerStreamForClient[workspacev1.WatchJobsResponse], error) {
		r := proto.Clone(base).(*workspacev1.WatchJobsRequest)
		r.Cursor = cursor
		r.Keepalive = true
		return j.call.svc.WatchJobs(ctx, connect.NewRequest(r))
	}, func(msg *workspacev1.WatchJobsResponse) (string, bool) {
		if keepalive := msg.GetKeepalive(); keepalive != nil {
			return keepalive.GetCursor(), true
		}
		return msg.GetCursor(), false
	})
	return w, nil
}

// Snapshot returns the jobs listed before the watch started, or nil when the
// request named a cursor.
func (w *JobWatch) Snapshot() []*workspacev1.Job { return w.snapshot }

// listJobs pages ListJobs with the watch's scope, and returns the jobs and
// the cursor the watch starts from.
func listJobs(ctx context.Context, svc workspacev1connect.WorkloadServiceClient, req *workspacev1.WatchJobsRequest) ([]*workspacev1.Job, string, error) {
	list := &workspacev1.ListJobsRequest{Project: req.GetProject(), Environment: req.GetEnvironment()}
	var jobs []*workspacev1.Job
	for {
		resp, err := svc.ListJobs(ctx, connect.NewRequest(list))
		if err != nil {
			return nil, "", err
		}
		jobs = append(jobs, resp.Msg.GetJobs()...)
		next := resp.Msg.GetNextPageToken()
		if next == "" {
			return jobs, resp.Msg.GetCursor(), nil
		}
		list.PageToken = next
	}
}

// SandboxWatch streams a workspace's sandbox changes. Receive reports each
// message, and the watch resumes from its last cursor after a dropped
// connection.
type SandboxWatch struct {
	core     *watch[workspacev1.WatchSandboxesResponse]
	snapshot []*workspacev1.Sandbox
}

// Receive waits for the next change. It returns false when the watch ended,
// and Err then says why. A keepalive is not reported as a change.
func (w *SandboxWatch) Receive() bool { return w.core.Receive() }

// Msg returns the last change Receive reported, or nil.
func (w *SandboxWatch) Msg() *workspacev1.WatchSandboxesResponse { return w.core.Msg() }

// Err returns why the watch ended, or nil while it has not.
func (w *SandboxWatch) Err() error { return w.core.Err() }

// Cursor returns the position to resume from.
func (w *SandboxWatch) Cursor() string { return w.core.Cursor() }

// Close ends the watch and releases its stream.
func (w *SandboxWatch) Close() error { return w.core.Close() }

// Watch starts a sandbox watch. With an empty cursor it lists the sandboxes
// first, page by page, and watches from the list's cursor; the listed
// sandboxes are available from Snapshot.
func (s SandboxesClient) Watch(ctx context.Context, req *workspacev1.WatchSandboxesRequest) (*SandboxWatch, error) {
	if req == nil {
		req = &workspacev1.WatchSandboxesRequest{}
	}
	base := proto.Clone(req).(*workspacev1.WatchSandboxesRequest)
	w := &SandboxWatch{}
	cursor := base.GetCursor()
	if cursor == "" {
		sandboxes, from, err := listSandboxes(ctx, s.call.svc, base)
		if err != nil {
			return nil, err
		}
		w.snapshot, cursor = sandboxes, from
	}
	w.core = newWatch(ctx, s.call.client, cursor, func(ctx context.Context, cursor string) (*connect.ServerStreamForClient[workspacev1.WatchSandboxesResponse], error) {
		r := proto.Clone(base).(*workspacev1.WatchSandboxesRequest)
		r.Cursor = cursor
		r.Keepalive = true
		return s.call.svc.WatchSandboxes(ctx, connect.NewRequest(r))
	}, func(msg *workspacev1.WatchSandboxesResponse) (string, bool) {
		if keepalive := msg.GetKeepalive(); keepalive != nil {
			return keepalive.GetCursor(), true
		}
		return msg.GetCursor(), false
	})
	return w, nil
}

// Snapshot returns the sandboxes listed before the watch started, or nil when
// the request named a cursor.
func (w *SandboxWatch) Snapshot() []*workspacev1.Sandbox { return w.snapshot }

// listSandboxes pages ListSandboxes with the watch's scope, and returns the
// sandboxes and the cursor the watch starts from.
func listSandboxes(ctx context.Context, svc workspacev1connect.WorkloadServiceClient, req *workspacev1.WatchSandboxesRequest) ([]*workspacev1.Sandbox, string, error) {
	list := &workspacev1.ListSandboxesRequest{Project: req.GetProject(), Environment: req.GetEnvironment()}
	var sandboxes []*workspacev1.Sandbox
	for {
		resp, err := svc.ListSandboxes(ctx, connect.NewRequest(list))
		if err != nil {
			return nil, "", err
		}
		sandboxes = append(sandboxes, resp.Msg.GetSandboxes()...)
		next := resp.Msg.GetNextPageToken()
		if next == "" {
			return sandboxes, resp.Msg.GetCursor(), nil
		}
		list.PageToken = next
	}
}

// ServiceWatch streams a workspace's service changes. Receive reports each
// message, and the watch resumes from its last cursor after a dropped
// connection.
type ServiceWatch struct {
	core     *watch[workspacev1.WatchServicesResponse]
	snapshot []*workspacev1.Service
}

// Receive waits for the next change. It returns false when the watch ended,
// and Err then says why. A keepalive is not reported as a change.
func (w *ServiceWatch) Receive() bool { return w.core.Receive() }

// Msg returns the last change Receive reported, or nil.
func (w *ServiceWatch) Msg() *workspacev1.WatchServicesResponse { return w.core.Msg() }

// Err returns why the watch ended, or nil while it has not.
func (w *ServiceWatch) Err() error { return w.core.Err() }

// Cursor returns the position to resume from.
func (w *ServiceWatch) Cursor() string { return w.core.Cursor() }

// Close ends the watch and releases its stream.
func (w *ServiceWatch) Close() error { return w.core.Close() }

// Watch starts a service watch. With an empty cursor it lists the services
// first, page by page, and watches from the list's cursor; the listed
// services are available from Snapshot.
func (s ServicesClient) Watch(ctx context.Context, req *workspacev1.WatchServicesRequest) (*ServiceWatch, error) {
	if req == nil {
		req = &workspacev1.WatchServicesRequest{}
	}
	base := proto.Clone(req).(*workspacev1.WatchServicesRequest)
	w := &ServiceWatch{}
	cursor := base.GetCursor()
	if cursor == "" {
		services, from, err := listServices(ctx, s.call.svc, base)
		if err != nil {
			return nil, err
		}
		w.snapshot, cursor = services, from
	}
	w.core = newWatch(ctx, s.call.client, cursor, func(ctx context.Context, cursor string) (*connect.ServerStreamForClient[workspacev1.WatchServicesResponse], error) {
		r := proto.Clone(base).(*workspacev1.WatchServicesRequest)
		r.Cursor = cursor
		r.Keepalive = true
		return s.call.svc.WatchServices(ctx, connect.NewRequest(r))
	}, func(msg *workspacev1.WatchServicesResponse) (string, bool) {
		if keepalive := msg.GetKeepalive(); keepalive != nil {
			return keepalive.GetCursor(), true
		}
		return msg.GetCursor(), false
	})
	return w, nil
}

// Snapshot returns the services listed before the watch started, or nil when
// the request named a cursor.
func (w *ServiceWatch) Snapshot() []*workspacev1.Service { return w.snapshot }

// listServices pages ListServices with the watch's scope, and returns the
// services and the cursor the watch starts from.
func listServices(ctx context.Context, svc workspacev1connect.WorkloadServiceClient, req *workspacev1.WatchServicesRequest) ([]*workspacev1.Service, string, error) {
	list := &workspacev1.ListServicesRequest{Project: req.GetProject(), Environment: req.GetEnvironment()}
	var services []*workspacev1.Service
	for {
		resp, err := svc.ListServices(ctx, connect.NewRequest(list))
		if err != nil {
			return nil, "", err
		}
		services = append(services, resp.Msg.GetServices()...)
		next := resp.Msg.GetNextPageToken()
		if next == "" {
			return services, resp.Msg.GetCursor(), nil
		}
		list.PageToken = next
	}
}

// EventWatch streams a workspace's events. With an empty cursor it lists
// events one page at a time first, reports them, then watches from the last
// cursor.
type EventWatch struct {
	core    *watch[workspacev1.WatchEventsResponse]
	svc     workspacev1connect.WorkloadServiceClient
	list    *workspacev1.ListEventsRequest
	page    []*workspacev1.Event
	index   int
	listing bool
	listed  bool
}

// newEventWatch starts an event watch. With an empty cursor it pages
// ListEvents, reports each listed event, and then watches from the last
// page's cursor.
func newEventWatch(ctx context.Context, call workloadCall, req *workspacev1.WatchEventsRequest) (*EventWatch, error) {
	if req == nil {
		req = &workspacev1.WatchEventsRequest{}
	}
	base := proto.Clone(req).(*workspacev1.WatchEventsRequest)
	w := &EventWatch{svc: call.svc, listing: base.GetCursor() == ""}
	if w.listing {
		w.list = &workspacev1.ListEventsRequest{
			Project:     base.GetProject(),
			Environment: base.GetEnvironment(),
			WorkloadId:  base.GetWorkloadId(),
		}
	}
	w.core = newWatch(ctx, call.client, base.GetCursor(), func(ctx context.Context, cursor string) (*connect.ServerStreamForClient[workspacev1.WatchEventsResponse], error) {
		r := proto.Clone(base).(*workspacev1.WatchEventsRequest)
		r.Cursor = cursor
		r.Keepalive = true
		return call.svc.WatchEvents(ctx, connect.NewRequest(r))
	}, func(msg *workspacev1.WatchEventsResponse) (string, bool) {
		if keepalive := msg.GetKeepalive(); keepalive != nil {
			return keepalive.GetCursor(), true
		}
		return msg.Event.GetCursor(), false
	})
	return w, nil
}

// Receive waits for the next event. It returns false when the watch ended,
// and Err then says why. A keepalive is not reported.
func (w *EventWatch) Receive() bool {
	if w.core.closed.Load() || w.core.Err() != nil {
		return false
	}
	for w.listing {
		if w.index < len(w.page) {
			event := w.page[w.index]
			w.index++
			w.core.setMsg(&workspacev1.WatchEventsResponse{Event: event})
			if event.GetCursor() != "" {
				w.core.setCursor(event.GetCursor())
			}
			return true
		}
		if w.listed {
			w.listing = false
			break
		}
		resp, err := w.svc.ListEvents(w.core.ctx, connect.NewRequest(w.list))
		if err != nil {
			if w.core.closed.Load() {
				return false
			}
			w.core.fail(err)
			return false
		}
		w.page = resp.Msg.GetEvents()
		w.index = 0
		if cursor := resp.Msg.GetCursor(); cursor != "" {
			w.core.setCursor(cursor)
		}
		if next := resp.Msg.GetNextPageToken(); next != "" {
			w.list.PageToken = next
		} else {
			w.listed = true
		}
	}
	return w.core.Receive()
}

// Msg returns the last change Receive reported, or nil.
func (w *EventWatch) Msg() *workspacev1.WatchEventsResponse { return w.core.Msg() }

// Err returns why the watch ended, or nil while it has not.
func (w *EventWatch) Err() error { return w.core.Err() }

// Cursor returns the position to resume from.
func (w *EventWatch) Cursor() string { return w.core.Cursor() }

// Close ends the watch and releases its stream.
func (w *EventWatch) Close() error { return w.core.Close() }

// errStreamEnded reports a stream that ended without an error while its
// context was still alive. It is retryable because it wraps
// io.ErrUnexpectedEOF.
var errStreamEnded = fmt.Errorf("%w: toshi: stream ended without an error", io.ErrUnexpectedEOF)
