package toshi

import (
	"context"
	"sync"
	"sync/atomic"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

// OutputWatch streams a running job's standard output and error. Resume
// offsets count what the caller received, so a reconnection continues where
// the last message stopped.
type OutputWatch struct {
	call   workloadCall
	ctx    context.Context
	cancel context.CancelFunc
	req    *workspacev1.WatchOutputRequest

	mu      sync.Mutex
	stream  *connect.ServerStreamForClient[workspacev1.WatchOutputResponse]
	msg     *workspacev1.WatchOutputResponse
	stdout  int64
	stderr  int64
	ended   bool
	err     error
	closed  atomic.Bool
	attempt int
}

// WatchOutput starts an output watch at the request's offsets. The stream
// opens on the first Receive, and an error opening it is returned by Err. A
// job whose output the server no longer holds ends it with
// FAILED_PRECONDITION; a refused offset ends it with OUT_OF_RANGE.
func (j JobsClient) WatchOutput(ctx context.Context, req *workspacev1.WatchOutputRequest) (*OutputWatch, error) {
	if req == nil {
		req = &workspacev1.WatchOutputRequest{}
	}
	watchCtx, cancel := context.WithCancel(ctx)
	w := &OutputWatch{
		call:   j.call,
		ctx:    watchCtx,
		cancel: cancel,
		req:    proto.Clone(req).(*workspacev1.WatchOutputRequest),
		stdout: req.GetStdoutOffset(),
		stderr: req.GetStderrOffset(),
	}
	return w, nil
}

// open starts the stream at the current offsets.
func (w *OutputWatch) open() error {
	w.req.StdoutOffset = w.stdout
	w.req.StderrOffset = w.stderr
	for {
		stream, err := w.call.svc.WatchOutput(w.ctx, connect.NewRequest(w.req))
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
func (w *OutputWatch) retry(cause error) error {
	if !retryable(cause) {
		return cause
	}
	if w.attempt >= w.call.client.maxReconnects() {
		return cause
	}
	if err := sleepBackoff(w.ctx, w.call.client.retry, w.attempt); err != nil {
		return err
	}
	w.attempt++
	return nil
}

// Receive waits for the next chunk or the end. It returns false when the
// watch ended, and Err then says why; after the end message it returns false
// with a nil error.
func (w *OutputWatch) Receive() bool {
	if w.closed.Load() {
		return false
	}
	w.mu.Lock()
	if w.ended || w.err != nil {
		w.mu.Unlock()
		return false
	}
	w.msg = nil
	stream := w.stream
	w.mu.Unlock()
	if stream == nil {
		if err := w.open(); err != nil {
			w.fail(err)
			return false
		}
		w.mu.Lock()
		stream = w.stream
		w.mu.Unlock()
	}
	for {
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
			if err := w.open(); err != nil {
				w.fail(err)
				return false
			}
			w.mu.Lock()
			stream = w.stream
			w.mu.Unlock()
			continue
		}
		msg := stream.Msg()
		w.attempt = 0
		end := false
		w.mu.Lock()
		w.msg = msg
		switch {
		case msg.GetEnd() != nil:
			w.ended = true
			w.stream = nil
			end = true
		case len(msg.GetStdout()) > 0:
			w.stdout += int64(len(msg.GetStdout()))
		case len(msg.GetStderr()) > 0:
			w.stderr += int64(len(msg.GetStderr()))
		}
		w.mu.Unlock()
		if end {
			_ = stream.Close()
			w.cancel()
		}
		return true
	}
}

// fail records the watch's terminal error.
func (w *OutputWatch) fail(err error) {
	w.mu.Lock()
	w.err = err
	w.mu.Unlock()
}

// Msg returns the last chunk or end message Receive reported, or nil.
func (w *OutputWatch) Msg() *workspacev1.WatchOutputResponse {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.msg
}

// Err returns why the watch ended, or nil while it has not.
func (w *OutputWatch) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// Offsets returns how many bytes of standard output and error the caller has
// received, including the request's starting offsets.
func (w *OutputWatch) Offsets() (stdout, stderr int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stdout, w.stderr
}

// Close ends the watch and releases its stream.
func (w *OutputWatch) Close() error {
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
