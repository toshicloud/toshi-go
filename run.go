package toshi

import (
	"context"
	"errors"
	"io"
	"sync"

	workspacev1 "go.toshicloud.io/api/workspacev1"
)

// Run creates a job and follows its output, as `toshi run` does. It returns
// once the job exists: read Stdout and Stderr at the same time, and Wait
// follows the job to its end. A caller that stops reading does not stop the
// job; cancel the job with Workload.Jobs.Cancel.
func (c *Client) Run(ctx context.Context, req *workspacev1.CreateJobRequest) (*JobRun, error) {
	if req == nil {
		return nil, errors.New("toshi: Run needs a create request")
	}
	resp, err := c.Workload.Jobs.Create(ctx, req)
	if err != nil {
		return nil, err
	}
	job := resp.GetJob()
	if job.GetId() == "" {
		return nil, errors.New("toshi: Run: CreateJob answered no job")
	}

	runCtx, cancel := context.WithCancel(ctx)
	run := &JobRun{
		job:    job,
		cancel: cancel,
		stdout: newOutputBuffer(),
		stderr: newOutputBuffer(),
		done:   make(chan struct{}),
	}

	// A job that ended within the call carries its output and its exit, and
	// no later call returns them.
	if output := resp.GetOutput(); output != nil {
		_, _ = run.stdout.Write(output.GetStdout())
		_, _ = run.stderr.Write(output.GetStderr())
		status := job.GetStatus()
		run.finish(&workspacev1.OutputEnd{
			ExitCode:  status.ExitCode,
			Cancelled: status.GetEndReason() == workspacev1.EndReason_END_REASON_CANCELLED,
			Truncated: output.GetTruncated(),
		})
		cancel()
		return run, nil
	}

	watch, err := c.Workload.Jobs.WatchOutput(runCtx, &workspacev1.WatchOutputRequest{
		Project:     job.GetProject(),
		Environment: job.GetEnvironment(),
		Id:          job.GetId(),
	})
	if err != nil {
		cancel()
		return nil, err
	}
	run.watch = watch
	go run.pump()
	return run, nil
}

// JobRun is a job Run created and is following.
type JobRun struct {
	job    *workspacev1.Job
	watch  *OutputWatch
	stdout *outputBuffer
	stderr *outputBuffer
	cancel context.CancelFunc
	done   chan struct{}

	mu         sync.Mutex
	end        *workspacev1.OutputEnd
	err        error
	finishOnce sync.Once
}

// Job returns the job as Run created it.
func (r *JobRun) Job() *workspacev1.Job { return r.job }

// Stdout returns the job's standard output.
func (r *JobRun) Stdout() io.Reader { return r.stdout }

// Stderr returns the job's standard error.
func (r *JobRun) Stderr() io.Reader { return r.stderr }

// Wait follows the job to its end. It returns nil when the job exited 0, a
// JobExitError when it did not, and the error that ended the output stream
// when the output was lost first.
func (r *JobRun) Wait() error {
	<-r.done
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	if r.end.GetCancelled() || r.end.ExitCode == nil || r.end.GetExitCode() != 0 {
		return &JobExitError{JobID: r.job.GetId(), End: r.end}
	}
	return nil
}

// pump copies the output stream into the buffers, at the reader's pace, and
// ends the run at the stream's end.
func (r *JobRun) pump() {
	defer r.cancel()
	for r.watch.Receive() {
		msg := r.watch.Msg()
		if len(msg.GetStdout()) > 0 {
			_, _ = r.stdout.Write(msg.GetStdout())
		}
		if len(msg.GetStderr()) > 0 {
			_, _ = r.stderr.Write(msg.GetStderr())
		}
		if end := msg.GetEnd(); end != nil {
			r.finish(end)
			return
		}
	}
	err := r.watch.Err()
	if err == nil {
		err = errStreamEnded
	}
	r.fail(err)
}

// finish ends the run at the job's end.
func (r *JobRun) finish(end *workspacev1.OutputEnd) {
	r.finishOnce.Do(func() {
		r.mu.Lock()
		r.end = end
		r.mu.Unlock()
		_ = r.stdout.close(nil)
		_ = r.stderr.close(nil)
		close(r.done)
	})
}

// fail ends the run with the error that ended its stream.
func (r *JobRun) fail(err error) {
	r.finishOnce.Do(func() {
		r.mu.Lock()
		r.err = err
		r.mu.Unlock()
		_ = r.stdout.close(err)
		_ = r.stderr.close(err)
		close(r.done)
	})
}

// outputBuffer is a byte queue the reader drains at its own pace. The writer
// never waits, so following a job does not depend on the caller reading.
type outputBuffer struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    []byte
	closed bool
	err    error
}

// newOutputBuffer returns an empty buffer.
func newOutputBuffer() *outputBuffer {
	b := &outputBuffer{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Write appends p to the buffer.
func (b *outputBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, io.ErrClosedPipe
	}
	b.buf = append(b.buf, p...)
	b.cond.Broadcast()
	return len(p), nil
}

// Read returns the buffered bytes, waiting for more until the buffer closes.
func (b *outputBuffer) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.buf) == 0 && !b.closed {
		b.cond.Wait()
	}
	if len(b.buf) > 0 {
		n := copy(p, b.buf)
		b.buf = b.buf[n:]
		return n, nil
	}
	if b.err != nil {
		return 0, b.err
	}
	return 0, io.EOF
}

// close ends the buffer: a reader takes what is left, then err or io.EOF.
func (b *outputBuffer) close(err error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	b.err = err
	b.cond.Broadcast()
	return nil
}
