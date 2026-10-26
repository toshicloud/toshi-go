package toshi

import (
	"context"
	"errors"
	"io"
	"sync"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
)

// stdinChunk is the largest standard-input message Execute sends, well under
// the API's 1 MiB request limit.
const stdinChunk = 64 << 10

// Execute starts a command in a running sandbox and returns a Process. It
// fills the start's scope and an empty request key, sends the start once, and
// never retries: a lost stream comes back as a StreamLostError with the key.
func (s SandboxesClient) Execute(ctx context.Context, start *workspacev1.ExecuteStart) (*Process, error) {
	if start == nil {
		return nil, errors.New("toshi: Execute needs a start message")
	}
	fillRequestKey(start)
	key := start.GetRequestKey()

	procCtx, cancel := context.WithCancel(ctx)
	stream := s.call.svc.Execute(procCtx)
	// Send reports a server that refused the stream as an error wrapping
	// io.EOF; the server's own error is on Receive.
	msg, err := func() (*workspacev1.ExecuteResponse, error) {
		if sendErr := stream.Send(&workspacev1.ExecuteRequest{
			Message: &workspacev1.ExecuteRequest_Start{Start: start},
		}); sendErr != nil && !errors.Is(sendErr, io.EOF) {
			return nil, sendErr
		}
		return stream.Receive()
	}()
	if err != nil {
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
		cancel()
		if lost(err) {
			return nil, &StreamLostError{RequestKey: key, Err: err}
		}
		return nil, err
	}
	if msg.GetStarted() == nil {
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
		cancel()
		return nil, errors.New("toshi: the Execute stream did not answer with a started message")
	}

	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	p := &Process{
		executionID: msg.GetStarted().GetExecutionId(),
		requestKey:  key,
		stream:      stream,
		ctx:         procCtx,
		cancel:      cancel,
		stdoutR:     stdoutR,
		stdoutW:     stdoutW,
		stderrR:     stderrR,
		stderrW:     stderrW,
		done:        make(chan struct{}),
	}
	p.stdin = &processStdin{p: p}
	go p.receive()
	go p.watchContext()
	return p, nil
}

// lost reports whether err ended a stream: a dropped connection or a clean
// close before the expected message.
func lost(err error) bool {
	return retryable(err) || errors.Is(err, io.EOF)
}

// Process is one execution started by Execute. Standard input is written to
// Stdin, stdout and stderr are read at the same time, and Wait returns its
// exit as an error.
type Process struct {
	executionID string
	requestKey  string
	stream      *connect.BidiStreamForClient[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]
	ctx         context.Context
	cancel      context.CancelFunc

	stdoutR *io.PipeReader
	stdoutW *io.PipeWriter
	stderrR *io.PipeReader
	stderrW *io.PipeWriter
	stdin   *processStdin

	mu         sync.Mutex
	exit       *workspacev1.ExecutionExit
	err        error
	done       chan struct{}
	finishOnce sync.Once
}

// receive reads the stream, writes output into the pipes, and ends the
// process at the exit or a lost stream. It is the only goroutine that reads
// the response, so it closes both sides when it returns.
func (p *Process) receive() {
	defer func() {
		p.stdin.mu.Lock()
		if !p.stdin.closed {
			p.stdin.closed = true
		}
		_ = p.stream.CloseRequest()
		p.stdin.mu.Unlock()
		_ = p.stream.CloseResponse()
	}()
	for {
		msg, err := p.stream.Receive()
		if err != nil {
			p.finish(&StreamLostError{ExecutionID: p.executionID, RequestKey: p.requestKey, Err: err})
			return
		}
		switch m := msg.GetMessage().(type) {
		case *workspacev1.ExecuteResponse_Stdout:
			if _, err := p.stdoutW.Write(m.Stdout); err != nil {
				p.finish(err)
				return
			}
		case *workspacev1.ExecuteResponse_Stderr:
			if _, err := p.stderrW.Write(m.Stderr); err != nil {
				p.finish(err)
				return
			}
		case *workspacev1.ExecuteResponse_Exit:
			p.finishExit(m.Exit)
			return
		}
	}
}

// watchContext ends the process with the context's error when the caller's
// context ends first.
func (p *Process) watchContext() {
	select {
	case <-p.ctx.Done():
		p.finish(ctxError(p.ctx))
	case <-p.done:
	}
}

// finish ends the process with err, unblocking both readers and Wait.
func (p *Process) finish(err error) {
	p.finishOnce.Do(func() {
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		_ = p.stdoutW.CloseWithError(err)
		_ = p.stderrW.CloseWithError(err)
		p.cancel()
		p.stdin.mu.Lock()
		p.stdin.closed = true
		_ = p.stream.CloseRequest()
		p.stdin.mu.Unlock()
		close(p.done)
	})
}

// finishExit ends the process at a decoded exit message.
func (p *Process) finishExit(exit *workspacev1.ExecutionExit) {
	p.finishOnce.Do(func() {
		p.mu.Lock()
		p.exit = exit
		p.mu.Unlock()
		_ = p.stdoutW.Close()
		_ = p.stderrW.Close()
		p.cancel()
		p.stdin.mu.Lock()
		p.stdin.closed = true
		_ = p.stream.CloseRequest()
		p.stdin.mu.Unlock()
		close(p.done)
	})
}

// ExecutionID returns the execution's ID.
func (p *Process) ExecutionID() string { return p.executionID }

// RequestKey returns the request key the start carried, including one the
// client filled.
func (p *Process) RequestKey() string { return p.requestKey }

// Stdin returns the writer for the command's standard input. Close sends the
// close message.
func (p *Process) Stdin() io.WriteCloser { return p.stdin }

// Stdout returns the command's standard output.
func (p *Process) Stdout() io.Reader { return p.stdoutR }

// Stderr returns the command's standard error.
func (p *Process) Stderr() io.Reader { return p.stderrR }

// Wait waits for the execution to end and returns nil for exit 0, an
// ExitError for any other exit, and a StreamLostError when the stream ended
// first.
func (p *Process) Wait() error {
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exit != nil {
		if p.exit.ExitCode != nil && p.exit.GetExitCode() == 0 {
			return nil
		}
		return &ExitError{ExecutionID: p.executionID, Exit: p.exit}
	}
	return p.err
}

// doneErr returns why the process ended, or nil while it runs.
func (p *Process) doneErr() error {
	select {
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.err
	default:
		return nil
	}
}

// processStdin sends standard input in bounded chunks. Writes are serialized,
// so a chunk is never interleaved with another write.
type processStdin struct {
	p      *Process
	mu     sync.Mutex
	closed bool
}

func (s *processStdin) Write(b []byte) (int, error) {
	s.mu.Lock()
	if s.closed {
		if s.p.ctx.Err() != nil {
			s.mu.Unlock()
			<-s.p.done
			if err := s.p.doneErr(); err != nil {
				return 0, err
			}
			return 0, io.ErrClosedPipe
		}
		s.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if s.p.isDone() {
		err := s.p.doneErr()
		s.mu.Unlock()
		if err != nil {
			return 0, err
		}
		return 0, io.ErrClosedPipe
	}
	written := 0
	for len(b) > 0 {
		n := len(b)
		if n > stdinChunk {
			n = stdinChunk
		}
		chunk := append([]byte(nil), b[:n]...)
		err := s.p.stream.Send(&workspacev1.ExecuteRequest{
			Message: &workspacev1.ExecuteRequest_Stdin{Stdin: chunk},
		})
		if err != nil {
			// Let the process finish close the request side, then report its
			// error rather than the transport's EOF or cancellation.
			s.mu.Unlock()
			<-s.p.done
			if doneErr := s.p.doneErr(); doneErr != nil {
				return written, doneErr
			}
			return written, err
		}
		written += n
		b = b[n:]
	}
	s.mu.Unlock()
	return written, nil
}

func (s *processStdin) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.p.isDone() {
		return nil
	}
	return s.p.stream.Send(&workspacev1.ExecuteRequest{
		Message: &workspacev1.ExecuteRequest_CloseStdin{CloseStdin: true},
	})
}

// isDone reports whether the process ended.
func (p *Process) isDone() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}
