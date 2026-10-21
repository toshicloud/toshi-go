package toshi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

func started(id string) *workspacev1.ExecuteResponse {
	return &workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Started{
		Started: &workspacev1.ExecutionStarted{ExecutionId: id},
	}}
}

func stdout(data []byte) *workspacev1.ExecuteResponse {
	return &workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Stdout{Stdout: data}}
}

func stderr(data []byte) *workspacev1.ExecuteResponse {
	return &workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Stderr{Stderr: data}}
}

func exitWith(code *int32, cancelled bool) *workspacev1.ExecuteResponse {
	return &workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Exit{
		Exit: &workspacev1.ExecutionExit{ExitCode: code, Cancelled: cancelled},
	}}
}

func TestExecuteEchoProcess(t *testing.T) {
	var mu sync.Mutex
	var start *workspacev1.ExecuteStart
	var auth, tenant string
	server := newTestServer(t, &backend{
		execute: func(ctx context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
			first, err := stream.Receive()
			if err != nil {
				return err
			}
			mu.Lock()
			start = first.GetStart()
			auth = stream.RequestHeader().Get("Authorization")
			tenant = stream.RequestHeader().Get("Toshi-Tenant")
			mu.Unlock()
			if err := stream.Send(started("ex1")); err != nil {
				return err
			}
			for {
				req, err := stream.Receive()
				if err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}
				if data := req.GetStdin(); data != nil {
					if err := stream.Send(stdout(data)); err != nil {
						return err
					}
					if err := stream.Send(stderr(data)); err != nil {
						return err
					}
					continue
				}
				if req.GetCloseStdin() {
					zero := int32(0)
					return stream.Send(exitWith(&zero, false))
				}
			}
		},
	})
	client := newTestClient(t, server, WithScope("proj", "env"))
	p, err := client.Execute(context.Background(), &workspacev1.ExecuteStart{WorkloadId: "w1"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if p.ExecutionID() != "ex1" {
		t.Fatalf("execution %q", p.ExecutionID())
	}
	if key := p.RequestKey(); len(key) != 32 {
		t.Fatalf("request key %q", key)
	}
	mu.Lock()
	if start.GetProject() != "proj" || start.GetEnvironment() != "env" {
		t.Fatalf("scope %q/%q", start.GetProject(), start.GetEnvironment())
	}
	if start.GetRequestKey() != p.RequestKey() {
		t.Fatalf("start key %q, process key %q", start.GetRequestKey(), p.RequestKey())
	}
	if auth != "Bearer test-token" || tenant != "" {
		t.Fatalf("headers %q tenant %q", auth, tenant)
	}
	mu.Unlock()

	stdoutCh := make(chan []byte, 1)
	stderrCh := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(p.Stdout())
		stdoutCh <- b
	}()
	go func() {
		b, _ := io.ReadAll(p.Stderr())
		stderrCh <- b
	}()
	if _, err := p.Stdin().Write([]byte("hello")); err != nil {
		t.Fatalf("stdin: %v", err)
	}
	if err := p.Stdin().Close(); err != nil {
		t.Fatalf("stdin close: %v", err)
	}
	if err := p.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := string(<-stdoutCh); got != "hello" {
		t.Fatalf("stdout %q", got)
	}
	if got := string(<-stderrCh); got != "hello" {
		t.Fatalf("stderr %q", got)
	}
}

func TestExecuteStdinChunks(t *testing.T) {
	var mu sync.Mutex
	var sizes []int
	server := newTestServer(t, &backend{
		execute: func(ctx context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
			if _, err := stream.Receive(); err != nil {
				return err
			}
			if err := stream.Send(started("ex1")); err != nil {
				return err
			}
			for {
				req, err := stream.Receive()
				if err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}
				if data := req.GetStdin(); data != nil {
					mu.Lock()
					sizes = append(sizes, len(data))
					mu.Unlock()
					continue
				}
				if req.GetCloseStdin() {
					zero := int32(0)
					return stream.Send(exitWith(&zero, false))
				}
			}
		},
	})
	client := newTestClient(t, server)
	p, err := client.Execute(context.Background(), &workspacev1.ExecuteStart{WorkloadId: "w1"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := p.Stdin().Write(bytes.Repeat([]byte("x"), 100<<10)); err != nil {
		t.Fatalf("stdin: %v", err)
	}
	if err := p.Stdin().Close(); err != nil {
		t.Fatalf("stdin close: %v", err)
	}
	if err := p.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sizes) != 2 || sizes[0] != 64<<10 || sizes[1] != 36<<10 {
		t.Fatalf("stdin chunk sizes %v", sizes)
	}
}

func TestExecuteExit(t *testing.T) {
	zero := int32(0)
	three := int32(3)
	cases := []struct {
		name      string
		exit      *workspacev1.ExecutionExit
		cancelled bool
	}{
		{name: "success", exit: &workspacev1.ExecutionExit{ExitCode: &zero}},
		{name: "status", exit: &workspacev1.ExecutionExit{ExitCode: &three}},
		{name: "cancelled", exit: &workspacev1.ExecutionExit{Cancelled: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newTestServer(t, &backend{
				execute: func(_ context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
					if _, err := stream.Receive(); err != nil {
						return err
					}
					if err := stream.Send(started("ex1")); err != nil {
						return err
					}
					return stream.Send(&workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Exit{Exit: tc.exit}})
				},
			})
			client := newTestClient(t, server)
			p, err := client.Execute(context.Background(), &workspacev1.ExecuteStart{WorkloadId: "w1"})
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			err = p.Wait()
			switch tc.name {
			case "success":
				if err != nil {
					t.Fatalf("Wait: %v", err)
				}
			case "status":
				var exitErr *ExitError
				if !errors.As(err, &exitErr) || exitErr.Exit.GetExitCode() != 3 || exitErr.ExecutionID != "ex1" {
					t.Fatalf("Wait: %v", err)
				}
			case "cancelled":
				var exitErr *ExitError
				if !errors.As(err, &exitErr) || !exitErr.Exit.GetCancelled() {
					t.Fatalf("Wait: %v", err)
				}
			}
		})
	}
}

func TestExecuteKeepsGivenRequestKey(t *testing.T) {
	server := newTestServer(t, &backend{
		execute: func(_ context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
			if _, err := stream.Receive(); err != nil {
				return err
			}
			if err := stream.Send(started("ex1")); err != nil {
				return err
			}
			zero := int32(0)
			return stream.Send(exitWith(&zero, false))
		},
	})
	client := newTestClient(t, server)
	start := &workspacev1.ExecuteStart{WorkloadId: "w1", RequestKey: "given-key"}
	p, err := client.Execute(context.Background(), start)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if p.RequestKey() != "given-key" || start.GetRequestKey() != "given-key" {
		t.Fatalf("key %q, start %q", p.RequestKey(), start.GetRequestKey())
	}
	if err := p.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestExecuteLostBeforeStarted(t *testing.T) {
	var calls atomic.Int32
	server := newTestServer(t, &backend{
		execute: func(context.Context, *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
			calls.Add(1)
			return connect.NewError(connect.CodeUnavailable, errors.New("down"))
		},
	})
	client := newTestClient(t, server)
	start := &workspacev1.ExecuteStart{WorkloadId: "w1"}
	_, err := client.Execute(context.Background(), start)
	var lostErr *StreamLostError
	if !errors.As(err, &lostErr) {
		t.Fatalf("err %v, want StreamLostError", err)
	}
	if lostErr.ExecutionID != "" || lostErr.RequestKey == "" || lostErr.RequestKey != start.GetRequestKey() {
		t.Fatalf("lost %+v, start key %q", lostErr, start.GetRequestKey())
	}
	if calls.Load() != 1 {
		t.Fatalf("%d Execute calls, want one", calls.Load())
	}
}

func TestExecuteLostMidStream(t *testing.T) {
	server := newTestServer(t, &backend{
		execute: func(_ context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
			if _, err := stream.Receive(); err != nil {
				return err
			}
			if err := stream.Send(started("ex1")); err != nil {
				return err
			}
			if err := stream.Send(stdout([]byte("x"))); err != nil {
				return err
			}
			return connect.NewError(connect.CodeUnavailable, errors.New("down"))
		},
	})
	client := newTestClient(t, server)
	p, err := client.Execute(context.Background(), &workspacev1.ExecuteStart{WorkloadId: "w1"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	type read struct {
		data []byte
		err  error
	}
	outCh := make(chan read, 1)
	errCh := make(chan read, 1)
	go func() {
		b, err := io.ReadAll(p.Stdout())
		outCh <- read{b, err}
	}()
	go func() {
		b, err := io.ReadAll(p.Stderr())
		errCh <- read{b, err}
	}()
	waitErr := p.Wait()
	var lostErr *StreamLostError
	if !errors.As(waitErr, &lostErr) || lostErr.ExecutionID != "ex1" {
		t.Fatalf("Wait: %v", waitErr)
	}
	out := <-outCh
	if string(out.data) != "x" {
		t.Fatalf("stdout %q", out.data)
	}
	var readerErr *StreamLostError
	if !errors.As(out.err, &readerErr) {
		t.Fatalf("stdout reader error %v", out.err)
	}
	errOut := <-errCh
	if !errors.As(errOut.err, &readerErr) {
		t.Fatalf("stderr reader error %v", errOut.err)
	}
}

func TestExecuteContextCancelUnderBackpressure(t *testing.T) {
	sending := make(chan struct{})
	server := newTestServer(t, &backend{
		execute: func(ctx context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
			if _, err := stream.Receive(); err != nil {
				return err
			}
			if err := stream.Send(started("ex1")); err != nil {
				return err
			}
			close(sending)
			chunk := bytes.Repeat([]byte("x"), 1<<20)
			for {
				if err := stream.Send(stdout(chunk)); err != nil {
					return err
				}
			}
		},
	})
	client := newTestClient(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	p, err := client.Execute(ctx, &workspacev1.ExecuteStart{WorkloadId: "w1"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	select {
	case <-sending:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler did not start sending")
	}
	cancel()

	waitErr := make(chan error, 1)
	go func() { waitErr <- p.Wait() }()
	select {
	case err := <-waitErr:
		if connect.CodeOf(err) != connect.CodeCanceled {
			t.Fatalf("Wait: %v, want canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Wait did not return after cancel")
	}
	readErr := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(p.Stdout())
		readErr <- err
	}()
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("Stdout ended without an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Stdout did not return after cancel")
	}
}

func TestExecuteStdinUnblocksOnCancel(t *testing.T) {
	ready := make(chan struct{})
	handlerDone := make(chan struct{})
	server := newTestServer(t, &backend{
		execute: func(ctx context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
			if _, err := stream.Receive(); err != nil {
				return err
			}
			if err := stream.Send(started("ex1")); err != nil {
				return err
			}
			close(ready)
			<-ctx.Done()
			close(handlerDone)
			return nil
		},
	})
	client := newTestClient(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	p, err := client.Execute(ctx, &workspacev1.ExecuteStart{WorkloadId: "w1"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	writeErr := make(chan error, 1)
	go func() {
		_, err := p.Stdin().Write(bytes.Repeat([]byte("x"), 8<<20))
		writeErr <- err
	}()
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler was not ready")
	}
	cancel()
	select {
	case err := <-writeErr:
		if err == nil {
			t.Fatal("Write returned no error after cancel")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Write did not return after cancel")
	}
	waitErr := make(chan error, 1)
	go func() { waitErr <- p.Wait() }()
	select {
	case <-waitErr:
	case <-time.After(10 * time.Second):
		t.Fatal("Wait did not return after cancel")
	}
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler's context was not done after Wait")
	}
}

// TestExecuteStartRefused proves a server that refuses the start is reported
// as its own Connect error, not as a lost stream.
func TestExecuteStartRefused(t *testing.T) {
	server := newTestServer(t, &backend{
		execute: func(context.Context, *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
			return connect.NewError(connect.CodeFailedPrecondition, errors.New("the workload is not running"))
		},
	})
	client := newTestClient(t, server)
	start := &workspacev1.ExecuteStart{
		WorkloadId: "w1",
		Command: &workspacev1.Command{
			Argv: []string{"true"},
			Env:  map[string]string{"BIG": strings.Repeat("x", 256<<10)},
		},
	}
	_, err := client.Execute(context.Background(), start)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("err %v, want FAILED_PRECONDITION", err)
	}
	var lostErr *StreamLostError
	if errors.As(err, &lostErr) {
		t.Fatalf("err %v is a StreamLostError, want the server's error", err)
	}
}

// TestExecuteStdinServerError proves a server error while standard input is
// written comes back as that error, not as a bare EOF from Send.
func TestExecuteStdinServerError(t *testing.T) {
	server := newTestServer(t, &backend{
		execute: func(_ context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
			if _, err := stream.Receive(); err != nil {
				return err
			}
			if err := stream.Send(started("ex1")); err != nil {
				return err
			}
			return connect.NewError(connect.CodeFailedPrecondition, errors.New("the execution stopped"))
		},
	})
	client := newTestClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	p, err := client.Execute(ctx, &workspacev1.ExecuteStart{WorkloadId: "w1"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	chunk := make([]byte, 1<<20)
	for i := 0; i < 1<<12; i++ {
		_, err := p.Stdin().Write(chunk)
		if err == nil {
			continue
		}
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("stdin write: %v (code %v), want FAILED_PRECONDITION", err, connect.CodeOf(err))
		}
		return
	}
	t.Fatal("stdin writes did not fail")
}
