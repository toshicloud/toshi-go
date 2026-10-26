// Package toshitest serves the Toshi Workspace API from a test.
//
// A test starts a fake Toshi, tells it what to answer, and calls the client:
//
//	tt := toshitest.New(t)
//	tt.Job("job-1").Stdout("deployed\n").ExitCode(0)
//	client := tt.Client()
//
// New starts a TLS server speaking HTTP/2 that serves every service of
// toshi.workspace.v1 through the generated Connect handlers, and closes it
// with t.Cleanup. Client returns a *toshi.Client wired to it, with the token
// "test-token" and the scope default/production.
//
// # What the fake answers
//
// The fake holds one workspace and has no clock: every call answers at once
// with what the test set. It knows the job and the sandbox lifecycle and
// nothing else.
//
//   - Job, Sandbox and Execute set what the fake answers: a job's or a
//     sandbox's state, and the standard output, the standard error and the
//     exit code of a job or of a command run in a sandbox. A job or a sandbox
//     the test set no state for is running; a job or a command the test set no
//     exit code for exits 0.
//   - CreateJob answers the jobs registered with Job, in the order they were
//     registered, and then creates job-1, job-2, ... of its own. CreateSandbox
//     does the same for Sandbox and sandbox-1, sandbox-2, .... A create whose
//     request key the fake has already answered returns the same workload.
//   - GetJob, ListJobs, CancelJob, WaitJob and WatchOutput answer from the
//     jobs that exist; GetSandbox, ListSandboxes, WaitSandbox and
//     WatchSandboxes from the sandboxes. An id the fake does not hold is
//     NOT_FOUND. The lists answer every workload the fake holds, in creation
//     order: they do not apply the request's filters or page size.
//   - WaitJob and WaitSandbox answer at once, reached, with the workload as
//     the test set it.
//   - WatchOutput streams a job's standard output and standard error from the
//     offsets the request names, then the job's end.
//   - WatchJobs and WatchSandboxes report each change made after the cursor
//     the request names: a workload the fake creates, and each state the test
//     sets. ListJobs and ListSandboxes answer the cursor a watch starts at.
//   - Execute answers the start message with an execution id, the standard
//     output, the standard error and the exit code the test set for the
//     sandbox. It does not read standard input.
//
// Every other procedure is hook-only. An unhooked procedure the state machine
// does not cover answers UNIMPLEMENTED, naming the procedure and the function
// that supplies an answer: On for a unary procedure, OnServerStream for a
// server stream, OnBidi for a bidirectional stream. A hook replaces the state
// machine for its procedure.
//
// Calls returns every call the fake received, in order, with its request and
// its headers.
package toshitest

import (
	"context"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	workspacev1 "go.toshicloud.io/api/workspacev1"

	"go.toshicloud.io/toshi"
)

// Token is the token Client sends.
const Token = "test-token"

const (
	// project and environment are the scope Client fills into a request.
	project     = "default"
	environment = "production"
)

// retryPolicy answers a fake's errors at once: a test that hooks an
// UNAVAILABLE answer does not wait through the client's backoff.
var retryPolicy = toshi.RetryPolicy{
	MaxAttempts:    5,
	InitialBackoff: time.Millisecond,
	MaxBackoff:     2 * time.Millisecond,
}

// Fake is a running fake Toshi: a TLS server serving the Workspace API, and
// the state its calls answer from. Build one with New.
type Fake struct {
	tb     testing.TB
	server *httptest.Server

	closeOnce sync.Once

	mu            sync.Mutex
	hooks         map[string]unaryHook
	streamHooks   map[string]streamHook
	builtin       map[string]unaryHook
	builtinStream map[string]streamHook
	calls         []Call
	mounted       map[string]bool

	jobs       map[string]*job
	jobQueue   []string
	jobCreated []string
	jobKeys    map[string]*job
	jobLog     []*workspacev1.WatchJobsResponse
	jobChanged chan struct{}
	nextJob    int

	sandboxes      map[string]*sandbox
	sandboxQueue   []string
	sandboxCreated []string
	sandboxKeys    map[string]*sandbox
	sandboxLog     []*workspacev1.WatchSandboxesResponse
	sandboxChanged chan struct{}
	nextSandbox    int

	executions    map[string]*execution
	nextExecution int
}

// New starts a fake Toshi. It closes the fake with tb.Cleanup; tb may be nil,
// for a caller that closes the fake itself with Close, as an example does.
func New(tb testing.TB) *Fake {
	f := &Fake{
		tb:             tb,
		hooks:          map[string]unaryHook{},
		streamHooks:    map[string]streamHook{},
		builtin:        map[string]unaryHook{},
		builtinStream:  map[string]streamHook{},
		mounted:        map[string]bool{},
		jobs:           map[string]*job{},
		jobKeys:        map[string]*job{},
		jobChanged:     make(chan struct{}),
		sandboxes:      map[string]*sandbox{},
		sandboxKeys:    map[string]*sandbox{},
		sandboxChanged: make(chan struct{}),
		executions:     map[string]*execution{},
	}
	f.registerBuiltins()
	f.start()
	if tb != nil {
		tb.Helper()
		tb.Cleanup(f.Close)
	}
	return f
}

// Close stops the fake's server. It is safe to call more than once.
func (f *Fake) Close() {
	f.closeOnce.Do(func() { f.server.Close() })
}

// URL returns the fake's base URL.
func (f *Fake) URL() string { return f.server.URL }

// HTTPClient returns an HTTP client that trusts the fake's certificate.
func (f *Fake) HTTPClient() *http.Client { return f.server.Client() }

// CA returns the fake's certificate as a PEM block, for a client built with
// toshi.WithCAFile or TOSHI_CA_FILE.
func (f *Fake) CA() []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: f.server.Certificate().Raw,
	})
}

// Client returns a client wired to the fake: its URL, the token Token, an
// HTTP client that trusts it, the scope default/production, and a retry
// policy that answers at once. opts change any of that, and are applied last.
func (f *Fake) Client(opts ...toshi.Option) *toshi.Client {
	base := []toshi.Option{
		toshi.WithServer(f.URL()),
		toshi.WithToken(Token),
		toshi.WithHTTPClient(f.HTTPClient()),
		toshi.WithScope(project, environment),
		toshi.WithRetryPolicy(retryPolicy),
	}
	client, err := toshi.New(append(base, opts...)...)
	if err != nil {
		f.fatalf("toshitest: build a client: %v", err)
	}
	return client
}

// Call is one call the fake received.
type Call struct {
	// Procedure is the procedure the client called, for example
	// workspacev1connect.WorkloadServiceCreateJobProcedure.
	Procedure string
	// Request is the message the client sent. A bidirectional stream's call
	// is recorded when its start message arrives.
	Request proto.Message
	// Header holds the request headers the client sent.
	Header http.Header
}

// Calls returns the calls the fake received, oldest first.
func (f *Fake) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Call(nil), f.calls...)
}

// fatalf fails the test, or panics when New was given no testing.TB.
func (f *Fake) fatalf(format string, args ...any) {
	if f.tb != nil {
		f.tb.Helper()
		f.tb.Fatalf(format, args...)
		return
	}
	panic(fmt.Sprintf(format, args...))
}

// record appends a call.
func (f *Fake) record(call Call) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if call.Request != nil {
		call.Request = proto.Clone(call.Request)
	}
	f.calls = append(f.calls, call)
}

// unaryHook answers one unary procedure.
type unaryHook func(context.Context, connect.AnyRequest) (connect.AnyResponse, error)

// streamHook answers one streaming procedure. request is the message a server
// stream received, and nil for a bidirectional stream.
type streamHook func(context.Context, connect.StreamingHandlerConn, proto.Message) error
