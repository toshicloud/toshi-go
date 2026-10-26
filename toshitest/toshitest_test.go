package toshitest_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi"
	"go.toshicloud.io/toshi/toshitest"
)

// TestRun proves the two-line case: a job the test described runs, streams its
// output and exits 0.
func TestRun(t *testing.T) {
	tt := toshitest.New(t)
	tt.Job("job-1").Stdout("built\n").ExitCode(0)
	client := tt.Client()

	run, err := client.Run(context.Background(), &workspacev1.CreateJobRequest{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if run.Job().GetId() != "job-1" {
		t.Fatalf("job %q, want job-1", run.Job().GetId())
	}
	stdout, err := io.ReadAll(run.Stdout())
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if string(stdout) != "built\n" {
		t.Fatalf("stdout %q, want %q", stdout, "built\n")
	}
	if err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	jobs, err := client.Workload().Jobs().List(context.Background(), &workspacev1.ListJobsRequest{})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs.GetJobs()) != 1 || jobs.GetJobs()[0].GetId() != "job-1" {
		t.Fatalf("jobs %v, want job-1", jobs.GetJobs())
	}
}

// TestCreateJobIsIdempotent proves a repeated request key returns the same
// job.
func TestCreateJobIsIdempotent(t *testing.T) {
	tt := toshitest.New(t)
	client := tt.Client()
	first, err := client.Workload().Jobs().Create(context.Background(), &workspacev1.CreateJobRequest{RequestKey: "key-1"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	second, err := client.Workload().Jobs().Create(context.Background(), &workspacev1.CreateJobRequest{RequestKey: "key-1"})
	if err != nil {
		t.Fatalf("second CreateJob: %v", err)
	}
	if first.GetJob().GetId() != second.GetJob().GetId() {
		t.Fatalf("ids %q and %q, want the same job", first.GetJob().GetId(), second.GetJob().GetId())
	}
}

// TestRunExitCode proves a job that ends with a status comes back as a
// *toshi.JobExitError.
func TestRunExitCode(t *testing.T) {
	tt := toshitest.New(t)
	tt.Job("job-1").Stderr("boom\n").ExitCode(2)

	run, err := tt.Client().Run(context.Background(), &workspacev1.CreateJobRequest{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	stderr, err := io.ReadAll(run.Stderr())
	if err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	if string(stderr) != "boom\n" {
		t.Fatalf("stderr %q, want %q", stderr, "boom\n")
	}
	var exit *toshi.JobExitError
	if err := run.Wait(); !errors.As(err, &exit) || exit.End.GetExitCode() != 2 {
		t.Fatalf("Wait: %v, want a JobExitError with status 2", err)
	}
}

// TestExecute proves a command returns the output and the exit status the
// test set for its sandbox.
func TestExecute(t *testing.T) {
	tt := toshitest.New(t)
	tt.Execute("sb-1").Stdout("hello\n").Stderr("warn\n").ExitCode(0)

	process, err := tt.Client().Workload().Sandboxes().Execute(context.Background(), &workspacev1.ExecuteStart{
		SandboxId: "sb-1",
		Command:   &workspacev1.Command{Argv: []string{"echo", "hello"}},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	var stderr []byte
	var stderrErr error
	var read sync.WaitGroup
	read.Go(func() {
		stderr, stderrErr = io.ReadAll(process.Stderr())
	})
	stdout, err := io.ReadAll(process.Stdout())
	read.Wait()
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if stderrErr != nil {
		t.Fatalf("read stderr: %v", stderrErr)
	}
	if string(stdout) != "hello\n" || string(stderr) != "warn\n" {
		t.Fatalf("stdout %q, stderr %q", stdout, stderr)
	}
	if err := process.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

// TestExecuteHooked proves OnBidi replaces the state machine for Execute.
func TestExecuteHooked(t *testing.T) {
	tt := toshitest.New(t)
	toshitest.OnBidi(tt, workspacev1connect.WorkloadServiceExecuteProcedure,
		func(_ context.Context, stream *toshitest.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
			if _, err := stream.Receive(); err != nil {
				return err
			}
			if err := stream.Send(&workspacev1.ExecuteResponse{
				Message: &workspacev1.ExecuteResponse_Started{Started: &workspacev1.ExecutionStarted{ExecutionId: "exec-hooked"}},
			}); err != nil {
				return err
			}
			exit := int32(0)
			return stream.Send(&workspacev1.ExecuteResponse{
				Message: &workspacev1.ExecuteResponse_Exit{Exit: &workspacev1.ExecutionExit{ExitCode: &exit}},
			})
		})

	process, err := tt.Client().Workload().Sandboxes().Execute(context.Background(), &workspacev1.ExecuteStart{SandboxId: "sb-1"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if process.ExecutionID() != "exec-hooked" {
		t.Fatalf("execution %q, want exec-hooked", process.ExecutionID())
	}
	if err := process.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

// TestWait proves Wait answers at once with the state the test set.
func TestWait(t *testing.T) {
	tt := toshitest.New(t)
	tt.Job("job-1").State(workspacev1.JobState_JOB_STATE_ENDED)

	response, err := tt.Client().Workload().Jobs().Wait(context.Background(), &workspacev1.WaitJobRequest{
		Id:     "job-1",
		States: []workspacev1.JobState{workspacev1.JobState_JOB_STATE_ENDED},
	})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !response.GetReached() || response.GetJob().GetStatus().GetState() != workspacev1.JobState_JOB_STATE_ENDED {
		t.Fatalf("reached %v, state %v", response.GetReached(), response.GetJob().GetStatus().GetState())
	}
}

// TestWatchJobs proves a job watch reports a state the test sets after it
// starts, from the cursor the list answered.
func TestWatchJobs(t *testing.T) {
	ctx := context.Background()
	tt := toshitest.New(t)
	client := tt.Client()
	if _, err := client.Workload().Jobs().Create(ctx, &workspacev1.CreateJobRequest{}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	watch, err := client.Workload().Jobs().Watch(ctx, &workspacev1.WatchJobsRequest{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer func() { _ = watch.Close() }()
	if len(watch.Snapshot()) != 1 {
		t.Fatalf("snapshot of %d jobs, want 1", len(watch.Snapshot()))
	}

	tt.Job("job-1").ExitCode(0)
	if !watch.Receive() {
		t.Fatalf("Receive: %v", watch.Err())
	}
	change := watch.Msg()
	if change.GetJob().GetId() != "job-1" || change.GetJob().GetStatus().GetState() != workspacev1.JobState_JOB_STATE_ENDED {
		t.Fatalf("change %v", change)
	}
}

// TestWatchOutput proves the output watch starts at the offset the request
// names and ends with the job's exit status.
func TestWatchOutput(t *testing.T) {
	tt := toshitest.New(t)
	tt.Job("job-1").Stdout("hello\n").ExitCode(0)

	watch, err := tt.Client().Workload().Jobs().WatchOutput(context.Background(), &workspacev1.WatchOutputRequest{
		Id:           "job-1",
		StdoutOffset: 2,
	})
	if err != nil {
		t.Fatalf("WatchOutput: %v", err)
	}
	if !watch.Receive() {
		t.Fatalf("Receive: %v", watch.Err())
	}
	if got := string(watch.Msg().GetStdout()); got != "llo\n" {
		t.Fatalf("stdout %q, want %q", got, "llo\n")
	}
	if !watch.Receive() {
		t.Fatalf("second Receive: %v", watch.Err())
	}
	if end := watch.Msg().GetEnd(); end.GetExitCode() != 0 {
		t.Fatalf("end %v, want exit 0", end)
	}
}

// TestOn proves a unary hook replaces the state machine for its procedure.
func TestOn(t *testing.T) {
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceCreateJobProcedure,
		func(_ context.Context, _ *workspacev1.CreateJobRequest) (*workspacev1.CreateJobResponse, error) {
			return &workspacev1.CreateJobResponse{Job: &workspacev1.Job{Id: "hooked"}}, nil
		})

	response, err := tt.Client().Workload().Jobs().Create(context.Background(), &workspacev1.CreateJobRequest{})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if response.GetJob().GetId() != "hooked" {
		t.Fatalf("job %q, want hooked", response.GetJob().GetId())
	}
}

// TestCalls proves the fake records the calls the client made, with their
// requests and headers.
func TestCalls(t *testing.T) {
	tt := toshitest.New(t)
	client := tt.Client()
	if _, err := client.Workload().Jobs().Create(context.Background(), &workspacev1.CreateJobRequest{}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	calls := tt.Calls()
	if len(calls) != 1 {
		t.Fatalf("%d calls, want 1", len(calls))
	}
	if calls[0].Procedure != workspacev1connect.WorkloadServiceCreateJobProcedure {
		t.Fatalf("procedure %q", calls[0].Procedure)
	}
	request, ok := calls[0].Request.(*workspacev1.CreateJobRequest)
	if !ok {
		t.Fatalf("request %T, want *workspacev1.CreateJobRequest", calls[0].Request)
	}
	if request.GetProject() != "default" || request.GetEnvironment() != "production" {
		t.Fatalf("scope %q/%q, want default/production", request.GetProject(), request.GetEnvironment())
	}
	if got := calls[0].Header.Get("Authorization"); got != "Bearer "+toshitest.Token {
		t.Fatalf("Authorization %q", got)
	}
}

// TestUnimplemented proves an unhooked procedure answers UNIMPLEMENTED with
// the procedure and the function that hooks it.
func TestUnimplemented(t *testing.T) {
	tt := toshitest.New(t)
	_, err := tt.Client().Project().ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{}))
	if connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("err %v, want UNIMPLEMENTED", err)
	}
	if !strings.Contains(err.Error(), workspacev1connect.ProjectServiceListProjectsProcedure) {
		t.Errorf("message %q does not name the procedure", err)
	}
	if !strings.Contains(err.Error(), "toshitest.On") {
		t.Errorf("message %q does not name the hook function", err)
	}
}
