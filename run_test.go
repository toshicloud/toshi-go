package toshi

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
)

// TestRunFollowsJob proves Run creates a job, follows its output and ends
// with its exit.
func TestRunFollowsJob(t *testing.T) {
	zero := int32(0)
	var mu sync.Mutex
	var watched *workspacev1.WatchOutputRequest
	server := newTestServer(t, &backend{
		createJob: func(_ context.Context, req *connect.Request[workspacev1.CreateJobRequest]) (*connect.Response[workspacev1.CreateJobResponse], error) {
			if req.Msg.GetRequestKey() == "" {
				t.Error("CreateJob left the request key empty")
			}
			return connect.NewResponse(&workspacev1.CreateJobResponse{Job: &workspacev1.Job{
				Id:          "job-1",
				Project:     "jobproj",
				Environment: "jobenv",
				Status:      &workspacev1.JobStatus{State: workspacev1.JobState_JOB_STATE_QUEUED},
			}}), nil
		},
		watchOutput: func(_ context.Context, req *connect.Request[workspacev1.WatchOutputRequest], stream *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
			mu.Lock()
			watched = req.Msg
			mu.Unlock()
			if err := stream.Send(&workspacev1.WatchOutputResponse{Message: &workspacev1.WatchOutputResponse_Stdout{Stdout: []byte("abc")}}); err != nil {
				return err
			}
			if err := stream.Send(&workspacev1.WatchOutputResponse{Message: &workspacev1.WatchOutputResponse_Stderr{Stderr: []byte("de")}}); err != nil {
				return err
			}
			return stream.Send(&workspacev1.WatchOutputResponse{Message: &workspacev1.WatchOutputResponse_End{End: &workspacev1.OutputEnd{ExitCode: &zero}}})
		},
	})
	client := newTestClient(t, server, WithScope("proj", "env"))
	run, err := client.Run(context.Background(), &workspacev1.CreateJobRequest{
		Spec: &workspacev1.JobSpec{Command: &workspacev1.Command{Argv: []string{"true"}}},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if run.Job().GetId() != "job-1" {
		t.Fatalf("job %q", run.Job().GetId())
	}
	if err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	stdout, err := io.ReadAll(run.Stdout())
	if err != nil {
		t.Fatalf("Stdout: %v", err)
	}
	stderr, err := io.ReadAll(run.Stderr())
	if err != nil {
		t.Fatalf("Stderr: %v", err)
	}
	if string(stdout) != "abc" || string(stderr) != "de" {
		t.Fatalf("output %q/%q", stdout, stderr)
	}
	mu.Lock()
	defer mu.Unlock()
	if watched.GetId() != "job-1" || watched.GetProject() != "jobproj" || watched.GetEnvironment() != "jobenv" {
		t.Fatalf("output watch %v, want the job's own scope", watched)
	}
}

// TestRunReportsNonZeroExit proves Wait reports a job that did not exit 0.
func TestRunReportsNonZeroExit(t *testing.T) {
	three := int32(3)
	server := newTestServer(t, &backend{
		createJob: func(context.Context, *connect.Request[workspacev1.CreateJobRequest]) (*connect.Response[workspacev1.CreateJobResponse], error) {
			return connect.NewResponse(&workspacev1.CreateJobResponse{Job: &workspacev1.Job{Id: "job-1"}}), nil
		},
		watchOutput: func(_ context.Context, _ *connect.Request[workspacev1.WatchOutputRequest], stream *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
			return stream.Send(&workspacev1.WatchOutputResponse{Message: &workspacev1.WatchOutputResponse_End{End: &workspacev1.OutputEnd{ExitCode: &three}}})
		},
	})
	client := newTestClient(t, server)
	run, err := client.Run(context.Background(), &workspacev1.CreateJobRequest{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	err = run.Wait()
	var exit *JobExitError
	if !errors.As(err, &exit) {
		t.Fatalf("Wait: %v, want a JobExitError", err)
	}
	if exit.JobID != "job-1" || exit.End.GetExitCode() != 3 {
		t.Fatalf("exit %+v", exit)
	}
}

// TestRunJobEndedInTheCall proves a job that ended within CreateJob needs no
// output watch: its output and its exit come back with it.
func TestRunJobEndedInTheCall(t *testing.T) {
	zero := int32(0)
	var watches atomic.Int32
	server := newTestServer(t, &backend{
		createJob: func(context.Context, *connect.Request[workspacev1.CreateJobRequest]) (*connect.Response[workspacev1.CreateJobResponse], error) {
			return connect.NewResponse(&workspacev1.CreateJobResponse{
				Job: &workspacev1.Job{
					Id:     "job-1",
					Status: &workspacev1.JobStatus{State: workspacev1.JobState_JOB_STATE_ENDED, ExitCode: &zero},
				},
				Output: &workspacev1.Output{Stdout: []byte("done"), Stderr: []byte("warn")},
			}), nil
		},
		watchOutput: func(context.Context, *connect.Request[workspacev1.WatchOutputRequest], *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
			watches.Add(1)
			return unimplemented()
		},
	})
	client := newTestClient(t, server)
	run, err := client.Run(context.Background(), &workspacev1.CreateJobRequest{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := run.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	stdout, _ := io.ReadAll(run.Stdout())
	stderr, _ := io.ReadAll(run.Stderr())
	if string(stdout) != "done" || string(stderr) != "warn" {
		t.Fatalf("output %q/%q", stdout, stderr)
	}
	if watches.Load() != 0 {
		t.Fatalf("%d output watches, want none", watches.Load())
	}
}

// TestRunLostStream proves Wait reports an output stream that ended without
// the job's end.
func TestRunLostStream(t *testing.T) {
	server := newTestServer(t, &backend{
		createJob: func(context.Context, *connect.Request[workspacev1.CreateJobRequest]) (*connect.Response[workspacev1.CreateJobResponse], error) {
			return connect.NewResponse(&workspacev1.CreateJobResponse{Job: &workspacev1.Job{Id: "job-1"}}), nil
		},
		watchOutput: func(context.Context, *connect.Request[workspacev1.WatchOutputRequest], *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
			return nil
		},
	})
	client := newTestClient(t, server, WithRetryPolicy(RetryPolicy{MaxAttempts: 2, InitialBackoff: 0, MaxBackoff: 0}))
	run, err := client.Run(context.Background(), &workspacev1.CreateJobRequest{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	err = run.Wait()
	var exit *JobExitError
	if err == nil || errors.As(err, &exit) {
		t.Fatalf("Wait: %v, want the stream's error", err)
	}
}
