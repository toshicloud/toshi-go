package toshi_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi/toshitest"
)

// TestExecuteCommandThroughTheClient proves a command's start, output and
// exit come back on the stream, and that the client fills a request key and
// the scope.
func TestExecuteCommandThroughTheClient(t *testing.T) {
	ctx := context.Background()
	tt := toshitest.New(t)

	toshitest.OnServerStream(tt, workspacev1connect.WorkloadServiceExecuteCommandProcedure,
		func(_ context.Context, request *workspacev1.ExecuteCommandRequest, stream *toshitest.ServerStream[workspacev1.ExecuteCommandResponse]) error {
			if request.GetSandboxId() != "sb-1" || len(request.GetCommand().GetArgv()) != 2 {
				t.Errorf("ExecuteCommand asked for %v", request)
			}
			if request.GetProject() != "default" || request.GetEnvironment() != "production" {
				t.Errorf("ExecuteCommand's scope is %q/%q", request.GetProject(), request.GetEnvironment())
			}
			if request.GetRequestKey() == "" {
				t.Error("the client sent no request key")
			}
			messages := []*workspacev1.ExecuteCommandResponse{
				{Message: &workspacev1.ExecuteCommandResponse_Started{Started: &workspacev1.ExecutionStarted{ExecutionId: "exec-1"}}},
				{Message: &workspacev1.ExecuteCommandResponse_Stdout{Stdout: []byte("hello\n")}},
				{Message: &workspacev1.ExecuteCommandResponse_Stderr{Stderr: []byte("warning\n")}},
				{Message: &workspacev1.ExecuteCommandResponse_Exit{Exit: &workspacev1.ExecutionExit{ExitCode: proto.Int32(0)}}},
			}
			for _, message := range messages {
				if err := stream.Send(message); err != nil {
					return err
				}
			}
			return nil
		})

	stream, err := tt.Client().Workload().Sandboxes().ExecuteCommand(ctx, &workspacev1.ExecuteCommandRequest{
		SandboxId: "sb-1",
		Command:   &workspacev1.Command{Argv: []string{"echo", "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var started *workspacev1.ExecutionStarted
	var stdout, stderr []byte
	var exit *workspacev1.ExecutionExit
	for stream.Receive() {
		switch message := stream.Msg().GetMessage().(type) {
		case *workspacev1.ExecuteCommandResponse_Started:
			started = message.Started
		case *workspacev1.ExecuteCommandResponse_Stdout:
			stdout = append(stdout, message.Stdout...)
		case *workspacev1.ExecuteCommandResponse_Stderr:
			stderr = append(stderr, message.Stderr...)
		case *workspacev1.ExecuteCommandResponse_Exit:
			exit = message.Exit
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if started.GetExecutionId() != "exec-1" {
		t.Fatalf("the stream started %v", started)
	}
	if string(stdout) != "hello\n" || string(stderr) != "warning\n" {
		t.Fatalf("the stream carried stdout %q and stderr %q", stdout, stderr)
	}
	if exit == nil || exit.GetExitCode() != 0 || exit.GetCancelled() {
		t.Fatalf("the stream ended %v", exit)
	}
}
