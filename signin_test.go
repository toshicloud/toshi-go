package toshi_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi/toshitest"
)

// TestStartSignInThroughTheClient proves a sign-in streams its started,
// output and ended messages through the client, the caller answers it with
// the started id, and the client fills the scope.
func TestStartSignInThroughTheClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	endsAt := time.Date(2026, 10, 31, 10, 15, 0, 0, time.UTC)
	tt := toshitest.New(t)
	answers := make(chan string, 1)

	toshitest.OnServerStream(tt, workspacev1connect.WorkloadServiceStartServiceSignInProcedure,
		func(ctx context.Context, request *workspacev1.StartServiceSignInRequest, stream *toshitest.ServerStream[workspacev1.StartServiceSignInResponse]) error {
			if request.GetServiceId() != "svc-1" || request.GetProgram() != "login" {
				t.Errorf("StartSignIn asked for %v", request)
			}
			if request.GetProject() != "default" || request.GetEnvironment() != "production" {
				t.Errorf("StartSignIn's scope is %q/%q", request.GetProject(), request.GetEnvironment())
			}
			if err := stream.Send(&workspacev1.StartServiceSignInResponse{Message: &workspacev1.StartServiceSignInResponse_Started{
				Started: &workspacev1.ServiceSignInStarted{SignInId: "sign-1", EndsAt: timestamppb.New(endsAt)},
			}}); err != nil {
				return err
			}
			if err := stream.Send(&workspacev1.StartServiceSignInResponse{Message: &workspacev1.StartServiceSignInResponse_Output{
				Output: []byte("Username: "),
			}}); err != nil {
				return err
			}
			select {
			case answer := <-answers:
				if answer != "alice" {
					t.Errorf("StartSignIn was answered %q, want alice", answer)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			if err := stream.Send(&workspacev1.StartServiceSignInResponse{Message: &workspacev1.StartServiceSignInResponse_Output{
				Output: []byte("Password: "),
			}}); err != nil {
				return err
			}
			return stream.Send(&workspacev1.StartServiceSignInResponse{Message: &workspacev1.StartServiceSignInResponse_Ended{
				Ended: &workspacev1.ServiceSignInEnded{
					Outcome:  workspacev1.ServiceSignInOutcome_SERVICE_SIGN_IN_OUTCOME_SIGNED_IN,
					ExitCode: proto.Int32(0),
				},
			}})
		})
	toshitest.On(tt, workspacev1connect.WorkloadServiceAnswerServiceSignInProcedure,
		func(_ context.Context, request *workspacev1.AnswerServiceSignInRequest) (*workspacev1.AnswerServiceSignInResponse, error) {
			if request.GetServiceId() != "svc-1" || request.GetSignInId() != "sign-1" || request.GetText() != "alice" {
				t.Errorf("AnswerSignIn asked for %v", request)
			}
			if request.GetProject() != "default" || request.GetEnvironment() != "production" {
				t.Errorf("AnswerSignIn's scope is %q/%q", request.GetProject(), request.GetEnvironment())
			}
			answers <- request.GetText()
			return &workspacev1.AnswerServiceSignInResponse{}, nil
		})

	client := tt.Client()
	stream, err := client.Workload().Services().StartSignIn(ctx, &workspacev1.StartServiceSignInRequest{
		ServiceId: "svc-1", Program: "login",
	})
	if err != nil {
		t.Fatalf("StartSignIn: %v", err)
	}
	var started *workspacev1.ServiceSignInStarted
	var output []byte
	var ended *workspacev1.ServiceSignInEnded
	for stream.Receive() {
		switch message := stream.Msg().GetMessage().(type) {
		case *workspacev1.StartServiceSignInResponse_Started:
			started = message.Started
		case *workspacev1.StartServiceSignInResponse_Output:
			output = append(output, message.Output...)
			if string(message.Output) == "Username: " {
				if started.GetSignInId() == "" {
					t.Fatal("the sign-in asked its first question before it started")
				}
				if _, err := client.Workload().Services().AnswerSignIn(ctx, &workspacev1.AnswerServiceSignInRequest{
					ServiceId: "svc-1", SignInId: started.GetSignInId(), Text: "alice",
				}); err != nil {
					t.Fatalf("AnswerSignIn: %v", err)
				}
			}
		case *workspacev1.StartServiceSignInResponse_Ended:
			ended = message.Ended
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("the sign-in stream: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if started.GetSignInId() != "sign-1" || !started.GetEndsAt().AsTime().Equal(endsAt) {
		t.Fatalf("the sign-in started %v", started)
	}
	if string(output) != "Username: Password: " {
		t.Fatalf("the sign-in wrote %q", output)
	}
	if ended == nil || ended.GetOutcome() != workspacev1.ServiceSignInOutcome_SERVICE_SIGN_IN_OUTCOME_SIGNED_IN ||
		ended.GetExitCode() != 0 {
		t.Fatalf("the sign-in ended %v", ended)
	}
}

// TestCancelSignInThroughTheClient proves a sign-in is cancelled with the
// started id, and its stream ends CANCELLED.
func TestCancelSignInThroughTheClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tt := toshitest.New(t)
	cancelled := make(chan struct{}, 1)

	toshitest.OnServerStream(tt, workspacev1connect.WorkloadServiceStartServiceSignInProcedure,
		func(ctx context.Context, request *workspacev1.StartServiceSignInRequest, stream *toshitest.ServerStream[workspacev1.StartServiceSignInResponse]) error {
			if request.GetServiceId() != "svc-1" || request.GetProgram() != "login" {
				t.Errorf("StartSignIn asked for %v", request)
			}
			if err := stream.Send(&workspacev1.StartServiceSignInResponse{Message: &workspacev1.StartServiceSignInResponse_Started{
				Started: &workspacev1.ServiceSignInStarted{SignInId: "sign-2"},
			}}); err != nil {
				return err
			}
			select {
			case <-cancelled:
			case <-ctx.Done():
				return ctx.Err()
			}
			return stream.Send(&workspacev1.StartServiceSignInResponse{Message: &workspacev1.StartServiceSignInResponse_Ended{
				Ended: &workspacev1.ServiceSignInEnded{
					Outcome: workspacev1.ServiceSignInOutcome_SERVICE_SIGN_IN_OUTCOME_CANCELLED,
				},
			}})
		})
	toshitest.On(tt, workspacev1connect.WorkloadServiceCancelServiceSignInProcedure,
		func(_ context.Context, request *workspacev1.CancelServiceSignInRequest) (*workspacev1.CancelServiceSignInResponse, error) {
			if request.GetServiceId() != "svc-1" || request.GetSignInId() != "sign-2" {
				t.Errorf("CancelSignIn asked for %v", request)
			}
			if request.GetProject() != "default" || request.GetEnvironment() != "production" {
				t.Errorf("CancelSignIn's scope is %q/%q", request.GetProject(), request.GetEnvironment())
			}
			cancelled <- struct{}{}
			return &workspacev1.CancelServiceSignInResponse{}, nil
		})

	client := tt.Client()
	stream, err := client.Workload().Services().StartSignIn(ctx, &workspacev1.StartServiceSignInRequest{
		ServiceId: "svc-1", Program: "login",
	})
	if err != nil {
		t.Fatalf("StartSignIn: %v", err)
	}
	var started *workspacev1.ServiceSignInStarted
	var ended *workspacev1.ServiceSignInEnded
	for stream.Receive() {
		switch message := stream.Msg().GetMessage().(type) {
		case *workspacev1.StartServiceSignInResponse_Started:
			started = message.Started
			if _, err := client.Workload().Services().CancelSignIn(ctx, &workspacev1.CancelServiceSignInRequest{
				ServiceId: "svc-1", SignInId: started.GetSignInId(),
			}); err != nil {
				t.Fatalf("CancelSignIn: %v", err)
			}
		case *workspacev1.StartServiceSignInResponse_Ended:
			ended = message.Ended
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("the sign-in stream: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if started.GetSignInId() != "sign-2" {
		t.Fatalf("the sign-in started %v", started)
	}
	if ended == nil || ended.GetOutcome() != workspacev1.ServiceSignInOutcome_SERVICE_SIGN_IN_OUTCOME_CANCELLED {
		t.Fatalf("the sign-in ended %v", ended)
	}
}

// TestGetSignInStatusThroughTheClient proves a service's sign-in statuses
// come back through the client, with the scope it fills in.
func TestGetSignInStatusThroughTheClient(t *testing.T) {
	ctx := context.Background()
	tt := toshitest.New(t)

	toshitest.On(tt, workspacev1connect.WorkloadServiceGetServiceSignInStatusProcedure,
		func(_ context.Context, request *workspacev1.GetServiceSignInStatusRequest) (*workspacev1.GetServiceSignInStatusResponse, error) {
			if request.GetServiceId() != "svc-1" || request.GetProgram() != "login" {
				t.Errorf("GetSignInStatus asked for %v", request)
			}
			if request.GetProject() != "default" || request.GetEnvironment() != "production" {
				t.Errorf("GetSignInStatus's scope is %q/%q", request.GetProject(), request.GetEnvironment())
			}
			return &workspacev1.GetServiceSignInStatusResponse{Statuses: []*workspacev1.ServiceSignInStatus{{
				Program:  "login",
				SignedIn: true,
				ExitCode: 0,
				Output:   "user@example.com\n",
			}}}, nil
		})

	status, err := tt.Client().Workload().Services().GetSignInStatus(ctx, &workspacev1.GetServiceSignInStatusRequest{
		ServiceId: "svc-1", Program: "login",
	})
	if err != nil {
		t.Fatalf("GetSignInStatus: %v", err)
	}
	statuses := status.GetStatuses()
	if len(statuses) != 1 || statuses[0].GetProgram() != "login" || !statuses[0].GetSignedIn() ||
		statuses[0].GetExitCode() != 0 || statuses[0].GetOutput() != "user@example.com\n" {
		t.Fatalf("GetSignInStatus answered %v", statuses)
	}
}
