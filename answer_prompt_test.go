package toshi_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi/toshitest"
)

// TestAnswerPromptThroughTheClient proves an answer reaches the server with
// the scope the client fills in, and the event that records it comes back.
func TestAnswerPromptThroughTheClient(t *testing.T) {
	ctx := context.Background()
	answered := time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC)
	tt := toshitest.New(t)

	toshitest.On(tt, workspacev1connect.WorkloadServiceAnswerPromptProcedure,
		func(_ context.Context, request *workspacev1.AnswerPromptRequest) (*workspacev1.AnswerPromptResponse, error) {
			if request.GetProject() != "default" || request.GetEnvironment() != "production" {
				t.Errorf("AnswerPrompt's scope is %q/%q", request.GetProject(), request.GetEnvironment())
			}
			if request.GetWorkloadId() != "job-1" || request.GetPromptId() != "prompt-1" || request.GetAnswer() != "approved" {
				t.Errorf("AnswerPrompt asked for %v", request)
			}
			return &workspacev1.AnswerPromptResponse{Event: &workspacev1.Event{
				Id:         "event-1",
				Type:       workspacev1.EventType_EVENT_TYPE_AGENT_ANSWERED,
				WorkloadId: "job-1",
				Time:       timestamppb.New(answered),
			}}, nil
		})

	answer, err := tt.Client().Workload().AnswerPrompt(ctx, &workspacev1.AnswerPromptRequest{
		WorkloadId: "job-1",
		PromptId:   "prompt-1",
		Answer:     "approved",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := answer.GetEvent(); got.GetId() != "event-1" ||
		got.GetType() != workspacev1.EventType_EVENT_TYPE_AGENT_ANSWERED ||
		got.GetWorkloadId() != "job-1" || !got.GetTime().AsTime().Equal(answered) {
		t.Fatalf("AnswerPrompt answered %v", got)
	}
}
