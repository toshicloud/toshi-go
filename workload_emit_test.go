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

// TestEmitEventThroughTheClient proves an emitted event reaches the server
// with the workload, name and data given, and that the client fills the
// scope.
func TestEmitEventThroughTheClient(t *testing.T) {
	ctx := context.Background()
	emittedAt := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	tt := toshitest.New(t)

	toshitest.On(tt, workspacev1connect.WorkloadServiceEmitWorkloadEventProcedure,
		func(_ context.Context, request *workspacev1.EmitWorkloadEventRequest) (*workspacev1.EmitWorkloadEventResponse, error) {
			if request.GetWorkloadId() != "job-1" || request.GetName() != "deploy.done" ||
				string(request.GetData()) != `{"ok":true}` {
				t.Errorf("EmitEvent asked for %v", request)
			}
			if request.GetProject() != "default" || request.GetEnvironment() != "production" {
				t.Errorf("EmitEvent's scope is %q/%q", request.GetProject(), request.GetEnvironment())
			}
			return &workspacev1.EmitWorkloadEventResponse{Event: &workspacev1.Event{
				Id:         "event-1",
				Type:       workspacev1.EventType_EVENT_TYPE_EMITTED,
				Name:       "deploy.done",
				WorkloadId: "job-1",
				Time:       timestamppb.New(emittedAt),
			}}, nil
		})

	resp, err := tt.Client().Workload().EmitEvent(ctx, &workspacev1.EmitWorkloadEventRequest{
		WorkloadId: "job-1",
		Name:       "deploy.done",
		Data:       []byte(`{"ok":true}`),
	})
	if err != nil {
		t.Fatalf("EmitEvent: %v", err)
	}
	event := resp.GetEvent()
	if event.GetId() != "event-1" || event.GetType() != workspacev1.EventType_EVENT_TYPE_EMITTED ||
		event.GetName() != "deploy.done" || event.GetWorkloadId() != "job-1" ||
		!event.GetTime().AsTime().Equal(emittedAt) {
		t.Fatalf("EmitEvent answered %v", event)
	}
}
