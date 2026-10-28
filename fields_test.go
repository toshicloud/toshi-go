package toshi_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi/toshitest"
)

// TestWebhookEndpointCursorThroughTheClient proves an endpoint's expired
// cursor and its retention boundary come back through the client, and that
// a cursor moves by the update mask.
func TestWebhookEndpointCursorThroughTheClient(t *testing.T) {
	ctx := context.Background()
	boundary := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	tt := toshitest.New(t)

	toshitest.On(tt, workspacev1connect.WebhookServiceGetWebhookEndpointProcedure,
		func(context.Context, *workspacev1.GetWebhookEndpointRequest) (*workspacev1.GetWebhookEndpointResponse, error) {
			return &workspacev1.GetWebhookEndpointResponse{Endpoint: &workspacev1.WebhookEndpoint{
				Id: "wh-1", Url: "https://example.com/hook",
				CursorExpired:     true,
				RetentionBoundary: timestamppb.New(boundary),
			}}, nil
		})
	toshitest.On(tt, workspacev1connect.WebhookServiceUpdateWebhookEndpointProcedure,
		func(_ context.Context, request *workspacev1.UpdateWebhookEndpointRequest) (*workspacev1.UpdateWebhookEndpointResponse, error) {
			if got := request.GetUpdateMask().GetPaths(); len(got) != 1 || got[0] != "cursor" {
				t.Errorf("the update mask is %v", got)
			}
			return &workspacev1.UpdateWebhookEndpointResponse{Endpoint: &workspacev1.WebhookEndpoint{
				Id: "wh-1", Url: "https://example.com/hook", Cursor: request.GetEndpoint().GetCursor(),
			}}, nil
		})

	client := tt.Client()
	got, err := client.Webhook().GetWebhookEndpoint(ctx, connect.NewRequest(&workspacev1.GetWebhookEndpointRequest{Id: "wh-1"}))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := got.Msg.GetEndpoint()
	if !endpoint.GetCursorExpired() {
		t.Error("an endpoint whose cursor expired came back unexpired")
	}
	if !endpoint.GetRetentionBoundary().AsTime().Equal(boundary) {
		t.Errorf("the retention boundary is %v", endpoint.GetRetentionBoundary())
	}

	moved, err := client.Webhook().UpdateWebhookEndpoint(ctx, connect.NewRequest(&workspacev1.UpdateWebhookEndpointRequest{
		Id:         "wh-1",
		Endpoint:   &workspacev1.WebhookEndpoint{Cursor: "100"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"cursor"}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if moved.Msg.GetEndpoint().GetCursor() != "100" {
		t.Fatalf("the moved endpoint has cursor %q", moved.Msg.GetEndpoint().GetCursor())
	}
}

// TestCatalogEntryStateThroughTheClient proves an entry's listing state,
// its reason and its successor come back through the client.
func TestCatalogEntryStateThroughTheClient(t *testing.T) {
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.ProductServiceListCatalogEntriesProcedure,
		func(context.Context, *workspacev1.ListCatalogEntriesRequest) (*workspacev1.ListCatalogEntriesResponse, error) {
			return &workspacev1.ListCatalogEntriesResponse{Entries: []*workspacev1.CatalogEntry{{
				Id: "acme_ci", Name: "Acme CI", Version: "1.2.0",
				Stale:           true,
				State:           workspacev1.CatalogEntryState_CATALOG_ENTRY_STATE_DEPRECATED,
				StateReason:     "superseded",
				Successor:       "acme_ci@2.0.0",
				SourceExpiresAt: timestamppb.Now(),
			}}}, nil
		})

	got, err := tt.Client().Product().ListCatalogEntries(context.Background(), connect.NewRequest(&workspacev1.ListCatalogEntriesRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	entry := got.Msg.GetEntries()[0]
	if !entry.GetStale() || entry.GetSourceExpiresAt() == nil {
		t.Errorf("the entry's staleness is %v at %v", entry.GetStale(), entry.GetSourceExpiresAt())
	}
	if entry.GetState() != workspacev1.CatalogEntryState_CATALOG_ENTRY_STATE_DEPRECATED {
		t.Errorf("the entry's state is %v", entry.GetState())
	}
	if entry.GetStateReason() != "superseded" || entry.GetSuccessor() != "acme_ci@2.0.0" {
		t.Errorf("the entry's reason and successor are %q and %q", entry.GetStateReason(), entry.GetSuccessor())
	}
}

// TestTriggerResultThroughTheClient proves a trigger's outcome carries the
// reason and, for a process that exited or a setup that failed, its exit code
// and message.
func TestTriggerResultThroughTheClient(t *testing.T) {
	tt := toshitest.New(t)
	exitCode := int32(3)
	toshitest.On(tt, workspacev1connect.WorkloadServiceListEventsProcedure,
		func(context.Context, *workspacev1.ListEventsRequest) (*workspacev1.ListEventsResponse, error) {
			return &workspacev1.ListEventsResponse{Events: []*workspacev1.Event{
				{
					Id: "ev-1", Type: workspacev1.EventType_EVENT_TYPE_TRIGGER_REMOVED, Name: "trigger.removed",
					Payload: &workspacev1.Event_Trigger{Trigger: &workspacev1.TriggerResult{
						TriggerId: "tr-1", TriggerName: "preview", Key: "acme/widgets#7",
						WorkloadId: "sb-1", Reason: "process_exited",
						ExitCode: &exitCode,
						Message:  "exit status 3: boom",
					}},
				},
			}}, nil
		})

	got, err := tt.Client().Workload().ListEvents(context.Background(), &workspacev1.ListEventsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	events := got.GetEvents()
	if len(events) != 1 {
		t.Fatalf("%d events, want 1", len(events))
	}
	result := events[0].GetTrigger()
	if result.GetTriggerName() != "preview" || result.GetReason() != "process_exited" {
		t.Fatalf("the trigger result is %v", result)
	}
	if result.ExitCode == nil || result.GetExitCode() != 3 {
		t.Errorf("the exit code is %v", result.GetExitCode())
	}
	if result.GetMessage() != "exit status 3: boom" {
		t.Errorf("the message is %q", result.GetMessage())
	}
}
