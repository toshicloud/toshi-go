package toshi_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi"
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

// TestWorkspaceUpdateThroughTheClient proves the update mask and the etag a
// caller sends reach the API, and the workspace's settings and new etag come
// back.
func TestWorkspaceUpdateThroughTheClient(t *testing.T) {
	ctx := context.Background()
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkspaceServiceUpdateWorkspaceProcedure,
		func(_ context.Context, request *workspacev1.UpdateWorkspaceRequest) (*workspacev1.UpdateWorkspaceResponse, error) {
			if got := request.GetUpdateMask().GetPaths(); len(got) != 1 || got[0] != "settings.sign_in" {
				t.Errorf("the update mask is %v", got)
			}
			if request.GetWorkspace().GetEtag() != "etag-1" {
				t.Errorf("the etag is %q", request.GetWorkspace().GetEtag())
			}
			return &workspacev1.UpdateWorkspaceResponse{Workspace: &workspacev1.Workspace{
				Id:   "ws-1",
				Etag: "etag-2",
				Settings: &workspacev1.WorkspaceSettings{
					HistoryDays: proto.Int32(30),
					SignIn:      request.GetWorkspace().GetSettings().GetSignIn(),
				},
			}}, nil
		})

	updated, err := tt.Client().Workspace().UpdateWorkspace(ctx, connect.NewRequest(&workspacev1.UpdateWorkspaceRequest{
		Workspace: &workspacev1.Workspace{
			Etag:     "etag-1",
			Settings: &workspacev1.WorkspaceSettings{SignIn: &workspacev1.WorkspaceSignIn{Provider: "github"}},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"settings.sign_in"}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	workspace := updated.Msg.GetWorkspace()
	if workspace.GetEtag() != "etag-2" {
		t.Errorf("the workspace's etag is %q", workspace.GetEtag())
	}
	if workspace.GetSettings().GetHistoryDays() != 30 {
		t.Errorf("the workspace's history days are %d", workspace.GetSettings().GetHistoryDays())
	}
	if workspace.GetSettings().GetSignIn().GetProvider() != "github" {
		t.Errorf("the workspace's sign-in provider is %q", workspace.GetSettings().GetSignIn().GetProvider())
	}
}

// TestConnectionTargetThroughTheClient proves a connection's delivery target
// moves by the update mask, and that a delivery run names the delivery it
// posted to the target.
func TestConnectionTargetThroughTheClient(t *testing.T) {
	ctx := context.Background()
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.ConnectionServiceUpdateConnectionProcedure,
		func(_ context.Context, request *workspacev1.UpdateConnectionRequest) (*workspacev1.UpdateConnectionResponse, error) {
			if got := request.GetUpdateMask().GetPaths(); len(got) != 1 || got[0] != "target" {
				t.Errorf("the update mask is %v", got)
			}
			return &workspacev1.UpdateConnectionResponse{Connection: &workspacev1.Connection{
				Id:     request.GetConnection().GetId(),
				Target: request.GetConnection().GetTarget(),
			}}, nil
		})
	toshitest.On(tt, workspacev1connect.ConnectionServiceGetConnectionDeliveryProcedure,
		func(context.Context, *workspacev1.GetConnectionDeliveryRequest) (*workspacev1.GetConnectionDeliveryResponse, error) {
			return &workspacev1.GetConnectionDeliveryResponse{
				Delivery: &workspacev1.ConnectionDelivery{Id: "del-1"},
				Runs: []*workspacev1.ConnectionDeliveryRun{{
					Run: 1, Outcome: "delivered", TargetDeliveryId: "target-del-1",
				}},
			}, nil
		})

	client := tt.Client()
	updated, err := client.Connection().UpdateConnection(ctx, connect.NewRequest(&workspacev1.UpdateConnectionRequest{
		Connection: &workspacev1.Connection{
			Id:     "con-1",
			Target: &workspacev1.ConnectionTarget{Url: "https://example.com/hooks", Secret: testWebhookSecret()},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"target"}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got := updated.Msg.GetConnection().GetTarget().GetUrl(); got != "https://example.com/hooks" {
		t.Errorf("the connection's target is %q", got)
	}

	delivery, err := client.Connection().GetConnectionDelivery(ctx, connect.NewRequest(&workspacev1.GetConnectionDeliveryRequest{Id: "del-1"}))
	if err != nil {
		t.Fatal(err)
	}
	run := delivery.Msg.GetRuns()[0]
	if run.GetOutcome() != "delivered" || run.GetTargetDeliveryId() != "target-del-1" {
		t.Errorf("the run is %q posted as %q", run.GetOutcome(), run.GetTargetDeliveryId())
	}
}

// TestWorkloadImageThroughTheClient proves a job and a sandbox carry the
// image they run in, and read back the image version the API fixed.
func TestWorkloadImageThroughTheClient(t *testing.T) {
	ctx := context.Background()
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceCreateJobProcedure,
		func(_ context.Context, request *workspacev1.CreateJobRequest) (*workspacev1.CreateJobResponse, error) {
			if got := request.GetSpec().GetImage(); got != "build" {
				t.Errorf("the job's image is %q", got)
			}
			return &workspacev1.CreateJobResponse{Job: &workspacev1.Job{Spec: &workspacev1.JobSpec{
				Image: "build", ImageVersionId: "imgv-1",
			}}}, nil
		})
	toshitest.On(tt, workspacev1connect.WorkloadServiceCreateSandboxProcedure,
		func(_ context.Context, request *workspacev1.CreateSandboxRequest) (*workspacev1.CreateSandboxResponse, error) {
			if got := request.GetSpec().GetImage(); got != "dev" {
				t.Errorf("the sandbox's image is %q", got)
			}
			return &workspacev1.CreateSandboxResponse{Sandbox: &workspacev1.Sandbox{Spec: &workspacev1.SandboxSpec{
				Image: "dev", ImageVersionId: "imgv-2",
			}}}, nil
		})

	client := tt.Client(toshi.WithScope("proj", "env"))
	job, err := client.Workload().Jobs().Create(ctx, &workspacev1.CreateJobRequest{
		Spec: &workspacev1.JobSpec{Image: "build"}, RequestKey: "key-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.GetJob().GetSpec().GetImageVersionId() != "imgv-1" {
		t.Errorf("the job's image version is %q", job.GetJob().GetSpec().GetImageVersionId())
	}

	sandbox, err := client.Workload().Sandboxes().Create(ctx, &workspacev1.CreateSandboxRequest{
		Spec: &workspacev1.SandboxSpec{Image: "dev"}, RequestKey: "key-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sandbox.GetSandbox().GetSpec().GetImageVersionId() != "imgv-2" {
		t.Errorf("the sandbox's image version is %q", sandbox.GetSandbox().GetSpec().GetImageVersionId())
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

// testWebhookSecret returns a signing secret in the API's form, "whsec_" and
// the base64 of 32 bytes, built at run time so no literal of that form sits in
// the tree.
func testWebhookSecret() string {
	sum := sha256.Sum256([]byte("sdk-test-webhook-secret"))
	return "whsec_" + base64.StdEncoding.EncodeToString(sum[:])
}
