package toshi

import (
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoreflect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"
)

// backend serves the ten services from hooks; an unhooked call is
// UNIMPLEMENTED.
type backend struct {
	workspacev1connect.UnimplementedWorkloadServiceHandler
	workspacev1connect.UnimplementedProjectServiceHandler
	workspacev1connect.UnimplementedSecretServiceHandler
	workspacev1connect.UnimplementedWebhookServiceHandler
	workspacev1connect.UnimplementedDisplayServiceHandler
	workspacev1connect.UnimplementedChannelServiceHandler
	workspacev1connect.UnimplementedRecordingServiceHandler
	workspacev1connect.UnimplementedVolumeServiceHandler
	workspacev1connect.UnimplementedArtifactServiceHandler
	workspacev1connect.UnimplementedArtifactUploadServiceHandler
	workspacev1connect.UnimplementedWorkspaceServiceHandler
	workspacev1connect.UnimplementedHostJobServiceHandler
	workspacev1connect.UnimplementedHostServiceHandler
	workspacev1connect.UnimplementedTokenServiceHandler
	workspacev1connect.UnimplementedAuditEventServiceHandler
	workspacev1connect.UnimplementedFillerServiceHandler
	workspacev1connect.UnimplementedStorageProviderServiceHandler

	createJob            func(context.Context, *connect.Request[workspacev1.CreateJobRequest]) (*connect.Response[workspacev1.CreateJobResponse], error)
	getJob               func(context.Context, *connect.Request[workspacev1.GetJobRequest]) (*connect.Response[workspacev1.GetJobResponse], error)
	listJobs             func(context.Context, *connect.Request[workspacev1.ListJobsRequest]) (*connect.Response[workspacev1.ListJobsResponse], error)
	updateJob            func(context.Context, *connect.Request[workspacev1.UpdateJobRequest]) (*connect.Response[workspacev1.UpdateJobResponse], error)
	waitJob              func(context.Context, *connect.Request[workspacev1.WaitJobRequest]) (*connect.Response[workspacev1.WaitJobResponse], error)
	waitOperation        func(context.Context, *connect.Request[workspacev1.WaitOperationRequest]) (*connect.Response[workspacev1.WaitOperationResponse], error)
	listEvents           func(context.Context, *connect.Request[workspacev1.ListEventsRequest]) (*connect.Response[workspacev1.ListEventsResponse], error)
	listProjects         func(context.Context, *connect.Request[workspacev1.ListProjectsRequest]) (*connect.Response[workspacev1.ListProjectsResponse], error)
	putSecret            func(context.Context, *connect.Request[workspacev1.PutSecretRequest]) (*connect.Response[workspacev1.PutSecretResponse], error)
	listWebhookEndpoints func(context.Context, *connect.Request[workspacev1.ListWebhookEndpointsRequest]) (*connect.Response[workspacev1.ListWebhookEndpointsResponse], error)
	openDisplaySession   func(context.Context, *connect.Request[workspacev1.OpenDisplaySessionRequest]) (*connect.Response[workspacev1.OpenDisplaySessionResponse], error)
	listChannels         func(context.Context, *connect.Request[workspacev1.ListChannelsRequest]) (*connect.Response[workspacev1.ListChannelsResponse], error)
	listRecordings       func(context.Context, *connect.Request[workspacev1.ListRecordingsRequest]) (*connect.Response[workspacev1.ListRecordingsResponse], error)
	listVolumes          func(context.Context, *connect.Request[workspacev1.ListVolumesRequest]) (*connect.Response[workspacev1.ListVolumesResponse], error)
	listArtifacts        func(context.Context, *connect.Request[workspacev1.ListArtifactsRequest]) (*connect.Response[workspacev1.ListArtifactsResponse], error)
	reserveUpload        func(context.Context, *connect.Request[workspacev1.ReserveUploadRequest]) (*connect.Response[workspacev1.ReserveUploadResponse], error)
	listWorkspaces       func(context.Context, *connect.Request[workspacev1.ListWorkspacesRequest]) (*connect.Response[workspacev1.ListWorkspacesResponse], error)
	listHostJobs         func(context.Context, *connect.Request[workspacev1.ListHostJobsRequest]) (*connect.Response[workspacev1.ListHostJobsResponse], error)
	listHosts            func(context.Context, *connect.Request[workspacev1.ListHostsRequest]) (*connect.Response[workspacev1.ListHostsResponse], error)
	listTokens           func(context.Context, *connect.Request[workspacev1.ListTokensRequest]) (*connect.Response[workspacev1.ListTokensResponse], error)
	listAuditEvents      func(context.Context, *connect.Request[workspacev1.ListAuditEventsRequest]) (*connect.Response[workspacev1.ListAuditEventsResponse], error)
	listFillers          func(context.Context, *connect.Request[workspacev1.ListFillersRequest]) (*connect.Response[workspacev1.ListFillersResponse], error)
	listStorageProviders func(context.Context, *connect.Request[workspacev1.ListStorageProvidersRequest]) (*connect.Response[workspacev1.ListStorageProvidersResponse], error)
	watchJobs            func(context.Context, *connect.Request[workspacev1.WatchJobsRequest], *connect.ServerStream[workspacev1.WatchJobsResponse]) error
	watchEvents          func(context.Context, *connect.Request[workspacev1.WatchEventsRequest], *connect.ServerStream[workspacev1.WatchEventsResponse]) error
	watchOutput          func(context.Context, *connect.Request[workspacev1.WatchOutputRequest], *connect.ServerStream[workspacev1.WatchOutputResponse]) error
	watchTelemetry       func(context.Context, *connect.Request[workspacev1.WatchTelemetryRequest], *connect.ServerStream[workspacev1.WatchTelemetryResponse]) error
	execute              func(context.Context, *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error
}

func (b *backend) CreateJob(ctx context.Context, req *connect.Request[workspacev1.CreateJobRequest]) (*connect.Response[workspacev1.CreateJobResponse], error) {
	if b.createJob != nil {
		return b.createJob(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) GetJob(ctx context.Context, req *connect.Request[workspacev1.GetJobRequest]) (*connect.Response[workspacev1.GetJobResponse], error) {
	if b.getJob != nil {
		return b.getJob(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListJobs(ctx context.Context, req *connect.Request[workspacev1.ListJobsRequest]) (*connect.Response[workspacev1.ListJobsResponse], error) {
	if b.listJobs != nil {
		return b.listJobs(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) UpdateJob(ctx context.Context, req *connect.Request[workspacev1.UpdateJobRequest]) (*connect.Response[workspacev1.UpdateJobResponse], error) {
	if b.updateJob != nil {
		return b.updateJob(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) WaitJob(ctx context.Context, req *connect.Request[workspacev1.WaitJobRequest]) (*connect.Response[workspacev1.WaitJobResponse], error) {
	if b.waitJob != nil {
		return b.waitJob(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) WaitOperation(ctx context.Context, req *connect.Request[workspacev1.WaitOperationRequest]) (*connect.Response[workspacev1.WaitOperationResponse], error) {
	if b.waitOperation != nil {
		return b.waitOperation(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListEvents(ctx context.Context, req *connect.Request[workspacev1.ListEventsRequest]) (*connect.Response[workspacev1.ListEventsResponse], error) {
	if b.listEvents != nil {
		return b.listEvents(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListProjects(ctx context.Context, req *connect.Request[workspacev1.ListProjectsRequest]) (*connect.Response[workspacev1.ListProjectsResponse], error) {
	if b.listProjects != nil {
		return b.listProjects(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) PutSecret(ctx context.Context, req *connect.Request[workspacev1.PutSecretRequest]) (*connect.Response[workspacev1.PutSecretResponse], error) {
	if b.putSecret != nil {
		return b.putSecret(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListWebhookEndpoints(ctx context.Context, req *connect.Request[workspacev1.ListWebhookEndpointsRequest]) (*connect.Response[workspacev1.ListWebhookEndpointsResponse], error) {
	if b.listWebhookEndpoints != nil {
		return b.listWebhookEndpoints(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) OpenDisplaySession(ctx context.Context, req *connect.Request[workspacev1.OpenDisplaySessionRequest]) (*connect.Response[workspacev1.OpenDisplaySessionResponse], error) {
	if b.openDisplaySession != nil {
		return b.openDisplaySession(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListChannels(ctx context.Context, req *connect.Request[workspacev1.ListChannelsRequest]) (*connect.Response[workspacev1.ListChannelsResponse], error) {
	if b.listChannels != nil {
		return b.listChannels(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListRecordings(ctx context.Context, req *connect.Request[workspacev1.ListRecordingsRequest]) (*connect.Response[workspacev1.ListRecordingsResponse], error) {
	if b.listRecordings != nil {
		return b.listRecordings(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListVolumes(ctx context.Context, req *connect.Request[workspacev1.ListVolumesRequest]) (*connect.Response[workspacev1.ListVolumesResponse], error) {
	if b.listVolumes != nil {
		return b.listVolumes(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListArtifacts(ctx context.Context, req *connect.Request[workspacev1.ListArtifactsRequest]) (*connect.Response[workspacev1.ListArtifactsResponse], error) {
	if b.listArtifacts != nil {
		return b.listArtifacts(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ReserveUpload(ctx context.Context, req *connect.Request[workspacev1.ReserveUploadRequest]) (*connect.Response[workspacev1.ReserveUploadResponse], error) {
	if b.reserveUpload != nil {
		return b.reserveUpload(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListWorkspaces(ctx context.Context, req *connect.Request[workspacev1.ListWorkspacesRequest]) (*connect.Response[workspacev1.ListWorkspacesResponse], error) {
	if b.listWorkspaces != nil {
		return b.listWorkspaces(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListHostJobs(ctx context.Context, req *connect.Request[workspacev1.ListHostJobsRequest]) (*connect.Response[workspacev1.ListHostJobsResponse], error) {
	if b.listHostJobs != nil {
		return b.listHostJobs(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListHosts(ctx context.Context, req *connect.Request[workspacev1.ListHostsRequest]) (*connect.Response[workspacev1.ListHostsResponse], error) {
	if b.listHosts != nil {
		return b.listHosts(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListTokens(ctx context.Context, req *connect.Request[workspacev1.ListTokensRequest]) (*connect.Response[workspacev1.ListTokensResponse], error) {
	if b.listTokens != nil {
		return b.listTokens(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListAuditEvents(ctx context.Context, req *connect.Request[workspacev1.ListAuditEventsRequest]) (*connect.Response[workspacev1.ListAuditEventsResponse], error) {
	if b.listAuditEvents != nil {
		return b.listAuditEvents(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListFillers(ctx context.Context, req *connect.Request[workspacev1.ListFillersRequest]) (*connect.Response[workspacev1.ListFillersResponse], error) {
	if b.listFillers != nil {
		return b.listFillers(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListStorageProviders(ctx context.Context, req *connect.Request[workspacev1.ListStorageProvidersRequest]) (*connect.Response[workspacev1.ListStorageProvidersResponse], error) {
	if b.listStorageProviders != nil {
		return b.listStorageProviders(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) WatchJobs(ctx context.Context, req *connect.Request[workspacev1.WatchJobsRequest], stream *connect.ServerStream[workspacev1.WatchJobsResponse]) error {
	if b.watchJobs != nil {
		return b.watchJobs(ctx, req, stream)
	}
	return unimplemented()
}

func (b *backend) WatchEvents(ctx context.Context, req *connect.Request[workspacev1.WatchEventsRequest], stream *connect.ServerStream[workspacev1.WatchEventsResponse]) error {
	if b.watchEvents != nil {
		return b.watchEvents(ctx, req, stream)
	}
	return unimplemented()
}

func (b *backend) WatchOutput(ctx context.Context, req *connect.Request[workspacev1.WatchOutputRequest], stream *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
	if b.watchOutput != nil {
		return b.watchOutput(ctx, req, stream)
	}
	return unimplemented()
}

func (b *backend) WatchTelemetry(ctx context.Context, req *connect.Request[workspacev1.WatchTelemetryRequest], stream *connect.ServerStream[workspacev1.WatchTelemetryResponse]) error {
	if b.watchTelemetry != nil {
		return b.watchTelemetry(ctx, req, stream)
	}
	return unimplemented()
}

func (b *backend) Execute(ctx context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
	if b.execute != nil {
		return b.execute(ctx, stream)
	}
	return unimplemented()
}

func unimplemented() error {
	return connect.NewError(connect.CodeUnimplemented, errors.New("not implemented in this test"))
}

// testServer mounts the ten services for one backend over TLS with HTTP/2.
type testServer struct {
	*httptest.Server
	backend *backend
}

func newTestServer(t *testing.T, b *backend) *testServer {
	t.Helper()
	mux := http.NewServeMux()
	for _, mount := range []func() (string, http.Handler){
		func() (string, http.Handler) { return workspacev1connect.NewWorkloadServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewProjectServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewSecretServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewWebhookServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewDisplayServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewChannelServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewRecordingServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewVolumeServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewArtifactServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewArtifactUploadServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewWorkspaceServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewHostJobServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewHostServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewTokenServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewAuditEventServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewFillerServiceHandler(b) },
		func() (string, http.Handler) { return workspacev1connect.NewStorageProviderServiceHandler(b) },
	} {
		path, handler := mount()
		mux.Handle(path, handler)
	}
	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	return &testServer{Server: server, backend: b}
}

// fastPolicy keeps a retrying test quick; the backoff is a millisecond.
func fastPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 4, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}
}

// newTestClient builds a client for server with a token and a fast retry
// policy, clearing TOSHI_CA_FILE so the HTTP client option is usable.
func newTestClient(t *testing.T, server *testServer, opts ...Option) *Client {
	t.Helper()
	t.Setenv("TOSHI_CA_FILE", "")
	base := []Option{
		WithServer(server.URL),
		WithToken("test-token"),
		WithHTTPClient(server.Client()),
		WithRetryPolicy(fastPolicy()),
	}
	client, err := New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return client
}

func TestRetryMapCoversEveryProcedure(t *testing.T) {
	files := []protoreflect.FileDescriptor{
		workspacev1.File_toshi_workspace_v1_workspace_proto,
		workspacev1.File_toshi_workspace_v1_workloads_proto,
		workspacev1.File_toshi_workspace_v1_secrets_proto,
		workspacev1.File_toshi_workspace_v1_projects_proto,
		workspacev1.File_toshi_workspace_v1_webhook_proto,
		workspacev1.File_toshi_workspace_v1_display_proto,
		workspacev1.File_toshi_workspace_v1_channels_proto,
		workspacev1.File_toshi_workspace_v1_recordings_proto,
		workspacev1.File_toshi_workspace_v1_volumes_proto,
		workspacev1.File_toshi_workspace_v1_account_proto,
		workspacev1.File_toshi_workspace_v1_artifacts_proto,
		workspacev1.File_toshi_workspace_v1_audit_events_proto,
		workspacev1.File_toshi_workspace_v1_fillers_proto,
		workspacev1.File_toshi_workspace_v1_hostjobs_proto,
		workspacev1.File_toshi_workspace_v1_hosts_proto,
		workspacev1.File_toshi_workspace_v1_storage_providers_proto,
		workspacev1.File_toshi_workspace_v1_tokens_proto,
		workspacev1.File_toshi_workspace_v1_workspace_service_proto,
		workspacev1.File_toshi_workspace_v1_products_proto,
		workspacev1.File_toshi_workspace_v1_discovery_proto,
		workspacev1.File_toshi_workspace_v1_connections_proto,
		workspacev1.File_toshi_workspace_v1_images_proto,
		workspacev1.File_toshi_workspace_v1_roles_proto,
		workspacev1.File_toshi_workspace_v1_triggers_proto,
		workspacev1.File_toshi_workspace_v1_grants_proto,
		workspacev1.File_toshi_workspace_v1_domains_proto,
	}
	seen := 0
	for _, file := range files {
		services := file.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			methods := service.Methods()
			for j := 0; j < methods.Len(); j++ {
				procedure := "/" + string(service.FullName()) + "/" + string(methods.Get(j).Name())
				if _, ok := procedureRetryClass[procedure]; !ok {
					t.Errorf("%s is missing from procedureRetryClass", procedure)
				}
				seen++
			}
		}
	}
	if seen != 230 {
		t.Errorf("checked %d procedures, want 230", seen)
	}
}

func TestRootCAs(t *testing.T) {
	server := newTestServer(t, &backend{})
	path := filepath.Join(t.TempDir(), "ca.pem")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(file, &pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	pool, err := rootCAs(path)
	if err != nil {
		t.Fatalf("rootCAs: %v", err)
	}
	if pool == nil {
		t.Fatal("no pool")
	}
}
