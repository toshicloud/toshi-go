package toshi

import (
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
	workspacev1connect "github.com/toshicloud/toshi-api/workspacev1/workspacev1connect"
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

func TestNewRequiresServerAndToken(t *testing.T) {
	t.Setenv("TOSHI_SERVER", "")
	t.Setenv("TOSHI_TOKEN", "")
	t.Setenv("TOSHI_TOKEN_FILE", "")
	if _, err := New(); err == nil || !strings.Contains(err.Error(), "no server") {
		t.Fatalf("no server: err %v", err)
	}
	if _, err := New(WithServer("http://127.0.0.1:1")); err == nil || !strings.Contains(err.Error(), "no token") {
		t.Fatalf("no token: err %v", err)
	}
}

func TestTokenAndServerPrecedence(t *testing.T) {
	var got atomic.Value
	server := newTestServer(t, &backend{listProjects: func(_ context.Context, req *connect.Request[workspacev1.ListProjectsRequest]) (*connect.Response[workspacev1.ListProjectsResponse], error) {
		got.Store(req.Header().Get("Authorization"))
		return connect.NewResponse(&workspacev1.ListProjectsResponse{}), nil
	}})

	call := func(client *Client) {
		t.Helper()
		if _, err := client.Project.ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{})); err != nil {
			t.Fatalf("ListProjects: %v", err)
		}
	}

	t.Run("environment", func(t *testing.T) {
		t.Setenv("TOSHI_SERVER", server.URL)
		t.Setenv("TOSHI_TOKEN", "env-token")
		t.Setenv("TOSHI_TOKEN_FILE", "")
		t.Setenv("TOSHI_CA_FILE", "")
		client, err := New(WithHTTPClient(server.Client()))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		call(client)
		if v, _ := got.Load().(string); v != "Bearer env-token" {
			t.Fatalf("Authorization %q", v)
		}
	})

	t.Run("options win", func(t *testing.T) {
		t.Setenv("TOSHI_SERVER", "http://127.0.0.1:1")
		t.Setenv("TOSHI_TOKEN", "env-token")
		t.Setenv("TOSHI_TOKEN_FILE", "")
		t.Setenv("TOSHI_CA_FILE", "")
		client, err := New(WithServer(server.URL), WithToken("option-token"), WithHTTPClient(server.Client()))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		call(client)
		if v, _ := got.Load().(string); v != "Bearer option-token" {
			t.Fatalf("Authorization %q", v)
		}
	})

	t.Run("token file option wins over environment", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(path, []byte("  file-token \n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("TOSHI_SERVER", server.URL)
		t.Setenv("TOSHI_TOKEN", "env-token")
		t.Setenv("TOSHI_TOKEN_FILE", "")
		t.Setenv("TOSHI_CA_FILE", "")
		client, err := New(WithTokenFile(path), WithHTTPClient(server.Client()))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		call(client)
		if v, _ := got.Load().(string); v != "Bearer file-token" {
			t.Fatalf("Authorization %q", v)
		}
	})

	t.Run("token file from environment", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(path, []byte("env-file-token\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("TOSHI_SERVER", server.URL)
		t.Setenv("TOSHI_TOKEN", "")
		t.Setenv("TOSHI_TOKEN_FILE", path)
		t.Setenv("TOSHI_CA_FILE", "")
		client, err := New(WithHTTPClient(server.Client()))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		call(client)
		if v, _ := got.Load().(string); v != "Bearer env-file-token" {
			t.Fatalf("Authorization %q", v)
		}
	})

	t.Run("empty token file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(path, []byte(" \n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("TOSHI_SERVER", server.URL)
		t.Setenv("TOSHI_TOKEN", "")
		t.Setenv("TOSHI_TOKEN_FILE", "")
		t.Setenv("TOSHI_CA_FILE", "")
		if _, err := New(WithTokenFile(path), WithHTTPClient(server.Client())); err == nil {
			t.Fatal("empty token file accepted")
		}
	})
}

func TestCAFileTrustsServer(t *testing.T) {
	server := newTestServer(t, &backend{listProjects: func(context.Context, *connect.Request[workspacev1.ListProjectsRequest]) (*connect.Response[workspacev1.ListProjectsResponse], error) {
		return connect.NewResponse(&workspacev1.ListProjectsResponse{}), nil
	}})
	path := filepath.Join(t.TempDir(), "ca.pem")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(file, &pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	t.Setenv("TOSHI_CA_FILE", "")
	client, err := New(WithServer(server.URL), WithToken("test-token"), WithCAFile(path), WithRetryPolicy(fastPolicy()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := client.Project.ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{})); err != nil {
		t.Fatalf("ListProjects over the CA file: %v", err)
	}

	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(WithServer(server.URL), WithToken("t"), WithCAFile(empty)); err == nil {
		t.Fatal("CA file without a certificate accepted")
	}
}

func TestHTTPClientAndCAFileConflict(t *testing.T) {
	server := newTestServer(t, &backend{})
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TOSHI_CA_FILE", "")
	if _, err := New(WithServer(server.URL), WithToken("t"), WithHTTPClient(server.Client()), WithCAFile(path)); err == nil {
		t.Fatal("WithHTTPClient and WithCAFile accepted together")
	}
	t.Setenv("TOSHI_CA_FILE", path)
	if _, err := New(WithServer(server.URL), WithToken("t"), WithHTTPClient(server.Client())); err == nil {
		t.Fatal("WithHTTPClient and TOSHI_CA_FILE accepted together")
	}
}

func TestBearerOnUnaryAndStreams(t *testing.T) {
	var unary, stream atomic.Value
	server := newTestServer(t, &backend{
		listProjects: func(_ context.Context, req *connect.Request[workspacev1.ListProjectsRequest]) (*connect.Response[workspacev1.ListProjectsResponse], error) {
			unary.Store(req.Header().Get("Authorization") + "\n" + req.Header().Get("Toshi-Tenant"))
			return connect.NewResponse(&workspacev1.ListProjectsResponse{}), nil
		},
		watchOutput: func(_ context.Context, req *connect.Request[workspacev1.WatchOutputRequest], s *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
			stream.Store(req.Header().Get("Authorization") + "\n" + req.Header().Get("Toshi-Tenant"))
			return s.Send(&workspacev1.WatchOutputResponse{Message: &workspacev1.WatchOutputResponse_End{End: &workspacev1.OutputEnd{}}})
		},
	})
	client := newTestClient(t, server)
	ctx := context.Background()
	if _, err := client.Project.ListProjects(ctx, connect.NewRequest(&workspacev1.ListProjectsRequest{})); err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	watch, err := client.Workload.Jobs.WatchOutput(ctx, &workspacev1.WatchOutputRequest{})
	if err != nil {
		t.Fatalf("WatchOutput: %v", err)
	}
	if !watch.Receive() {
		t.Fatalf("WatchOutput ended: %v", watch.Err())
	}
	for _, got := range []atomic.Value{unary, stream} {
		if v, _ := got.Load().(string); v != "Bearer test-token\n" {
			t.Fatalf("headers %q, want bearer and no tenant", v)
		}
	}
}

// TestClientFieldsCallTheirServices proves each field of Client reaches its
// own service.
func TestClientFieldsCallTheirServices(t *testing.T) {
	var seen atomic.Int32
	count := func() { seen.Add(1) }
	server := newTestServer(t, &backend{
		createJob: func(context.Context, *connect.Request[workspacev1.CreateJobRequest]) (*connect.Response[workspacev1.CreateJobResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.CreateJobResponse{Job: &workspacev1.Job{Id: "job-1"}}), nil
		},
		listProjects: func(context.Context, *connect.Request[workspacev1.ListProjectsRequest]) (*connect.Response[workspacev1.ListProjectsResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListProjectsResponse{}), nil
		},
		putSecret: func(context.Context, *connect.Request[workspacev1.PutSecretRequest]) (*connect.Response[workspacev1.PutSecretResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.PutSecretResponse{}), nil
		},
		listWebhookEndpoints: func(context.Context, *connect.Request[workspacev1.ListWebhookEndpointsRequest]) (*connect.Response[workspacev1.ListWebhookEndpointsResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListWebhookEndpointsResponse{}), nil
		},
		openDisplaySession: func(context.Context, *connect.Request[workspacev1.OpenDisplaySessionRequest]) (*connect.Response[workspacev1.OpenDisplaySessionResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.OpenDisplaySessionResponse{}), nil
		},
		listChannels: func(context.Context, *connect.Request[workspacev1.ListChannelsRequest]) (*connect.Response[workspacev1.ListChannelsResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListChannelsResponse{}), nil
		},
		listRecordings: func(context.Context, *connect.Request[workspacev1.ListRecordingsRequest]) (*connect.Response[workspacev1.ListRecordingsResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListRecordingsResponse{}), nil
		},
		listVolumes: func(context.Context, *connect.Request[workspacev1.ListVolumesRequest]) (*connect.Response[workspacev1.ListVolumesResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListVolumesResponse{}), nil
		},
		listArtifacts: func(context.Context, *connect.Request[workspacev1.ListArtifactsRequest]) (*connect.Response[workspacev1.ListArtifactsResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListArtifactsResponse{}), nil
		},
		reserveUpload: func(context.Context, *connect.Request[workspacev1.ReserveUploadRequest]) (*connect.Response[workspacev1.ReserveUploadResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ReserveUploadResponse{}), nil
		},
		listWorkspaces: func(context.Context, *connect.Request[workspacev1.ListWorkspacesRequest]) (*connect.Response[workspacev1.ListWorkspacesResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListWorkspacesResponse{}), nil
		},
		listHostJobs: func(context.Context, *connect.Request[workspacev1.ListHostJobsRequest]) (*connect.Response[workspacev1.ListHostJobsResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListHostJobsResponse{}), nil
		},
		listHosts: func(context.Context, *connect.Request[workspacev1.ListHostsRequest]) (*connect.Response[workspacev1.ListHostsResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListHostsResponse{}), nil
		},
		listTokens: func(context.Context, *connect.Request[workspacev1.ListTokensRequest]) (*connect.Response[workspacev1.ListTokensResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListTokensResponse{}), nil
		},
		listAuditEvents: func(context.Context, *connect.Request[workspacev1.ListAuditEventsRequest]) (*connect.Response[workspacev1.ListAuditEventsResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListAuditEventsResponse{}), nil
		},
		listFillers: func(context.Context, *connect.Request[workspacev1.ListFillersRequest]) (*connect.Response[workspacev1.ListFillersResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListFillersResponse{}), nil
		},
		listStorageProviders: func(context.Context, *connect.Request[workspacev1.ListStorageProvidersRequest]) (*connect.Response[workspacev1.ListStorageProvidersResponse], error) {
			count()
			return connect.NewResponse(&workspacev1.ListStorageProvidersResponse{}), nil
		},
	})
	client := newTestClient(t, server)
	ctx := context.Background()

	check := func(name string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	_, err := client.Workload.Jobs.Create(ctx, &workspacev1.CreateJobRequest{})
	check("Workload.Jobs", err)
	_, err = client.Project.ListProjects(ctx, connect.NewRequest(&workspacev1.ListProjectsRequest{}))
	check("Project", err)
	_, err = client.Secret.PutSecret(ctx, connect.NewRequest(&workspacev1.PutSecretRequest{Name: "k", Value: []byte("v")}))
	check("Secret", err)
	_, err = client.Webhook.ListWebhookEndpoints(ctx, connect.NewRequest(&workspacev1.ListWebhookEndpointsRequest{}))
	check("Webhook", err)
	_, err = client.Display.OpenDisplaySession(ctx, connect.NewRequest(&workspacev1.OpenDisplaySessionRequest{}))
	check("Display", err)
	_, err = client.Channel.ListChannels(ctx, connect.NewRequest(&workspacev1.ListChannelsRequest{JobId: "job-1"}))
	check("Channel", err)
	_, err = client.Recording.ListRecordings(ctx, connect.NewRequest(&workspacev1.ListRecordingsRequest{WorkloadId: "job-1"}))
	check("Recording", err)
	_, err = client.Volume.ListVolumes(ctx, connect.NewRequest(&workspacev1.ListVolumesRequest{}))
	check("Volume", err)
	_, err = client.Artifact.ListArtifacts(ctx, connect.NewRequest(&workspacev1.ListArtifactsRequest{WorkloadId: "job-1"}))
	check("Artifact", err)
	_, err = client.ArtifactUpload.ReserveUpload(ctx, connect.NewRequest(&workspacev1.ReserveUploadRequest{}))
	check("ArtifactUpload", err)
	_, err = client.Workspace.ListWorkspaces(ctx, connect.NewRequest(&workspacev1.ListWorkspacesRequest{}))
	check("Workspace", err)
	_, err = client.HostJob.ListHostJobs(ctx, connect.NewRequest(&workspacev1.ListHostJobsRequest{}))
	check("HostJob", err)
	_, err = client.Host.ListHosts(ctx, connect.NewRequest(&workspacev1.ListHostsRequest{}))
	check("Host", err)
	_, err = client.Token.ListTokens(ctx, connect.NewRequest(&workspacev1.ListTokensRequest{}))
	check("Token", err)
	_, err = client.AuditEvent.ListAuditEvents(ctx, connect.NewRequest(&workspacev1.ListAuditEventsRequest{}))
	check("AuditEvent", err)
	_, err = client.Filler.ListFillers(ctx, connect.NewRequest(&workspacev1.ListFillersRequest{}))
	check("Filler", err)
	_, err = client.StorageProvider.ListStorageProviders(ctx, connect.NewRequest(&workspacev1.ListStorageProvidersRequest{}))
	check("StorageProvider", err)
	if seen.Load() != 17 {
		t.Fatalf("%d services answered, want 17", seen.Load())
	}
}

func TestScopeFilling(t *testing.T) {
	var created *workspacev1.CreateJobRequest
	var secret *workspacev1.PutSecretRequest
	server := newTestServer(t, &backend{
		createJob: func(_ context.Context, req *connect.Request[workspacev1.CreateJobRequest]) (*connect.Response[workspacev1.CreateJobResponse], error) {
			created = proto.Clone(req.Msg).(*workspacev1.CreateJobRequest)
			return connect.NewResponse(&workspacev1.CreateJobResponse{}), nil
		},
		putSecret: func(_ context.Context, req *connect.Request[workspacev1.PutSecretRequest]) (*connect.Response[workspacev1.PutSecretResponse], error) {
			secret = proto.Clone(req.Msg).(*workspacev1.PutSecretRequest)
			return connect.NewResponse(&workspacev1.PutSecretResponse{}), nil
		},
	})
	ctx := context.Background()

	t.Run("workload", func(t *testing.T) {
		client := newTestClient(t, server, WithScope("proj", "env"))
		if _, err := client.Workload.Jobs.Create(ctx, &workspacev1.CreateJobRequest{}); err != nil {
			t.Fatal(err)
		}
		if created.GetProject() != "proj" || created.GetEnvironment() != "env" {
			t.Fatalf("scope %q/%q", created.GetProject(), created.GetEnvironment())
		}
	})

	t.Run("set values stay", func(t *testing.T) {
		client := newTestClient(t, server, WithScope("proj", "env"))
		if _, err := client.Workload.Jobs.Create(ctx, &workspacev1.CreateJobRequest{Project: "p2", Environment: "e2"}); err != nil {
			t.Fatal(err)
		}
		if created.GetProject() != "p2" || created.GetEnvironment() != "e2" {
			t.Fatalf("scope %q/%q", created.GetProject(), created.GetEnvironment())
		}
	})

	t.Run("no scope fills nothing", func(t *testing.T) {
		client := newTestClient(t, server)
		if _, err := client.Workload.Jobs.Create(ctx, &workspacev1.CreateJobRequest{}); err != nil {
			t.Fatal(err)
		}
		if created.GetProject() != "" || created.GetEnvironment() != "" {
			t.Fatalf("scope %q/%q", created.GetProject(), created.GetEnvironment())
		}
	})

	t.Run("shared secret keeps environment empty", func(t *testing.T) {
		client := newTestClient(t, server, WithScope("proj", "env"))
		if _, err := client.Secret.PutSecret(ctx, connect.NewRequest(&workspacev1.PutSecretRequest{Shared: true, Name: "k", Value: []byte("v")})); err != nil {
			t.Fatal(err)
		}
		if secret.GetProject() != "proj" || secret.GetEnvironment() != "" {
			t.Fatalf("scope %q/%q", secret.GetProject(), secret.GetEnvironment())
		}
	})

	t.Run("environment secret", func(t *testing.T) {
		client := newTestClient(t, server, WithScope("proj", "env"))
		if _, err := client.Secret.PutSecret(ctx, connect.NewRequest(&workspacev1.PutSecretRequest{Name: "k", Value: []byte("v")})); err != nil {
			t.Fatal(err)
		}
		if secret.GetEnvironment() != "env" {
			t.Fatalf("environment %q", secret.GetEnvironment())
		}
	})

	t.Run("both set pass through", func(t *testing.T) {
		client := newTestClient(t, server, WithScope("proj", "env"))
		if _, err := client.Secret.PutSecret(ctx, connect.NewRequest(&workspacev1.PutSecretRequest{Shared: true, Environment: "e3", Name: "k", Value: []byte("v")})); err != nil {
			t.Fatal(err)
		}
		if secret.GetEnvironment() != "e3" {
			t.Fatalf("environment %q", secret.GetEnvironment())
		}
	})
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
		workspacev1.File_toshi_workspace_v1_artifacts_proto,
		workspacev1.File_toshi_workspace_v1_audit_events_proto,
		workspacev1.File_toshi_workspace_v1_fillers_proto,
		workspacev1.File_toshi_workspace_v1_hostjobs_proto,
		workspacev1.File_toshi_workspace_v1_hosts_proto,
		workspacev1.File_toshi_workspace_v1_storage_providers_proto,
		workspacev1.File_toshi_workspace_v1_tokens_proto,
		workspacev1.File_toshi_workspace_v1_workspace_service_proto,
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
	if seen != 150 {
		t.Errorf("checked %d procedures, want 150", seen)
	}
}

func TestReadRetriedOnUnavailable(t *testing.T) {
	var calls atomic.Int32
	server := newTestServer(t, &backend{getJob: func(_ context.Context, _ *connect.Request[workspacev1.GetJobRequest]) (*connect.Response[workspacev1.GetJobResponse], error) {
		if calls.Add(1) < 3 {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
		}
		return connect.NewResponse(&workspacev1.GetJobResponse{Job: &workspacev1.Job{Id: "job-1"}}), nil
	}})
	client := newTestClient(t, server)
	resp, err := client.Workload.Jobs.Get(context.Background(), &workspacev1.GetJobRequest{})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if resp.GetJob().GetId() != "job-1" || calls.Load() != 3 {
		t.Fatalf("job %q after %d calls", resp.GetJob().GetId(), calls.Load())
	}
}

func TestErrorsAreNotRetried(t *testing.T) {
	for _, code := range []connect.Code{
		connect.CodeResourceExhausted,
		connect.CodeFailedPrecondition,
		connect.CodeInvalidArgument,
		connect.CodePermissionDenied,
		connect.CodeNotFound,
	} {
		t.Run(code.String(), func(t *testing.T) {
			var calls atomic.Int32
			server := newTestServer(t, &backend{getJob: func(context.Context, *connect.Request[workspacev1.GetJobRequest]) (*connect.Response[workspacev1.GetJobResponse], error) {
				calls.Add(1)
				return nil, connect.NewError(code, errors.New("refused"))
			}})
			client := newTestClient(t, server)
			_, err := client.Workload.Jobs.Get(context.Background(), &workspacev1.GetJobRequest{})
			if connect.CodeOf(err) != code || calls.Load() != 1 {
				t.Fatalf("code %v after %d calls", connect.CodeOf(err), calls.Load())
			}
		})
	}
}

func TestNoneClassIsNotRetried(t *testing.T) {
	var displays, secrets atomic.Int32
	server := newTestServer(t, &backend{
		openDisplaySession: func(context.Context, *connect.Request[workspacev1.OpenDisplaySessionRequest]) (*connect.Response[workspacev1.OpenDisplaySessionResponse], error) {
			displays.Add(1)
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
		},
		putSecret: func(context.Context, *connect.Request[workspacev1.PutSecretRequest]) (*connect.Response[workspacev1.PutSecretResponse], error) {
			secrets.Add(1)
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
		},
	})
	client := newTestClient(t, server, WithScope("proj", "env"))
	if _, err := client.Display.OpenDisplaySession(context.Background(), connect.NewRequest(&workspacev1.OpenDisplaySessionRequest{})); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("open display: %v", err)
	}
	if _, err := client.Secret.PutSecret(context.Background(), connect.NewRequest(&workspacev1.PutSecretRequest{Name: "k", Value: []byte("v")})); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("put secret: %v", err)
	}
	if displays.Load() != 1 || secrets.Load() != 1 {
		t.Fatalf("calls display=%d secret=%d, want one each", displays.Load(), secrets.Load())
	}
}

func TestKeyedWriteFillsAndKeepsKey(t *testing.T) {
	var keys []string
	var calls atomic.Int32
	server := newTestServer(t, &backend{
		createJob: func(_ context.Context, req *connect.Request[workspacev1.CreateJobRequest]) (*connect.Response[workspacev1.CreateJobResponse], error) {
			keys = append(keys, req.Msg.GetRequestKey())
			if calls.Add(1) == 1 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
			}
			return connect.NewResponse(&workspacev1.CreateJobResponse{}), nil
		},
		updateJob: func(_ context.Context, req *connect.Request[workspacev1.UpdateJobRequest]) (*connect.Response[workspacev1.UpdateJobResponse], error) {
			keys = append(keys, req.Msg.GetRequestKey())
			if calls.Add(1) == 3 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
			}
			return connect.NewResponse(&workspacev1.UpdateJobResponse{}), nil
		},
	})
	client := newTestClient(t, server, WithScope("proj", "env"))
	create := &workspacev1.CreateJobRequest{}
	if _, err := client.Workload.Jobs.Create(context.Background(), create); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("keys %v", keys)
	}
	if create.GetRequestKey() != keys[0] {
		t.Fatalf("caller's key %q, want %q", create.GetRequestKey(), keys[0])
	}
	if len(keys[0]) != 32 {
		t.Fatalf("key %q is not 32 hex characters", keys[0])
	}

	keys = nil
	calls.Store(2)
	update := &workspacev1.UpdateJobRequest{RequestKey: "given-key"}
	if _, err := client.Workload.Jobs.Update(context.Background(), update); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if len(keys) != 2 || keys[0] != "given-key" || keys[1] != "given-key" || update.GetRequestKey() != "given-key" {
		t.Fatalf("keys %v, request %q", keys, update.GetRequestKey())
	}
}

func TestBackoffStopsOnContextEnd(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := newTestServer(t, &backend{getJob: func(context.Context, *connect.Request[workspacev1.GetJobRequest]) (*connect.Response[workspacev1.GetJobResponse], error) {
		calls.Add(1)
		// The caller gives up during the first try: the retry's backoff
		// would wait an hour, so the retry answers with the context's error.
		cancel()
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
	}})
	client := newTestClient(t, server, WithRetryPolicy(RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Hour, MaxBackoff: time.Hour}))
	_, err := client.Workload.Jobs.Get(ctx, &workspacev1.GetJobRequest{})
	if connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("err %v, want canceled", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("%d calls, want 1", calls.Load())
	}
}

func TestWaitJobAsksAgain(t *testing.T) {
	var calls atomic.Int32
	server := newTestServer(t, &backend{waitJob: func(context.Context, *connect.Request[workspacev1.WaitJobRequest]) (*connect.Response[workspacev1.WaitJobResponse], error) {
		if calls.Add(1) < 3 {
			return connect.NewResponse(&workspacev1.WaitJobResponse{Reached: false}), nil
		}
		return connect.NewResponse(&workspacev1.WaitJobResponse{Reached: true, Job: &workspacev1.Job{Id: "job-1"}}), nil
	}})
	client := newTestClient(t, server)
	resp, err := client.Workload.Jobs.Wait(context.Background(), &workspacev1.WaitJobRequest{})
	if err != nil {
		t.Fatalf("WaitJob: %v", err)
	}
	if resp.GetJob().GetId() != "job-1" || calls.Load() != 3 {
		t.Fatalf("job %q after %d calls", resp.GetJob().GetId(), calls.Load())
	}
}

func TestWaitJobStopsOnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	server := newTestServer(t, &backend{waitJob: func(context.Context, *connect.Request[workspacev1.WaitJobRequest]) (*connect.Response[workspacev1.WaitJobResponse], error) {
		if calls.Add(1) == 3 {
			cancel()
		}
		return connect.NewResponse(&workspacev1.WaitJobResponse{Reached: false}), nil
	}})
	client := newTestClient(t, server)
	_, err := client.Workload.Jobs.Wait(ctx, &workspacev1.WaitJobRequest{})
	if connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("err %v, want canceled", err)
	}
}

func TestWaitOperationAsksAgain(t *testing.T) {
	var calls atomic.Int32
	server := newTestServer(t, &backend{waitOperation: func(context.Context, *connect.Request[workspacev1.WaitOperationRequest]) (*connect.Response[workspacev1.WaitOperationResponse], error) {
		if calls.Add(1) < 3 {
			return connect.NewResponse(&workspacev1.WaitOperationResponse{Operation: &workspacev1.Operation{Done: false}}), nil
		}
		return connect.NewResponse(&workspacev1.WaitOperationResponse{Operation: &workspacev1.Operation{Id: "op1", Done: true}}), nil
	}})
	client := newTestClient(t, server)
	op, err := client.Workload.WaitOperation(context.Background(), &workspacev1.WaitOperationRequest{})
	if err != nil {
		t.Fatalf("WaitOperation: %v", err)
	}
	if op.GetId() != "op1" || calls.Load() != 3 {
		t.Fatalf("operation %q after %d calls", op.GetId(), calls.Load())
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	policy := DefaultRetryPolicy()
	if policy.MaxAttempts != 5 || policy.InitialBackoff != 200*time.Millisecond || policy.MaxBackoff != 10*time.Second {
		t.Fatalf("policy %+v", policy)
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
