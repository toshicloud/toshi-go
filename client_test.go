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

// backend serves the five services from hooks; an unhooked call is
// UNIMPLEMENTED.
type backend struct {
	workspacev1connect.UnimplementedWorkspaceServiceHandler
	workspacev1connect.UnimplementedProjectServiceHandler
	workspacev1connect.UnimplementedSecretServiceHandler
	workspacev1connect.UnimplementedWebhookServiceHandler
	workspacev1connect.UnimplementedDisplayServiceHandler

	createWorkload     func(context.Context, *connect.Request[workspacev1.CreateWorkloadRequest]) (*connect.Response[workspacev1.CreateWorkloadResponse], error)
	getWorkload        func(context.Context, *connect.Request[workspacev1.GetWorkloadRequest]) (*connect.Response[workspacev1.GetWorkloadResponse], error)
	listWorkloads      func(context.Context, *connect.Request[workspacev1.ListWorkloadsRequest]) (*connect.Response[workspacev1.ListWorkloadsResponse], error)
	patchWorkload      func(context.Context, *connect.Request[workspacev1.PatchWorkloadRequest]) (*connect.Response[workspacev1.PatchWorkloadResponse], error)
	waitWorkload       func(context.Context, *connect.Request[workspacev1.WaitWorkloadRequest]) (*connect.Response[workspacev1.WaitWorkloadResponse], error)
	waitOperation      func(context.Context, *connect.Request[workspacev1.WaitOperationRequest]) (*connect.Response[workspacev1.WaitOperationResponse], error)
	listEvents         func(context.Context, *connect.Request[workspacev1.ListEventsRequest]) (*connect.Response[workspacev1.ListEventsResponse], error)
	listProjects       func(context.Context, *connect.Request[workspacev1.ListProjectsRequest]) (*connect.Response[workspacev1.ListProjectsResponse], error)
	setSecret          func(context.Context, *connect.Request[workspacev1.SetSecretRequest]) (*connect.Response[workspacev1.SetSecretResponse], error)
	openDisplaySession func(context.Context, *connect.Request[workspacev1.OpenDisplaySessionRequest]) (*connect.Response[workspacev1.OpenDisplaySessionResponse], error)
	watchWorkloads     func(context.Context, *connect.Request[workspacev1.WatchWorkloadsRequest], *connect.ServerStream[workspacev1.WatchWorkloadsResponse]) error
	watchEvents        func(context.Context, *connect.Request[workspacev1.WatchEventsRequest], *connect.ServerStream[workspacev1.WatchEventsResponse]) error
	watchOutput        func(context.Context, *connect.Request[workspacev1.WatchOutputRequest], *connect.ServerStream[workspacev1.WatchOutputResponse]) error
	execute            func(context.Context, *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error
}

func (b *backend) CreateWorkload(ctx context.Context, req *connect.Request[workspacev1.CreateWorkloadRequest]) (*connect.Response[workspacev1.CreateWorkloadResponse], error) {
	if b.createWorkload != nil {
		return b.createWorkload(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) GetWorkload(ctx context.Context, req *connect.Request[workspacev1.GetWorkloadRequest]) (*connect.Response[workspacev1.GetWorkloadResponse], error) {
	if b.getWorkload != nil {
		return b.getWorkload(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) ListWorkloads(ctx context.Context, req *connect.Request[workspacev1.ListWorkloadsRequest]) (*connect.Response[workspacev1.ListWorkloadsResponse], error) {
	if b.listWorkloads != nil {
		return b.listWorkloads(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) PatchWorkload(ctx context.Context, req *connect.Request[workspacev1.PatchWorkloadRequest]) (*connect.Response[workspacev1.PatchWorkloadResponse], error) {
	if b.patchWorkload != nil {
		return b.patchWorkload(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) WaitWorkload(ctx context.Context, req *connect.Request[workspacev1.WaitWorkloadRequest]) (*connect.Response[workspacev1.WaitWorkloadResponse], error) {
	if b.waitWorkload != nil {
		return b.waitWorkload(ctx, req)
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

func (b *backend) SetSecret(ctx context.Context, req *connect.Request[workspacev1.SetSecretRequest]) (*connect.Response[workspacev1.SetSecretResponse], error) {
	if b.setSecret != nil {
		return b.setSecret(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) OpenDisplaySession(ctx context.Context, req *connect.Request[workspacev1.OpenDisplaySessionRequest]) (*connect.Response[workspacev1.OpenDisplaySessionResponse], error) {
	if b.openDisplaySession != nil {
		return b.openDisplaySession(ctx, req)
	}
	return nil, unimplemented()
}

func (b *backend) WatchWorkloads(ctx context.Context, req *connect.Request[workspacev1.WatchWorkloadsRequest], stream *connect.ServerStream[workspacev1.WatchWorkloadsResponse]) error {
	if b.watchWorkloads != nil {
		return b.watchWorkloads(ctx, req, stream)
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

func (b *backend) Execute(ctx context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
	if b.execute != nil {
		return b.execute(ctx, stream)
	}
	return unimplemented()
}

func unimplemented() error {
	return connect.NewError(connect.CodeUnimplemented, errors.New("not implemented in this test"))
}

// testServer mounts the five services for one backend over TLS with HTTP/2.
type testServer struct {
	*httptest.Server
	backend *backend
}

func newTestServer(t *testing.T, b *backend) *testServer {
	t.Helper()
	mux := http.NewServeMux()
	path, handler := workspacev1connect.NewWorkspaceServiceHandler(b)
	mux.Handle(path, handler)
	path, handler = workspacev1connect.NewProjectServiceHandler(b)
	mux.Handle(path, handler)
	path, handler = workspacev1connect.NewSecretServiceHandler(b)
	mux.Handle(path, handler)
	path, handler = workspacev1connect.NewWebhookServiceHandler(b)
	mux.Handle(path, handler)
	path, handler = workspacev1connect.NewDisplayServiceHandler(b)
	mux.Handle(path, handler)
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
		if _, err := client.Projects.ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{})); err != nil {
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
	if _, err := client.Projects.ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{})); err != nil {
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
	if _, err := client.Projects.ListProjects(ctx, connect.NewRequest(&workspacev1.ListProjectsRequest{})); err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	watch, err := client.WatchOutput(ctx, &workspacev1.WatchOutputRequest{})
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

func TestScopeFilling(t *testing.T) {
	var created *workspacev1.CreateWorkloadRequest
	var secret *workspacev1.SetSecretRequest
	server := newTestServer(t, &backend{
		createWorkload: func(_ context.Context, req *connect.Request[workspacev1.CreateWorkloadRequest]) (*connect.Response[workspacev1.CreateWorkloadResponse], error) {
			created = proto.Clone(req.Msg).(*workspacev1.CreateWorkloadRequest)
			return connect.NewResponse(&workspacev1.CreateWorkloadResponse{}), nil
		},
		setSecret: func(_ context.Context, req *connect.Request[workspacev1.SetSecretRequest]) (*connect.Response[workspacev1.SetSecretResponse], error) {
			secret = proto.Clone(req.Msg).(*workspacev1.SetSecretRequest)
			return connect.NewResponse(&workspacev1.SetSecretResponse{}), nil
		},
	})
	ctx := context.Background()

	t.Run("workload", func(t *testing.T) {
		client := newTestClient(t, server, WithScope("proj", "env"))
		if _, err := client.Workspace.CreateWorkload(ctx, connect.NewRequest(&workspacev1.CreateWorkloadRequest{})); err != nil {
			t.Fatal(err)
		}
		if created.GetProject() != "proj" || created.GetEnvironment() != "env" {
			t.Fatalf("scope %q/%q", created.GetProject(), created.GetEnvironment())
		}
	})

	t.Run("set values stay", func(t *testing.T) {
		client := newTestClient(t, server, WithScope("proj", "env"))
		if _, err := client.Workspace.CreateWorkload(ctx, connect.NewRequest(&workspacev1.CreateWorkloadRequest{Project: "p2", Environment: "e2"})); err != nil {
			t.Fatal(err)
		}
		if created.GetProject() != "p2" || created.GetEnvironment() != "e2" {
			t.Fatalf("scope %q/%q", created.GetProject(), created.GetEnvironment())
		}
	})

	t.Run("no scope fills nothing", func(t *testing.T) {
		client := newTestClient(t, server)
		if _, err := client.Workspace.CreateWorkload(ctx, connect.NewRequest(&workspacev1.CreateWorkloadRequest{})); err != nil {
			t.Fatal(err)
		}
		if created.GetProject() != "" || created.GetEnvironment() != "" {
			t.Fatalf("scope %q/%q", created.GetProject(), created.GetEnvironment())
		}
	})

	t.Run("shared secret keeps environment empty", func(t *testing.T) {
		client := newTestClient(t, server, WithScope("proj", "env"))
		if _, err := client.Secrets.SetSecret(ctx, connect.NewRequest(&workspacev1.SetSecretRequest{Shared: true, Name: "k", Value: []byte("v")})); err != nil {
			t.Fatal(err)
		}
		if secret.GetProject() != "proj" || secret.GetEnvironment() != "" {
			t.Fatalf("scope %q/%q", secret.GetProject(), secret.GetEnvironment())
		}
	})

	t.Run("environment secret", func(t *testing.T) {
		client := newTestClient(t, server, WithScope("proj", "env"))
		if _, err := client.Secrets.SetSecret(ctx, connect.NewRequest(&workspacev1.SetSecretRequest{Name: "k", Value: []byte("v")})); err != nil {
			t.Fatal(err)
		}
		if secret.GetEnvironment() != "env" {
			t.Fatalf("environment %q", secret.GetEnvironment())
		}
	})

	t.Run("both set pass through", func(t *testing.T) {
		client := newTestClient(t, server, WithScope("proj", "env"))
		if _, err := client.Secrets.SetSecret(ctx, connect.NewRequest(&workspacev1.SetSecretRequest{Shared: true, Environment: "e3", Name: "k", Value: []byte("v")})); err != nil {
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
		workspacev1.File_toshi_workspace_v1_secrets_proto,
		workspacev1.File_toshi_workspace_v1_projects_proto,
		workspacev1.File_toshi_workspace_v1_webhook_proto,
		workspacev1.File_toshi_workspace_v1_display_proto,
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
	if seen != 56 {
		t.Errorf("checked %d procedures, want 56", seen)
	}
}

func TestReadRetriedOnUnavailable(t *testing.T) {
	var calls atomic.Int32
	server := newTestServer(t, &backend{getWorkload: func(_ context.Context, _ *connect.Request[workspacev1.GetWorkloadRequest]) (*connect.Response[workspacev1.GetWorkloadResponse], error) {
		if calls.Add(1) < 3 {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
		}
		return connect.NewResponse(&workspacev1.GetWorkloadResponse{Workload: &workspacev1.Workload{Id: "w1"}}), nil
	}})
	client := newTestClient(t, server)
	resp, err := client.Workspace.GetWorkload(context.Background(), connect.NewRequest(&workspacev1.GetWorkloadRequest{}))
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if resp.Msg.GetWorkload().GetId() != "w1" || calls.Load() != 3 {
		t.Fatalf("workload %q after %d calls", resp.Msg.GetWorkload().GetId(), calls.Load())
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
			server := newTestServer(t, &backend{getWorkload: func(context.Context, *connect.Request[workspacev1.GetWorkloadRequest]) (*connect.Response[workspacev1.GetWorkloadResponse], error) {
				calls.Add(1)
				return nil, connect.NewError(code, errors.New("refused"))
			}})
			client := newTestClient(t, server)
			_, err := client.Workspace.GetWorkload(context.Background(), connect.NewRequest(&workspacev1.GetWorkloadRequest{}))
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
		setSecret: func(context.Context, *connect.Request[workspacev1.SetSecretRequest]) (*connect.Response[workspacev1.SetSecretResponse], error) {
			secrets.Add(1)
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
		},
	})
	client := newTestClient(t, server, WithScope("proj", "env"))
	if _, err := client.Displays.OpenDisplaySession(context.Background(), connect.NewRequest(&workspacev1.OpenDisplaySessionRequest{})); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("open display: %v", err)
	}
	if _, err := client.Secrets.SetSecret(context.Background(), connect.NewRequest(&workspacev1.SetSecretRequest{Name: "k", Value: []byte("v")})); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("set secret: %v", err)
	}
	if displays.Load() != 1 || secrets.Load() != 1 {
		t.Fatalf("calls display=%d secret=%d, want one each", displays.Load(), secrets.Load())
	}
}

func TestKeyedWriteFillsAndKeepsKey(t *testing.T) {
	var keys []string
	var calls atomic.Int32
	server := newTestServer(t, &backend{
		createWorkload: func(_ context.Context, req *connect.Request[workspacev1.CreateWorkloadRequest]) (*connect.Response[workspacev1.CreateWorkloadResponse], error) {
			keys = append(keys, req.Msg.GetRequestKey())
			if calls.Add(1) == 1 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
			}
			return connect.NewResponse(&workspacev1.CreateWorkloadResponse{}), nil
		},
		patchWorkload: func(_ context.Context, req *connect.Request[workspacev1.PatchWorkloadRequest]) (*connect.Response[workspacev1.PatchWorkloadResponse], error) {
			keys = append(keys, req.Msg.GetRequestKey())
			if calls.Add(1) == 3 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
			}
			return connect.NewResponse(&workspacev1.PatchWorkloadResponse{}), nil
		},
	})
	client := newTestClient(t, server, WithScope("proj", "env"))
	create := &workspacev1.CreateWorkloadRequest{}
	if _, err := client.Workspace.CreateWorkload(context.Background(), connect.NewRequest(create)); err != nil {
		t.Fatalf("CreateWorkload: %v", err)
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
	patch := &workspacev1.PatchWorkloadRequest{RequestKey: "given-key"}
	if _, err := client.Workspace.PatchWorkload(context.Background(), connect.NewRequest(patch)); err != nil {
		t.Fatalf("PatchWorkload: %v", err)
	}
	if len(keys) != 2 || keys[0] != "given-key" || keys[1] != "given-key" || patch.GetRequestKey() != "given-key" {
		t.Fatalf("keys %v, request %q", keys, patch.GetRequestKey())
	}
}

func TestBackoffStopsOnContextEnd(t *testing.T) {
	var calls atomic.Int32
	server := newTestServer(t, &backend{getWorkload: func(context.Context, *connect.Request[workspacev1.GetWorkloadRequest]) (*connect.Response[workspacev1.GetWorkloadResponse], error) {
		calls.Add(1)
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
	}})
	client := newTestClient(t, server, WithRetryPolicy(RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Hour, MaxBackoff: time.Hour}))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := client.Workspace.GetWorkload(ctx, connect.NewRequest(&workspacev1.GetWorkloadRequest{}))
	if connect.CodeOf(err) != connect.CodeDeadlineExceeded {
		t.Fatalf("err %v, want deadline exceeded", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("%d calls, want 1", calls.Load())
	}
}

func TestWaitWorkloadAsksAgain(t *testing.T) {
	var calls atomic.Int32
	server := newTestServer(t, &backend{waitWorkload: func(context.Context, *connect.Request[workspacev1.WaitWorkloadRequest]) (*connect.Response[workspacev1.WaitWorkloadResponse], error) {
		if calls.Add(1) < 3 {
			return connect.NewResponse(&workspacev1.WaitWorkloadResponse{Reached: false}), nil
		}
		return connect.NewResponse(&workspacev1.WaitWorkloadResponse{Reached: true, Workload: &workspacev1.Workload{Id: "w1"}}), nil
	}})
	client := newTestClient(t, server)
	resp, err := client.WaitWorkload(context.Background(), &workspacev1.WaitWorkloadRequest{})
	if err != nil {
		t.Fatalf("WaitWorkload: %v", err)
	}
	if resp.GetWorkload().GetId() != "w1" || calls.Load() != 3 {
		t.Fatalf("workload %q after %d calls", resp.GetWorkload().GetId(), calls.Load())
	}
}

func TestWaitWorkloadStopsOnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	server := newTestServer(t, &backend{waitWorkload: func(context.Context, *connect.Request[workspacev1.WaitWorkloadRequest]) (*connect.Response[workspacev1.WaitWorkloadResponse], error) {
		if calls.Add(1) == 3 {
			cancel()
		}
		return connect.NewResponse(&workspacev1.WaitWorkloadResponse{Reached: false}), nil
	}})
	client := newTestClient(t, server)
	_, err := client.WaitWorkload(ctx, &workspacev1.WaitWorkloadRequest{})
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
	op, err := client.WaitOperation(context.Background(), &workspacev1.WaitOperationRequest{})
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
