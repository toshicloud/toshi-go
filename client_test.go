package toshi_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi"
	"go.toshicloud.io/toshi/toshitest"
)

// fastPolicy keeps a retrying test quick; the backoff is a millisecond.
func fastPolicy() toshi.RetryPolicy {
	return toshi.RetryPolicy{MaxAttempts: 4, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}
}

// answerListProjects answers ListProjects, which the fake has no state for.
func answerListProjects(tt *toshitest.Fake) {
	toshitest.On(tt, workspacev1connect.ProjectServiceListProjectsProcedure,
		func(context.Context, *workspacev1.ListProjectsRequest) (*workspacev1.ListProjectsResponse, error) {
			return &workspacev1.ListProjectsResponse{}, nil
		})
}

func TestNewRequiresServerAndToken(t *testing.T) {
	t.Setenv("TOSHI_SERVER", "")
	t.Setenv("TOSHI_TOKEN", "")
	t.Setenv("TOSHI_TOKEN_FILE", "")
	if _, err := toshi.New(); err == nil || !strings.Contains(err.Error(), "no server") {
		t.Fatalf("no server: err %v", err)
	}
	if _, err := toshi.New(toshi.WithServer("http://127.0.0.1:1")); err == nil || !strings.Contains(err.Error(), "no token") {
		t.Fatalf("no token: err %v", err)
	}
}

func TestTokenAndServerPrecedence(t *testing.T) {
	tt := toshitest.New(t)
	answerListProjects(tt)

	// authorization returns the header the client sent with its last call.
	authorization := func(t *testing.T, client *toshi.Client) string {
		t.Helper()
		before := len(tt.Calls())
		if _, err := client.Project.ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{})); err != nil {
			t.Fatalf("ListProjects: %v", err)
		}
		calls := tt.Calls()
		if len(calls) != before+1 {
			t.Fatalf("%d calls recorded, want %d", len(calls), before+1)
		}
		return calls[before].Header.Get("Authorization")
	}

	t.Run("environment", func(t *testing.T) {
		t.Setenv("TOSHI_SERVER", tt.URL())
		t.Setenv("TOSHI_TOKEN", "env-token")
		t.Setenv("TOSHI_TOKEN_FILE", "")
		t.Setenv("TOSHI_CA_FILE", "")
		client, err := toshi.New(toshi.WithHTTPClient(tt.HTTPClient()))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if got := authorization(t, client); got != "Bearer env-token" {
			t.Fatalf("Authorization %q", got)
		}
	})

	t.Run("options win", func(t *testing.T) {
		t.Setenv("TOSHI_SERVER", "http://127.0.0.1:1")
		t.Setenv("TOSHI_TOKEN", "env-token")
		t.Setenv("TOSHI_TOKEN_FILE", "")
		t.Setenv("TOSHI_CA_FILE", "")
		client, err := toshi.New(toshi.WithServer(tt.URL()), toshi.WithToken("option-token"), toshi.WithHTTPClient(tt.HTTPClient()))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if got := authorization(t, client); got != "Bearer option-token" {
			t.Fatalf("Authorization %q", got)
		}
	})

	t.Run("token file option wins over environment", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(path, []byte("  file-token \n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("TOSHI_SERVER", tt.URL())
		t.Setenv("TOSHI_TOKEN", "env-token")
		t.Setenv("TOSHI_TOKEN_FILE", "")
		t.Setenv("TOSHI_CA_FILE", "")
		client, err := toshi.New(toshi.WithTokenFile(path), toshi.WithHTTPClient(tt.HTTPClient()))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if got := authorization(t, client); got != "Bearer file-token" {
			t.Fatalf("Authorization %q", got)
		}
	})

	t.Run("token file from environment", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(path, []byte("env-file-token\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("TOSHI_SERVER", tt.URL())
		t.Setenv("TOSHI_TOKEN", "")
		t.Setenv("TOSHI_TOKEN_FILE", path)
		t.Setenv("TOSHI_CA_FILE", "")
		client, err := toshi.New(toshi.WithHTTPClient(tt.HTTPClient()))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if got := authorization(t, client); got != "Bearer env-file-token" {
			t.Fatalf("Authorization %q", got)
		}
	})

	t.Run("empty token file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(path, []byte(" \n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("TOSHI_SERVER", tt.URL())
		t.Setenv("TOSHI_TOKEN", "")
		t.Setenv("TOSHI_TOKEN_FILE", "")
		t.Setenv("TOSHI_CA_FILE", "")
		if _, err := toshi.New(toshi.WithTokenFile(path), toshi.WithHTTPClient(tt.HTTPClient())); err == nil {
			t.Fatal("empty token file accepted")
		}
	})
}

func TestCAFileTrustsServer(t *testing.T) {
	tt := toshitest.New(t)
	answerListProjects(tt)
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, tt.CA(), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("TOSHI_CA_FILE", "")
	client, err := toshi.New(toshi.WithServer(tt.URL()), toshi.WithToken(toshitest.Token), toshi.WithCAFile(path), toshi.WithRetryPolicy(fastPolicy()))
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
	if _, err := toshi.New(toshi.WithServer(tt.URL()), toshi.WithToken("t"), toshi.WithCAFile(empty)); err == nil {
		t.Fatal("CA file without a certificate accepted")
	}
}

func TestHTTPClientAndCAFileConflict(t *testing.T) {
	tt := toshitest.New(t)
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TOSHI_CA_FILE", "")
	if _, err := toshi.New(toshi.WithServer(tt.URL()), toshi.WithToken("t"), toshi.WithHTTPClient(tt.HTTPClient()), toshi.WithCAFile(path)); err == nil {
		t.Fatal("WithHTTPClient and WithCAFile accepted together")
	}
	t.Setenv("TOSHI_CA_FILE", path)
	if _, err := toshi.New(toshi.WithServer(tt.URL()), toshi.WithToken("t"), toshi.WithHTTPClient(tt.HTTPClient())); err == nil {
		t.Fatal("WithHTTPClient and TOSHI_CA_FILE accepted together")
	}
}

func TestBearerOnUnaryAndStreams(t *testing.T) {
	tt := toshitest.New(t)
	answerListProjects(tt)
	tt.Job("job-1").ExitCode(0)

	client := tt.Client()
	ctx := context.Background()
	if _, err := client.Project.ListProjects(ctx, connect.NewRequest(&workspacev1.ListProjectsRequest{})); err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	watch, err := client.Workload.Jobs.WatchOutput(ctx, &workspacev1.WatchOutputRequest{Id: "job-1"})
	if err != nil {
		t.Fatalf("WatchOutput: %v", err)
	}
	if !watch.Receive() {
		t.Fatalf("WatchOutput ended: %v", watch.Err())
	}

	calls := tt.Calls()
	if len(calls) != 2 {
		t.Fatalf("%d calls, want an unary call and a stream", len(calls))
	}
	for _, call := range calls {
		if got := call.Header.Get("Authorization"); got != "Bearer "+toshitest.Token {
			t.Fatalf("%s: Authorization %q", call.Procedure, got)
		}
		if got := call.Header.Get("Toshi-Tenant"); got != "" {
			t.Fatalf("%s: Toshi-Tenant %q, want none", call.Procedure, got)
		}
	}
}

// TestClientFieldsCallTheirServices proves each field of Client reaches its
// own service. The fake answers UNIMPLEMENTED for the procedures it has no
// state for, which still proves the call reached that service.
func TestClientFieldsCallTheirServices(t *testing.T) {
	tt := toshitest.New(t)
	client := tt.Client()
	ctx := context.Background()

	check := func(name string, err error) {
		t.Helper()
		if err != nil && connect.CodeOf(err) != connect.CodeUnimplemented {
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

	want := []string{
		workspacev1connect.WorkloadServiceCreateJobProcedure,
		workspacev1connect.ProjectServiceListProjectsProcedure,
		workspacev1connect.SecretServicePutSecretProcedure,
		workspacev1connect.WebhookServiceListWebhookEndpointsProcedure,
		workspacev1connect.DisplayServiceOpenDisplaySessionProcedure,
		workspacev1connect.ChannelServiceListChannelsProcedure,
		workspacev1connect.RecordingServiceListRecordingsProcedure,
		workspacev1connect.VolumeServiceListVolumesProcedure,
		workspacev1connect.ArtifactServiceListArtifactsProcedure,
		workspacev1connect.ArtifactUploadServiceReserveUploadProcedure,
		workspacev1connect.WorkspaceServiceListWorkspacesProcedure,
		workspacev1connect.HostJobServiceListHostJobsProcedure,
		workspacev1connect.HostServiceListHostsProcedure,
		workspacev1connect.TokenServiceListTokensProcedure,
		workspacev1connect.AuditEventServiceListAuditEventsProcedure,
		workspacev1connect.FillerServiceListFillersProcedure,
		workspacev1connect.StorageProviderServiceListStorageProvidersProcedure,
	}
	calls := tt.Calls()
	if len(calls) != len(want) {
		t.Fatalf("%d calls, want %d", len(calls), len(want))
	}
	for i := range want {
		if calls[i].Procedure != want[i] {
			t.Errorf("call %d is %s, want %s", i, calls[i].Procedure, want[i])
		}
	}
}

func TestScopeFilling(t *testing.T) {
	var created *workspacev1.CreateJobRequest
	var secret *workspacev1.PutSecretRequest
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceCreateJobProcedure,
		func(_ context.Context, req *workspacev1.CreateJobRequest) (*workspacev1.CreateJobResponse, error) {
			created = proto.Clone(req).(*workspacev1.CreateJobRequest)
			return &workspacev1.CreateJobResponse{}, nil
		})
	toshitest.On(tt, workspacev1connect.SecretServicePutSecretProcedure,
		func(_ context.Context, req *workspacev1.PutSecretRequest) (*workspacev1.PutSecretResponse, error) {
			secret = proto.Clone(req).(*workspacev1.PutSecretRequest)
			return &workspacev1.PutSecretResponse{}, nil
		})
	ctx := context.Background()

	t.Run("workload", func(t *testing.T) {
		client := tt.Client(toshi.WithScope("proj", "env"))
		if _, err := client.Workload.Jobs.Create(ctx, &workspacev1.CreateJobRequest{}); err != nil {
			t.Fatal(err)
		}
		if created.GetProject() != "proj" || created.GetEnvironment() != "env" {
			t.Fatalf("scope %q/%q", created.GetProject(), created.GetEnvironment())
		}
	})

	t.Run("set values stay", func(t *testing.T) {
		client := tt.Client(toshi.WithScope("proj", "env"))
		if _, err := client.Workload.Jobs.Create(ctx, &workspacev1.CreateJobRequest{Project: "p2", Environment: "e2"}); err != nil {
			t.Fatal(err)
		}
		if created.GetProject() != "p2" || created.GetEnvironment() != "e2" {
			t.Fatalf("scope %q/%q", created.GetProject(), created.GetEnvironment())
		}
	})

	t.Run("no scope fills nothing", func(t *testing.T) {
		client := tt.Client(toshi.WithScope("", ""))
		if _, err := client.Workload.Jobs.Create(ctx, &workspacev1.CreateJobRequest{}); err != nil {
			t.Fatal(err)
		}
		if created.GetProject() != "" || created.GetEnvironment() != "" {
			t.Fatalf("scope %q/%q", created.GetProject(), created.GetEnvironment())
		}
	})

	t.Run("shared secret keeps environment empty", func(t *testing.T) {
		client := tt.Client(toshi.WithScope("proj", "env"))
		if _, err := client.Secret.PutSecret(ctx, connect.NewRequest(&workspacev1.PutSecretRequest{Shared: true, Name: "k", Value: []byte("v")})); err != nil {
			t.Fatal(err)
		}
		if secret.GetProject() != "proj" || secret.GetEnvironment() != "" {
			t.Fatalf("scope %q/%q", secret.GetProject(), secret.GetEnvironment())
		}
	})

	t.Run("environment secret", func(t *testing.T) {
		client := tt.Client(toshi.WithScope("proj", "env"))
		if _, err := client.Secret.PutSecret(ctx, connect.NewRequest(&workspacev1.PutSecretRequest{Name: "k", Value: []byte("v")})); err != nil {
			t.Fatal(err)
		}
		if secret.GetEnvironment() != "env" {
			t.Fatalf("environment %q", secret.GetEnvironment())
		}
	})

	t.Run("both set pass through", func(t *testing.T) {
		client := tt.Client(toshi.WithScope("proj", "env"))
		if _, err := client.Secret.PutSecret(ctx, connect.NewRequest(&workspacev1.PutSecretRequest{Shared: true, Environment: "e3", Name: "k", Value: []byte("v")})); err != nil {
			t.Fatal(err)
		}
		if secret.GetEnvironment() != "e3" {
			t.Fatalf("environment %q", secret.GetEnvironment())
		}
	})
}

func TestReadRetriedOnUnavailable(t *testing.T) {
	var calls atomic.Int32
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceGetJobProcedure,
		func(_ context.Context, _ *workspacev1.GetJobRequest) (*workspacev1.GetJobResponse, error) {
			if calls.Add(1) < 3 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
			}
			return &workspacev1.GetJobResponse{Job: &workspacev1.Job{Id: "job-1"}}, nil
		})
	client := tt.Client(toshi.WithRetryPolicy(fastPolicy()))
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
			tt := toshitest.New(t)
			toshitest.On(tt, workspacev1connect.WorkloadServiceGetJobProcedure,
				func(context.Context, *workspacev1.GetJobRequest) (*workspacev1.GetJobResponse, error) {
					calls.Add(1)
					return nil, connect.NewError(code, errors.New("refused"))
				})
			client := tt.Client(toshi.WithRetryPolicy(fastPolicy()))
			_, err := client.Workload.Jobs.Get(context.Background(), &workspacev1.GetJobRequest{})
			if connect.CodeOf(err) != code || calls.Load() != 1 {
				t.Fatalf("code %v after %d calls", connect.CodeOf(err), calls.Load())
			}
		})
	}
}

func TestNoneClassIsNotRetried(t *testing.T) {
	var displays, secrets atomic.Int32
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.DisplayServiceOpenDisplaySessionProcedure,
		func(context.Context, *workspacev1.OpenDisplaySessionRequest) (*workspacev1.OpenDisplaySessionResponse, error) {
			displays.Add(1)
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
		})
	toshitest.On(tt, workspacev1connect.SecretServicePutSecretProcedure,
		func(context.Context, *workspacev1.PutSecretRequest) (*workspacev1.PutSecretResponse, error) {
			secrets.Add(1)
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
		})
	client := tt.Client(toshi.WithScope("proj", "env"), toshi.WithRetryPolicy(fastPolicy()))
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
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceCreateJobProcedure,
		func(_ context.Context, req *workspacev1.CreateJobRequest) (*workspacev1.CreateJobResponse, error) {
			keys = append(keys, req.GetRequestKey())
			if calls.Add(1) == 1 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
			}
			return &workspacev1.CreateJobResponse{}, nil
		})
	toshitest.On(tt, workspacev1connect.WorkloadServiceUpdateJobProcedure,
		func(_ context.Context, req *workspacev1.UpdateJobRequest) (*workspacev1.UpdateJobResponse, error) {
			keys = append(keys, req.GetRequestKey())
			if calls.Add(1) == 3 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
			}
			return &workspacev1.UpdateJobResponse{}, nil
		})
	client := tt.Client(toshi.WithScope("proj", "env"), toshi.WithRetryPolicy(fastPolicy()))
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
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceGetJobProcedure,
		func(context.Context, *workspacev1.GetJobRequest) (*workspacev1.GetJobResponse, error) {
			calls.Add(1)
			// The caller gives up during the first try: the retry's backoff
			// would wait an hour, so the retry answers with the context's error.
			cancel()
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
		})
	client := tt.Client(toshi.WithRetryPolicy(toshi.RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Hour, MaxBackoff: time.Hour}))
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
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceWaitJobProcedure,
		func(context.Context, *workspacev1.WaitJobRequest) (*workspacev1.WaitJobResponse, error) {
			if calls.Add(1) < 3 {
				return &workspacev1.WaitJobResponse{Reached: false}, nil
			}
			return &workspacev1.WaitJobResponse{Reached: true, Job: &workspacev1.Job{Id: "job-1"}}, nil
		})
	client := tt.Client(toshi.WithRetryPolicy(fastPolicy()))
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
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceWaitJobProcedure,
		func(context.Context, *workspacev1.WaitJobRequest) (*workspacev1.WaitJobResponse, error) {
			if calls.Add(1) == 3 {
				cancel()
			}
			return &workspacev1.WaitJobResponse{Reached: false}, nil
		})
	client := tt.Client(toshi.WithRetryPolicy(fastPolicy()))
	_, err := client.Workload.Jobs.Wait(ctx, &workspacev1.WaitJobRequest{})
	if connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("err %v, want canceled", err)
	}
}

func TestWaitOperationAsksAgain(t *testing.T) {
	var calls atomic.Int32
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceWaitOperationProcedure,
		func(context.Context, *workspacev1.WaitOperationRequest) (*workspacev1.WaitOperationResponse, error) {
			if calls.Add(1) < 3 {
				return &workspacev1.WaitOperationResponse{Operation: &workspacev1.Operation{Done: false}}, nil
			}
			return &workspacev1.WaitOperationResponse{Operation: &workspacev1.Operation{Id: "op1", Done: true}}, nil
		})
	client := tt.Client(toshi.WithRetryPolicy(fastPolicy()))
	op, err := client.Workload.WaitOperation(context.Background(), &workspacev1.WaitOperationRequest{})
	if err != nil {
		t.Fatalf("WaitOperation: %v", err)
	}
	if op.GetId() != "op1" || calls.Load() != 3 {
		t.Fatalf("operation %q after %d calls", op.GetId(), calls.Load())
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	policy := toshi.DefaultRetryPolicy()
	if policy.MaxAttempts != 5 || policy.InitialBackoff != 200*time.Millisecond || policy.MaxBackoff != 10*time.Second {
		t.Fatalf("policy %+v", policy)
	}
}
