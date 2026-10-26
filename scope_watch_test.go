package toshi

import (
	"context"
	"sync"
	"testing"

	"connectrpc.com/connect"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

// TestScopeOnWatchStreams proves WithScope reaches the server-streaming
// calls, whose requests do not pass through the unary scope interceptor.
func TestScopeOnWatchStreams(t *testing.T) {
	t.Run("watch jobs", func(t *testing.T) {
		var mu sync.Mutex
		var listProject, listEnvironment string
		var streamProject, streamEnvironment string
		server := newTestServer(t, &backend{
			listJobs: func(_ context.Context, req *connect.Request[workspacev1.ListJobsRequest]) (*connect.Response[workspacev1.ListJobsResponse], error) {
				mu.Lock()
				listProject, listEnvironment = req.Msg.GetProject(), req.Msg.GetEnvironment()
				mu.Unlock()
				return connect.NewResponse(&workspacev1.ListJobsResponse{Cursor: "c1"}), nil
			},
			watchJobs: func(ctx context.Context, req *connect.Request[workspacev1.WatchJobsRequest], stream *connect.ServerStream[workspacev1.WatchJobsResponse]) error {
				mu.Lock()
				streamProject, streamEnvironment = req.Msg.GetProject(), req.Msg.GetEnvironment()
				mu.Unlock()
				if err := stream.Send(&workspacev1.WatchJobsResponse{Job: &workspacev1.Job{Id: "job-1"}, Cursor: "c2"}); err != nil {
					return err
				}
				<-ctx.Done()
				return nil
			},
		})
		client := newTestClient(t, server, WithScope("proj", "env"))
		check := func(t *testing.T, req *workspacev1.WatchJobsRequest, wantProject string) {
			t.Helper()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			watch, err := client.Workload.Jobs.Watch(ctx, req)
			if err != nil {
				t.Fatalf("Watch: %v", err)
			}
			if !watch.Receive() {
				t.Fatalf("Receive ended: %v", watch.Err())
			}
			_ = watch.Close()
			mu.Lock()
			defer mu.Unlock()
			if listProject != wantProject || listEnvironment != "env" {
				t.Fatalf("list scope %q/%q, want %q/env", listProject, listEnvironment, wantProject)
			}
			if streamProject != wantProject || streamEnvironment != "env" {
				t.Fatalf("stream scope %q/%q, want %q/env", streamProject, streamEnvironment, wantProject)
			}
		}
		t.Run("empty request", func(t *testing.T) { check(t, &workspacev1.WatchJobsRequest{}, "proj") })
		t.Run("caller value kept", func(t *testing.T) {
			check(t, &workspacev1.WatchJobsRequest{Project: "caller"}, "caller")
		})
	})

	t.Run("watch events", func(t *testing.T) {
		var mu sync.Mutex
		var listProject, listEnvironment string
		var streamProject, streamEnvironment string
		server := newTestServer(t, &backend{
			listEvents: func(_ context.Context, req *connect.Request[workspacev1.ListEventsRequest]) (*connect.Response[workspacev1.ListEventsResponse], error) {
				mu.Lock()
				listProject, listEnvironment = req.Msg.GetProject(), req.Msg.GetEnvironment()
				mu.Unlock()
				return connect.NewResponse(&workspacev1.ListEventsResponse{Cursor: "c1"}), nil
			},
			watchEvents: func(ctx context.Context, req *connect.Request[workspacev1.WatchEventsRequest], stream *connect.ServerStream[workspacev1.WatchEventsResponse]) error {
				mu.Lock()
				streamProject, streamEnvironment = req.Msg.GetProject(), req.Msg.GetEnvironment()
				mu.Unlock()
				if err := stream.Send(&workspacev1.WatchEventsResponse{Event: &workspacev1.Event{Id: "e1", Cursor: "c2"}}); err != nil {
					return err
				}
				<-ctx.Done()
				return nil
			},
		})
		client := newTestClient(t, server, WithScope("proj", "env"))
		check := func(t *testing.T, req *workspacev1.WatchEventsRequest, wantProject string) {
			t.Helper()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			watch, err := client.Workload.WatchEvents(ctx, req)
			if err != nil {
				t.Fatalf("WatchEvents: %v", err)
			}
			if !watch.Receive() {
				t.Fatalf("Receive ended: %v", watch.Err())
			}
			_ = watch.Close()
			mu.Lock()
			defer mu.Unlock()
			if listProject != wantProject || listEnvironment != "env" {
				t.Fatalf("list scope %q/%q, want %q/env", listProject, listEnvironment, wantProject)
			}
			if streamProject != wantProject || streamEnvironment != "env" {
				t.Fatalf("stream scope %q/%q, want %q/env", streamProject, streamEnvironment, wantProject)
			}
		}
		t.Run("empty request", func(t *testing.T) { check(t, &workspacev1.WatchEventsRequest{}, "proj") })
		t.Run("caller value kept", func(t *testing.T) {
			check(t, &workspacev1.WatchEventsRequest{Project: "caller"}, "caller")
		})
	})

	t.Run("watch output", func(t *testing.T) {
		var mu sync.Mutex
		var streamProject, streamEnvironment string
		server := newTestServer(t, &backend{
			watchOutput: func(_ context.Context, req *connect.Request[workspacev1.WatchOutputRequest], stream *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
				mu.Lock()
				streamProject, streamEnvironment = req.Msg.GetProject(), req.Msg.GetEnvironment()
				mu.Unlock()
				return stream.Send(&workspacev1.WatchOutputResponse{Message: &workspacev1.WatchOutputResponse_End{End: &workspacev1.OutputEnd{}}})
			},
		})
		client := newTestClient(t, server, WithScope("proj", "env"))
		check := func(t *testing.T, req *workspacev1.WatchOutputRequest, wantProject string) {
			t.Helper()
			watch, err := client.Workload.Jobs.WatchOutput(context.Background(), req)
			if err != nil {
				t.Fatalf("WatchOutput: %v", err)
			}
			if !watch.Receive() {
				t.Fatalf("Receive ended: %v", watch.Err())
			}
			_ = watch.Close()
			mu.Lock()
			defer mu.Unlock()
			if streamProject != wantProject || streamEnvironment != "env" {
				t.Fatalf("stream scope %q/%q, want %q/env", streamProject, streamEnvironment, wantProject)
			}
		}
		t.Run("empty request", func(t *testing.T) { check(t, &workspacev1.WatchOutputRequest{}, "proj") })
		t.Run("caller value kept", func(t *testing.T) {
			check(t, &workspacev1.WatchOutputRequest{Project: "caller"}, "caller")
		})
	})
}

// TestScopeOnRawStreams proves the raw server streams the SDK hands back carry
// WithScope too, not only the watch helpers.
func TestScopeOnRawStreams(t *testing.T) {
	var mu sync.Mutex
	seen := map[string][2]string{}
	record := func(name, project, environment string) {
		mu.Lock()
		seen[name] = [2]string{project, environment}
		mu.Unlock()
	}
	server := newTestServer(t, &backend{
		watchEvents: func(_ context.Context, req *connect.Request[workspacev1.WatchEventsRequest], stream *connect.ServerStream[workspacev1.WatchEventsResponse]) error {
			record("events", req.Msg.GetProject(), req.Msg.GetEnvironment())
			return stream.Send(&workspacev1.WatchEventsResponse{Event: &workspacev1.Event{Id: "e1", Cursor: "c1"}})
		},
		watchTelemetry: func(_ context.Context, req *connect.Request[workspacev1.WatchTelemetryRequest], stream *connect.ServerStream[workspacev1.WatchTelemetryResponse]) error {
			record("telemetry", req.Msg.GetProject(), req.Msg.GetEnvironment())
			return stream.Send(&workspacev1.WatchTelemetryResponse{})
		},
	})
	client := newTestClient(t, server, WithScope("proj", "env"))
	ctx := context.Background()
	assert := func(name, project, environment string) {
		t.Helper()
		mu.Lock()
		defer mu.Unlock()
		if got := seen[name]; got != [2]string{project, environment} {
			t.Fatalf("%s scope %v, want %s/%s", name, got, project, environment)
		}
	}

	events, err := client.Workload.WatchEvents(ctx, &workspacev1.WatchEventsRequest{Cursor: "c0"})
	if err != nil {
		t.Fatalf("WatchEvents: %v", err)
	}
	if !events.Receive() {
		t.Fatalf("WatchEvents: %v", events.Err())
	}
	assert("events", "proj", "env")
	_ = events.Close()

	telemetry, err := client.Workload.Sandboxes.WatchTelemetry(ctx, &workspacev1.WatchTelemetryRequest{Id: "sb-1"})
	if err != nil {
		t.Fatalf("WatchTelemetry: %v", err)
	}
	if !telemetry.Receive() {
		t.Fatalf("WatchTelemetry: %v", telemetry.Err())
	}
	assert("telemetry", "proj", "env")
	_ = telemetry.Close()

	explicit, err := client.Workload.WatchEvents(ctx, &workspacev1.WatchEventsRequest{Cursor: "c0", Project: "p2", Environment: "e2"})
	if err != nil {
		t.Fatalf("WatchEvents explicit: %v", err)
	}
	if !explicit.Receive() {
		t.Fatalf("WatchEvents explicit: %v", explicit.Err())
	}
	assert("events", "p2", "e2")
	_ = explicit.Close()
}

// TestScopeOnExecuteStart proves a start sent through Execute carries
// WithScope, explicit values kept.
func TestScopeOnExecuteStart(t *testing.T) {
	var mu sync.Mutex
	var seen [2]string
	server := newTestServer(t, &backend{
		execute: func(_ context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
			req, err := stream.Receive()
			if err != nil {
				return err
			}
			mu.Lock()
			seen = [2]string{req.GetStart().GetProject(), req.GetStart().GetEnvironment()}
			mu.Unlock()
			return stream.Send(started("ex1"))
		},
	})
	client := newTestClient(t, server, WithScope("proj", "env"))
	check := func(t *testing.T, want [2]string, project, environment string) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		p, err := client.Workload.Sandboxes.Execute(ctx, &workspacev1.ExecuteStart{
			SandboxId:   "sb-1",
			Project:     project,
			Environment: environment,
		})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if p.ExecutionID() != "ex1" {
			t.Fatalf("execution %q", p.ExecutionID())
		}
		mu.Lock()
		got := seen
		mu.Unlock()
		if got != want {
			t.Fatalf("scope %v, want %v", got, want)
		}
	}
	check(t, [2]string{"proj", "env"}, "", "")
	check(t, [2]string{"p2", "e2"}, "p2", "e2")
}
