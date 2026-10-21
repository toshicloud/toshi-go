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
	t.Run("watch workloads", func(t *testing.T) {
		var mu sync.Mutex
		var listProject, listEnvironment string
		var streamProject, streamEnvironment string
		server := newTestServer(t, &backend{
			listWorkloads: func(_ context.Context, req *connect.Request[workspacev1.ListWorkloadsRequest]) (*connect.Response[workspacev1.ListWorkloadsResponse], error) {
				mu.Lock()
				listProject, listEnvironment = req.Msg.GetProject(), req.Msg.GetEnvironment()
				mu.Unlock()
				return connect.NewResponse(&workspacev1.ListWorkloadsResponse{Cursor: "c1"}), nil
			},
			watchWorkloads: func(ctx context.Context, req *connect.Request[workspacev1.WatchWorkloadsRequest], stream *connect.ServerStream[workspacev1.WatchWorkloadsResponse]) error {
				mu.Lock()
				streamProject, streamEnvironment = req.Msg.GetProject(), req.Msg.GetEnvironment()
				mu.Unlock()
				if err := stream.Send(&workspacev1.WatchWorkloadsResponse{Workload: &workspacev1.Workload{Id: "w1"}, Cursor: "c2"}); err != nil {
					return err
				}
				<-ctx.Done()
				return nil
			},
		})
		client := newTestClient(t, server, WithScope("proj", "env"))
		check := func(t *testing.T, req *workspacev1.WatchWorkloadsRequest, wantProject string) {
			t.Helper()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			watch, err := client.WatchWorkloads(ctx, req)
			if err != nil {
				t.Fatalf("WatchWorkloads: %v", err)
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
		t.Run("empty request", func(t *testing.T) { check(t, &workspacev1.WatchWorkloadsRequest{}, "proj") })
		t.Run("caller value kept", func(t *testing.T) { check(t, &workspacev1.WatchWorkloadsRequest{Project: "caller"}, "caller") })
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
			watch, err := client.WatchEvents(ctx, req)
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
		t.Run("caller value kept", func(t *testing.T) { check(t, &workspacev1.WatchEventsRequest{Project: "caller"}, "caller") })
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
			watch, err := client.WatchOutput(context.Background(), req)
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
		t.Run("caller value kept", func(t *testing.T) { check(t, &workspacev1.WatchOutputRequest{Project: "caller"}, "caller") })
	})
}

// TestScopeOnGeneratedStreams proves the generated streaming clients carry
// WithScope on their requests, not only the watch helpers.
func TestScopeOnGeneratedStreams(t *testing.T) {
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
		watchWorkloads: func(_ context.Context, req *connect.Request[workspacev1.WatchWorkloadsRequest], stream *connect.ServerStream[workspacev1.WatchWorkloadsResponse]) error {
			record("workloads", req.Msg.GetProject(), req.Msg.GetEnvironment())
			return stream.Send(&workspacev1.WatchWorkloadsResponse{Workload: &workspacev1.Workload{Id: "w1"}, Cursor: "c1"})
		},
		watchOutput: func(_ context.Context, req *connect.Request[workspacev1.WatchOutputRequest], stream *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
			record("output", req.Msg.GetProject(), req.Msg.GetEnvironment())
			return stream.Send(&workspacev1.WatchOutputResponse{Message: &workspacev1.WatchOutputResponse_Stdout{Stdout: []byte("x")}})
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

	events, err := client.Workspace.WatchEvents(ctx, connect.NewRequest(&workspacev1.WatchEventsRequest{Cursor: "c0"}))
	if err != nil {
		t.Fatalf("WatchEvents: %v", err)
	}
	if !events.Receive() {
		t.Fatalf("WatchEvents: %v", events.Err())
	}
	assert("events", "proj", "env")
	_ = events.Close()

	workloads, err := client.Workspace.WatchWorkloads(ctx, connect.NewRequest(&workspacev1.WatchWorkloadsRequest{Cursor: "c0"}))
	if err != nil {
		t.Fatalf("WatchWorkloads: %v", err)
	}
	if !workloads.Receive() {
		t.Fatalf("WatchWorkloads: %v", workloads.Err())
	}
	assert("workloads", "proj", "env")
	_ = workloads.Close()

	output, err := client.Workspace.WatchOutput(ctx, connect.NewRequest(&workspacev1.WatchOutputRequest{Id: "w1"}))
	if err != nil {
		t.Fatalf("WatchOutput: %v", err)
	}
	if !output.Receive() {
		t.Fatalf("WatchOutput: %v", output.Err())
	}
	assert("output", "proj", "env")
	_ = output.Close()

	explicit, err := client.Workspace.WatchEvents(ctx, connect.NewRequest(&workspacev1.WatchEventsRequest{Cursor: "c0", Project: "p2", Environment: "e2"}))
	if err != nil {
		t.Fatalf("WatchEvents explicit: %v", err)
	}
	if !explicit.Receive() {
		t.Fatalf("WatchEvents explicit: %v", explicit.Err())
	}
	assert("events", "p2", "e2")
	_ = explicit.Close()
}

// TestScopeOnGeneratedExecuteStart proves a start sent through the generated
// Execute stream carries WithScope, explicit values kept.
func TestScopeOnGeneratedExecuteStart(t *testing.T) {
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
	ctx := context.Background()
	send := func(t *testing.T, project, environment string) {
		t.Helper()
		stream := client.Workspace.Execute(ctx)
		if err := stream.Send(&workspacev1.ExecuteRequest{Message: &workspacev1.ExecuteRequest_Start{Start: &workspacev1.ExecuteStart{
			WorkloadId:  "w1",
			Project:     project,
			Environment: environment,
		}}}); err != nil {
			t.Fatalf("Execute Send: %v", err)
		}
		if _, err := stream.Receive(); err != nil {
			t.Fatalf("Execute Receive: %v", err)
		}
		_ = stream.CloseResponse()
	}
	send(t, "", "")
	mu.Lock()
	got := seen
	mu.Unlock()
	if got != [2]string{"proj", "env"} {
		t.Fatalf("scope %v, want proj/env", got)
	}
	send(t, "p2", "e2")
	mu.Lock()
	got = seen
	mu.Unlock()
	if got != [2]string{"p2", "e2"} {
		t.Fatalf("scope %v, want p2/e2", got)
	}
}
