package toshi

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

func TestWatchJobsSnapshotAndResume(t *testing.T) {
	var mu sync.Mutex
	var listTokens []string
	var watchCursors []string
	watchCalls := 0

	server := newTestServer(t, &backend{
		listJobs: func(_ context.Context, req *connect.Request[workspacev1.ListJobsRequest]) (*connect.Response[workspacev1.ListJobsResponse], error) {
			mu.Lock()
			listTokens = append(listTokens, req.Msg.GetPageToken())
			mu.Unlock()
			switch req.Msg.GetPageToken() {
			case "":
				return connect.NewResponse(&workspacev1.ListJobsResponse{
					Jobs:          []*workspacev1.Job{{Id: "w1"}},
					NextPageToken: "p2",
					Cursor:        "c1",
				}), nil
			case "p2":
				return connect.NewResponse(&workspacev1.ListJobsResponse{
					Jobs:   []*workspacev1.Job{{Id: "w2"}},
					Cursor: "c1",
				}), nil
			default:
				t.Errorf("unexpected page token %q", req.Msg.GetPageToken())
				return connect.NewResponse(&workspacev1.ListJobsResponse{}), nil
			}
		},
		watchJobs: func(ctx context.Context, req *connect.Request[workspacev1.WatchJobsRequest], stream *connect.ServerStream[workspacev1.WatchJobsResponse]) error {
			mu.Lock()
			watchCursors = append(watchCursors, req.Msg.GetCursor())
			watchCalls++
			call := watchCalls
			mu.Unlock()
			switch call {
			case 1:
				if err := stream.Send(&workspacev1.WatchJobsResponse{Job: &workspacev1.Job{Id: "w1"}, Cursor: "c2"}); err != nil {
					return err
				}
				return nil
			case 2:
				if err := stream.Send(&workspacev1.WatchJobsResponse{Keepalive: &workspacev1.Keepalive{Cursor: "c3"}}); err != nil {
					return err
				}
				return nil
			default:
				if err := stream.Send(&workspacev1.WatchJobsResponse{Job: &workspacev1.Job{Id: "w2"}, Cursor: "c4"}); err != nil {
					return err
				}
				<-ctx.Done()
				return nil
			}
		},
	})

	client := newTestClient(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch, err := client.Workload.Jobs.Watch(ctx, &workspacev1.WatchJobsRequest{})
	if err != nil {
		t.Fatalf("WatchJobs: %v", err)
	}
	defer func() { _ = watch.Close() }()

	snapshot := watch.Snapshot()
	if len(snapshot) != 2 || snapshot[0].GetId() != "w1" || snapshot[1].GetId() != "w2" {
		t.Fatalf("snapshot %v", snapshot)
	}
	if watch.Cursor() != "c1" {
		t.Fatalf("cursor %q, want the list's c1", watch.Cursor())
	}
	mu.Lock()
	tokens := append([]string(nil), listTokens...)
	mu.Unlock()
	if len(tokens) != 2 || tokens[0] != "" || tokens[1] != "p2" {
		t.Fatalf("list page tokens %v", tokens)
	}

	if !watch.Receive() {
		t.Fatalf("first receive ended: %v", watch.Err())
	}
	if watch.Msg().GetJob().GetId() != "w1" || watch.Cursor() != "c2" {
		t.Fatalf("first message %v, cursor %q", watch.Msg(), watch.Cursor())
	}
	if !watch.Receive() {
		t.Fatalf("second receive ended: %v", watch.Err())
	}
	if watch.Msg().GetJob().GetId() != "w2" || watch.Cursor() != "c4" {
		t.Fatalf("second message %v, cursor %q", watch.Msg(), watch.Cursor())
	}
	mu.Lock()
	cursors := append([]string(nil), watchCursors...)
	mu.Unlock()
	if len(cursors) != 3 || cursors[0] != "c1" || cursors[1] != "c2" || cursors[2] != "c3" {
		t.Fatalf("watch cursors %v, want c1, c2, c3", cursors)
	}
	if err := watch.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if watch.Err() != nil {
		t.Fatalf("Err after Close: %v", watch.Err())
	}
}

func TestWatchJobsRefusedCursor(t *testing.T) {
	var mu sync.Mutex
	var cursors []string
	server := newTestServer(t, &backend{
		watchJobs: func(_ context.Context, req *connect.Request[workspacev1.WatchJobsRequest], _ *connect.ServerStream[workspacev1.WatchJobsResponse]) error {
			mu.Lock()
			cursors = append(cursors, req.Msg.GetCursor())
			mu.Unlock()
			return connect.NewError(connect.CodeOutOfRange, errors.New("cursor is old"))
		},
	})
	client := newTestClient(t, server)
	watch, err := client.Workload.Jobs.Watch(context.Background(), &workspacev1.WatchJobsRequest{Cursor: "old"})
	if err == nil {
		if watch.Receive() {
			t.Fatal("Receive returned a message")
		}
		err = watch.Err()
		_ = watch.Close()
	}
	if connect.CodeOf(err) != connect.CodeOutOfRange {
		t.Fatalf("err %v, want OUT_OF_RANGE", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(cursors) != 1 || cursors[0] != "old" {
		t.Fatalf("cursors %v, want one at old and no restart", cursors)
	}
}

func TestWatchEventsPagesThenWatch(t *testing.T) {
	var mu sync.Mutex
	var listTokens []string
	var watchCursors []string

	server := newTestServer(t, &backend{
		listEvents: func(_ context.Context, req *connect.Request[workspacev1.ListEventsRequest]) (*connect.Response[workspacev1.ListEventsResponse], error) {
			mu.Lock()
			listTokens = append(listTokens, req.Msg.GetPageToken())
			mu.Unlock()
			switch req.Msg.GetPageToken() {
			case "":
				return connect.NewResponse(&workspacev1.ListEventsResponse{
					Events:        []*workspacev1.Event{{Id: "e1", Cursor: "c1"}, {Id: "e2", Cursor: "c2"}},
					Cursor:        "c2",
					NextPageToken: "p2",
				}), nil
			case "p2":
				return connect.NewResponse(&workspacev1.ListEventsResponse{
					Events: []*workspacev1.Event{{Id: "e3", Cursor: "c3"}, {Id: "e4", Cursor: "c4"}},
					Cursor: "c4",
				}), nil
			default:
				t.Errorf("unexpected page token %q", req.Msg.GetPageToken())
				return connect.NewResponse(&workspacev1.ListEventsResponse{}), nil
			}
		},
		watchEvents: func(ctx context.Context, req *connect.Request[workspacev1.WatchEventsRequest], stream *connect.ServerStream[workspacev1.WatchEventsResponse]) error {
			mu.Lock()
			watchCursors = append(watchCursors, req.Msg.GetCursor())
			mu.Unlock()
			if err := stream.Send(&workspacev1.WatchEventsResponse{Event: &workspacev1.Event{Id: "e3", Cursor: "c3"}}); err != nil {
				return err
			}
			if err := stream.Send(&workspacev1.WatchEventsResponse{Event: &workspacev1.Event{Id: "e4", Cursor: "c4"}}); err != nil {
				return err
			}
			<-ctx.Done()
			return nil
		},
	})
	client := newTestClient(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first, err := client.Workload.WatchEvents(ctx, &workspacev1.WatchEventsRequest{})
	if err != nil {
		t.Fatalf("WatchEvents: %v", err)
	}
	if !first.Receive() || first.Msg().Event.GetId() != "e1" || first.Cursor() != "c1" {
		t.Fatalf("first event %v, cursor %q", first.Msg(), first.Cursor())
	}
	if !first.Receive() || first.Msg().Event.GetId() != "e2" || first.Cursor() != "c2" {
		t.Fatalf("second event %v, cursor %q", first.Msg(), first.Cursor())
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := client.Workload.WatchEvents(ctx, &workspacev1.WatchEventsRequest{Cursor: first.Cursor()})
	if err != nil {
		t.Fatalf("WatchEvents resume: %v", err)
	}
	defer func() { _ = second.Close() }()
	for _, want := range []string{"e3", "e4"} {
		if !second.Receive() {
			t.Fatalf("receive %s ended: %v", want, second.Err())
		}
		if got := second.Msg().Event.GetId(); got != want {
			t.Fatalf("event %q, want %q", got, want)
		}
	}
	if second.Cursor() != "c4" {
		t.Fatalf("cursor %q, want c4", second.Cursor())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(listTokens) != 1 || listTokens[0] != "" {
		t.Fatalf("list page tokens %v, want only the first page", listTokens)
	}
	if len(watchCursors) != 1 || watchCursors[0] != "c2" {
		t.Fatalf("watch cursors %v, want one at the resume cursor c2", watchCursors)
	}
}

func TestWatchEventsEmptyPageStartsWatch(t *testing.T) {
	var mu sync.Mutex
	var watchCursors []string
	server := newTestServer(t, &backend{
		listEvents: func(context.Context, *connect.Request[workspacev1.ListEventsRequest]) (*connect.Response[workspacev1.ListEventsResponse], error) {
			return connect.NewResponse(&workspacev1.ListEventsResponse{Cursor: "p0"}), nil
		},
		watchEvents: func(ctx context.Context, req *connect.Request[workspacev1.WatchEventsRequest], stream *connect.ServerStream[workspacev1.WatchEventsResponse]) error {
			mu.Lock()
			watchCursors = append(watchCursors, req.Msg.GetCursor())
			mu.Unlock()
			if err := stream.Send(&workspacev1.WatchEventsResponse{Event: &workspacev1.Event{Id: "e1", Cursor: "c1"}}); err != nil {
				return err
			}
			<-ctx.Done()
			return nil
		},
	})
	client := newTestClient(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch, err := client.Workload.WatchEvents(ctx, &workspacev1.WatchEventsRequest{})
	if err != nil {
		t.Fatalf("WatchEvents: %v", err)
	}
	defer func() { _ = watch.Close() }()
	if !watch.Receive() || watch.Msg().Event.GetId() != "e1" {
		t.Fatalf("event %v, err %v", watch.Msg(), watch.Err())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(watchCursors) != 1 || watchCursors[0] != "p0" {
		t.Fatalf("watch cursors %v, want p0", watchCursors)
	}
}

func TestWatchEventsKeepaliveCursorResumes(t *testing.T) {
	var mu sync.Mutex
	var cursors []string
	var calls atomic.Int32
	server := newTestServer(t, &backend{
		watchEvents: func(ctx context.Context, req *connect.Request[workspacev1.WatchEventsRequest], stream *connect.ServerStream[workspacev1.WatchEventsResponse]) error {
			mu.Lock()
			cursors = append(cursors, req.Msg.GetCursor())
			mu.Unlock()
			if calls.Add(1) == 1 {
				if err := stream.Send(&workspacev1.WatchEventsResponse{Keepalive: &workspacev1.Keepalive{Cursor: "c1"}}); err != nil {
					return err
				}
				return nil
			}
			if err := stream.Send(&workspacev1.WatchEventsResponse{Event: &workspacev1.Event{Id: "e2", Cursor: "c2"}}); err != nil {
				return err
			}
			<-ctx.Done()
			return nil
		},
	})
	client := newTestClient(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch, err := client.Workload.WatchEvents(ctx, &workspacev1.WatchEventsRequest{Cursor: "c0"})
	if err != nil {
		t.Fatalf("WatchEvents: %v", err)
	}
	defer func() { _ = watch.Close() }()
	if !watch.Receive() {
		t.Fatalf("Receive ended: %v", watch.Err())
	}
	if watch.Msg().Event.GetId() != "e2" || watch.Cursor() != "c2" {
		t.Fatalf("event %v, cursor %q", watch.Msg(), watch.Cursor())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(cursors) != 2 || cursors[0] != "c0" || cursors[1] != "c1" {
		t.Fatalf("cursors %v, want c0 then the keepalive cursor c1", cursors)
	}
}

func TestWatchEventsRefusedCursor(t *testing.T) {
	server := newTestServer(t, &backend{
		watchEvents: func(context.Context, *connect.Request[workspacev1.WatchEventsRequest], *connect.ServerStream[workspacev1.WatchEventsResponse]) error {
			return connect.NewError(connect.CodeOutOfRange, errors.New("cursor is old"))
		},
	})
	client := newTestClient(t, server)
	watch, err := client.Workload.WatchEvents(context.Background(), &workspacev1.WatchEventsRequest{Cursor: "old"})
	if err == nil {
		if watch.Receive() {
			t.Fatal("Receive returned an event")
		}
		err = watch.Err()
		_ = watch.Close()
	}
	if connect.CodeOf(err) != connect.CodeOutOfRange {
		t.Fatalf("err %v, want OUT_OF_RANGE", err)
	}
}

func TestWatchOutputResumesOffsets(t *testing.T) {
	var mu sync.Mutex
	var offsets [][2]int64
	var calls atomic.Int32
	server := newTestServer(t, &backend{
		watchOutput: func(ctx context.Context, req *connect.Request[workspacev1.WatchOutputRequest], stream *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
			mu.Lock()
			offsets = append(offsets, [2]int64{req.Msg.GetStdoutOffset(), req.Msg.GetStderrOffset()})
			mu.Unlock()
			if calls.Add(1) == 1 {
				if err := stream.Send(&workspacev1.WatchOutputResponse{Message: &workspacev1.WatchOutputResponse_Stdout{Stdout: []byte("abc")}}); err != nil {
					return err
				}
				if err := stream.Send(&workspacev1.WatchOutputResponse{Message: &workspacev1.WatchOutputResponse_Stderr{Stderr: []byte("de")}}); err != nil {
					return err
				}
				return nil
			}
			if err := stream.Send(&workspacev1.WatchOutputResponse{Message: &workspacev1.WatchOutputResponse_Stdout{Stdout: []byte("f")}}); err != nil {
				return err
			}
			code := int32(3)
			return stream.Send(&workspacev1.WatchOutputResponse{Message: &workspacev1.WatchOutputResponse_End{End: &workspacev1.OutputEnd{ExitCode: &code}}})
		},
	})
	client := newTestClient(t, server)
	watch, err := client.Workload.Jobs.WatchOutput(context.Background(), &workspacev1.WatchOutputRequest{StdoutOffset: 5, StderrOffset: 7})
	if err != nil {
		t.Fatalf("WatchOutput: %v", err)
	}
	defer func() { _ = watch.Close() }()

	if !watch.Receive() || string(watch.Msg().GetStdout()) != "abc" {
		t.Fatalf("first message %v", watch.Msg())
	}
	if out, errOut := watch.Offsets(); out != 8 || errOut != 7 {
		t.Fatalf("offsets %d/%d, want 8/7", out, errOut)
	}
	if !watch.Receive() || string(watch.Msg().GetStderr()) != "de" {
		t.Fatalf("second message %v", watch.Msg())
	}
	if out, errOut := watch.Offsets(); out != 8 || errOut != 9 {
		t.Fatalf("offsets %d/%d, want 8/9", out, errOut)
	}
	if !watch.Receive() || string(watch.Msg().GetStdout()) != "f" {
		t.Fatalf("resumed message %v", watch.Msg())
	}
	if out, errOut := watch.Offsets(); out != 9 || errOut != 9 {
		t.Fatalf("offsets %d/%d, want 9/9", out, errOut)
	}
	if !watch.Receive() || watch.Msg().GetEnd() == nil {
		t.Fatalf("end message %v", watch.Msg())
	}
	if watch.Receive() {
		t.Fatal("Receive returned a message after the end")
	}
	if watch.Err() != nil {
		t.Fatalf("Err after end: %v", watch.Err())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(offsets) != 2 || offsets[0] != [2]int64{5, 7} || offsets[1] != [2]int64{8, 9} {
		t.Fatalf("offsets %v", offsets)
	}
}

func TestWatchOutputRefused(t *testing.T) {
	for _, code := range []connect.Code{connect.CodeFailedPrecondition, connect.CodeOutOfRange} {
		t.Run(code.String(), func(t *testing.T) {
			var calls atomic.Int32
			server := newTestServer(t, &backend{
				watchOutput: func(context.Context, *connect.Request[workspacev1.WatchOutputRequest], *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
					calls.Add(1)
					return connect.NewError(code, errors.New("refused"))
				},
			})
			client := newTestClient(t, server)
			watch, err := client.Workload.Jobs.WatchOutput(context.Background(), &workspacev1.WatchOutputRequest{})
			if err == nil {
				if watch.Receive() {
					t.Fatal("Receive returned a message")
				}
				err = watch.Err()
				_ = watch.Close()
			}
			if connect.CodeOf(err) != code || calls.Load() != 1 {
				t.Fatalf("err %v after %d calls", err, calls.Load())
			}
		})
	}
}

// blockUntilDone is a streaming handler that sends nothing until its context
// ends.
func blockUntilDone(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// TestWatchConstructorsDoNotOpen proves the constructors return without
// waiting for the stream's first frame; the first Receive opens it.
func TestWatchConstructorsDoNotOpen(t *testing.T) {
	server := newTestServer(t, &backend{
		listJobs: func(context.Context, *connect.Request[workspacev1.ListJobsRequest]) (*connect.Response[workspacev1.ListJobsResponse], error) {
			return connect.NewResponse(&workspacev1.ListJobsResponse{Cursor: "c1"}), nil
		},
		watchJobs: func(ctx context.Context, _ *connect.Request[workspacev1.WatchJobsRequest], _ *connect.ServerStream[workspacev1.WatchJobsResponse]) error {
			return blockUntilDone(ctx)
		},
		watchEvents: func(ctx context.Context, _ *connect.Request[workspacev1.WatchEventsRequest], _ *connect.ServerStream[workspacev1.WatchEventsResponse]) error {
			return blockUntilDone(ctx)
		},
		watchOutput: func(ctx context.Context, _ *connect.Request[workspacev1.WatchOutputRequest], _ *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
			return blockUntilDone(ctx)
		},
	})
	client := newTestClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	workloads, err := client.Workload.Jobs.Watch(ctx, &workspacev1.WatchJobsRequest{})
	if err != nil {
		t.Fatalf("WatchJobs: %v after %v", err, time.Since(start))
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("WatchJobs took %v, want the stream opened on Receive", elapsed)
	}

	start = time.Now()
	events, err := client.Workload.WatchEvents(ctx, &workspacev1.WatchEventsRequest{Cursor: "c1"})
	if err != nil {
		t.Fatalf("WatchEvents: %v after %v", err, time.Since(start))
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("WatchEvents took %v, want the stream opened on Receive", elapsed)
	}

	start = time.Now()
	output, err := client.Workload.Jobs.WatchOutput(ctx, &workspacev1.WatchOutputRequest{Id: "w1"})
	if err != nil {
		t.Fatalf("WatchOutput: %v after %v", err, time.Since(start))
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("WatchOutput took %v, want the stream opened on Receive", elapsed)
	}

	_ = workloads.Close()
	_ = events.Close()
	_ = output.Close()
}

// TestWatchOpenErrorOnReceive proves a refused stream surfaces on the first
// Receive, not from the constructor.
func TestWatchOpenErrorOnReceive(t *testing.T) {
	refuse := func() error { return connect.NewError(connect.CodePermissionDenied, errors.New("refused")) }
	server := newTestServer(t, &backend{
		watchJobs: func(context.Context, *connect.Request[workspacev1.WatchJobsRequest], *connect.ServerStream[workspacev1.WatchJobsResponse]) error {
			return refuse()
		},
		watchEvents: func(context.Context, *connect.Request[workspacev1.WatchEventsRequest], *connect.ServerStream[workspacev1.WatchEventsResponse]) error {
			return refuse()
		},
		watchOutput: func(context.Context, *connect.Request[workspacev1.WatchOutputRequest], *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
			return refuse()
		},
	})
	client := newTestClient(t, server)
	ctx := context.Background()

	workloads, err := client.Workload.Jobs.Watch(ctx, &workspacev1.WatchJobsRequest{Cursor: "c1"})
	if err != nil {
		t.Fatalf("WatchJobs constructor returned the open error: %v", err)
	}
	if workloads.Receive() {
		t.Fatal("WatchJobs Receive returned a message")
	}
	if connect.CodeOf(workloads.Err()) != connect.CodePermissionDenied {
		t.Fatalf("WatchJobs Err %v, want PERMISSION_DENIED", workloads.Err())
	}
	_ = workloads.Close()

	events, err := client.Workload.WatchEvents(ctx, &workspacev1.WatchEventsRequest{Cursor: "c1"})
	if err != nil {
		t.Fatalf("WatchEvents constructor returned the open error: %v", err)
	}
	if events.Receive() {
		t.Fatal("WatchEvents Receive returned an event")
	}
	if connect.CodeOf(events.Err()) != connect.CodePermissionDenied {
		t.Fatalf("WatchEvents Err %v, want PERMISSION_DENIED", events.Err())
	}
	_ = events.Close()

	output, err := client.Workload.Jobs.WatchOutput(ctx, &workspacev1.WatchOutputRequest{Id: "w1"})
	if err != nil {
		t.Fatalf("WatchOutput constructor returned the open error: %v", err)
	}
	if output.Receive() {
		t.Fatal("WatchOutput Receive returned a message")
	}
	if connect.CodeOf(output.Err()) != connect.CodePermissionDenied {
		t.Fatalf("WatchOutput Err %v, want PERMISSION_DENIED", output.Err())
	}
	_ = output.Close()
}
