package toshi

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	workspacev1 "go.toshicloud.io/api/workspacev1"
)

// TestListEventsSendsTheRequestUnchanged proves ListEvents reaches the server
// with the workload, cursor and page fields it was given, and returns the
// page the server sent.
func TestListEventsSendsTheRequestUnchanged(t *testing.T) {
	want := &workspacev1.ListEventsRequest{
		WorkloadId: "job-1",
		PageToken:  "cursor-1",
		PageSize:   25,
	}
	var got *workspacev1.ListEventsRequest
	server := newTestServer(t, &backend{
		listEvents: func(_ context.Context, req *connect.Request[workspacev1.ListEventsRequest]) (*connect.Response[workspacev1.ListEventsResponse], error) {
			got = proto.Clone(req.Msg).(*workspacev1.ListEventsRequest)
			return connect.NewResponse(&workspacev1.ListEventsResponse{
				Events:        []*workspacev1.Event{{WorkloadId: "job-1", Cursor: "event-1"}},
				Cursor:        "cursor-2",
				NextPageToken: "page-2",
			}), nil
		},
	})
	client := newTestClient(t, server)

	resp, err := client.Workload.ListEvents(context.Background(), want)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("request %v, want %v", got, want)
	}
	if len(resp.GetEvents()) != 1 || resp.GetEvents()[0].GetWorkloadId() != "job-1" {
		t.Fatalf("events %v, want the server's event", resp.GetEvents())
	}
	if resp.GetCursor() != "cursor-2" || resp.GetNextPageToken() != "page-2" {
		t.Fatalf("cursor %q next %q, want cursor-2 and page-2", resp.GetCursor(), resp.GetNextPageToken())
	}
}

// TestListEventsRetriedAsRead proves ListEvents is retried like GetOperation:
// an UNAVAILABLE answer is asked again.
func TestListEventsRetriedAsRead(t *testing.T) {
	var calls atomic.Int32
	server := newTestServer(t, &backend{
		listEvents: func(context.Context, *connect.Request[workspacev1.ListEventsRequest]) (*connect.Response[workspacev1.ListEventsResponse], error) {
			if calls.Add(1) < 3 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
			}
			return connect.NewResponse(&workspacev1.ListEventsResponse{Cursor: "c1"}), nil
		},
	})
	client := newTestClient(t, server)

	resp, err := client.Workload.ListEvents(context.Background(), &workspacev1.ListEventsRequest{})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if resp.GetCursor() != "c1" || calls.Load() != 3 {
		t.Fatalf("cursor %q after %d calls, want c1 after 3", resp.GetCursor(), calls.Load())
	}
}
