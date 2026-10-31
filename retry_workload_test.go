package toshi

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"
)

// workloadRetries answers the workload procedures the retry tests call: an
// UNAVAILABLE answer every time, except that the second RunServiceAlarmNow
// succeeds.
type workloadRetries struct {
	workspacev1connect.UnimplementedWorkloadServiceHandler

	setAlarmCalls    atomic.Int32
	emitEventCalls   atomic.Int32
	runAlarmNowCalls atomic.Int32

	mu              sync.Mutex
	runAlarmNowKeys []string
}

func (b *workloadRetries) SetServiceAlarm(context.Context, *connect.Request[workspacev1.SetServiceAlarmRequest]) (*connect.Response[workspacev1.SetServiceAlarmResponse], error) {
	b.setAlarmCalls.Add(1)
	return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
}

func (b *workloadRetries) EmitWorkloadEvent(context.Context, *connect.Request[workspacev1.EmitWorkloadEventRequest]) (*connect.Response[workspacev1.EmitWorkloadEventResponse], error) {
	b.emitEventCalls.Add(1)
	return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
}

func (b *workloadRetries) RunServiceAlarmNow(_ context.Context, req *connect.Request[workspacev1.RunServiceAlarmNowRequest]) (*connect.Response[workspacev1.RunServiceAlarmNowResponse], error) {
	b.mu.Lock()
	b.runAlarmNowKeys = append(b.runAlarmNowKeys, req.Msg.GetRequestKey())
	calls := len(b.runAlarmNowKeys)
	b.mu.Unlock()
	b.runAlarmNowCalls.Add(1)
	if calls == 1 {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
	}
	return connect.NewResponse(&workspacev1.RunServiceAlarmNowResponse{Occurrence: &workspacev1.ServiceAlarmOccurrence{
		Generation: 1,
		Manual:     true,
	}}), nil
}

// workloadRetriesHandler serves the workload procedures from b.
func workloadRetriesHandler(b *workloadRetries) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(workspacev1connect.NewWorkloadServiceHandler(b))
	return mux
}

// TestSetAlarmAndEmitEventAreNotRetried proves SetAlarm and EmitEvent make
// one try, and the UNAVAILABLE answer reaches the caller.
func TestSetAlarmAndEmitEventAreNotRetried(t *testing.T) {
	sleeps(t, nil)
	b := &workloadRetries{}
	client := waitClient(t, workloadRetriesHandler(b))
	ctx := context.Background()

	if _, err := client.Workload().Services().SetAlarm(ctx, &workspacev1.SetServiceAlarmRequest{
		ServiceId: "svc-1", Name: "nightly",
	}); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("SetAlarm: %v, want UNAVAILABLE", err)
	}
	if calls := b.setAlarmCalls.Load(); calls != 1 {
		t.Fatalf("SetAlarm made %d tries, want 1", calls)
	}

	if _, err := client.Workload().EmitEvent(ctx, &workspacev1.EmitWorkloadEventRequest{
		WorkloadId: "job-1", Name: "deploy.done",
	}); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("EmitEvent: %v, want UNAVAILABLE", err)
	}
	if calls := b.emitEventCalls.Load(); calls != 1 {
		t.Fatalf("EmitEvent made %d tries, want 1", calls)
	}
}

// TestRunAlarmNowRetriedWithTheSameRequestKey proves RunAlarmNow is retried,
// and every try carries the request key the interceptor filled.
func TestRunAlarmNowRetriedWithTheSameRequestKey(t *testing.T) {
	sleeps(t, nil)
	b := &workloadRetries{}
	client := waitClient(t, workloadRetriesHandler(b))

	resp, err := client.Workload().Services().RunAlarmNow(context.Background(), &workspacev1.RunServiceAlarmNowRequest{
		ServiceId: "svc-1", Name: "nightly",
	})
	if err != nil {
		t.Fatalf("RunAlarmNow: %v", err)
	}
	if occurrence := resp.GetOccurrence(); !occurrence.GetManual() || occurrence.GetGeneration() != 1 {
		t.Fatalf("RunAlarmNow answered %v", occurrence)
	}
	if calls := b.runAlarmNowCalls.Load(); calls != 2 {
		t.Fatalf("RunAlarmNow made %d tries, want 2", calls)
	}
	if len(b.runAlarmNowKeys) != 2 || b.runAlarmNowKeys[0] == "" || b.runAlarmNowKeys[0] != b.runAlarmNowKeys[1] {
		t.Fatalf("RunAlarmNow's request keys are %q, want the same non-empty key twice", b.runAlarmNowKeys)
	}
}
