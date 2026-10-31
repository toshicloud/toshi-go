package toshi_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi"
	"go.toshicloud.io/toshi/toshitest"
)

// TestHoldWorkloadThroughTheClient proves HoldWorkload reaches the server with
// the workload and hold given, and that the client fills the scope.
func TestHoldWorkloadThroughTheClient(t *testing.T) {
	ctx := context.Background()
	tt := toshitest.New(t)

	toshitest.On(tt, workspacev1connect.WorkloadServiceHoldWorkloadProcedure,
		func(_ context.Context, request *workspacev1.HoldWorkloadRequest) (*workspacev1.HoldWorkloadResponse, error) {
			if request.GetWorkloadId() != "sandbox-1" ||
				request.GetHold() != workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_CONFIRMED {
				t.Errorf("HoldWorkload asked for %v", request)
			}
			if request.GetProject() != "default" || request.GetEnvironment() != "production" {
				t.Errorf("HoldWorkload's scope is %q/%q", request.GetProject(), request.GetEnvironment())
			}
			return &workspacev1.HoldWorkloadResponse{}, nil
		})

	if _, err := tt.Client().Workload().HoldWorkload(ctx, &workspacev1.HoldWorkloadRequest{
		WorkloadId: "sandbox-1",
		Hold:       workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_CONFIRMED,
	}); err != nil {
		t.Fatalf("HoldWorkload: %v", err)
	}
}

// TestReleaseWorkloadThroughTheClient proves ReleaseWorkload reaches the
// server with the workload given, and that the client fills the scope.
func TestReleaseWorkloadThroughTheClient(t *testing.T) {
	ctx := context.Background()
	tt := toshitest.New(t)

	toshitest.On(tt, workspacev1connect.WorkloadServiceReleaseWorkloadProcedure,
		func(_ context.Context, request *workspacev1.ReleaseWorkloadRequest) (*workspacev1.ReleaseWorkloadResponse, error) {
			if request.GetWorkloadId() != "service-1" {
				t.Errorf("ReleaseWorkload asked for %v", request)
			}
			if request.GetProject() != "default" || request.GetEnvironment() != "production" {
				t.Errorf("ReleaseWorkload's scope is %q/%q", request.GetProject(), request.GetEnvironment())
			}
			return &workspacev1.ReleaseWorkloadResponse{}, nil
		})

	if _, err := tt.Client().Workload().ReleaseWorkload(ctx, &workspacev1.ReleaseWorkloadRequest{
		WorkloadId: "service-1",
	}); err != nil {
		t.Fatalf("ReleaseWorkload: %v", err)
	}
}

// TestOperatorHeadersReachTheServerOnHoldWorkload proves the operator-call
// interceptor the README shows puts the reason and workspace on the wire.
func TestOperatorHeadersReachTheServerOnHoldWorkload(t *testing.T) {
	ctx := context.Background()
	tt := toshitest.New(t)
	operator := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Toshi-Operator-Reason", "abuse-1234")
			req.Header().Set("Toshi-Workspace", "acme")
			return next(ctx, req)
		}
	})
	toshitest.On(tt, workspacev1connect.WorkloadServiceHoldWorkloadProcedure,
		func(context.Context, *workspacev1.HoldWorkloadRequest) (*workspacev1.HoldWorkloadResponse, error) {
			return &workspacev1.HoldWorkloadResponse{}, nil
		})

	if _, err := tt.Client(toshi.WithInterceptors(operator)).Workload().HoldWorkload(ctx,
		&workspacev1.HoldWorkloadRequest{
			WorkloadId: "service-1",
			Hold:       workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED,
		}); err != nil {
		t.Fatalf("HoldWorkload: %v", err)
	}

	calls := tt.Calls()
	if len(calls) != 1 {
		t.Fatalf("%d calls, want 1", len(calls))
	}
	if got := calls[0].Header.Get("Toshi-Operator-Reason"); got != "abuse-1234" {
		t.Errorf("Toshi-Operator-Reason %q, want abuse-1234", got)
	}
	if got := calls[0].Header.Get("Toshi-Workspace"); got != "acme" {
		t.Errorf("Toshi-Workspace %q, want acme", got)
	}
}
