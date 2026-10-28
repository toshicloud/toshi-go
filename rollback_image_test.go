package toshi_test

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi"
	"go.toshicloud.io/toshi/toshitest"
)

// TestRollbackImageErrorTellsTheTwoAnswersApart proves a rollback the image
// check refused carries its reason, so a caller can tell a registry that does
// not serve the image from one that could not say.
func TestRollbackImageErrorTellsTheTwoAnswersApart(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
		want   toshi.RollbackImageReason
	}{
		{"not served", "IMAGE_NOT_SERVED", toshi.ReasonImageNotServed},
		{"check failed", "IMAGE_CHECK_FAILED", toshi.ReasonImageCheckFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := toshitest.New(t)
			toshitest.On(tt, workspacev1connect.WorkloadServiceRollbackServiceProcedure,
				func(context.Context, *workspacev1.RollbackServiceRequest) (*workspacev1.RollbackServiceResponse, error) {
					return nil, refusal(t, connect.CodeFailedPrecondition, "toshi.run", tc.reason, nil)
				})

			_, err := tt.Client().Workload().Services().Rollback(context.Background(),
				&workspacev1.RollbackServiceRequest{Id: "svc-1", Revision: 2})
			var imageErr *toshi.RollbackImageError
			if !errors.As(err, &imageErr) {
				t.Fatalf("errors.As found no RollbackImageError in %v", err)
			}
			if imageErr.Reason != tc.want || imageErr.Code != connect.CodeFailedPrecondition {
				t.Fatalf("RollbackImageError %+v, want reason %s and FAILED_PRECONDITION", *imageErr, tc.want)
			}
			if connect.CodeOf(err) != connect.CodeFailedPrecondition || imageErr.Error() != err.Error() {
				t.Fatalf("code %v, text %q", connect.CodeOf(err), err.Error())
			}
			var productErr *toshi.ProductError
			if errors.As(err, &productErr) {
				t.Fatal("a rollback's image refusal decoded as a ProductError")
			}
		})
	}
}
