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

// TestRefusalErrorTellsTheReasonsApart proves every reason the API names on
// a refusal that is not a product's comes back as a RefusalError with that
// reason and the API's code.
func TestRefusalErrorTellsTheReasonsApart(t *testing.T) {
	for _, tc := range []struct {
		reason string
		code   connect.Code
		want   toshi.RefusalReason
	}{
		{"DISPLAY_INPUT_NOT_ENABLED", connect.CodeFailedPrecondition, toshi.ReasonDisplayInputNotEnabled},
		{"IMAGE_BUILD_IN_PROGRESS", connect.CodeFailedPrecondition, toshi.ReasonImageBuildInProgress},
		{"IMAGE_CHECK_FAILED", connect.CodeFailedPrecondition, toshi.ReasonImageCheckFailed},
		{"IMAGE_IN_USE", connect.CodeFailedPrecondition, toshi.ReasonImageInUse},
		{"IMAGE_NAME_TAKEN", connect.CodeAlreadyExists, toshi.ReasonImageNameTaken},
		{"IMAGE_NOT_READY", connect.CodeFailedPrecondition, toshi.ReasonImageNotReady},
		{"IMAGE_NOT_SERVED", connect.CodeFailedPrecondition, toshi.ReasonImageNotServed},
		{"LAST_IDENTITY", connect.CodeFailedPrecondition, toshi.ReasonLastIdentity},
		{"LAST_MEMBER", connect.CodeFailedPrecondition, toshi.ReasonLastMember},
		{"LAST_OWNER", connect.CodeFailedPrecondition, toshi.ReasonLastOwner},
		{"PERMISSION_MISSING", connect.CodePermissionDenied, toshi.ReasonPermissionMissing},
		{"PLAN_EXCLUDES_IMAGES", connect.CodeFailedPrecondition, toshi.ReasonPlanExcludesImages},
		{"TOSHI_LICENSED_WINDOWS", connect.CodeFailedPrecondition, toshi.ReasonToshiLicensedWindows},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			tt := toshitest.New(t)
			toshitest.On(tt, workspacev1connect.ProjectServiceListProjectsProcedure,
				func(context.Context, *workspacev1.ListProjectsRequest) (*workspacev1.ListProjectsResponse, error) {
					return nil, refusal(t, tc.code, "toshi.run", tc.reason, nil)
				})

			_, err := tt.Client().Project().ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{}))
			var refused *toshi.RefusalError
			if !errors.As(err, &refused) {
				t.Fatalf("errors.As found no RefusalError in %v", err)
			}
			if refused.Reason != tc.want || refused.Code != tc.code {
				t.Fatalf("RefusalError %+v, want reason %s and %v", *refused, tc.want, tc.code)
			}
			if connect.CodeOf(err) != tc.code || refused.Error() != err.Error() {
				t.Fatalf("code %v, text %q", connect.CodeOf(err), err.Error())
			}
			var productErr *toshi.ProductError
			if errors.As(err, &productErr) {
				t.Fatal("a refusal that is not a product's decoded as a ProductError")
			}
		})
	}
}

// TestUnknownReasonsStayConnectErrors proves a refusal whose reason this
// package does not know is left as it is.
func TestUnknownReasonsStayConnectErrors(t *testing.T) {
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.ProjectServiceListProjectsProcedure,
		func(context.Context, *workspacev1.ListProjectsRequest) (*workspacev1.ListProjectsResponse, error) {
			return nil, refusal(t, connect.CodeFailedPrecondition, "toshi.run", "QUOTA_EXCEEDED", nil)
		})

	_, err := tt.Client().Project().ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{}))
	var refused *toshi.RefusalError
	if err == nil || errors.As(err, &refused) {
		t.Fatalf("err %v, RefusalError %v", err, refused)
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("code %v", connect.CodeOf(err))
	}
}
