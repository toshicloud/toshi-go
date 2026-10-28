package toshi_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi/toshitest"
)

const revisionDigest = "sha256:6f1c0b6e0a1f4d2f8e3a5b7c9d1e2f3a4b5c6d7e8f90112233445566778899aa"

// TestServiceRevisionCarriesItsRollbackAnswers proves the client carries a
// revision's container image, the digest it pulls, whether a rollback may
// select it, why not, and what the registry last answered about its image,
// from the API through to the caller.
func TestServiceRevisionCarriesItsRollbackAnswers(t *testing.T) {
	checkedAt := time.Date(2026, 10, 28, 2, 30, 0, 0, time.UTC)
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceListServiceRevisionsProcedure,
		func(context.Context, *workspacev1.ListServiceRevisionsRequest) (*workspacev1.ListServiceRevisionsResponse, error) {
			return &workspacev1.ListServiceRevisionsResponse{Revisions: []*workspacev1.ServiceRevision{
				{
					Number:               1,
					ContainerImage:       "ghcr.io/example/app:v1",
					ContainerImageDigest: revisionDigest,
					Eligible:             true,
				},
				{
					Number:           2,
					IneligibleReason: "the registry answered 404 Not Found for the revision's digest: it does not serve the revision's image",
					ImageCheck: &workspacev1.ServiceRevisionImageCheck{
						Outcome:   workspacev1.ServiceRevisionImageOutcome_SERVICE_REVISION_IMAGE_OUTCOME_NOT_SERVED,
						Status:    "404 Not Found",
						Message:   "manifest unknown",
						CheckedAt: timestamppb.New(checkedAt),
					},
				},
			}}, nil
		})

	response, err := tt.Client().Workload().Services().ListRevisions(context.Background(),
		&workspacev1.ListServiceRevisionsRequest{Id: "svc-1"})
	if err != nil {
		t.Fatal(err)
	}
	revisions := response.GetRevisions()
	if len(revisions) != 2 {
		t.Fatalf("%d revisions, want 2", len(revisions))
	}

	eligible := revisions[0]
	if !eligible.GetEligible() {
		t.Error("an eligible revision came back ineligible")
	}
	if eligible.GetContainerImage() != "ghcr.io/example/app:v1" {
		t.Errorf("container image is %q", eligible.GetContainerImage())
	}
	if eligible.GetContainerImageDigest() != revisionDigest {
		t.Errorf("container image digest is %q", eligible.GetContainerImageDigest())
	}
	if eligible.GetIneligibleReason() != "" {
		t.Errorf("an eligible revision came back with the reason %q", eligible.GetIneligibleReason())
	}
	if eligible.GetImageCheck() != nil {
		t.Errorf("a revision no rollback checked came back with the check %v", eligible.GetImageCheck())
	}

	retired := revisions[1]
	if retired.GetEligible() {
		t.Error("an ineligible revision came back eligible")
	}
	if retired.GetIneligibleReason() == "" {
		t.Error("an ineligible revision came back without its reason")
	}
	check := retired.GetImageCheck()
	if got := check.GetOutcome(); got != workspacev1.ServiceRevisionImageOutcome_SERVICE_REVISION_IMAGE_OUTCOME_NOT_SERVED {
		t.Errorf("image check outcome is %v", got)
	}
	if check.GetStatus() != "404 Not Found" {
		t.Errorf("image check status is %q", check.GetStatus())
	}
	if check.GetMessage() != "manifest unknown" {
		t.Errorf("image check message is %q", check.GetMessage())
	}
	if !check.GetCheckedAt().AsTime().Equal(checkedAt) {
		t.Errorf("image check time is %v, want %v", check.GetCheckedAt().AsTime(), checkedAt)
	}
}
