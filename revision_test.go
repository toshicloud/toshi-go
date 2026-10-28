package toshi_test

import (
	"context"
	"testing"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi/toshitest"
)

// TestServiceRevisionCarriesRollbackEligibility proves the client carries a
// revision's container image, the digest it pulls, whether a rollback may
// select it and why not, from the API through to the caller.
func TestServiceRevisionCarriesRollbackEligibility(t *testing.T) {
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceListServiceRevisionsProcedure,
		func(context.Context, *workspacev1.ListServiceRevisionsRequest) (*workspacev1.ListServiceRevisionsResponse, error) {
			return &workspacev1.ListServiceRevisionsResponse{Revisions: []*workspacev1.ServiceRevision{
				{
					Number:               1,
					ContainerImage:       "ghcr.io/example/app:v1",
					ContainerImageDigest: "sha256:6f1c0b6e0a1f4d2f8e3a5b7c9d1e2f3a4b5c6d7e8f90112233445566778899aa",
					Eligible:             true,
				},
				{
					Number:           2,
					IneligibleReason: "only the 10 newest revisions can be selected",
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
	if eligible.GetContainerImageDigest() != "sha256:6f1c0b6e0a1f4d2f8e3a5b7c9d1e2f3a4b5c6d7e8f90112233445566778899aa" {
		t.Errorf("container image digest is %q", eligible.GetContainerImageDigest())
	}
	if eligible.GetIneligibleReason() != "" {
		t.Errorf("an eligible revision came back with the reason %q", eligible.GetIneligibleReason())
	}

	retired := revisions[1]
	if retired.GetEligible() {
		t.Error("an ineligible revision came back eligible")
	}
	if retired.GetIneligibleReason() != "only the 10 newest revisions can be selected" {
		t.Errorf("ineligible reason is %q", retired.GetIneligibleReason())
	}
}
