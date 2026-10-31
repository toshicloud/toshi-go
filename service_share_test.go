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

// TestServiceShareLinksThroughTheClient proves a service's share links are
// minted, listed a page at a time and revoked through the client, and that
// AllServiceShareLinks follows the pages.
func TestServiceShareLinksThroughTheClient(t *testing.T) {
	ctx := context.Background()
	mintedAt := time.Date(2026, 10, 29, 11, 0, 0, 0, time.UTC)
	link := func(id string, at time.Time) *workspacev1.ServiceShareLink {
		return &workspacev1.ServiceShareLink{
			Id:        id,
			ServiceId: "svc-1",
			Endpoint:  "front",
			State:     workspacev1.PreviewShareLinkState_PREVIEW_SHARE_LINK_STATE_OPEN,
			CreatedAt: timestamppb.New(at),
		}
	}
	tt := toshitest.New(t)

	toshitest.On(tt, workspacev1connect.WorkloadServiceCreateServiceShareLinkProcedure,
		func(_ context.Context, request *workspacev1.CreateServiceShareLinkRequest) (*workspacev1.CreateServiceShareLinkResponse, error) {
			if request.GetServiceId() != "svc-1" || request.GetEndpoint() != "" {
				t.Errorf("CreateServiceShareLink asked for %v", request)
			}
			return &workspacev1.CreateServiceShareLinkResponse{
				ShareLink: link("link-2", mintedAt),
				Url:       "https://shop.example.com/?link=link-2",
			}, nil
		})
	toshitest.On(tt, workspacev1connect.WorkloadServiceListServiceShareLinksProcedure,
		func(_ context.Context, request *workspacev1.ListServiceShareLinksRequest) (*workspacev1.ListServiceShareLinksResponse, error) {
			if request.GetServiceId() != "svc-1" {
				t.Errorf("ListServiceShareLinks asked for %q", request.GetServiceId())
			}
			if request.GetPageToken() == "" {
				return &workspacev1.ListServiceShareLinksResponse{
					ShareLinks:    []*workspacev1.ServiceShareLink{link("link-2", mintedAt)},
					NextPageToken: "page-2",
				}, nil
			}
			return &workspacev1.ListServiceShareLinksResponse{
				ShareLinks: []*workspacev1.ServiceShareLink{link("link-1", mintedAt.Add(-time.Hour))},
			}, nil
		})
	toshitest.On(tt, workspacev1connect.WorkloadServiceRevokeServiceShareLinkProcedure,
		func(_ context.Context, request *workspacev1.RevokeServiceShareLinkRequest) (*workspacev1.RevokeServiceShareLinkResponse, error) {
			if request.GetServiceId() != "svc-1" || request.GetShareLinkId() != "link-2" {
				t.Errorf("RevokeServiceShareLink asked for %v", request)
			}
			revoked := link("link-2", mintedAt)
			revoked.State = workspacev1.PreviewShareLinkState_PREVIEW_SHARE_LINK_STATE_REVOKED
			return &workspacev1.RevokeServiceShareLinkResponse{ShareLink: revoked}, nil
		})

	client := tt.Client()
	created, err := client.Workload().Services().CreateServiceShareLink(ctx, &workspacev1.CreateServiceShareLinkRequest{
		ServiceId: "svc-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := created.GetShareLink(); got.GetId() != "link-2" || got.GetEndpoint() != "front" ||
		!got.GetCreatedAt().AsTime().Equal(mintedAt) {
		t.Fatalf("CreateServiceShareLink answered %v", got)
	}
	if created.GetUrl() != "https://shop.example.com/?link=link-2" {
		t.Fatalf("the link's URL is %q", created.GetUrl())
	}

	first, err := client.Workload().Services().ListServiceShareLinks(ctx, &workspacev1.ListServiceShareLinksRequest{ServiceId: "svc-1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.Workload().Services().ListServiceShareLinks(ctx, &workspacev1.ListServiceShareLinksRequest{
		ServiceId: "svc-1", PageToken: first.GetNextPageToken(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := second.GetShareLinks(); len(got) != 1 || got[0].GetId() != "link-1" {
		t.Fatalf("the second page is %v", got)
	}

	var ids []string
	for shareLink, err := range client.Workload().Services().AllServiceShareLinks(ctx, &workspacev1.ListServiceShareLinksRequest{ServiceId: "svc-1"}) {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, shareLink.GetId())
	}
	if len(ids) != 2 || ids[0] != "link-2" || ids[1] != "link-1" {
		t.Fatalf("AllServiceShareLinks yielded %v", ids)
	}

	revoked, err := client.Workload().Services().RevokeServiceShareLink(ctx, &workspacev1.RevokeServiceShareLinkRequest{
		ServiceId: "svc-1", ShareLinkId: "link-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := revoked.GetShareLink().GetState(); got != workspacev1.PreviewShareLinkState_PREVIEW_SHARE_LINK_STATE_REVOKED {
		t.Fatalf("RevokeServiceShareLink answered state %v", got)
	}
}
