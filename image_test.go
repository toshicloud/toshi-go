package toshi_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi/toshitest"
)

// TestImagesThroughTheClient proves the five image calls reach the API and
// that an image, its versions and a build's answer come back through the
// generated types.
func TestImagesThroughTheClient(t *testing.T) {
	ctx := context.Background()
	tt := toshitest.New(t)

	toshitest.On(tt, workspacev1connect.ImageServiceCreateImageProcedure,
		func(_ context.Context, request *workspacev1.CreateImageRequest) (*workspacev1.CreateImageResponse, error) {
			if request.GetName() != "base" || request.GetKind() != workspacev1.ImageKind_IMAGE_KIND_DISK {
				t.Errorf("CreateImage asked for %q kind %v", request.GetName(), request.GetKind())
			}
			return &workspacev1.CreateImageResponse{
				Image: &workspacev1.Image{
					Id: "img-1", Name: "base", Kind: workspacev1.ImageKind_IMAGE_KIND_DISK,
					Base: "ubuntu-24.04", Size: "toshi-linux-2x",
					Latest: &workspacev1.ImageVersion{Number: 1, State: workspacev1.ImageVersionState_IMAGE_VERSION_STATE_READY},
				},
				Version: &workspacev1.ImageVersion{
					Id: "iv-2", Number: 2, State: workspacev1.ImageVersionState_IMAGE_VERSION_STATE_BUILDING,
					BuildJobId: "job-1",
				},
			}, nil
		})
	toshitest.On(tt, workspacev1connect.ImageServiceGetImageProcedure,
		func(context.Context, *workspacev1.GetImageRequest) (*workspacev1.GetImageResponse, error) {
			return &workspacev1.GetImageResponse{
				Image: &workspacev1.Image{Id: "img-1", Name: "base"},
				Versions: []*workspacev1.ImageVersion{
					{
						Number: 2, State: workspacev1.ImageVersionState_IMAGE_VERSION_STATE_PENDING_DELETION,
						SizeBytes: 1 << 30, Failure: "",
						PinnedBy: []*workspacev1.ImageVersionPin{{ServiceId: "svc-1", ServiceName: "web", Revision: 3}},
					},
					{Number: 1, State: workspacev1.ImageVersionState_IMAGE_VERSION_STATE_READY, SizeBytes: 1 << 30},
				},
			}, nil
		})
	toshitest.On(tt, workspacev1connect.ImageServiceListImagesProcedure,
		func(context.Context, *workspacev1.ListImagesRequest) (*workspacev1.ListImagesResponse, error) {
			return &workspacev1.ListImagesResponse{
				Images:        []*workspacev1.Image{{Id: "img-1", Name: "base"}},
				NextPageToken: "",
			}, nil
		})
	toshitest.On(tt, workspacev1connect.ImageServiceBuildImageProcedure,
		func(context.Context, *workspacev1.BuildImageRequest) (*workspacev1.BuildImageResponse, error) {
			return &workspacev1.BuildImageResponse{
				Version: &workspacev1.ImageVersion{Id: "iv-3", Number: 3, State: workspacev1.ImageVersionState_IMAGE_VERSION_STATE_BUILDING},
			}, nil
		})
	toshitest.On(tt, workspacev1connect.ImageServiceDeleteImageProcedure,
		func(context.Context, *workspacev1.DeleteImageRequest) (*workspacev1.DeleteImageResponse, error) {
			return &workspacev1.DeleteImageResponse{}, nil
		})

	client := tt.Client()
	created, err := client.Image().CreateImage(ctx, connect.NewRequest(&workspacev1.CreateImageRequest{
		Name: "base", Kind: workspacev1.ImageKind_IMAGE_KIND_DISK, Base: "ubuntu-24.04", Size: "toshi-linux-2x",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if created.Msg.GetImage().GetName() != "base" || created.Msg.GetVersion().GetNumber() != 2 {
		t.Fatalf("CreateImage answered %v, %v", created.Msg.GetImage(), created.Msg.GetVersion())
	}
	if got := created.Msg.GetVersion().GetState(); got != workspacev1.ImageVersionState_IMAGE_VERSION_STATE_BUILDING {
		t.Fatalf("the first build is %v", got)
	}

	got, err := client.Image().GetImage(ctx, connect.NewRequest(&workspacev1.GetImageRequest{Name: "base"}))
	if err != nil {
		t.Fatal(err)
	}
	versions := got.Msg.GetVersions()
	if len(versions) != 2 || versions[0].GetNumber() != 2 {
		t.Fatalf("GetImage answered %d versions", len(versions))
	}
	if pinned := versions[0].GetPinnedBy(); len(pinned) != 1 || pinned[0].GetServiceName() != "web" || pinned[0].GetRevision() != 3 {
		t.Fatalf("PinnedBy is %v", pinned)
	}
	if versions[1].GetState() != workspacev1.ImageVersionState_IMAGE_VERSION_STATE_READY || versions[1].GetSizeBytes() != 1<<30 {
		t.Fatalf("the ready version is %v", versions[1])
	}

	listed, err := client.Image().ListImages(ctx, connect.NewRequest(&workspacev1.ListImagesRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Msg.GetImages()) != 1 || listed.Msg.GetImages()[0].GetId() != "img-1" {
		t.Fatalf("ListImages answered %v", listed.Msg.GetImages())
	}

	built, err := client.Image().BuildImage(ctx, connect.NewRequest(&workspacev1.BuildImageRequest{Name: "base"}))
	if err != nil {
		t.Fatal(err)
	}
	if built.Msg.GetVersion().GetBuildJobId() != "" || built.Msg.GetVersion().GetId() != "iv-3" {
		t.Fatalf("BuildImage answered %v", built.Msg.GetVersion())
	}

	if _, err := client.Image().DeleteImage(ctx, connect.NewRequest(&workspacev1.DeleteImageRequest{Name: "base"})); err != nil {
		t.Fatal(err)
	}
}
