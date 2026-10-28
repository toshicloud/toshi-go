package toshi_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi/toshitest"
)

// TestVolumeVersionsThroughTheClient proves a volume's saved versions and a
// restore come back through the client, newest first, and that a volume
// reports its newest version.
func TestVolumeVersionsThroughTheClient(t *testing.T) {
	ctx := context.Background()
	saved := time.Date(2026, 10, 28, 8, 0, 0, 0, time.UTC)
	tt := toshitest.New(t)

	toshitest.On(tt, workspacev1connect.VolumeServiceListVolumesProcedure,
		func(context.Context, *workspacev1.ListVolumesRequest) (*workspacev1.ListVolumesResponse, error) {
			return &workspacev1.ListVolumesResponse{Volumes: []*workspacev1.Volume{{
				Id: "vol-1", Name: "cache",
				LatestVersion:     2,
				LatestVersionTime: timestamppb.New(saved),
			}}}, nil
		})
	toshitest.On(tt, workspacev1connect.VolumeServiceListVolumeVersionsProcedure,
		func(_ context.Context, request *workspacev1.ListVolumeVersionsRequest) (*workspacev1.ListVolumeVersionsResponse, error) {
			if request.GetVolumeId() != "vol-1" {
				t.Errorf("ListVolumeVersions asked for %q", request.GetVolumeId())
			}
			if request.GetPageToken() == "" {
				return &workspacev1.ListVolumeVersionsResponse{
					Versions: []*workspacev1.VolumeVersion{{
						Number: 2, CreateTime: timestamppb.New(saved), SizeBytes: 4096,
						Sha256: "0f1e", RestoredFrom: 1,
					}},
					NextPageToken: "page-2",
				}, nil
			}
			return &workspacev1.ListVolumeVersionsResponse{
				Versions: []*workspacev1.VolumeVersion{{Number: 1, CreateTime: timestamppb.New(saved.Add(-time.Hour)), SizeBytes: 2048}},
			}, nil
		})
	toshitest.On(tt, workspacev1connect.VolumeServiceRestoreVolumeProcedure,
		func(_ context.Context, request *workspacev1.RestoreVolumeRequest) (*workspacev1.RestoreVolumeResponse, error) {
			if request.GetVolumeId() != "vol-1" || request.GetVersion() != 1 {
				t.Errorf("RestoreVolume asked for %q version %d", request.GetVolumeId(), request.GetVersion())
			}
			return &workspacev1.RestoreVolumeResponse{
				Version: &workspacev1.VolumeVersion{Number: 3, RestoredFrom: 1, SizeBytes: 2048},
				Volume:  &workspacev1.Volume{Id: "vol-1", LatestVersion: 3, LatestVersionTime: timestamppb.New(saved.Add(time.Minute))},
			}, nil
		})

	client := tt.Client()
	volumes, err := client.Volume().ListVolumes(ctx, connect.NewRequest(&workspacev1.ListVolumesRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	volume := volumes.Msg.GetVolumes()[0]
	if volume.GetLatestVersion() != 2 || !volume.GetLatestVersionTime().AsTime().Equal(saved) {
		t.Fatalf("the volume's newest version is %d at %v", volume.GetLatestVersion(), volume.GetLatestVersionTime())
	}

	first, err := client.Volume().ListVolumeVersions(ctx, connect.NewRequest(&workspacev1.ListVolumeVersionsRequest{VolumeId: "vol-1"}))
	if err != nil {
		t.Fatal(err)
	}
	if got := first.Msg.GetVersions()[0]; got.GetNumber() != 2 || got.GetRestoredFrom() != 1 ||
		got.GetSizeBytes() != 4096 || got.GetSha256() != "0f1e" || !got.GetCreateTime().AsTime().Equal(saved) {
		t.Fatalf("the newest version is %v", got)
	}
	second, err := client.Volume().ListVolumeVersions(ctx, connect.NewRequest(&workspacev1.ListVolumeVersionsRequest{
		VolumeId: "vol-1", PageToken: first.Msg.GetNextPageToken(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if second.Msg.GetNextPageToken() != "" || second.Msg.GetVersions()[0].GetNumber() != 1 {
		t.Fatalf("the second page is %v", second.Msg)
	}

	restored, err := client.Volume().RestoreVolume(ctx, connect.NewRequest(&workspacev1.RestoreVolumeRequest{VolumeId: "vol-1", Version: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if restored.Msg.GetVersion().GetNumber() != 3 || restored.Msg.GetVersion().GetRestoredFrom() != 1 {
		t.Fatalf("RestoreVolume answered %v", restored.Msg.GetVersion())
	}
	if restored.Msg.GetVolume().GetLatestVersion() != 3 {
		t.Fatalf("the restored volume reports version %d", restored.Msg.GetVolume().GetLatestVersion())
	}
}
