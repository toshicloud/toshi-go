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

// TestGrantsThroughTheClient proves a grant is offered, listed and accepted
// through the client's Grant service, with the token the client sends.
func TestGrantsThroughTheClient(t *testing.T) {
	ctx := context.Background()
	offered := time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)
	tt := toshitest.New(t)

	grant := &workspacev1.Grant{
		Id:      "grant-1",
		Owner:   &workspacev1.GrantWorkspace{Id: "default", Name: "Default"},
		Grantee: &workspacev1.GrantWorkspace{Id: "acme", Name: "Acme"},
		Target: &workspacev1.Grant_Sandbox{Sandbox: &workspacev1.GrantSandboxTarget{
			SandboxId: "sb-1", Project: "default", Environment: "production",
		}},
		Level:     workspacev1.GrantLevel_GRANT_LEVEL_CONNECT,
		State:     workspacev1.GrantState_GRANT_STATE_OFFERED,
		OfferedBy: "member:alice",
		OfferedAt: timestamppb.New(offered),
	}

	toshitest.On(tt, workspacev1connect.GrantServiceCreateGrantProcedure,
		func(_ context.Context, request *workspacev1.CreateGrantRequest) (*workspacev1.CreateGrantResponse, error) {
			if request.GetGranteeWorkspace() != "acme" || request.GetSandbox().GetSandboxId() != "sb-1" {
				t.Errorf("CreateGrant asked for %v", request)
			}
			return &workspacev1.CreateGrantResponse{Grant: grant}, nil
		})
	toshitest.On(tt, workspacev1connect.GrantServiceListGrantsProcedure,
		func(_ context.Context, request *workspacev1.ListGrantsRequest) (*workspacev1.ListGrantsResponse, error) {
			if request.GetSide() != workspacev1.GrantSide_GRANT_SIDE_RECEIVED {
				t.Errorf("ListGrants asked for side %v", request.GetSide())
			}
			return &workspacev1.ListGrantsResponse{Grants: []*workspacev1.Grant{grant}}, nil
		})
	toshitest.On(tt, workspacev1connect.GrantServiceAcceptGrantProcedure,
		func(_ context.Context, request *workspacev1.AcceptGrantRequest) (*workspacev1.AcceptGrantResponse, error) {
			if request.GetId() != "grant-1" {
				t.Errorf("AcceptGrant asked for %q", request.GetId())
			}
			return &workspacev1.AcceptGrantResponse{Grant: grant}, nil
		})

	client := tt.Client()
	created, err := client.Grant().CreateGrant(ctx, connect.NewRequest(&workspacev1.CreateGrantRequest{
		GranteeWorkspace: "acme",
		Target: &workspacev1.CreateGrantRequest_Sandbox{Sandbox: &workspacev1.GrantSandboxTarget{
			SandboxId: "sb-1", Project: "default", Environment: "production",
		}},
		Level: workspacev1.GrantLevel_GRANT_LEVEL_CONNECT,
	}))
	if err != nil {
		t.Fatal(err)
	}
	offeredGrant := created.Msg.GetGrant()
	if offeredGrant.GetId() != "grant-1" || offeredGrant.GetState() != workspacev1.GrantState_GRANT_STATE_OFFERED ||
		offeredGrant.GetSandbox().GetSandboxId() != "sb-1" || offeredGrant.GetGrantee().GetName() != "Acme" ||
		!offeredGrant.GetOfferedAt().AsTime().Equal(offered) {
		t.Fatalf("CreateGrant answered %v", offeredGrant)
	}

	listed, err := client.Grant().ListGrants(ctx, connect.NewRequest(&workspacev1.ListGrantsRequest{
		Side: workspacev1.GrantSide_GRANT_SIDE_RECEIVED,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got := listed.Msg.GetGrants(); len(got) != 1 || got[0].GetId() != "grant-1" {
		t.Fatalf("ListGrants answered %v", got)
	}

	accepted, err := client.Grant().AcceptGrant(ctx, connect.NewRequest(&workspacev1.AcceptGrantRequest{Id: "grant-1"}))
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Msg.GetGrant().GetLevel() != workspacev1.GrantLevel_GRANT_LEVEL_CONNECT {
		t.Fatalf("AcceptGrant answered %v", accepted.Msg.GetGrant())
	}

	calls := tt.Calls()
	if got := calls[len(calls)-1].Header.Get("Authorization"); got != "Bearer "+toshitest.Token {
		t.Fatalf("the call carries %q", got)
	}
}
