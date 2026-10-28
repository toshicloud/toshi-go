package toshi_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi/toshitest"
)

// TestAccountThroughTheClient proves the caller's account, its identities
// and an unlink come back through the client.
func TestAccountThroughTheClient(t *testing.T) {
	ctx := context.Background()
	tt := toshitest.New(t)

	toshitest.On(tt, workspacev1connect.AccountServiceGetAccountProcedure,
		func(context.Context, *workspacev1.GetAccountRequest) (*workspacev1.GetAccountResponse, error) {
			return &workspacev1.GetAccountResponse{
				Account: &workspacev1.Account{UserId: "user-1", Name: "Ada", Email: "ada@example.com"},
				Identities: []*workspacev1.Identity{
					{Provider: "github", Subject: "1", Login: "ada", LastSignInAt: timestamppb.Now()},
				},
				Workspaces: []string{"default", "zeta"},
			}, nil
		})
	toshitest.On(tt, workspacev1connect.AccountServiceListIdentitiesProcedure,
		func(context.Context, *workspacev1.ListIdentitiesRequest) (*workspacev1.ListIdentitiesResponse, error) {
			return &workspacev1.ListIdentitiesResponse{Identities: []*workspacev1.Identity{
				{Provider: "github", Subject: "1", Login: "ada"},
				{Provider: "https://issuer.example.com", Subject: "2", Login: "ada@example.com"},
			}}, nil
		})
	toshitest.On(tt, workspacev1connect.AccountServiceUnlinkIdentityProcedure,
		func(_ context.Context, request *workspacev1.UnlinkIdentityRequest) (*workspacev1.UnlinkIdentityResponse, error) {
			if request.GetProvider() != "github" || request.GetSubject() != "1" {
				t.Errorf("UnlinkIdentity asked for %q %q", request.GetProvider(), request.GetSubject())
			}
			return &workspacev1.UnlinkIdentityResponse{
				Identity:      &workspacev1.Identity{Provider: "github", Subject: "1", Login: "ada"},
				SessionsEnded: 2,
				GrantsRevoked: 1,
			}, nil
		})

	client := tt.Client()
	account, err := client.Account().GetAccount(ctx, connect.NewRequest(&workspacev1.GetAccountRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if account.Msg.GetAccount().GetUserId() != "user-1" || account.Msg.GetAccount().GetEmail() != "ada@example.com" {
		t.Fatalf("GetAccount answered %v", account.Msg.GetAccount())
	}
	if ids := account.Msg.GetIdentities(); len(ids) != 1 || ids[0].GetLogin() != "ada" {
		t.Fatalf("the account's identities are %v", ids)
	}
	if got := account.Msg.GetWorkspaces(); len(got) != 2 || got[0] != "default" {
		t.Fatalf("the account's workspaces are %v", got)
	}

	identities, err := client.Account().ListIdentities(ctx, connect.NewRequest(&workspacev1.ListIdentitiesRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if got := identities.Msg.GetIdentities(); len(got) != 2 || got[1].GetProvider() != "https://issuer.example.com" {
		t.Fatalf("ListIdentities answered %v", got)
	}

	unlinked, err := client.Account().UnlinkIdentity(ctx, connect.NewRequest(&workspacev1.UnlinkIdentityRequest{Provider: "github", Subject: "1"}))
	if err != nil {
		t.Fatal(err)
	}
	if unlinked.Msg.GetIdentity().GetLogin() != "ada" || unlinked.Msg.GetSessionsEnded() != 2 || unlinked.Msg.GetGrantsRevoked() != 1 {
		t.Fatalf("UnlinkIdentity answered %v", unlinked.Msg)
	}
}
