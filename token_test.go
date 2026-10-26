package toshi_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi"
	"go.toshicloud.io/toshi/toshitest"
)

type tokenKey struct{}

func TestTokenSourceIsAskedForEachRequest(t *testing.T) {
	var asked atomic.Int32
	source := func(ctx context.Context) (string, error) {
		if ctx.Value(tokenKey{}) != "call" {
			return "", errors.New("the source did not get the call's context")
		}
		return "token-" + strconv.Itoa(int(asked.Add(1))), nil
	}
	var calls atomic.Int32
	tt := toshitest.New(t)
	answerListProjects(tt)
	toshitest.On(tt, workspacev1connect.WorkloadServiceGetJobProcedure,
		func(context.Context, *workspacev1.GetJobRequest) (*workspacev1.GetJobResponse, error) {
			if calls.Add(1) == 1 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
			}
			return &workspacev1.GetJobResponse{}, nil
		})
	client := tt.Client(toshi.WithTokenSource(source), toshi.WithRetryPolicy(fastPolicy()))
	ctx := context.WithValue(context.Background(), tokenKey{}, "call")

	for range 2 {
		if _, err := client.Project().ListProjects(ctx, connect.NewRequest(&workspacev1.ListProjectsRequest{})); err != nil {
			t.Fatalf("ListProjects: %v", err)
		}
	}
	if _, err := client.Workload().Jobs().Get(ctx, &workspacev1.GetJobRequest{}); err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	var got []string
	for _, call := range tt.Calls() {
		got = append(got, call.Header.Get("Authorization"))
	}
	want := []string{"Bearer token-1", "Bearer token-2", "Bearer token-3", "Bearer token-4"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("Authorization headers %q, want %q", got, want)
	}
}

func TestTokenSourceErrorFailsTheCall(t *testing.T) {
	t.Setenv("TOSHI_TOKEN_FILE", filepath.Join(t.TempDir(), "missing"))
	failure := errors.New("vault sealed")
	var asked atomic.Int32
	tt := toshitest.New(t)
	answerListProjects(tt)
	client := tt.Client(
		toshi.WithTokenFile(filepath.Join(t.TempDir(), "missing")),
		toshi.WithTokenSource(func(context.Context) (string, error) {
			asked.Add(1)
			return "", failure
		}),
		toshi.WithRetryPolicy(fastPolicy()),
	)

	_, err := client.Workload().Jobs().Get(context.Background(), &workspacev1.GetJobRequest{})
	if connect.CodeOf(err) != connect.CodeUnauthenticated || !errors.Is(err, failure) {
		t.Fatalf("GetJob: code %v, err %v", connect.CodeOf(err), err)
	}
	if asked.Load() != 1 || len(tt.Calls()) != 0 {
		t.Fatalf("source asked %d times, %d requests reached the server", asked.Load(), len(tt.Calls()))
	}
}

func TestTokenFileIsReadAgainWhenItChanges(t *testing.T) {
	tt := toshitest.New(t)
	answerListProjects(tt)
	path := filepath.Join(t.TempDir(), "token")
	write := func(contents string, modTime time.Time) {
		t.Helper()
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now().Add(-time.Hour)
	write("token-1\n", start)
	t.Setenv("TOSHI_TOKEN", "")
	client, err := toshi.New(toshi.WithServer(tt.URL()), toshi.WithTokenFile(path), toshi.WithHTTPClient(tt.HTTPClient()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	list := func() error {
		_, err := client.Project().ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{}))
		return err
	}
	lastAuthorization := func() string {
		calls := tt.Calls()
		return calls[len(calls)-1].Header.Get("Authorization")
	}

	if err := list(); err != nil || lastAuthorization() != "Bearer token-1" {
		t.Fatalf("first call: %v, Authorization %q", err, lastAuthorization())
	}
	write("token-2\n", start.Add(time.Minute))
	if err := list(); err != nil || lastAuthorization() != "Bearer token-2" {
		t.Fatalf("after the rewrite: %v, Authorization %q", err, lastAuthorization())
	}

	write(" \n", start.Add(2*time.Minute))
	if err := list(); connect.CodeOf(err) != connect.CodeUnauthenticated || !strings.Contains(err.Error(), path) {
		t.Fatalf("empty file: code %v, err %v", connect.CodeOf(err), err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := list(); connect.CodeOf(err) != connect.CodeUnauthenticated || !strings.Contains(err.Error(), path) {
		t.Fatalf("removed file: code %v, err %v", connect.CodeOf(err), err)
	}
}
