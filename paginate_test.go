package toshi_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi"
	"go.toshicloud.io/toshi/toshitest"
)

// jobsPage is a page of jobs with the given ids and the token of the page
// after it.
func jobsPage(ids []string, next string) *workspacev1.ListJobsResponse {
	page := &workspacev1.ListJobsResponse{NextPageToken: next}
	for _, id := range ids {
		page.Jobs = append(page.Jobs, &workspacev1.Job{Id: id})
	}
	return page
}

// answerJobPages answers ListJobs with the page each page token names.
func answerJobPages(tt *toshitest.Fake, pages map[string]*workspacev1.ListJobsResponse) {
	toshitest.On(tt, workspacev1connect.WorkloadServiceListJobsProcedure,
		func(_ context.Context, req *workspacev1.ListJobsRequest) (*workspacev1.ListJobsResponse, error) {
			page, ok := pages[req.GetPageToken()]
			if !ok {
				return nil, connect.NewError(connect.CodeNotFound, errors.New("no page for token "+req.GetPageToken()))
			}
			return page, nil
		})
}

// requestedJobPages returns the page token of each ListJobs call the fake
// received, oldest first.
func requestedJobPages(t *testing.T, tt *toshitest.Fake) []string {
	t.Helper()
	var tokens []string
	for _, call := range tt.Calls() {
		req, ok := call.Request.(*workspacev1.ListJobsRequest)
		if !ok {
			t.Fatalf("call to %s carries %T, want ListJobsRequest", call.Procedure, call.Request)
		}
		tokens = append(tokens, req.GetPageToken())
	}
	return tokens
}

// TestAllFollowsThePageTokens proves All yields the pages in order, asking for
// each page with the token the one before it returned.
func TestAllFollowsThePageTokens(t *testing.T) {
	tt := toshitest.New(t)
	answerJobPages(tt, map[string]*workspacev1.ListJobsResponse{
		"":       jobsPage([]string{"job-1", "job-2"}, "page-2"),
		"page-2": jobsPage([]string{"job-3"}, "page-3"),
		"page-3": jobsPage([]string{"job-4"}, ""),
	})

	var got []string
	for job, err := range tt.Client().Workload.Jobs.All(context.Background(), &workspacev1.ListJobsRequest{}) {
		if err != nil {
			t.Fatalf("All: %v", err)
		}
		got = append(got, job.GetId())
	}
	if want := []string{"job-1", "job-2", "job-3", "job-4"}; !slices.Equal(got, want) {
		t.Fatalf("jobs %v, want %v", got, want)
	}
	if want := []string{"", "page-2", "page-3"}; !slices.Equal(requestedJobPages(t, tt), want) {
		t.Fatalf("page tokens %v, want %v", requestedJobPages(t, tt), want)
	}
}

// TestAllStopsWhenTheLoopBreaks proves a break stops the pages: the page the
// loop left is the only one asked for.
func TestAllStopsWhenTheLoopBreaks(t *testing.T) {
	tt := toshitest.New(t)
	answerJobPages(tt, map[string]*workspacev1.ListJobsResponse{
		"": jobsPage([]string{"job-1", "job-2"}, "page-2"),
	})

	seen := 0
	for job, err := range tt.Client().Workload.Jobs.All(context.Background(), &workspacev1.ListJobsRequest{}) {
		if err != nil {
			t.Fatalf("All: %v", err)
		}
		if job.GetId() != "job-1" {
			t.Fatalf("first job %q, want job-1", job.GetId())
		}
		seen++
		break
	}
	if seen != 1 {
		t.Fatalf("%d jobs, want 1", seen)
	}
	if tokens := requestedJobPages(t, tt); !slices.Equal(tokens, []string{""}) {
		t.Fatalf("page tokens %v, want one call with no token", tokens)
	}
}

// TestAllYieldsAnErrorOnce proves a failed page yields its error once, after
// the items of the page before it, and ends the sequence.
func TestAllYieldsAnErrorOnce(t *testing.T) {
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.WorkloadServiceListJobsProcedure,
		func(_ context.Context, req *workspacev1.ListJobsRequest) (*workspacev1.ListJobsResponse, error) {
			if req.GetPageToken() == "" {
				return jobsPage([]string{"job-1"}, "page-2"), nil
			}
			return nil, connect.NewError(connect.CodeNotFound, errors.New("page two is gone"))
		})

	var jobs []string
	var errs []error
	for job, err := range tt.Client().Workload.Jobs.All(context.Background(), &workspacev1.ListJobsRequest{}) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		jobs = append(jobs, job.GetId())
	}
	if !slices.Equal(jobs, []string{"job-1"}) {
		t.Fatalf("jobs %v, want job-1", jobs)
	}
	if len(errs) != 1 {
		t.Fatalf("%d errors, want 1", len(errs))
	}
	if code := connect.CodeOf(errs[0]); code != connect.CodeNotFound {
		t.Fatalf("error %v has code %v, want not_found", errs[0], code)
	}
}

// TestAllLeavesTheRequestUnchanged proves the caller's request keeps its
// fields, page_token included, and that the first page starts from it.
func TestAllLeavesTheRequestUnchanged(t *testing.T) {
	tt := toshitest.New(t)
	answerJobPages(tt, map[string]*workspacev1.ListJobsResponse{
		"page-1": jobsPage([]string{"job-2"}, "page-2"),
		"page-2": jobsPage([]string{"job-3"}, ""),
	})

	req := &workspacev1.ListJobsRequest{PageSize: 2, PageToken: "page-1"}
	want := proto.Clone(req).(*workspacev1.ListJobsRequest)
	for job, err := range tt.Client().Workload.Jobs.All(context.Background(), req) {
		if err != nil {
			t.Fatalf("All: %v", err)
		}
		if job.GetId() == "" {
			t.Fatal("job without an id")
		}
	}
	if !proto.Equal(req, want) {
		t.Fatalf("request %v, want %v", req, want)
	}
	if tokens := requestedJobPages(t, tt); !slices.Equal(tokens, []string{"page-1", "page-2"}) {
		t.Fatalf("page tokens %v, want the caller's token and then the next", tokens)
	}
}

// TestPaginateOverAGeneratedList proves Paginate follows the pages of a
// generated List call, which this module writes no iterator for.
func TestPaginateOverAGeneratedList(t *testing.T) {
	tt := toshitest.New(t)
	projects := map[string]*workspacev1.ListProjectsResponse{
		"":       {Projects: []*workspacev1.Project{{Name: "one"}}, NextPageToken: "page-2"},
		"page-2": {Projects: []*workspacev1.Project{{Name: "two"}}, NextPageToken: "page-3"},
		"page-3": {Projects: []*workspacev1.Project{{Name: "three"}}},
	}
	toshitest.On(tt, workspacev1connect.ProjectServiceListProjectsProcedure,
		func(_ context.Context, req *workspacev1.ListProjectsRequest) (*workspacev1.ListProjectsResponse, error) {
			page, ok := projects[req.GetPageToken()]
			if !ok {
				return nil, connect.NewError(connect.CodeNotFound, errors.New("no page for token "+req.GetPageToken()))
			}
			return page, nil
		})

	var got []string
	for project, err := range toshi.Paginate(context.Background(), &workspacev1.ListProjectsRequest{},
		tt.Client().Project.ListProjects,
		func(resp *workspacev1.ListProjectsResponse) []*workspacev1.Project { return resp.Projects }) {
		if err != nil {
			t.Fatalf("Paginate: %v", err)
		}
		got = append(got, project.GetName())
	}
	if want := []string{"one", "two", "three"}; !slices.Equal(got, want) {
		t.Fatalf("projects %v, want %v", got, want)
	}
	if calls := tt.Calls(); len(calls) != 3 {
		t.Fatalf("%d calls, want 3", len(calls))
	}
}
