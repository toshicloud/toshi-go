package toshi_test

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"testing"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi"
	"go.toshicloud.io/toshi/toshitest"
)

// deprecatingTransport stands in for a server that marks every call
// deprecated: it adds header to every response.
type deprecatingTransport struct {
	next   http.RoundTripper
	header http.Header
}

func (d deprecatingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := d.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	for name, values := range d.header {
		res.Header[name] = values
	}
	return res, nil
}

// warnings collects the warnings a client gives.
type warnings struct {
	mu   sync.Mutex
	list []string
}

func (w *warnings) add(message string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.list = append(w.list, message)
}

func (w *warnings) all() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.list)
}

// deprecatedClient is a client of tt whose every answer carries header, with
// its warnings going to w.
func deprecatedClient(tt *toshitest.Fake, header http.Header, w *warnings) *toshi.Client {
	httpClient := tt.HTTPClient()
	wrapped := *httpClient
	wrapped.Transport = deprecatingTransport{next: httpClient.Transport, header: header}
	return tt.Client(toshi.WithHTTPClient(&wrapped), toshi.WithWarn(w.add))
}

// TestDeprecatedCallWarnsOncePerProcess proves a call answered with
// Deprecation, Sunset and a deprecation link warns once per process, from
// any client, naming the call, the dates and the link.
func TestDeprecatedCallWarnsOncePerProcess(t *testing.T) {
	tt := toshitest.New(t)
	answerListProjects(tt)
	header := http.Header{
		"Deprecation": {"@1767225600"},
		"Sunset":      {"Wed, 01 Jul 2026 00:00:00 GMT"},
		"Link":        {`<https://example.com/docs/deprecations>; rel="deprecation"; type="text/html"`},
	}
	w := &warnings{}
	ctx := context.Background()
	for range 2 {
		client := deprecatedClient(tt, header, w)
		if _, err := client.Project().ListProjects(ctx, connect.NewRequest(&workspacev1.ListProjectsRequest{})); err != nil {
			t.Fatalf("ListProjects: %v", err)
		}
	}
	want := []string{
		"toshi: /toshi.workspace.v1.ProjectService/ListProjects is deprecated since 2026-01-01 and is removed after 2026-07-01; see https://example.com/docs/deprecations",
	}
	if got := w.all(); !slices.Equal(got, want) {
		t.Fatalf("warnings %q, want %q", got, want)
	}
}

// TestDeprecatedCallWarnsOnAnError proves an error answer that marks the call
// deprecated warns too and reaches the caller unchanged.
func TestDeprecatedCallWarnsOnAnError(t *testing.T) {
	tt := toshitest.New(t)
	toshitest.On(tt, workspacev1connect.ProjectServiceGetProjectProcedure,
		func(context.Context, *workspacev1.GetProjectRequest) (*workspacev1.GetProjectResponse, error) {
			return nil, connect.NewError(connect.CodeNotFound, nil)
		})
	w := &warnings{}
	client := deprecatedClient(tt, http.Header{"Deprecation": {"@1767225600"}}, w)
	_, err := client.Project().GetProject(context.Background(), connect.NewRequest(&workspacev1.GetProjectRequest{}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("GetProject: %v, want NOT_FOUND", err)
	}
	want := []string{"toshi: /toshi.workspace.v1.ProjectService/GetProject is deprecated since 2026-01-01"}
	if got := w.all(); !slices.Equal(got, want) {
		t.Fatalf("warnings %q, want %q", got, want)
	}
}

// TestDeprecatedStreamWarns proves a server stream warns once its response
// headers arrive.
func TestDeprecatedStreamWarns(t *testing.T) {
	tt := toshitest.New(t)
	toshitest.OnServerStream(tt, workspacev1connect.EndpointServiceExportEndpointLogProcedure,
		func(_ context.Context, _ *workspacev1.ExportEndpointLogRequest, stream *toshitest.ServerStream[workspacev1.ExportEndpointLogResponse]) error {
			return stream.Send(&workspacev1.ExportEndpointLogResponse{})
		})
	w := &warnings{}
	client := deprecatedClient(tt, http.Header{"Sunset": {"Wed, 01 Jul 2026 00:00:00 GMT"}}, w)
	stream, err := client.Endpoint().ExportEndpointLog(context.Background(), connect.NewRequest(&workspacev1.ExportEndpointLogRequest{}))
	if err != nil {
		t.Fatalf("ExportEndpointLog: %v", err)
	}
	for stream.Receive() {
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("ExportEndpointLog: %v", err)
	}
	want := []string{"toshi: /toshi.workspace.v1.EndpointService/ExportEndpointLog is deprecated and is removed after 2026-07-01"}
	if got := w.all(); !slices.Equal(got, want) {
		t.Fatalf("warnings %q, want %q", got, want)
	}
}

// TestCallWithoutDeprecationDoesNotWarn proves an answer without the headers
// gives no warning.
func TestCallWithoutDeprecationDoesNotWarn(t *testing.T) {
	tt := toshitest.New(t)
	answerListProjects(tt)
	w := &warnings{}
	client := tt.Client(toshi.WithWarn(w.add))
	if _, err := client.Project().ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{})); err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	if got := w.all(); len(got) != 0 {
		t.Fatalf("warnings %q, want none", got)
	}
}
