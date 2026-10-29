package toshi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"
)

// retryInfo is google.rpc.RetryInfo, built here so the tests need no module
// for it.
var retryInfo = func() protoreflect.MessageDescriptor {
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("google/rpc/retry_info.proto"),
		Package:    proto.String("google.rpc"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"google/protobuf/duration.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("RetryInfo"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     proto.String("retry_delay"),
				JsonName: proto.String("retryDelay"),
				Number:   proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".google.protobuf.Duration"),
			}},
		}},
	}, protoregistry.GlobalFiles)
	if err != nil {
		panic(err)
	}
	return file.Messages().ByName("RetryInfo")
}()

// exhausted is a RESOURCE_EXHAUSTED refusal as the API's rate limit makes
// one: a RetryInfo detail of retryInfoDelay and a Retry-After of
// retryAfter, each left out when zero or empty.
func exhausted(t *testing.T, retryInfoDelay time.Duration, retryAfter string) error {
	t.Helper()
	cerr := connect.NewError(connect.CodeResourceExhausted, errors.New("too many requests"))
	if retryAfter != "" {
		cerr.Meta().Set("Retry-After", retryAfter)
	}
	if retryInfoDelay > 0 {
		info := dynamicpb.NewMessage(retryInfo)
		info.Set(retryInfo.Fields().ByName("retry_delay"), protoreflect.ValueOfMessage(durationpb.New(retryInfoDelay).ProtoReflect()))
		detail, err := connect.NewErrorDetail(info)
		if err != nil {
			t.Fatal(err)
		}
		cerr.AddDetail(detail)
	}
	return cerr
}

// projects answers ListProjects with errs in turn, then with an empty list.
type projects struct {
	workspacev1connect.UnimplementedProjectServiceHandler
	errs  []error
	calls atomic.Int32
}

func (p *projects) ListProjects(context.Context, *connect.Request[workspacev1.ListProjectsRequest]) (*connect.Response[workspacev1.ListProjectsResponse], error) {
	n := int(p.calls.Add(1))
	if n <= len(p.errs) {
		return nil, p.errs[n-1]
	}
	return connect.NewResponse(&workspacev1.ListProjectsResponse{}), nil
}

// sleeps replaces retrySleep for the test: each wait is recorded and costs
// no time, and around runs before it returns.
func sleeps(t *testing.T, around func()) *[]time.Duration {
	t.Helper()
	var got []time.Duration
	real := retrySleep
	retrySleep = func(ctx context.Context, d time.Duration) error {
		got = append(got, d)
		if around != nil {
			around()
		}
		if err := ctx.Err(); err != nil {
			return ctxError(ctx)
		}
		return nil
	}
	t.Cleanup(func() { retrySleep = real })
	return &got
}

// waitPolicy backs off at most a millisecond on the first retry, so a
// recorded wait longer than that is the server's, and caps a wait at 5 s.
var waitPolicy = RetryPolicy{MaxAttempts: 4, InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Second}

func waitClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := New(WithServer(server.URL), WithToken("token"), WithRetryPolicy(waitPolicy))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func projectHandler(p *projects) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(workspacev1connect.NewProjectServiceHandler(p))
	return mux
}

func TestRetryWaitsForResourceExhausted(t *testing.T) {
	cases := []struct {
		name           string
		retryInfoDelay time.Duration
		retryAfter     string
		want           []time.Duration
	}{
		{name: "RetryInfo", retryInfoDelay: 3 * time.Second, want: []time.Duration{3 * time.Second}},
		{name: "Retry-After", retryAfter: "2", want: []time.Duration{2 * time.Second}},
		{name: "both, RetryInfo wins", retryInfoDelay: 4 * time.Second, retryAfter: "1", want: []time.Duration{4 * time.Second}},
		{name: "capped at MaxBackoff", retryInfoDelay: 30 * time.Second, retryAfter: "30", want: []time.Duration{5 * time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sleeps(t, nil)
			p := &projects{errs: []error{exhausted(t, tc.retryInfoDelay, tc.retryAfter)}}
			_, err := waitClient(t, projectHandler(p)).Project().ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{}))
			if err != nil {
				t.Fatalf("ListProjects: %v", err)
			}
			if calls := p.calls.Load(); calls != 2 {
				t.Fatalf("calls = %d, want 2", calls)
			}
			if len(*got) != len(tc.want) || (*got)[0] != tc.want[0] {
				t.Fatalf("waits = %v, want %v", *got, tc.want)
			}
		})
	}
}

func TestResourceExhaustedWithoutWaitIsNotRetried(t *testing.T) {
	got := sleeps(t, nil)
	p := &projects{errs: []error{exhausted(t, 0, "")}}
	_, err := waitClient(t, projectHandler(p)).Project().ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{}))
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("err = %v, want RESOURCE_EXHAUSTED", err)
	}
	if calls := p.calls.Load(); calls != 1 || len(*got) != 0 {
		t.Fatalf("calls = %d, waits = %v; want one call and no wait", calls, *got)
	}
}

func TestRetryWaitEndsWithTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := sleeps(t, cancel)
	p := &projects{errs: []error{exhausted(t, 3*time.Second, "3")}}
	_, err := waitClient(t, projectHandler(p)).Project().ListProjects(ctx, connect.NewRequest(&workspacev1.ListProjectsRequest{}))
	if connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("err = %v, want CANCELED", err)
	}
	if calls := p.calls.Load(); calls != 1 || len(*got) != 1 || (*got)[0] != 3*time.Second {
		t.Fatalf("calls = %d, waits = %v; want one call and one 3s wait", calls, *got)
	}
}

// plainFirst answers the first request with status and a Retry-After of
// retryAfter in plain text, as a proxy would, and the rest with next.
func plainFirst(status int, retryAfter string, next http.Handler) http.Handler {
	var served atomic.Bool
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if served.CompareAndSwap(false, true) {
			w.Header().Set("Retry-After", retryAfter)
			w.WriteHeader(status)
			_, _ = w.Write([]byte("slow down\n"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func TestRetryWaitsForHTTP429(t *testing.T) {
	got := sleeps(t, nil)
	p := &projects{}
	_, err := waitClient(t, plainFirst(http.StatusTooManyRequests, "2", projectHandler(p))).Project().ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{}))
	if err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	if len(*got) != 1 || (*got)[0] != 2*time.Second {
		t.Fatalf("waits = %v, want [2s]", *got)
	}
}

func TestUnavailableKeepsItsBackoff(t *testing.T) {
	got := sleeps(t, nil)
	p := &projects{}
	_, err := waitClient(t, plainFirst(http.StatusServiceUnavailable, "7", projectHandler(p))).Project().ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{}))
	if err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	if len(*got) > 1 || (len(*got) == 1 && (*got)[0] > time.Millisecond) {
		t.Fatalf("waits = %v, want the backoff alone, at most 1ms", *got)
	}
}
