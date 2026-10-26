package toshi_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi"
	"go.toshicloud.io/toshi/toshitest"
)

// errorInfo is google.rpc.ErrorInfo, built here so the tests need no module
// for the googleapis types.
var errorInfo = func() protoreflect.MessageDescriptor {
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("google/rpc/error_details.proto"),
		Package: proto.String("google.rpc"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("ErrorInfo"),
			Field: []*descriptorpb.FieldDescriptorProto{
				stringField("reason", 1),
				stringField("domain", 2),
				{
					Name:     proto.String("metadata"),
					JsonName: proto.String("metadata"),
					Number:   proto.Int32(3),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					TypeName: proto.String(".google.rpc.ErrorInfo.MetadataEntry"),
				},
			},
			NestedType: []*descriptorpb.DescriptorProto{{
				Name:    proto.String("MetadataEntry"),
				Field:   []*descriptorpb.FieldDescriptorProto{stringField("key", 1), stringField("value", 2)},
				Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
			}},
		}},
	}, nil)
	if err != nil {
		panic(err)
	}
	return file.Messages().ByName("ErrorInfo")
}()

func stringField(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		JsonName: proto.String(name),
		Number:   proto.Int32(number),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
	}
}

// refusal returns the API's refusal of a call for a product: code, with an
// ErrorInfo of domain and reason and the metadata.
func refusal(t *testing.T, code connect.Code, domain, reason string, metadata map[string]string) *connect.Error {
	t.Helper()
	info := dynamicpb.NewMessage(errorInfo)
	info.Set(errorInfo.Fields().ByName("reason"), protoreflect.ValueOfString(reason))
	info.Set(errorInfo.Fields().ByName("domain"), protoreflect.ValueOfString(domain))
	entries := info.Mutable(errorInfo.Fields().ByName("metadata")).Map()
	for k, v := range metadata {
		entries.Set(protoreflect.ValueOfString(k).MapKey(), protoreflect.ValueOfString(v))
	}
	detail, err := connect.NewErrorDetail(info)
	if err != nil {
		t.Fatalf("error detail: %v", err)
	}
	cerr := connect.NewError(code, errors.New("refused"))
	cerr.AddDetail(detail)
	return cerr
}

func TestProductErrorFields(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     connect.Code
		reason   string
		metadata map[string]string
		want     toshi.ProductError
	}{
		{
			name:     "not installed",
			code:     connect.CodeUnimplemented,
			reason:   "PRODUCT_NOT_INSTALLED",
			metadata: map[string]string{"product": "runners", "feature": "runners.enabled", "switch": "build"},
			want:     toshi.ProductError{Product: "runners", Reason: toshi.ReasonProductNotInstalled, Feature: "runners.enabled", Code: connect.CodeUnimplemented},
		},
		{
			name:     "disabled while draining",
			code:     connect.CodeFailedPrecondition,
			reason:   "PRODUCT_DISABLED",
			metadata: map[string]string{"product": "runners", "feature": "runners.enabled", "switch": "deployment", "state": "draining"},
			want:     toshi.ProductError{Product: "runners", Reason: toshi.ReasonProductDisabled, Feature: "runners.enabled", State: "draining", Code: connect.CodeFailedPrecondition},
		},
		{
			name:     "not entitled",
			code:     connect.CodePermissionDenied,
			reason:   "PRODUCT_NOT_ENTITLED",
			metadata: map[string]string{"product": "runners", "feature": "runners.enabled", "switch": "entitlement"},
			want:     toshi.ProductError{Product: "runners", Reason: toshi.ReasonProductNotEntitled, Feature: "runners.enabled", Code: connect.CodePermissionDenied},
		},
		{
			name:     "turned off",
			code:     connect.CodeFailedPrecondition,
			reason:   "PRODUCT_TURNED_OFF",
			metadata: map[string]string{"product": "acme_ci", "feature": "acme_ci.enabled", "switch": "workspace"},
			want:     toshi.ProductError{Product: "acme_ci", Reason: toshi.ReasonProductTurnedOff, Feature: "acme_ci.enabled", Code: connect.CodeFailedPrecondition},
		},
		{
			name:     "access not accepted",
			code:     connect.CodeFailedPrecondition,
			reason:   "ACCESS_NOT_ACCEPTED",
			metadata: map[string]string{"product": "acme_ci", "access": "projects.read,secrets.read"},
			want:     toshi.ProductError{Product: "acme_ci", Reason: toshi.ReasonAccessNotAccepted, Access: []string{"projects.read", "secrets.read"}, Code: connect.CodeFailedPrecondition},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := toshitest.New(t)
			toshitest.On(tt, workspacev1connect.ProjectServiceListProjectsProcedure,
				func(context.Context, *workspacev1.ListProjectsRequest) (*workspacev1.ListProjectsResponse, error) {
					return nil, refusal(t, tc.code, "toshi.run", tc.reason, tc.metadata)
				})
			_, err := tt.Client().Project().ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{}))
			var pe *toshi.ProductError
			if !errors.As(err, &pe) {
				t.Fatalf("errors.As found no ProductError in %v", err)
			}
			got := *pe
			got.Err = nil
			if got.Product != tc.want.Product || got.Reason != tc.want.Reason || got.Feature != tc.want.Feature ||
				got.State != tc.want.State || !slices.Equal(got.Access, tc.want.Access) || got.Code != tc.want.Code {
				t.Fatalf("ProductError %+v, want %+v", got, tc.want)
			}
			if connect.CodeOf(err) != tc.code || pe.Error() != err.Error() {
				t.Fatalf("code %v, text %q", connect.CodeOf(err), err.Error())
			}
		})
	}
}

func TestOtherErrorsAreNotProductErrors(t *testing.T) {
	for name, cerr := range map[string]*connect.Error{
		"no detail":    connect.NewError(connect.CodeFailedPrecondition, errors.New("refused")),
		"other domain": refusal(t, connect.CodeFailedPrecondition, "example.com", "PRODUCT_TURNED_OFF", map[string]string{"product": "x"}),
		"other reason": refusal(t, connect.CodeFailedPrecondition, "toshi.run", "QUOTA_EXCEEDED", map[string]string{"product": "x"}),
	} {
		t.Run(name, func(t *testing.T) {
			tt := toshitest.New(t)
			toshitest.On(tt, workspacev1connect.ProjectServiceListProjectsProcedure,
				func(context.Context, *workspacev1.ListProjectsRequest) (*workspacev1.ListProjectsResponse, error) {
					return nil, cerr
				})
			_, err := tt.Client().Project().ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{}))
			var pe *toshi.ProductError
			if err == nil || errors.As(err, &pe) {
				t.Fatalf("err %v, ProductError %v", err, pe)
			}
		})
	}
}

func TestProductErrorThroughWaitWatchAndExecute(t *testing.T) {
	turnedOff := func(t *testing.T) error {
		return refusal(t, connect.CodeFailedPrecondition, "toshi.run", "PRODUCT_TURNED_OFF",
			map[string]string{"product": "acme_ci", "feature": "acme_ci.enabled", "switch": "workspace"})
	}
	check := func(t *testing.T, err error) {
		t.Helper()
		var pe *toshi.ProductError
		if !errors.As(err, &pe) || pe.Reason != toshi.ReasonProductTurnedOff || pe.Product != "acme_ci" {
			t.Fatalf("no ProductError in %v", err)
		}
	}

	t.Run("wait", func(t *testing.T) {
		tt := toshitest.New(t)
		toshitest.On(tt, workspacev1connect.WorkloadServiceWaitJobProcedure,
			func(context.Context, *workspacev1.WaitJobRequest) (*workspacev1.WaitJobResponse, error) {
				return nil, turnedOff(t)
			})
		_, err := tt.Client().Workload().Jobs().Wait(context.Background(), &workspacev1.WaitJobRequest{})
		check(t, err)
	})

	t.Run("watch", func(t *testing.T) {
		tt := toshitest.New(t)
		toshitest.OnServerStream(tt, workspacev1connect.WorkloadServiceWatchJobsProcedure,
			func(context.Context, *workspacev1.WatchJobsRequest, *toshitest.ServerStream[workspacev1.WatchJobsResponse]) error {
				return turnedOff(t)
			})
		watch, err := tt.Client().Workload().Jobs().Watch(context.Background(), &workspacev1.WatchJobsRequest{Cursor: "c1"})
		if err != nil {
			t.Fatalf("Watch: %v", err)
		}
		defer func() { _ = watch.Close() }()
		for watch.Receive() {
		}
		check(t, watch.Err())
	})

	t.Run("execute", func(t *testing.T) {
		tt := toshitest.New(t)
		toshitest.OnBidi(tt, workspacev1connect.WorkloadServiceExecuteProcedure,
			func(context.Context, *toshitest.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
				return turnedOff(t)
			})
		client := tt.Client(toshi.WithScope("proj", "env"))
		_, err := client.Workload().Sandboxes().Execute(context.Background(), &workspacev1.ExecuteStart{SandboxId: "sb-1"})
		check(t, err)
	})
}
