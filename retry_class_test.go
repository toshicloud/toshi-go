package toshi

import (
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"go.toshicloud.io/api/toshipb"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"
)

// apiMethods returns every method of the Workspace API by procedure.
func apiMethods() map[string]protoreflect.MethodDescriptor {
	found := map[string]protoreflect.MethodDescriptor{}
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if file.Package() != "toshi.workspace.v1" {
			return true
		}
		for i := 0; i < file.Services().Len(); i++ {
			service := file.Services().Get(i)
			for j := 0; j < service.Methods().Len(); j++ {
				method := service.Methods().Get(j)
				found["/"+string(service.FullName())+"/"+string(method.Name())] = method
			}
		}
		return true
	})
	return found
}

// TestRetryClassFollowsTheDescriptors proves every call of the API gets the
// class its descriptor gives it, or its listed exception, with the schema the
// generated clients send and without one.
func TestRetryClassFollowsTheDescriptors(t *testing.T) {
	methods := apiMethods()
	if len(methods) == 0 {
		t.Fatal("no method of toshi.workspace.v1 is registered")
	}
	for procedure, method := range methods {
		want, ok := retryExceptions[procedure]
		if !ok {
			want = derivedRetryClass(method)
		}
		if got := retryClassFor(connect.Spec{Procedure: procedure, Schema: method}); got != want {
			t.Errorf("%s: class %d, want %d", procedure, got, want)
		}
		if got := retryClassFor(connect.Spec{Procedure: procedure}); got != want {
			t.Errorf("%s without a schema: class %d, want %d", procedure, got, want)
		}
	}
	for procedure, want := range map[string]retryClass{
		workspacev1connect.WorkloadServiceGetJobProcedure:    retryRead,
		workspacev1connect.WorkloadServiceCreateJobProcedure: retryKeyed,
		workspacev1connect.WorkloadServiceCancelJobProcedure: retryNone,
		workspacev1connect.WorkloadServiceWatchJobsProcedure: retryNone,
		workspacev1connect.SecretServicePutSecretProcedure:   retryNone,
		"/toshi.workspace.v1.NoService/NoCall":               retryNone,
	} {
		if got := retryClassFor(connect.Spec{Procedure: procedure}); got != want {
			t.Errorf("%s: class %d, want %d", procedure, got, want)
		}
	}
}

// TestRetryExceptionsAreNotDerivable fails when an exception names a call the
// API no longer has, or one the descriptors now give its listed class.
func TestRetryExceptionsAreNotDerivable(t *testing.T) {
	methods := apiMethods()
	for procedure, listed := range retryExceptions {
		method, ok := methods[procedure]
		if !ok {
			t.Errorf("%s is no longer a call of the API", procedure)
			continue
		}
		if derivedRetryClass(method) == listed {
			t.Errorf("%s follows the rule now; remove it from retryExceptions", procedure)
		}
	}
}

// TestRetryClassOfANewCall proves a call the API does not have yet gets a
// class from its descriptor alone.
func TestRetryClassOfANewCall(t *testing.T) {
	level := func(permission string) *descriptorpb.MethodOptions {
		options := &descriptorpb.MethodOptions{}
		proto.SetExtension(options, toshipb.E_Permission, permission)
		return options
	}
	stringField := func(name string) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{
			Name:   proto.String(name),
			Number: proto.Int32(1),
			Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		}
	}
	method := func(name, input string, options *descriptorpb.MethodOptions, stream bool) *descriptorpb.MethodDescriptorProto {
		return &descriptorpb.MethodDescriptorProto{
			Name:            proto.String(name),
			InputType:       proto.String(".example.v1." + input),
			OutputType:      proto.String(".example.v1.Thing"),
			Options:         options,
			ServerStreaming: proto.Bool(stream),
		}
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("example/v1/things.proto"),
		Package: proto.String("example.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("GetThingRequest"), Field: []*descriptorpb.FieldDescriptorProto{stringField("id")}},
			{Name: proto.String("CreateThingRequest"), Field: []*descriptorpb.FieldDescriptorProto{stringField("request_key")}},
			{Name: proto.String("Thing")},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("ThingService"),
			Method: []*descriptorpb.MethodDescriptorProto{
				method("GetThing", "GetThingRequest", level("example.thing.read"), false),
				method("CreateThing", "CreateThingRequest", level("example.thing.write"), false),
				method("RemoveThing", "GetThingRequest", level("example.thing.write"), false),
				method("WatchThings", "GetThingRequest", level("example.thing.read"), true),
			},
		}},
	}, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build the file: %v", err)
	}
	methods := file.Services().Get(0).Methods()
	for name, want := range map[protoreflect.Name]retryClass{
		"GetThing":    retryRead,
		"CreateThing": retryKeyed,
		"RemoveThing": retryNone,
		"WatchThings": retryNone,
	} {
		spec := connect.Spec{Procedure: "/example.v1.ThingService/" + string(name), Schema: methods.ByName(name)}
		if got := retryClassFor(spec); got != want {
			t.Errorf("%s: class %d, want %d", name, got, want)
		}
	}
}
