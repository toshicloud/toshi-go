package toshitest

import (
	"reflect"
	"sort"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// apiPackage holds the Workspace API's protos.
const apiPackage = "toshi.workspace.v1"

// procedure is one procedure of the Workspace API.
type procedure struct {
	name       string
	streamType connect.StreamType
	request    reflect.Type
	response   reflect.Type
}

// apiProcedures holds every procedure of the Workspace API, keyed by the path
// the client calls. It is built from the registered descriptors, so a service
// added to the API appears here without an edit.
var apiProcedures = buildProcedures()

func buildProcedures() map[string]procedure {
	found := map[string]procedure{}
	forEachService(func(service protoreflect.ServiceDescriptor) {
		methods := service.Methods()
		for i := 0; i < methods.Len(); i++ {
			method := methods.Get(i)
			name := "/" + string(service.FullName()) + "/" + string(method.Name())
			found[name] = procedure{
				name:       name,
				streamType: streamType(method),
				request:    messageType(method.Input()),
				response:   messageType(method.Output()),
			}
		}
	})
	return found
}

// forEachService calls visit for each service of the Workspace API.
func forEachService(visit func(protoreflect.ServiceDescriptor)) {
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if string(file.Package()) != apiPackage {
			return true
		}
		services := file.Services()
		for i := 0; i < services.Len(); i++ {
			visit(services.Get(i))
		}
		return true
	})
}

// serviceNames returns the full names of the Workspace API's services.
func serviceNames() []string {
	var names []string
	forEachService(func(service protoreflect.ServiceDescriptor) {
		names = append(names, string(service.FullName()))
	})
	sort.Strings(names)
	return names
}

// streamType returns how a method streams.
func streamType(method protoreflect.MethodDescriptor) connect.StreamType {
	switch {
	case method.IsStreamingClient() && method.IsStreamingServer():
		return connect.StreamTypeBidi
	case method.IsStreamingClient():
		return connect.StreamTypeClient
	case method.IsStreamingServer():
		return connect.StreamTypeServer
	}
	return connect.StreamTypeUnary
}

// messageType returns the Go type of a message, for example
// *workspacev1.CreateJobRequest.
func messageType(desc protoreflect.MessageDescriptor) reflect.Type {
	message, err := protoregistry.GlobalTypes.FindMessageByName(desc.FullName())
	if err != nil {
		panic("toshitest: " + string(desc.FullName()) + ": " + err.Error())
	}
	return reflect.TypeOf(message.New().Interface())
}
