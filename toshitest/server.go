package toshitest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"
)

// services serves the Workspace API from the generated Unimplemented
// handlers. The fake's interceptor answers every call before them.
type services struct {
	workspacev1connect.UnimplementedWorkloadServiceHandler
	workspacev1connect.UnimplementedProjectServiceHandler
	workspacev1connect.UnimplementedSecretServiceHandler
	workspacev1connect.UnimplementedWebhookServiceHandler
	workspacev1connect.UnimplementedDisplayServiceHandler
	workspacev1connect.UnimplementedChannelServiceHandler
	workspacev1connect.UnimplementedRecordingServiceHandler
	workspacev1connect.UnimplementedVolumeServiceHandler
	workspacev1connect.UnimplementedArtifactServiceHandler
	workspacev1connect.UnimplementedArtifactUploadServiceHandler
	workspacev1connect.UnimplementedWorkspaceServiceHandler
	workspacev1connect.UnimplementedHostJobServiceHandler
	workspacev1connect.UnimplementedHostServiceHandler
	workspacev1connect.UnimplementedTokenServiceHandler
	workspacev1connect.UnimplementedAuditEventServiceHandler
	workspacev1connect.UnimplementedFillerServiceHandler
	workspacev1connect.UnimplementedStorageProviderServiceHandler
	workspacev1connect.UnimplementedProductServiceHandler
	workspacev1connect.UnimplementedDiscoveryServiceHandler
	workspacev1connect.UnimplementedAccountServiceHandler
	workspacev1connect.UnimplementedImageServiceHandler
	workspacev1connect.UnimplementedConnectionServiceHandler
	workspacev1connect.UnimplementedRoleServiceHandler
	workspacev1connect.UnimplementedTriggerServiceHandler
	workspacev1connect.UnimplementedGrantServiceHandler
	workspacev1connect.UnimplementedDomainServiceHandler
}

// start mounts every service over TLS with HTTP/2.
func (f *Fake) start() {
	mux := http.NewServeMux()
	options := connect.WithInterceptors((*interceptor)(f))
	service := &services{}
	mount := func(path string, handler http.Handler) {
		mux.Handle(path, handler)
		f.mounted[path] = true
	}
	path, handler := workspacev1connect.NewWorkloadServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewProjectServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewSecretServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewWebhookServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewDisplayServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewChannelServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewRecordingServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewVolumeServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewArtifactServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewArtifactUploadServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewWorkspaceServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewHostJobServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewHostServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewTokenServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewAuditEventServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewFillerServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewStorageProviderServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewProductServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewDiscoveryServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewAccountServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewImageServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewConnectionServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewRoleServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewTriggerServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewGrantServiceHandler(service, options)
	mount(path, handler)
	path, handler = workspacev1connect.NewDomainServiceHandler(service, options)
	mount(path, handler)

	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	f.server = server
}

// interceptor answers every call from a hook or the state machine.
type interceptor Fake

// WrapUnary answers a unary call.
func (i *interceptor) WrapUnary(connect.UnaryFunc) connect.UnaryFunc {
	f := (*Fake)(i)
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		message, _ := request.Any().(proto.Message)
		f.record(Call{
			Procedure: request.Spec().Procedure,
			Request:   message,
			Header:    request.Header().Clone(),
		})
		hook, ok := f.unary(request.Spec().Procedure)
		if !ok {
			return nil, f.notImplemented(request.Spec().Procedure)
		}
		return hook(ctx, request)
	}
}

// WrapStreamingClient leaves the client's streams alone.
func (i *interceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// WrapStreamingHandler answers a streaming call.
func (i *interceptor) WrapStreamingHandler(connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	f := (*Fake)(i)
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		procedure := conn.Spec().Procedure
		var request proto.Message
		if conn.Spec().StreamType == connect.StreamTypeServer {
			known, ok := apiProcedures[procedure]
			if !ok {
				return f.notImplemented(procedure)
			}
			message := reflect.New(known.request.Elem()).Interface().(proto.Message)
			if err := conn.Receive(message); err != nil {
				return err
			}
			request = message
			f.record(Call{
				Procedure: procedure,
				Request:   message,
				Header:    conn.RequestHeader().Clone(),
			})
		}
		hook, ok := f.stream(procedure)
		if !ok {
			return f.notImplemented(procedure)
		}
		return hook(ctx, conn, request)
	}
}

// unary returns the hook that answers a unary procedure: the test's hook
// first, then the state machine.
func (f *Fake) unary(procedure string) (unaryHook, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if hook, ok := f.hooks[procedure]; ok {
		return hook, true
	}
	hook, ok := f.builtin[procedure]
	return hook, ok
}

// stream returns the hook that answers a streaming procedure: the test's hook
// first, then the state machine.
func (f *Fake) stream(procedure string) (streamHook, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if hook, ok := f.streamHooks[procedure]; ok {
		return hook, true
	}
	hook, ok := f.builtinStream[procedure]
	return hook, ok
}

// notImplemented answers a procedure the server has no answer for.
func (f *Fake) notImplemented(procedure string) error {
	name := "toshitest.On"
	if known, ok := apiProcedures[procedure]; ok {
		name = hookName(known.streamType)
	}
	return connect.NewError(connect.CodeUnimplemented,
		fmt.Errorf("toshitest: %s has no built-in behaviour; add a hook with %s", procedure, name))
}
