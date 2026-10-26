package toshi

import (
	"context"
	"net/http"
	"runtime"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

// headerClient sets the headers the client owns on every HTTP request: the
// token as a bearer token and the User-Agent. It sets them on a copy below
// every interceptor, so no interceptor sees the token. It never sets
// Toshi-Tenant. When there is no token it sends nothing and returns a
// CodeUnauthenticated error, which connect-go passes on with its code.
type headerClient struct {
	next      connect.HTTPClient
	token     func(context.Context) (string, error)
	userAgent string
}

func (h headerClient) Do(req *http.Request) (*http.Response, error) {
	token, err := h.token(req.Context())
	if err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, connect.NewError(connect.CodeUnauthenticated, err)
	}
	out := *req
	out.Header = req.Header.Clone()
	out.Header.Set("Authorization", "Bearer "+token)
	out.Header.Set("User-Agent", h.userAgent)
	return h.next.Do(&out)
}

// userAgent returns the User-Agent the client sends, with product in front
// when it is set.
func userAgent(product string) string {
	ua := "toshi-go/" + Version + " connect-go/" + connect.Version + " (" + runtime.Version() + ")"
	if product == "" {
		return ua
	}
	return product + " " + ua
}

// scopeInterceptor fills an empty project and environment on unary requests
// and on the messages a streaming client sends. It never overwrites a value
// the caller set.
type scopeInterceptor struct {
	project     string
	environment string
}

func (s scopeInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		fillScope(requestMessage(req), s.project, s.environment)
		return next(ctx, req)
	}
}

func (s scopeInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		return &scopeClientConn{
			StreamingClientConn: next(ctx, spec),
			project:             s.project,
			environment:         s.environment,
		}
	}
}

func (s scopeInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// scopeClientConn fills the scope on each message a streaming client sends.
type scopeClientConn struct {
	connect.StreamingClientConn
	project     string
	environment string
}

func (c *scopeClientConn) Send(msg any) error {
	if message, ok := msg.(proto.Message); ok {
		fillStreamScope(message, c.project, c.environment)
	}
	return c.StreamingClientConn.Send(msg)
}

// fillStreamScope fills a streaming request's scope, reaching the start a
// bidi Execute request carries.
func fillStreamScope(msg proto.Message, project, environment string) {
	if msg == nil {
		return
	}
	if request, ok := msg.(*workspacev1.ExecuteRequest); ok {
		if start := request.GetStart(); start != nil {
			fillScope(start, project, environment)
		}
		return
	}
	fillScope(msg, project, environment)
}

// requestMessage returns a unary request's protobuf message, or nil.
func requestMessage(req connect.AnyRequest) proto.Message {
	msg, _ := req.Any().(proto.Message)
	return msg
}

// fillScope sets an empty project and, unless the message selects a shared
// set, an empty environment on msg.
func fillScope(msg proto.Message, project, environment string) {
	if msg == nil {
		return
	}
	m := msg.ProtoReflect()
	if !m.IsValid() {
		return
	}
	fields := m.Descriptor().Fields()
	if project != "" {
		if fd := fields.ByName("project"); isString(fd) && m.Get(fd).String() == "" {
			m.Set(fd, protoreflect.ValueOfString(project))
		}
	}
	if environment != "" {
		if fd := fields.ByName("environment"); isString(fd) && m.Get(fd).String() == "" && !sharedSet(m, fields) {
			m.Set(fd, protoreflect.ValueOfString(environment))
		}
	}
}

// sharedSet reports whether msg has a true shared field.
func sharedSet(m protoreflect.Message, fields protoreflect.FieldDescriptors) bool {
	fd := fields.ByName("shared")
	return fd != nil && fd.Kind() == protoreflect.BoolKind && m.Get(fd).Bool()
}

// isString reports whether fd is a singular string field.
func isString(fd protoreflect.FieldDescriptor) bool {
	return fd != nil && fd.Kind() == protoreflect.StringKind && !fd.IsList() && !fd.IsMap()
}
