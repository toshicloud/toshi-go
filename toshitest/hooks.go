package toshitest

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

// On registers hook as the answer to a unary procedure, replacing the state
// machine. The hook's request and response types must be the procedure's, or
// On fails the test.
func On[Req, Resp any](f *Fake, procedure string, hook func(context.Context, *Req) (*Resp, error)) {
	f.addUnary(procedure, reflect.TypeOf((*Req)(nil)), reflect.TypeOf((*Resp)(nil)),
		func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
			message, ok := request.Any().(*Req)
			if !ok {
				return nil, unexpected(procedure, request.Any())
			}
			response, err := hook(ctx, message)
			if err != nil {
				return nil, err
			}
			if response == nil {
				return nil, connect.NewError(connect.CodeInternal,
					fmt.Errorf("toshitest: the hook for %s returned no response", procedure))
			}
			return connect.NewResponse(response), nil
		})
}

// OnServerStream registers hook as the answer to a server-stream procedure,
// replacing the state machine. The hook's request and response types must be
// the procedure's, or OnServerStream fails the test.
func OnServerStream[Req, Resp any](f *Fake, procedure string, hook func(context.Context, *Req, *ServerStream[Resp]) error) {
	f.addStream(procedure, connect.StreamTypeServer, reflect.TypeOf((*Req)(nil)), reflect.TypeOf((*Resp)(nil)),
		func(ctx context.Context, conn connect.StreamingHandlerConn, request proto.Message) error {
			message, ok := any(request).(*Req)
			if !ok {
				return unexpected(procedure, request)
			}
			return hook(ctx, message, &ServerStream[Resp]{conn: conn})
		})
}

// OnBidi registers hook as the answer to a bidirectional-stream procedure,
// replacing the state machine. The hook's request and response types must be
// the procedure's, or OnBidi fails the test.
func OnBidi[Req, Resp any](f *Fake, procedure string, hook func(context.Context, *BidiStream[Req, Resp]) error) {
	f.addStream(procedure, connect.StreamTypeBidi, reflect.TypeOf((*Req)(nil)), reflect.TypeOf((*Resp)(nil)),
		func(ctx context.Context, conn connect.StreamingHandlerConn, _ proto.Message) error {
			return hook(ctx, newBidiStream[Req, Resp](f, conn))
		})
}

// ServerStream sends the messages of a server stream.
type ServerStream[Resp any] struct {
	conn connect.StreamingHandlerConn
}

// Send sends msg.
func (s *ServerStream[Resp]) Send(msg *Resp) error { return s.conn.Send(msg) }

// BidiStream reads and writes the messages of a bidirectional stream.
type BidiStream[Req, Resp any] struct {
	fake *Fake
	conn connect.StreamingHandlerConn
	once sync.Once
}

// newBidiStream returns a stream over conn.
func newBidiStream[Req, Resp any](f *Fake, conn connect.StreamingHandlerConn) *BidiStream[Req, Resp] {
	return &BidiStream[Req, Resp]{fake: f, conn: conn}
}

// Receive waits for the client's next message.
func (s *BidiStream[Req, Resp]) Receive() (*Req, error) {
	message := new(Req)
	if err := s.conn.Receive(message); err != nil {
		return nil, err
	}
	s.once.Do(func() {
		s.fake.record(Call{
			Procedure: s.conn.Spec().Procedure,
			Request:   messageOf(message),
			Header:    s.conn.RequestHeader().Clone(),
		})
	})
	return message, nil
}

// Send sends msg.
func (s *BidiStream[Req, Resp]) Send(msg *Resp) error { return s.conn.Send(msg) }

// messageOf returns msg as a proto message, or nil.
func messageOf[T any](msg *T) proto.Message {
	message, _ := any(msg).(proto.Message)
	return message
}

// addUnary checks a unary hook against its procedure and registers it.
func (f *Fake) addUnary(procedure string, request, response reflect.Type, hook unaryHook) {
	f.checkHook(procedure, connect.StreamTypeUnary, request, response)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hooks[procedure] = hook
}

// addStream checks a streaming hook against its procedure and registers it.
func (f *Fake) addStream(procedure string, kind connect.StreamType, request, response reflect.Type, hook streamHook) {
	f.checkHook(procedure, kind, request, response)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streamHooks[procedure] = hook
}

// checkHook fails the test when a hook does not fit its procedure.
func (f *Fake) checkHook(procedure string, kind connect.StreamType, request, response reflect.Type) {
	if err := validateHook(procedure, kind, request, response); err != nil {
		f.fatalf("%v", err)
	}
}

// validateHook reports why a hook does not fit its procedure, or returns nil.
func validateHook(procedure string, kind connect.StreamType, request, response reflect.Type) error {
	known, ok := apiProcedures[procedure]
	if !ok {
		return fmt.Errorf("toshitest: %s is not a procedure of the Workspace API %s", procedure, apiPackage)
	}
	if known.streamType != kind {
		return fmt.Errorf("toshitest: %s is %s; hook it with %s",
			procedure, kindName(known.streamType), hookName(known.streamType))
	}
	if request != known.request || response != known.response {
		return fmt.Errorf("toshitest: %s takes %s and returns %s; the hook takes %s and returns %s",
			procedure, known.request, known.response, request, response)
	}
	return nil
}

// kindName names a procedure's kind.
func kindName(kind connect.StreamType) string {
	switch kind {
	case connect.StreamTypeUnary:
		return "a unary procedure"
	case connect.StreamTypeServer:
		return "a server stream"
	case connect.StreamTypeBidi:
		return "a bidirectional stream"
	case connect.StreamTypeClient:
		return "a client stream"
	}
	return "an unknown procedure"
}

// hookName names the function that hooks a procedure of this kind.
func hookName(kind connect.StreamType) string {
	switch kind {
	case connect.StreamTypeServer:
		return "toshitest.OnServerStream"
	case connect.StreamTypeBidi:
		return "toshitest.OnBidi"
	}
	return "toshitest.On"
}
