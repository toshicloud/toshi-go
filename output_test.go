package toshi

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

// TestWatchOutputEndReleasesStream proves the end message releases the held
// HTTP response at once, without the caller closing the watch.
func TestWatchOutputEndReleasesStream(t *testing.T) {
	handlerDone := make(chan struct{})
	server := newTestServer(t, &backend{
		watchOutput: func(ctx context.Context, _ *connect.Request[workspacev1.WatchOutputRequest], stream *connect.ServerStream[workspacev1.WatchOutputResponse]) error {
			code := int32(0)
			if err := stream.Send(&workspacev1.WatchOutputResponse{Message: &workspacev1.WatchOutputResponse_End{End: &workspacev1.OutputEnd{ExitCode: &code}}}); err != nil {
				return err
			}
			<-ctx.Done()
			close(handlerDone)
			return nil
		},
	})
	client := newTestClient(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch, err := client.Workload.Jobs.WatchOutput(ctx, &workspacev1.WatchOutputRequest{Id: "w1"})
	if err != nil {
		t.Fatalf("WatchOutput: %v", err)
	}
	defer func() { _ = watch.Close() }()
	if !watch.Receive() || watch.Msg().GetEnd() == nil {
		t.Fatalf("no end message: %v", watch.Err())
	}
	if watch.Receive() || watch.Err() != nil {
		t.Fatalf("watch did not end cleanly: %v", watch.Err())
	}
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the end message left the held response open")
	}
}
