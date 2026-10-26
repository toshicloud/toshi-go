package toshi

import (
	"context"
	"math/rand/v2"
	"time"

	"connectrpc.com/connect"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

// waitMinInterval is the shortest time between the start of one Wait call
// and the next, and waitMaxJitter the most added to it at random.
const (
	waitMinInterval = time.Second
	waitMaxJitter   = waitMinInterval / 5
)

// waitNow and waitAfter are the clock of the Wait loops. Tests replace them
// so that a pause costs no time.
var (
	waitNow   = time.Now
	waitAfter = time.After
)

// pauseWait returns once waitMinInterval plus up to waitMaxJitter has passed
// since start, at once when it already has, or when the context ends.
func pauseWait(ctx context.Context, start time.Time) error {
	d := waitMinInterval + time.Duration(rand.Int64N(int64(waitMaxJitter)+1)) - waitNow().Sub(start)
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitAfter(d):
		return nil
	}
}

// Wait asks again while the bound passed without the job reaching one of the
// requested states, and returns it as it then is. The caller's context ends
// the loop.
func (j JobsClient) Wait(ctx context.Context, req *workspacev1.WaitJobRequest) (*workspacev1.WaitJobResponse, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, ctxError(ctx)
		}
		start := waitNow()
		resp, err := j.call.svc.WaitJob(ctx, connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		if resp.Msg.GetReached() {
			return resp.Msg, nil
		}
		if err := pauseWait(ctx, start); err != nil {
			return nil, ctxError(ctx)
		}
	}
}

// Wait asks again while the bound passed without the sandbox reaching one of
// the requested states, and returns it as it then is. The caller's context
// ends the loop.
func (s SandboxesClient) Wait(ctx context.Context, req *workspacev1.WaitSandboxRequest) (*workspacev1.WaitSandboxResponse, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, ctxError(ctx)
		}
		start := waitNow()
		resp, err := s.call.svc.WaitSandbox(ctx, connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		if resp.Msg.GetReached() {
			return resp.Msg, nil
		}
		if err := pauseWait(ctx, start); err != nil {
			return nil, ctxError(ctx)
		}
	}
}

// Wait asks again while the bound passed without the service reaching one of
// the requested states, and returns it as it then is. The caller's context
// ends the loop.
func (s ServicesClient) Wait(ctx context.Context, req *workspacev1.WaitServiceRequest) (*workspacev1.WaitServiceResponse, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, ctxError(ctx)
		}
		start := waitNow()
		resp, err := s.call.svc.WaitService(ctx, connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		if resp.Msg.GetReached() {
			return resp.Msg, nil
		}
		if err := pauseWait(ctx, start); err != nil {
			return nil, ctxError(ctx)
		}
	}
}

// WaitOperation asks again while the operation is not done, and returns it
// when it is. The caller's context ends the loop.
func (w WorkloadClient) WaitOperation(ctx context.Context, req *workspacev1.WaitOperationRequest) (*workspacev1.Operation, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, ctxError(ctx)
		}
		start := waitNow()
		resp, err := w.call.svc.WaitOperation(ctx, connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		op := resp.Msg.GetOperation()
		if op.GetDone() {
			return op, nil
		}
		if err := pauseWait(ctx, start); err != nil {
			return nil, ctxError(ctx)
		}
	}
}
