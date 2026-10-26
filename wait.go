package toshi

import (
	"context"

	"connectrpc.com/connect"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

// Wait asks again while the bound passed without the job reaching one of the
// requested states, and returns it as it then is. The caller's context ends
// the loop.
func (j JobsClient) Wait(ctx context.Context, req *workspacev1.WaitJobRequest) (*workspacev1.WaitJobResponse, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, ctxError(ctx)
		}
		resp, err := j.call.svc.WaitJob(ctx, connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		if resp.Msg.GetReached() {
			return resp.Msg, nil
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
		resp, err := s.call.svc.WaitSandbox(ctx, connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		if resp.Msg.GetReached() {
			return resp.Msg, nil
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
		resp, err := s.call.svc.WaitService(ctx, connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		if resp.Msg.GetReached() {
			return resp.Msg, nil
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
		resp, err := w.call.svc.WaitOperation(ctx, connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		op := resp.Msg.GetOperation()
		if op.GetDone() {
			return op, nil
		}
	}
}
