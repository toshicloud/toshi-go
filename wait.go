package toshi

import (
	"context"

	"connectrpc.com/connect"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

// WaitWorkload asks again while the bound passed without the workload
// reaching one of the requested states, and returns the workload when it did.
// The caller's context ends the loop.
func (c *Client) WaitWorkload(ctx context.Context, req *workspacev1.WaitWorkloadRequest) (*workspacev1.WaitWorkloadResponse, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, ctxError(ctx)
		}
		resp, err := c.Workspace.WaitWorkload(ctx, connect.NewRequest(req))
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
func (c *Client) WaitOperation(ctx context.Context, req *workspacev1.WaitOperationRequest) (*workspacev1.Operation, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, ctxError(ctx)
		}
		resp, err := c.Workspace.WaitOperation(ctx, connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		op := resp.Msg.GetOperation()
		if op.GetDone() {
			return op, nil
		}
	}
}
