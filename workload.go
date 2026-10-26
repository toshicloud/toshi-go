package toshi

import (
	"context"

	"connectrpc.com/connect"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
	workspacev1connect "github.com/toshicloud/toshi-api/workspacev1/workspacev1connect"
)

// workloadCall is the generated WorkloadService client and the SDK client
// whose retry policy the calls follow. The three type clients carry it.
type workloadCall struct {
	client *Client
	svc    workspacev1connect.WorkloadServiceClient
}

// WorkloadClient holds the calls of WorkloadService: a job's calls under
// Jobs, a sandbox's under Sandboxes, a service's under Services, and the
// calls that span the three on Workload itself.
type WorkloadClient struct {
	// Jobs holds a job's calls.
	Jobs JobsClient
	// Sandboxes holds a sandbox's calls.
	Sandboxes SandboxesClient
	// Services holds a service's calls.
	Services ServicesClient

	call workloadCall
}

// JobsClient holds the calls of WorkloadService that act on a job.
type JobsClient struct{ call workloadCall }

// SandboxesClient holds the calls of WorkloadService that act on a sandbox.
type SandboxesClient struct{ call workloadCall }

// ServicesClient holds the calls of WorkloadService that act on a service.
type ServicesClient struct{ call workloadCall }

// newWorkloadClient builds the workload client over one generated client and
// the SDK client whose retry policy its calls follow.
func newWorkloadClient(client *Client, svc workspacev1connect.WorkloadServiceClient) WorkloadClient {
	call := workloadCall{client: client, svc: svc}
	return WorkloadClient{
		Jobs:      JobsClient{call: call},
		Sandboxes: SandboxesClient{call: call},
		Services:  ServicesClient{call: call},
		call:      call,
	}
}

// ListEvents returns a page of the event log from the position the request's
// page token names. WatchEvents is the live form: with an empty cursor it
// pages ListEvents, reports each listed event, and then watches from the last
// page's cursor.
func (w WorkloadClient) ListEvents(ctx context.Context, req *workspacev1.ListEventsRequest) (*workspacev1.ListEventsResponse, error) {
	resp, err := w.call.svc.ListEvents(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// WatchEvents starts an event watch. With an empty cursor it pages
// ListEvents, reports each listed event, and then watches from the last
// page's cursor.
func (w WorkloadClient) WatchEvents(ctx context.Context, req *workspacev1.WatchEventsRequest) (*EventWatch, error) {
	return newEventWatch(ctx, w.call, req)
}

// GetOperation returns one operation.
func (w WorkloadClient) GetOperation(ctx context.Context, req *workspacev1.GetOperationRequest) (*workspacev1.GetOperationResponse, error) {
	resp, err := w.call.svc.GetOperation(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Create creates a job and returns it.
func (j JobsClient) Create(ctx context.Context, req *workspacev1.CreateJobRequest) (*workspacev1.CreateJobResponse, error) {
	resp, err := j.call.svc.CreateJob(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Get returns one job.
func (j JobsClient) Get(ctx context.Context, req *workspacev1.GetJobRequest) (*workspacev1.GetJobResponse, error) {
	resp, err := j.call.svc.GetJob(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// List returns the jobs that match every filter that is set, a page at a
// time.
func (j JobsClient) List(ctx context.Context, req *workspacev1.ListJobsRequest) (*workspacev1.ListJobsResponse, error) {
	resp, err := j.call.svc.ListJobs(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Update changes the fields the mask names and returns the job.
func (j JobsClient) Update(ctx context.Context, req *workspacev1.UpdateJobRequest) (*workspacev1.UpdateJobResponse, error) {
	resp, err := j.call.svc.UpdateJob(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Delete cancels a job that has not ended and then removes it.
func (j JobsClient) Delete(ctx context.Context, req *workspacev1.DeleteJobRequest) (*workspacev1.DeleteJobResponse, error) {
	resp, err := j.call.svc.DeleteJob(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Cancel cancels a job that has not ended and returns it, kept.
func (j JobsClient) Cancel(ctx context.Context, req *workspacev1.CancelJobRequest) (*workspacev1.CancelJobResponse, error) {
	resp, err := j.call.svc.CancelJob(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Create creates a sandbox and returns it, queued.
func (s SandboxesClient) Create(ctx context.Context, req *workspacev1.CreateSandboxRequest) (*workspacev1.CreateSandboxResponse, error) {
	resp, err := s.call.svc.CreateSandbox(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Get returns one sandbox.
func (s SandboxesClient) Get(ctx context.Context, req *workspacev1.GetSandboxRequest) (*workspacev1.GetSandboxResponse, error) {
	resp, err := s.call.svc.GetSandbox(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// List returns the sandboxes that match every filter that is set, a page at
// a time.
func (s SandboxesClient) List(ctx context.Context, req *workspacev1.ListSandboxesRequest) (*workspacev1.ListSandboxesResponse, error) {
	resp, err := s.call.svc.ListSandboxes(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Update changes the fields the mask names and returns the sandbox.
func (s SandboxesClient) Update(ctx context.Context, req *workspacev1.UpdateSandboxRequest) (*workspacev1.UpdateSandboxResponse, error) {
	resp, err := s.call.svc.UpdateSandbox(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Delete stops a sandbox that has not ended and then removes it.
func (s SandboxesClient) Delete(ctx context.Context, req *workspacev1.DeleteSandboxRequest) (*workspacev1.DeleteSandboxResponse, error) {
	resp, err := s.call.svc.DeleteSandbox(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// CancelExecution stops an execution.
func (s SandboxesClient) CancelExecution(ctx context.Context, req *workspacev1.CancelExecutionRequest) (*workspacev1.CancelExecutionResponse, error) {
	resp, err := s.call.svc.CancelExecution(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// WatchTelemetry returns a sandbox's resource samples as its host reports
// them, from a cursor on.
func (s SandboxesClient) WatchTelemetry(ctx context.Context, req *workspacev1.WatchTelemetryRequest) (*connect.ServerStreamForClient[workspacev1.WatchTelemetryResponse], error) {
	return s.call.svc.WatchTelemetry(ctx, connect.NewRequest(req))
}

// WatchGuestLog returns what a sandbox's VM writes to its serial console,
// from a cursor on.
func (s SandboxesClient) WatchGuestLog(ctx context.Context, req *workspacev1.WatchGuestLogRequest) (*connect.ServerStreamForClient[workspacev1.WatchGuestLogResponse], error) {
	return s.call.svc.WatchGuestLog(ctx, connect.NewRequest(req))
}

// CreatePreviewShareLink mints a share link for one of a running sandbox's
// previews.
func (s SandboxesClient) CreatePreviewShareLink(ctx context.Context, req *workspacev1.CreatePreviewShareLinkRequest) (*workspacev1.CreatePreviewShareLinkResponse, error) {
	resp, err := s.call.svc.CreatePreviewShareLink(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ListPreviewShareLinks returns a sandbox's share links, oldest first, never
// with the link itself.
func (s SandboxesClient) ListPreviewShareLinks(ctx context.Context, req *workspacev1.ListPreviewShareLinksRequest) (*workspacev1.ListPreviewShareLinksResponse, error) {
	resp, err := s.call.svc.ListPreviewShareLinks(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// RevokePreviewShareLink revokes a share link.
func (s SandboxesClient) RevokePreviewShareLink(ctx context.Context, req *workspacev1.RevokePreviewShareLinkRequest) (*workspacev1.RevokePreviewShareLinkResponse, error) {
	resp, err := s.call.svc.RevokePreviewShareLink(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Create creates a service with its recipe as revision 1, created and not
// started.
func (s ServicesClient) Create(ctx context.Context, req *workspacev1.CreateServiceRequest) (*workspacev1.CreateServiceResponse, error) {
	resp, err := s.call.svc.CreateService(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Get returns one service.
func (s ServicesClient) Get(ctx context.Context, req *workspacev1.GetServiceRequest) (*workspacev1.GetServiceResponse, error) {
	resp, err := s.call.svc.GetService(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// List returns the services that match every filter that is set, a page at a
// time.
func (s ServicesClient) List(ctx context.Context, req *workspacev1.ListServicesRequest) (*workspacev1.ListServicesResponse, error) {
	resp, err := s.call.svc.ListServices(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Update changes the fields the mask names and returns the service.
func (s ServicesClient) Update(ctx context.Context, req *workspacev1.UpdateServiceRequest) (*workspacev1.UpdateServiceResponse, error) {
	resp, err := s.call.svc.UpdateService(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Delete stops a service that has not ended and then removes it.
func (s ServicesClient) Delete(ctx context.Context, req *workspacev1.DeleteServiceRequest) (*workspacev1.DeleteServiceResponse, error) {
	resp, err := s.call.svc.DeleteService(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Start starts a service.
func (s ServicesClient) Start(ctx context.Context, req *workspacev1.StartServiceRequest) (*workspacev1.StartServiceResponse, error) {
	resp, err := s.call.svc.StartService(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Stop stops a service: its workload moves back to created and its live run
// is stopped.
func (s ServicesClient) Stop(ctx context.Context, req *workspacev1.StopServiceRequest) (*workspacev1.StopServiceResponse, error) {
	resp, err := s.call.svc.StopService(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// SetMode sets a service's mode: always on, or on demand.
func (s ServicesClient) SetMode(ctx context.Context, req *workspacev1.SetServiceModeRequest) (*workspacev1.SetServiceModeResponse, error) {
	resp, err := s.call.svc.SetServiceMode(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Wake wakes a started on-demand service that is asleep.
func (s ServicesClient) Wake(ctx context.Context, req *workspacev1.WakeServiceRequest) (*workspacev1.WakeServiceResponse, error) {
	resp, err := s.call.svc.WakeService(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Hold takes or renews the caller's hold on a started service.
func (s ServicesClient) Hold(ctx context.Context, req *workspacev1.HoldServiceRequest) (*workspacev1.HoldServiceResponse, error) {
	resp, err := s.call.svc.HoldService(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Release drops the caller's hold on a service.
func (s ServicesClient) Release(ctx context.Context, req *workspacev1.ReleaseServiceRequest) (*workspacev1.ReleaseServiceResponse, error) {
	resp, err := s.call.svc.ReleaseService(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ListRuns returns a service's runs, the newest first.
func (s ServicesClient) ListRuns(ctx context.Context, req *workspacev1.ListServiceRunsRequest) (*workspacev1.ListServiceRunsResponse, error) {
	resp, err := s.call.svc.ListServiceRuns(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}
