package toshi

import (
	"context"
	"iter"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"
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
	jobs      JobsClient
	sandboxes SandboxesClient
	services  ServicesClient

	call workloadCall
}

// Jobs holds a job's calls.
func (w WorkloadClient) Jobs() JobsClient { return w.jobs }

// Sandboxes holds a sandbox's calls.
func (w WorkloadClient) Sandboxes() SandboxesClient { return w.sandboxes }

// Services holds a service's calls.
func (w WorkloadClient) Services() ServicesClient { return w.services }

// JobsClient holds the calls of WorkloadService that act on a job.
type JobsClient struct{ call workloadCall }

// SandboxesClient holds the calls of WorkloadService that act on a sandbox.
type SandboxesClient struct{ call workloadCall }

// ServicesClient holds the calls of WorkloadService that act on a service.
type ServicesClient struct{ call workloadCall }

// newWorkloadClient builds the workload client over one generated client and
// the SDK client whose retry policy its calls follow.
func newWorkloadClient(client *Client, svc workspacev1connect.WorkloadServiceClient) *WorkloadClient {
	call := workloadCall{client: client, svc: svc}
	return &WorkloadClient{
		jobs:      JobsClient{call: call},
		sandboxes: SandboxesClient{call: call},
		services:  ServicesClient{call: call},
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

// AllEvents yields every event ListEvents lists, following the page token
// from one page to the next.
func (w WorkloadClient) AllEvents(ctx context.Context, req *workspacev1.ListEventsRequest) iter.Seq2[*workspacev1.Event, error] {
	return Paginate(ctx, req, w.call.svc.ListEvents, func(resp *workspacev1.ListEventsResponse) []*workspacev1.Event {
		return resp.Events
	})
}

// WatchEvents starts an event watch. With an empty cursor it pages
// ListEvents, reports each listed event, and then watches from the last
// page's cursor.
func (w WorkloadClient) WatchEvents(ctx context.Context, req *workspacev1.WatchEventsRequest) (*EventWatch, error) {
	return newEventWatch(ctx, w.call, req)
}

// EmitEvent appends an event to a job's, a sandbox's or a service's event
// log, as its guest emits one, and returns it: type EMITTED, the name given
// and the data. The workload need not be running. The name is 1 to 200 bytes
// of UTF-8 with no space or control character, and its first dot-separated
// segment is none Toshi keeps for its own events (job, sandbox, run,
// service, recording, agent, takeover, spend, egress, host, github, webhook,
// trigger and mail); the data is empty or JSON of at most 64 KiB.
func (w WorkloadClient) EmitEvent(ctx context.Context, req *workspacev1.EmitWorkloadEventRequest) (*workspacev1.EmitWorkloadEventResponse, error) {
	resp, err := w.call.svc.EmitWorkloadEvent(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// GetOperation returns one operation.
func (w WorkloadClient) GetOperation(ctx context.Context, req *workspacev1.GetOperationRequest) (*workspacev1.GetOperationResponse, error) {
	resp, err := w.call.svc.GetOperation(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// AnswerPrompt answers a prompt a job, a sandbox or a service waits on, as
// the person the token acts for, and returns the event that records the
// answer and who gave it.
func (w WorkloadClient) AnswerPrompt(ctx context.Context, req *workspacev1.AnswerPromptRequest) (*workspacev1.AnswerPromptResponse, error) {
	resp, err := w.call.svc.AnswerPrompt(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// HoldWorkload places an operator's hold on a sandbox or service: a running
// one is stopped with reason POLICY and cause OPERATOR, and a stopped one only
// gets the hold. A job cannot be held; cancel it instead. Only an operator's
// token may call it, and the call carries the operator's reason and workspace
// in the Toshi-Operator-Reason and Toshi-Workspace headers, which an
// interceptor sets; see "Operator calls" in the README.
func (w WorkloadClient) HoldWorkload(ctx context.Context, req *workspacev1.HoldWorkloadRequest) (*workspacev1.HoldWorkloadResponse, error) {
	resp, err := w.call.svc.HoldWorkload(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ReleaseWorkload lifts an operator's hold on a sandbox or service and starts
// nothing. Only an operator's token may call it, with the same
// Toshi-Operator-Reason and Toshi-Workspace headers HoldWorkload needs; see
// "Operator calls" in the README.
func (w WorkloadClient) ReleaseWorkload(ctx context.Context, req *workspacev1.ReleaseWorkloadRequest) (*workspacev1.ReleaseWorkloadResponse, error) {
	resp, err := w.call.svc.ReleaseWorkload(ctx, connect.NewRequest(req))
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

// All yields every job that matches every filter that is set, following the
// page token from one page to the next.
func (j JobsClient) All(ctx context.Context, req *workspacev1.ListJobsRequest) iter.Seq2[*workspacev1.Job, error] {
	return Paginate(ctx, req, j.call.svc.ListJobs, func(resp *workspacev1.ListJobsResponse) []*workspacev1.Job {
		return resp.Jobs
	})
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

// All yields every sandbox that matches every filter that is set, following
// the page token from one page to the next.
func (s SandboxesClient) All(ctx context.Context, req *workspacev1.ListSandboxesRequest) iter.Seq2[*workspacev1.Sandbox, error] {
	return Paginate(ctx, req, s.call.svc.ListSandboxes, func(resp *workspacev1.ListSandboxesResponse) []*workspacev1.Sandbox {
		return resp.Sandboxes
	})
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

// Start queues a run of a sandbox on its kept VM and returns the sandbox.
func (s SandboxesClient) Start(ctx context.Context, req *workspacev1.StartSandboxRequest) (*workspacev1.StartSandboxResponse, error) {
	resp, err := s.call.svc.StartSandbox(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Stop ends a sandbox's run and every execution in it, keeps its VM and
// returns the sandbox, back in created.
func (s SandboxesClient) Stop(ctx context.Context, req *workspacev1.StopSandboxRequest) (*workspacev1.StopSandboxResponse, error) {
	resp, err := s.call.svc.StopSandbox(ctx, connect.NewRequest(req))
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

// AllPreviewShareLinks yields every share link ListPreviewShareLinks
// returns, oldest first and never with the link itself, following the page
// token from one page to the next.
func (s SandboxesClient) AllPreviewShareLinks(ctx context.Context, req *workspacev1.ListPreviewShareLinksRequest) iter.Seq2[*workspacev1.PreviewShareLink, error] {
	return Paginate(ctx, req, s.call.svc.ListPreviewShareLinks, func(resp *workspacev1.ListPreviewShareLinksResponse) []*workspacev1.PreviewShareLink {
		return resp.ShareLinks
	})
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

// All yields every service that matches every filter that is set, following
// the page token from one page to the next.
func (s ServicesClient) All(ctx context.Context, req *workspacev1.ListServicesRequest) iter.Seq2[*workspacev1.Service, error] {
	return Paginate(ctx, req, s.call.svc.ListServices, func(resp *workspacev1.ListServicesResponse) []*workspacev1.Service {
		return resp.Services
	})
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

// AllRuns yields every run ListRuns returns, the newest first, following the
// page token from one page to the next.
func (s ServicesClient) AllRuns(ctx context.Context, req *workspacev1.ListServiceRunsRequest) iter.Seq2[*workspacev1.ServiceRun, error] {
	return Paginate(ctx, req, s.call.svc.ListServiceRuns, func(resp *workspacev1.ListServiceRunsResponse) []*workspacev1.ServiceRun {
		return resp.Runs
	})
}

// ListRevisions returns a service's revisions, oldest first.
func (s ServicesClient) ListRevisions(ctx context.Context, req *workspacev1.ListServiceRevisionsRequest) (*workspacev1.ListServiceRevisionsResponse, error) {
	resp, err := s.call.svc.ListServiceRevisions(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// GetRevision returns one revision of a service.
func (s ServicesClient) GetRevision(ctx context.Context, req *workspacev1.GetServiceRevisionRequest) (*workspacev1.GetServiceRevisionResponse, error) {
	resp, err := s.call.svc.GetServiceRevision(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Rollback selects a revision the service already has as its current one.
func (s ServicesClient) Rollback(ctx context.Context, req *workspacev1.RollbackServiceRequest) (*workspacev1.RollbackServiceResponse, error) {
	resp, err := s.call.svc.RollbackService(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ListActivations returns a service's activations, oldest first: each
// selection of a revision and what came of it.
func (s ServicesClient) ListActivations(ctx context.Context, req *workspacev1.ListServiceActivationsRequest) (*workspacev1.ListServiceActivationsResponse, error) {
	resp, err := s.call.svc.ListServiceActivations(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// CreateServiceShareLink mints a share link for one of a service's provided
// names, the front endpoint's unless the request names another http endpoint.
// The link is in this answer only.
func (s ServicesClient) CreateServiceShareLink(ctx context.Context, req *workspacev1.CreateServiceShareLinkRequest) (*workspacev1.CreateServiceShareLinkResponse, error) {
	resp, err := s.call.svc.CreateServiceShareLink(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ListServiceShareLinks returns a service's share links, for all of its
// endpoints, the oldest first and never with the link itself.
func (s ServicesClient) ListServiceShareLinks(ctx context.Context, req *workspacev1.ListServiceShareLinksRequest) (*workspacev1.ListServiceShareLinksResponse, error) {
	resp, err := s.call.svc.ListServiceShareLinks(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// AllServiceShareLinks yields every share link ListServiceShareLinks
// returns, the oldest first and never with the link itself, following the
// page token from one page to the next.
func (s ServicesClient) AllServiceShareLinks(ctx context.Context, req *workspacev1.ListServiceShareLinksRequest) iter.Seq2[*workspacev1.ServiceShareLink, error] {
	return Paginate(ctx, req, s.call.svc.ListServiceShareLinks, func(resp *workspacev1.ListServiceShareLinksResponse) []*workspacev1.ServiceShareLink {
		return resp.ShareLinks
	})
}

// RevokeServiceShareLink revokes a service's share link: it admits no
// device again and its open connections end.
func (s ServicesClient) RevokeServiceShareLink(ctx context.Context, req *workspacev1.RevokeServiceShareLinkRequest) (*workspacev1.RevokeServiceShareLinkResponse, error) {
	resp, err := s.call.svc.RevokeServiceShareLink(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// SetAlarm creates or replaces the service's alarm with this name and
// returns it. At each instant of its schedule the alarm publishes its event,
// with its data, on one of the service's event endpoints. Replacing an alarm
// raises its generation and schedules it again from now.
func (s ServicesClient) SetAlarm(ctx context.Context, req *workspacev1.SetServiceAlarmRequest) (*workspacev1.SetServiceAlarmResponse, error) {
	resp, err := s.call.svc.SetServiceAlarm(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// GetAlarm returns one alarm and its most recent occurrences, newest first.
func (s ServicesClient) GetAlarm(ctx context.Context, req *workspacev1.GetServiceAlarmRequest) (*workspacev1.GetServiceAlarmResponse, error) {
	resp, err := s.call.svc.GetServiceAlarm(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ListAlarms returns all of the service's alarms, by name, in one answer.
// The request has no page token, so there is no All iterator.
func (s ServicesClient) ListAlarms(ctx context.Context, req *workspacev1.ListServiceAlarmsRequest) (*workspacev1.ListServiceAlarmsResponse, error) {
	resp, err := s.call.svc.ListServiceAlarms(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// DeleteAlarm removes an alarm and its occurrences. Messages it already
// published stay on the endpoint.
func (s ServicesClient) DeleteAlarm(ctx context.Context, req *workspacev1.DeleteServiceAlarmRequest) (*workspacev1.DeleteServiceAlarmResponse, error) {
	resp, err := s.call.svc.DeleteServiceAlarm(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// PauseAlarm stops an alarm publishing until it is resumed. Pausing a paused
// alarm changes nothing.
func (s ServicesClient) PauseAlarm(ctx context.Context, req *workspacev1.PauseServiceAlarmRequest) (*workspacev1.PauseServiceAlarmResponse, error) {
	resp, err := s.call.svc.PauseServiceAlarm(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ResumeAlarm starts a paused alarm again from the next instant of its
// schedule; instants that passed while it was paused are not published. An
// alarm with a single instant that has passed stays without a next instant.
// Resuming an alarm that is not paused changes nothing.
func (s ServicesClient) ResumeAlarm(ctx context.Context, req *workspacev1.ResumeServiceAlarmRequest) (*workspacev1.ResumeServiceAlarmResponse, error) {
	resp, err := s.call.svc.ResumeServiceAlarm(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// RunAlarmNow publishes the alarm's event once, now, paused or not, and
// returns the occurrence. It does not move the alarm's next instant.
func (s ServicesClient) RunAlarmNow(ctx context.Context, req *workspacev1.RunServiceAlarmNowRequest) (*workspacev1.RunServiceAlarmNowResponse, error) {
	resp, err := s.call.svc.RunServiceAlarmNow(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// StartSignIn runs one of the service's recipe's sign_in programs and streams
// what it writes until it ends: the started message names the sign-in and
// when it is stopped, the output messages carry its standard output and
// standard error, and the ended message says how it ended, with its exit code
// when it exited. The output is never stored.
//
// The caller receives the started message and keeps its sign_in_id and
// ends_at, then reads the output messages. When the program asks for
// something, call AnswerSignIn with the sign_in_id and one line of at most
// 4 KiB with no newline, or CancelSignIn to end it early. The ended message
// comes last; Close the stream after it.
//
// The stream is not retried. A stream that ends before the ended message, as
// it does when the client leaves, also ends the sign-in, cancelled: start a
// new sign-in, and ask GetSignInStatus whether the program is signed in. A
// sign-in runs for 15 minutes at most, past which it ends TIMED_OUT. Only one
// sign-in of a program runs at a time: a second fails with
// FAILED_PRECONDITION while the first is under way.
func (s ServicesClient) StartSignIn(ctx context.Context, req *workspacev1.StartServiceSignInRequest) (*connect.ServerStreamForClient[workspacev1.StartServiceSignInResponse], error) {
	return s.call.svc.StartServiceSignIn(ctx, connect.NewRequest(req))
}

// AnswerSignIn writes one line to a running sign-in's standard input, for
// its program to read. The answer is one line of at most 4 KiB, with no
// newline in it, and is never stored.
func (s ServicesClient) AnswerSignIn(ctx context.Context, req *workspacev1.AnswerServiceSignInRequest) (*workspacev1.AnswerServiceSignInResponse, error) {
	resp, err := s.call.svc.AnswerServiceSignIn(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// CancelSignIn ends a running sign-in: its program is cancelled and its
// stream ends CANCELLED. It returns once the cancel is asked for, not once
// the sign-in has ended.
func (s ServicesClient) CancelSignIn(ctx context.Context, req *workspacev1.CancelServiceSignInRequest) (*workspacev1.CancelServiceSignInResponse, error) {
	resp, err := s.call.svc.CancelServiceSignIn(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// GetSignInStatus runs the status commands of the service's recipe and
// reports, for each sign_in program, whether it is signed in and what its
// status command wrote. Program names one program; empty asks for every
// program the recipe declares, in its order. Each command runs for 30 seconds
// at most, past which it is not signed in. An output is cut at 4 KiB, ending
// with "[cut]". The status is not stored.
func (s ServicesClient) GetSignInStatus(ctx context.Context, req *workspacev1.GetServiceSignInStatusRequest) (*workspacev1.GetServiceSignInStatusResponse, error) {
	resp, err := s.call.svc.GetServiceSignInStatus(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}
