package toshitest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
	workspacev1connect "github.com/toshicloud/toshi-api/workspacev1/workspacev1connect"
)

// Job is one job of the fake, and what it answers about it.
type Job struct {
	fake *Fake
	id   string
}

// State sets the state the fake reports for the job.
func (j *Job) State(state workspacev1.JobState) *Job {
	created := j.set(func(record *job) {
		record.state = state
	})
	if created {
		j.fake.changedJob(j.id, workspacev1.EventType_EVENT_TYPE_WORKLOAD_STATE_CHANGED)
	}
	return j
}

// Stdout sets the standard output WatchOutput streams.
func (j *Job) Stdout(stdout string) *Job {
	j.set(func(record *job) { record.stdout = stdout })
	return j
}

// Stderr sets the standard error WatchOutput streams.
func (j *Job) Stderr(stderr string) *Job {
	j.set(func(record *job) { record.stderr = stderr })
	return j
}

// ExitCode ends the job with code: the state is ENDED and the end reason
// EXITED.
func (j *Job) ExitCode(code int) *Job {
	created := j.set(func(record *job) {
		record.exitCode = int32(code)
		record.state = workspacev1.JobState_JOB_STATE_ENDED
		record.endReason = workspacev1.EndReason_END_REASON_EXITED
	})
	if created {
		j.fake.changedJob(j.id, workspacev1.EventType_EVENT_TYPE_WORKLOAD_STATE_CHANGED)
	}
	return j
}

// set applies change to the job and reports whether the job was created.
func (j *Job) set(change func(*job)) bool {
	j.fake.mu.Lock()
	defer j.fake.mu.Unlock()
	record := j.fake.jobs[j.id]
	change(record)
	return record.created
}

// Sandbox is one sandbox of the fake, and what it answers about it.
type Sandbox struct {
	fake *Fake
	id   string
}

// State sets the state the fake reports for the sandbox.
func (s *Sandbox) State(state workspacev1.SandboxState) *Sandbox {
	s.fake.mu.Lock()
	record := s.fake.sandboxes[s.id]
	record.state = state
	created := record.created
	s.fake.mu.Unlock()
	if created {
		s.fake.changedSandbox(s.id, workspacev1.EventType_EVENT_TYPE_WORKLOAD_STATE_CHANGED)
	}
	return s
}

// Execution is what Execute answers for one sandbox.
type Execution struct {
	fake    *Fake
	sandbox string
}

// Stdout sets the standard output Execute streams.
func (e *Execution) Stdout(stdout string) *Execution {
	e.fake.mu.Lock()
	defer e.fake.mu.Unlock()
	e.fake.executions[e.sandbox].stdout = stdout
	return e
}

// Stderr sets the standard error Execute streams.
func (e *Execution) Stderr(stderr string) *Execution {
	e.fake.mu.Lock()
	defer e.fake.mu.Unlock()
	e.fake.executions[e.sandbox].stderr = stderr
	return e
}

// ExitCode sets the exit code the execution ends with.
func (e *Execution) ExitCode(code int) *Execution {
	e.fake.mu.Lock()
	defer e.fake.mu.Unlock()
	e.fake.executions[e.sandbox].exitCode = int32(code)
	return e
}

// Job returns the job with this id. A job the fake has not created yet is
// what the next CreateJob answers with. It is running until the test sets its
// state or its exit code.
func (f *Fake) Job(id string) *Job {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.jobs[id]; !ok {
		f.jobs[id] = &job{id: id, state: workspacev1.JobState_JOB_STATE_RUNNING}
		f.jobQueue = append(f.jobQueue, id)
	}
	return &Job{fake: f, id: id}
}

// Sandbox returns the sandbox with this id. A sandbox the fake has not created
// yet is what the next CreateSandbox answers with. It is running until the
// test sets its state.
func (f *Fake) Sandbox(id string) *Sandbox {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.sandboxes[id]; !ok {
		f.sandboxes[id] = &sandbox{id: id, state: workspacev1.SandboxState_SANDBOX_STATE_RUNNING}
		f.sandboxQueue = append(f.sandboxQueue, id)
	}
	return &Sandbox{fake: f, id: id}
}

// Execute returns what Execute answers in a sandbox: an empty command that
// exits 0 until the test sets its output or its exit code. The sandbox is
// registered with Sandbox, so a later GetSandbox finds it.
func (f *Fake) Execute(sandboxID string) *Execution {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.sandboxes[sandboxID]; !ok {
		f.sandboxes[sandboxID] = &sandbox{id: sandboxID, state: workspacev1.SandboxState_SANDBOX_STATE_RUNNING}
	}
	if _, ok := f.executions[sandboxID]; !ok {
		f.executions[sandboxID] = &execution{}
	}
	return &Execution{fake: f, sandbox: sandboxID}
}

// job is a job the fake holds.
type job struct {
	id          string
	project     string
	environment string
	spec        *workspacev1.JobSpec
	labels      map[string]string
	recording   workspacev1.RecordingPolicy

	state     workspacev1.JobState
	endReason workspacev1.EndReason
	exitCode  int32
	stdout    string
	stderr    string

	created bool
}

// proto returns the job as the API reports it.
func (r *job) proto() *workspacev1.Job {
	status := &workspacev1.JobStatus{State: r.state}
	if r.state == workspacev1.JobState_JOB_STATE_ENDED {
		status.EndReason = r.endReason
		if r.endReason == workspacev1.EndReason_END_REASON_EXITED {
			code := r.exitCode
			status.ExitCode = &code
		}
	}
	return &workspacev1.Job{
		Id:          r.id,
		Project:     r.project,
		Environment: r.environment,
		Spec:        r.spec,
		Labels:      r.labels,
		Recording:   r.recording,
		Status:      status,
	}
}

// sandbox is a sandbox the fake holds.
type sandbox struct {
	id          string
	project     string
	environment string
	spec        *workspacev1.SandboxSpec
	labels      map[string]string
	recording   workspacev1.RecordingPolicy

	state     workspacev1.SandboxState
	endReason workspacev1.EndReason

	created bool
}

// proto returns the sandbox as the API reports it.
func (r *sandbox) proto() *workspacev1.Sandbox {
	status := &workspacev1.SandboxStatus{State: r.state}
	if r.state == workspacev1.SandboxState_SANDBOX_STATE_ENDED {
		status.EndReason = r.endReason
	}
	return &workspacev1.Sandbox{
		Id:          r.id,
		Project:     r.project,
		Environment: r.environment,
		Spec:        r.spec,
		Labels:      r.labels,
		Recording:   r.recording,
		Status:      status,
	}
}

// execution is what Execute answers in one sandbox.
type execution struct {
	stdout    string
	stderr    string
	exitCode  int32
	cancelled bool
}

// registerBuiltins registers the procedures the state machine answers.
func (f *Fake) registerBuiltins() {
	f.builtin[workspacev1connect.WorkloadServiceCreateJobProcedure] = unaryAnswer(f.createJob)
	f.builtin[workspacev1connect.WorkloadServiceGetJobProcedure] = unaryAnswer(f.getJob)
	f.builtin[workspacev1connect.WorkloadServiceListJobsProcedure] = unaryAnswer(f.listJobs)
	f.builtin[workspacev1connect.WorkloadServiceCancelJobProcedure] = unaryAnswer(f.cancelJob)
	f.builtin[workspacev1connect.WorkloadServiceWaitJobProcedure] = unaryAnswer(f.waitJob)
	f.builtin[workspacev1connect.WorkloadServiceCreateSandboxProcedure] = unaryAnswer(f.createSandbox)
	f.builtin[workspacev1connect.WorkloadServiceGetSandboxProcedure] = unaryAnswer(f.getSandbox)
	f.builtin[workspacev1connect.WorkloadServiceListSandboxesProcedure] = unaryAnswer(f.listSandboxes)
	f.builtin[workspacev1connect.WorkloadServiceWaitSandboxProcedure] = unaryAnswer(f.waitSandbox)

	f.builtinStream[workspacev1connect.WorkloadServiceWatchJobsProcedure] = f.watchJobs
	f.builtinStream[workspacev1connect.WorkloadServiceWatchOutputProcedure] = f.watchOutput
	f.builtinStream[workspacev1connect.WorkloadServiceWatchSandboxesProcedure] = f.watchSandboxes
	f.builtinStream[workspacev1connect.WorkloadServiceExecuteProcedure] = f.execute
}

// unaryAnswer turns a typed handler into a unary hook.
func unaryAnswer[Req, Resp any](handler func(context.Context, *Req) (*Resp, error)) unaryHook {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		message, ok := request.Any().(*Req)
		if !ok {
			return nil, unexpected(request.Spec().Procedure, request.Any())
		}
		response, err := handler(ctx, message)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(response), nil
	}
}

// createJob answers the next registered job, or creates one.
func (f *Fake) createJob(_ context.Context, request *workspacev1.CreateJobRequest) (*workspacev1.CreateJobResponse, error) {
	f.mu.Lock()
	if record, ok := f.jobKeys[request.GetRequestKey()]; ok && request.GetRequestKey() != "" {
		job := record.proto()
		f.mu.Unlock()
		return &workspacev1.CreateJobResponse{Job: job}, nil
	}
	record := f.newJob()
	record.project = request.GetProject()
	record.environment = request.GetEnvironment()
	record.spec = request.GetSpec()
	record.labels = maps.Clone(request.GetLabels())
	record.recording = request.GetRecording()
	record.created = true
	if request.GetRequestKey() != "" {
		f.jobKeys[request.GetRequestKey()] = record
	}
	f.jobCreated = append(f.jobCreated, record.id)
	job := record.proto()
	f.mu.Unlock()
	f.changedJob(record.id, workspacev1.EventType_EVENT_TYPE_WORKLOAD_CREATED)
	return &workspacev1.CreateJobResponse{Job: job}, nil
}

// newJob returns the next job CreateJob answers with.
func (f *Fake) newJob() *job {
	if len(f.jobQueue) > 0 {
		id := f.jobQueue[0]
		f.jobQueue = f.jobQueue[1:]
		return f.jobs[id]
	}
	for {
		f.nextJob++
		id := "job-" + strconv.Itoa(f.nextJob)
		if _, taken := f.jobs[id]; taken {
			continue
		}
		record := &job{id: id, state: workspacev1.JobState_JOB_STATE_RUNNING}
		f.jobs[id] = record
		return record
	}
}

func (f *Fake) getJob(_ context.Context, request *workspacev1.GetJobRequest) (*workspacev1.GetJobResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.jobs[request.GetId()]
	if !ok {
		return nil, notFound("job", request.GetId())
	}
	return &workspacev1.GetJobResponse{Job: record.proto()}, nil
}

func (f *Fake) listJobs(context.Context, *workspacev1.ListJobsRequest) (*workspacev1.ListJobsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	response := &workspacev1.ListJobsResponse{Cursor: strconv.Itoa(len(f.jobLog))}
	for _, id := range f.jobCreated {
		response.Jobs = append(response.Jobs, f.jobs[id].proto())
	}
	return response, nil
}

func (f *Fake) cancelJob(_ context.Context, request *workspacev1.CancelJobRequest) (*workspacev1.CancelJobResponse, error) {
	f.mu.Lock()
	record, ok := f.jobs[request.GetId()]
	if !ok {
		f.mu.Unlock()
		return nil, notFound("job", request.GetId())
	}
	if record.state != workspacev1.JobState_JOB_STATE_ENDED {
		record.state = workspacev1.JobState_JOB_STATE_ENDED
		record.endReason = workspacev1.EndReason_END_REASON_CANCELLED
	}
	job := record.proto()
	f.mu.Unlock()
	f.changedJob(request.GetId(), workspacev1.EventType_EVENT_TYPE_WORKLOAD_STATE_CHANGED)
	return &workspacev1.CancelJobResponse{Job: job}, nil
}

func (f *Fake) waitJob(_ context.Context, request *workspacev1.WaitJobRequest) (*workspacev1.WaitJobResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.jobs[request.GetId()]
	if !ok {
		return nil, notFound("job", request.GetId())
	}
	return &workspacev1.WaitJobResponse{Job: record.proto(), Reached: true}, nil
}

// createJob answers the next registered sandbox, or creates one.
func (f *Fake) createSandbox(_ context.Context, request *workspacev1.CreateSandboxRequest) (*workspacev1.CreateSandboxResponse, error) {
	f.mu.Lock()
	if record, ok := f.sandboxKeys[request.GetRequestKey()]; ok && request.GetRequestKey() != "" {
		sandbox := record.proto()
		f.mu.Unlock()
		return &workspacev1.CreateSandboxResponse{Sandbox: sandbox}, nil
	}
	record := f.newSandbox()
	record.project = request.GetProject()
	record.environment = request.GetEnvironment()
	record.spec = request.GetSpec()
	record.labels = maps.Clone(request.GetLabels())
	record.recording = request.GetRecording()
	record.created = true
	if request.GetRequestKey() != "" {
		f.sandboxKeys[request.GetRequestKey()] = record
	}
	f.sandboxCreated = append(f.sandboxCreated, record.id)
	sandbox := record.proto()
	f.mu.Unlock()
	f.changedSandbox(record.id, workspacev1.EventType_EVENT_TYPE_WORKLOAD_CREATED)
	return &workspacev1.CreateSandboxResponse{Sandbox: sandbox}, nil
}

// newSandbox returns the next sandbox CreateSandbox answers with.
func (f *Fake) newSandbox() *sandbox {
	if len(f.sandboxQueue) > 0 {
		id := f.sandboxQueue[0]
		f.sandboxQueue = f.sandboxQueue[1:]
		return f.sandboxes[id]
	}
	for {
		f.nextSandbox++
		id := "sandbox-" + strconv.Itoa(f.nextSandbox)
		if _, taken := f.sandboxes[id]; taken {
			continue
		}
		record := &sandbox{id: id, state: workspacev1.SandboxState_SANDBOX_STATE_RUNNING}
		f.sandboxes[id] = record
		return record
	}
}

func (f *Fake) getSandbox(_ context.Context, request *workspacev1.GetSandboxRequest) (*workspacev1.GetSandboxResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.sandboxes[request.GetId()]
	if !ok {
		return nil, notFound("sandbox", request.GetId())
	}
	return &workspacev1.GetSandboxResponse{Sandbox: record.proto()}, nil
}

func (f *Fake) listSandboxes(context.Context, *workspacev1.ListSandboxesRequest) (*workspacev1.ListSandboxesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	response := &workspacev1.ListSandboxesResponse{Cursor: strconv.Itoa(len(f.sandboxLog))}
	for _, id := range f.sandboxCreated {
		response.Sandboxes = append(response.Sandboxes, f.sandboxes[id].proto())
	}
	return response, nil
}

func (f *Fake) waitSandbox(_ context.Context, request *workspacev1.WaitSandboxRequest) (*workspacev1.WaitSandboxResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.sandboxes[request.GetId()]
	if !ok {
		return nil, notFound("sandbox", request.GetId())
	}
	return &workspacev1.WaitSandboxResponse{Sandbox: record.proto(), Reached: true}, nil
}

// watchOutput streams a job's output and its end.
func (f *Fake) watchOutput(_ context.Context, conn connect.StreamingHandlerConn, request proto.Message) error {
	message, ok := request.(*workspacev1.WatchOutputRequest)
	if !ok {
		return unexpected(conn.Spec().Procedure, request)
	}
	f.mu.Lock()
	record, found := f.jobs[message.GetId()]
	if !found {
		f.mu.Unlock()
		return notFound("job", message.GetId())
	}
	stdout := fromOffset(record.stdout, message.GetStdoutOffset())
	stderr := fromOffset(record.stderr, message.GetStderrOffset())
	code := record.exitCode
	cancelled := record.endReason == workspacev1.EndReason_END_REASON_CANCELLED
	f.mu.Unlock()

	stream := &ServerStream[workspacev1.WatchOutputResponse]{conn: conn}
	if stdout != "" {
		err := stream.Send(&workspacev1.WatchOutputResponse{
			Message: &workspacev1.WatchOutputResponse_Stdout{Stdout: []byte(stdout)},
		})
		if err != nil {
			return err
		}
	}
	if stderr != "" {
		err := stream.Send(&workspacev1.WatchOutputResponse{
			Message: &workspacev1.WatchOutputResponse_Stderr{Stderr: []byte(stderr)},
		})
		if err != nil {
			return err
		}
	}
	return stream.Send(&workspacev1.WatchOutputResponse{
		Message: &workspacev1.WatchOutputResponse_End{End: &workspacev1.OutputEnd{
			ExitCode:  &code,
			Cancelled: cancelled,
		}},
	})
}

// fromOffset returns the part of s after offset bytes.
func fromOffset(s string, offset int64) string {
	if offset <= 0 {
		return s
	}
	if offset >= int64(len(s)) {
		return ""
	}
	return s[offset:]
}

// watchJobs reports the changes made after the cursor the request names, and
// then each change made while the stream is open.
func (f *Fake) watchJobs(ctx context.Context, conn connect.StreamingHandlerConn, request proto.Message) error {
	message, ok := request.(*workspacev1.WatchJobsRequest)
	if !ok {
		return unexpected(conn.Spec().Procedure, request)
	}
	from, err := parseCursor(message.GetCursor())
	if err != nil {
		return err
	}
	stream := &ServerStream[workspacev1.WatchJobsResponse]{conn: conn}
	for {
		f.mu.Lock()
		if from > len(f.jobLog) {
			f.mu.Unlock()
			return staleCursor(from)
		}
		pending := f.jobLog[from:]
		changed := f.jobChanged
		f.mu.Unlock()
		for _, change := range pending {
			if err := stream.Send(change); err != nil {
				return err
			}
			from++
		}
		if len(pending) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-changed:
		}
	}
}

// watchSandboxes reports the changes made after the cursor the request names,
// and then each change made while the stream is open.
func (f *Fake) watchSandboxes(ctx context.Context, conn connect.StreamingHandlerConn, request proto.Message) error {
	message, ok := request.(*workspacev1.WatchSandboxesRequest)
	if !ok {
		return unexpected(conn.Spec().Procedure, request)
	}
	from, err := parseCursor(message.GetCursor())
	if err != nil {
		return err
	}
	stream := &ServerStream[workspacev1.WatchSandboxesResponse]{conn: conn}
	for {
		f.mu.Lock()
		if from > len(f.sandboxLog) {
			f.mu.Unlock()
			return staleCursor(from)
		}
		pending := f.sandboxLog[from:]
		changed := f.sandboxChanged
		f.mu.Unlock()
		for _, change := range pending {
			if err := stream.Send(change); err != nil {
				return err
			}
			from++
		}
		if len(pending) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-changed:
		}
	}
}

// Execute answers a command with the outcome the test set for its sandbox.
func (f *Fake) execute(_ context.Context, conn connect.StreamingHandlerConn, _ proto.Message) error {
	stream := newBidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse](f, conn)
	request, err := stream.Receive()
	if err != nil {
		return err
	}
	start := request.GetStart()
	if start == nil {
		return connect.NewError(connect.CodeInvalidArgument,
			errors.New("toshitest: Execute needs a start message"))
	}
	f.mu.Lock()
	record, ok := f.executions[start.GetSandboxId()]
	if !ok {
		record = &execution{}
	}
	f.nextExecution++
	id := "exec-" + strconv.Itoa(f.nextExecution)
	stdout, stderr := record.stdout, record.stderr
	code, cancelled := record.exitCode, record.cancelled
	f.mu.Unlock()

	err = stream.Send(&workspacev1.ExecuteResponse{
		Message: &workspacev1.ExecuteResponse_Started{Started: &workspacev1.ExecutionStarted{ExecutionId: id}},
	})
	if err != nil {
		return err
	}
	if stdout != "" {
		err := stream.Send(&workspacev1.ExecuteResponse{
			Message: &workspacev1.ExecuteResponse_Stdout{Stdout: []byte(stdout)},
		})
		if err != nil {
			return err
		}
	}
	if stderr != "" {
		err := stream.Send(&workspacev1.ExecuteResponse{
			Message: &workspacev1.ExecuteResponse_Stderr{Stderr: []byte(stderr)},
		})
		if err != nil {
			return err
		}
	}
	return stream.Send(&workspacev1.ExecuteResponse{
		Message: &workspacev1.ExecuteResponse_Exit{Exit: &workspacev1.ExecutionExit{
			ExitCode:  &code,
			Cancelled: cancelled,
		}},
	})
}

// changedJob records a job change and wakes the open job watches.
func (f *Fake) changedJob(id string, eventType workspacev1.EventType) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.jobs[id]
	if !ok || !record.created {
		return
	}
	f.jobLog = append(f.jobLog, &workspacev1.WatchJobsResponse{
		Type:   eventType,
		Job:    record.proto(),
		Cursor: strconv.Itoa(len(f.jobLog) + 1),
	})
	close(f.jobChanged)
	f.jobChanged = make(chan struct{})
}

// changedSandbox records a sandbox change and wakes the open sandbox watches.
func (f *Fake) changedSandbox(id string, eventType workspacev1.EventType) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.sandboxes[id]
	if !ok || !record.created {
		return
	}
	f.sandboxLog = append(f.sandboxLog, &workspacev1.WatchSandboxesResponse{
		Type:    eventType,
		Sandbox: record.proto(),
		Cursor:  strconv.Itoa(len(f.sandboxLog) + 1),
	})
	close(f.sandboxChanged)
	f.sandboxChanged = make(chan struct{})
}

// parseCursor reads the position a watch starts at; an empty cursor starts at
// the beginning.
func parseCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	position, err := strconv.Atoi(cursor)
	if err != nil || position < 0 {
		return 0, connect.NewError(connect.CodeOutOfRange,
			fmt.Errorf("toshitest: %q is not a cursor of this workspace", cursor))
	}
	return position, nil
}

// notFound answers an id the fake does not hold.
func notFound(kind, id string) error {
	return connect.NewError(connect.CodeNotFound, fmt.Errorf("toshitest: no %s %q", kind, id))
}

// staleCursor answers a cursor past the last change.
func staleCursor(position int) error {
	return connect.NewError(connect.CodeOutOfRange,
		fmt.Errorf("toshitest: cursor %d is past the last change", position))
}

// unexpected answers a request of the wrong type, which a validated hook and
// the state machine cannot send.
func unexpected(procedure string, request any) error {
	return connect.NewError(connect.CodeInternal,
		fmt.Errorf("toshitest: %s received a %T request", procedure, request))
}
