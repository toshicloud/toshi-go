package toshitest

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"
)

// TestEveryProcedureIsAnswered proves every procedure of the Workspace API is
// either answered by the state machine or answered UNIMPLEMENTED until a test
// hooks it, that every built-in names a procedure the API defines with the
// right stream type, and that every service is mounted.
func TestEveryProcedureIsAnswered(t *testing.T) {
	tt := New(t)
	if len(apiProcedures) == 0 {
		t.Fatal("the API defines no procedures")
	}

	builtin := make(map[string]connect.StreamType, len(tt.builtin)+len(tt.builtinStream))
	for name := range tt.builtin {
		builtin[name] = connect.StreamTypeUnary
	}
	for name := range tt.builtinStream {
		known, ok := apiProcedures[name]
		if !ok {
			t.Errorf("%s is built in but the API does not define it", name)
			continue
		}
		builtin[name] = known.streamType
	}

	unanswered := make(map[string]bool)
	for _, name := range tt.unanswered() {
		unanswered[name] = true
	}

	for name, known := range apiProcedures {
		kind, isBuiltin := builtin[name]
		switch {
		case isBuiltin && unanswered[name]:
			t.Errorf("%s is both built in and unanswered", name)
		case isBuiltin && kind != known.streamType:
			t.Errorf("%s is built in as %s; the API defines %s", name, kindName(kind), kindName(known.streamType))
		case !isBuiltin && !unanswered[name]:
			t.Errorf("%s is neither built in nor unanswered", name)
		}
	}
	for name := range builtin {
		if _, ok := apiProcedures[name]; !ok {
			t.Errorf("%s is built in but the API does not define it", name)
		}
	}
	for _, service := range serviceNames() {
		if !tt.mounted["/"+service+"/"] {
			t.Errorf("service %s is not mounted", service)
		}
	}

	// A procedure without a built-in answer answers UNIMPLEMENTED until a test
	// sets a hook. The fake holds no projects, so ListProjects is one.
	if !unanswered[workspacev1connect.ProjectServiceListProjectsProcedure] {
		t.Fatalf("%s has a built-in answer now; check another unanswered procedure here",
			workspacev1connect.ProjectServiceListProjectsProcedure)
	}
	_, err := tt.Client().Project().ListProjects(context.Background(),
		connect.NewRequest(&workspacev1.ListProjectsRequest{}))
	if got := connect.CodeOf(err); got != connect.CodeUnimplemented {
		t.Fatalf("ListProjects without a hook: %v, want %s", err, connect.CodeUnimplemented)
	}
}

// unanswered returns, in order, the procedures of the Workspace API that the
// fake has no built-in answer for: they answer UNIMPLEMENTED until a test sets
// a hook.
func (f *Fake) unanswered() []string {
	var names []string
	for name := range apiProcedures {
		if _, ok := f.builtin[name]; ok {
			continue
		}
		if _, ok := f.builtinStream[name]; ok {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// TestValidateHook proves a hook whose types do not match its procedure is
// refused with a message naming both.
func TestValidateHook(t *testing.T) {
	t.Run("wrong request", func(t *testing.T) {
		err := validateHook(
			workspacev1connect.WorkloadServiceCreateJobProcedure,
			connect.StreamTypeUnary,
			reflect.TypeOf((*workspacev1.GetJobRequest)(nil)),
			reflect.TypeOf((*workspacev1.CreateJobResponse)(nil)),
		)
		if err == nil {
			t.Fatal("a hook with the wrong request type was accepted")
		}
		for _, want := range []string{
			workspacev1connect.WorkloadServiceCreateJobProcedure,
			"*workspacev1.CreateJobRequest",
			"*workspacev1.GetJobRequest",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("message %q does not name %s", err, want)
			}
		}
	})

	t.Run("wrong kind", func(t *testing.T) {
		err := validateHook(
			workspacev1connect.WorkloadServiceWatchOutputProcedure,
			connect.StreamTypeUnary,
			reflect.TypeOf((*workspacev1.WatchOutputRequest)(nil)),
			reflect.TypeOf((*workspacev1.WatchOutputResponse)(nil)),
		)
		if err == nil {
			t.Fatal("a unary hook for a server stream was accepted")
		}
		if !strings.Contains(err.Error(), "toshitest.OnServerStream") {
			t.Errorf("message %q does not name the function that hooks a server stream", err)
		}
	})

	t.Run("unknown procedure", func(t *testing.T) {
		err := validateHook("/toshi.workspace.v1.WorkloadService/Run",
			connect.StreamTypeUnary,
			reflect.TypeOf((*workspacev1.CreateJobRequest)(nil)),
			reflect.TypeOf((*workspacev1.CreateJobResponse)(nil)),
		)
		if err == nil {
			t.Fatal("a hook for an unknown procedure was accepted")
		}
	})
}
