package toshitest

import (
	"reflect"
	"strings"
	"testing"

	"connectrpc.com/connect"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
	workspacev1connect "github.com/toshicloud/toshi-api/workspacev1/workspacev1connect"
)

// TestEveryProcedureIsAnswered proves every procedure of the Workspace API is
// either answered by the state machine or listed as hook-only, and that every
// service is mounted.
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

	for name, known := range apiProcedures {
		kind, isBuiltin := builtin[name]
		switch {
		case isBuiltin && hookOnly[name]:
			t.Errorf("%s is both built in and listed as hook-only", name)
		case isBuiltin && kind != known.streamType:
			t.Errorf("%s is built in as %s; the API defines %s", name, kindName(kind), kindName(known.streamType))
		case !isBuiltin && !hookOnly[name]:
			t.Errorf("%s is neither built in nor listed as hook-only", name)
		}
	}
	for name := range hookOnly {
		if _, ok := apiProcedures[name]; !ok {
			t.Errorf("%s is listed as hook-only but the API does not define it", name)
		}
	}
	for _, service := range serviceNames() {
		if !tt.mounted["/"+service+"/"] {
			t.Errorf("service %s is not mounted", service)
		}
	}
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
