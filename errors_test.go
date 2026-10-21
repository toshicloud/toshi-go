package toshi

import (
	"errors"
	"testing"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

// TestExitErrorText covers the three ways an execution can end.
func TestExitErrorText(t *testing.T) {
	three := int32(3)
	cases := []struct {
		name string
		exit *workspacev1.ExecutionExit
		want string
	}{
		{name: "cancelled", exit: &workspacev1.ExecutionExit{Cancelled: true}, want: "toshi: execution ex1 was cancelled"},
		{name: "status", exit: &workspacev1.ExecutionExit{ExitCode: &three}, want: "toshi: execution ex1 exited with status 3"},
		{name: "no status", exit: &workspacev1.ExecutionExit{}, want: "toshi: execution ex1 ended without an exit status"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := &ExitError{ExecutionID: "ex1", Exit: tc.exit}
			if got := err.Error(); got != tc.want {
				t.Fatalf("Error() %q, want %q", got, tc.want)
			}
		})
	}
}

// TestStreamLostErrorText covers the two stream-loss messages.
func TestStreamLostErrorText(t *testing.T) {
	cause := errors.New("broken")
	cases := []struct {
		name string
		err  *StreamLostError
		want string
	}{
		{
			name: "before the start was acknowledged",
			err:  &StreamLostError{RequestKey: "key", Err: cause},
			want: "toshi: execution stream lost before the start was acknowledged (request key key): broken",
		},
		{
			name: "after the started message",
			err:  &StreamLostError{ExecutionID: "ex1", RequestKey: "key", Err: cause},
			want: "toshi: execution ex1 stream lost (request key key): broken",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Error(); got != tc.want {
				t.Fatalf("Error() %q, want %q", got, tc.want)
			}
		})
	}
}
