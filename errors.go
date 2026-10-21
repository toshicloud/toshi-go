package toshi

import (
	"fmt"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

// ExitError reports a command that ended with a non-zero status, or was
// cancelled before it had one.
type ExitError struct {
	ExecutionID string
	Exit        *workspacev1.ExecutionExit
}

// Error says how the command ended.
func (e *ExitError) Error() string {
	switch {
	case e.Exit.GetCancelled():
		return fmt.Sprintf("toshi: execution %s was cancelled", e.ExecutionID)
	case e.Exit.ExitCode == nil:
		return fmt.Sprintf("toshi: execution %s ended without an exit status", e.ExecutionID)
	default:
		return fmt.Sprintf("toshi: execution %s exited with status %d", e.ExecutionID, e.Exit.GetExitCode())
	}
}

// StreamLostError reports an Execute stream that ended before its exit
// message. Standard input cannot be replayed, so the caller decides whether
// to start again with the same request key.
type StreamLostError struct {
	ExecutionID string
	RequestKey  string
	Err         error
}

// Error says whether the stream was lost before or after the start was
// acknowledged.
func (e *StreamLostError) Error() string {
	if e.ExecutionID == "" {
		return fmt.Sprintf("toshi: execution stream lost before the start was acknowledged (request key %s): %v", e.RequestKey, e.Err)
	}
	return fmt.Sprintf("toshi: execution %s stream lost (request key %s): %v", e.ExecutionID, e.RequestKey, e.Err)
}

// Unwrap returns the error that ended the stream.
func (e *StreamLostError) Unwrap() error { return e.Err }
