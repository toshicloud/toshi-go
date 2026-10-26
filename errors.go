package toshi

import (
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"

	workspacev1 "go.toshicloud.io/api/workspacev1"
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

// JobExitError reports a job that Run followed and that did not exit 0.
type JobExitError struct {
	JobID string
	End   *workspacev1.OutputEnd
}

// Error says how the job ended.
func (e *JobExitError) Error() string {
	switch {
	case e.End.GetCancelled():
		return fmt.Sprintf("toshi: job %s was cancelled", e.JobID)
	case e.End.ExitCode == nil:
		return fmt.Sprintf("toshi: job %s ended without an exit status", e.JobID)
	default:
		return fmt.Sprintf("toshi: job %s exited with status %d", e.JobID, e.End.GetExitCode())
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

// ProductReason says why the API refused a call for a product: the reason of
// the refusal's ErrorInfo detail.
type ProductReason string

// The reasons a call is refused for a product.
const (
	// ReasonProductNotInstalled: the deployment's build does not include the
	// product. The code is CodeUnimplemented.
	ReasonProductNotInstalled ProductReason = "PRODUCT_NOT_INSTALLED"
	// ReasonProductDisabled: the deployment turned the product off. The
	// code is CodeFailedPrecondition.
	ReasonProductDisabled ProductReason = "PRODUCT_DISABLED"
	// ReasonProductNotEntitled: the workspace's entitlements leave the
	// product out. The code is CodePermissionDenied.
	ReasonProductNotEntitled ProductReason = "PRODUCT_NOT_ENTITLED"
	// ReasonProductTurnedOff: the workspace's settings turn the product off,
	// or an outside product's installation is turned off. The code is
	// CodeFailedPrecondition.
	ReasonProductTurnedOff ProductReason = "PRODUCT_TURNED_OFF"
	// ReasonAccessNotAccepted: turning an outside product on installs it, and
	// the call did not accept the access it asks for. The code is
	// CodeFailedPrecondition.
	ReasonAccessNotAccepted ProductReason = "ACCESS_NOT_ACCEPTED"
)

// productErrorDomain is the ErrorInfo domain of a product's refusal.
const productErrorDomain = "toshi.run"

// ProductError is the API's refusal of a call for a product: one that is off
// for the workspace, or an outside product whose access the call did not
// accept. errors.As finds it on the error of any call a Client makes, Wait,
// Watch and Execute included. It wraps the *connect.Error, so
// connect.CodeOf reads its code as before.
type ProductError struct {
	// Product is the product's ID, such as runners.
	Product string
	Reason  ProductReason
	// Feature is the setting that turns the product on, such as
	// runners.enabled; empty for ReasonAccessNotAccepted.
	Feature string
	// State is how far a product the deployment disables has wound down:
	// no_new_work, draining or stopped; empty otherwise.
	State string
	// Access is the access an outside product asks for, for
	// ReasonAccessNotAccepted: accept exactly this list to install it.
	Access []string
	Code   connect.Code
	Err    *connect.Error
}

// Error returns the API's message.
func (e *ProductError) Error() string { return e.Err.Error() }

// Unwrap returns the API's error.
func (e *ProductError) Unwrap() error { return e.Err }

// asProductError returns err as a *ProductError when it is a product's
// refusal, and err unchanged otherwise.
func asProductError(err error) error {
	var cerr *connect.Error
	if err == nil || !errors.As(err, &cerr) {
		return err
	}
	var already *ProductError
	if errors.As(err, &already) {
		return err
	}
	for _, d := range cerr.Details() {
		if d.Type() != "google.rpc.ErrorInfo" {
			continue
		}
		reason, domain, metadata, ok := decodeErrorInfo(d.Bytes())
		if !ok || domain != productErrorDomain {
			continue
		}
		switch r := ProductReason(reason); r {
		case ReasonProductNotInstalled, ReasonProductDisabled, ReasonProductNotEntitled, ReasonProductTurnedOff, ReasonAccessNotAccepted:
			pe := &ProductError{
				Product: metadata["product"],
				Reason:  r,
				Feature: metadata["feature"],
				State:   metadata["state"],
				Code:    cerr.Code(),
				Err:     cerr,
			}
			if access := metadata["access"]; access != "" {
				pe.Access = strings.Split(access, ",")
			}
			return pe
		}
	}
	return err
}

// decodeErrorInfo reads a google.rpc.ErrorInfo message: reason (1), domain
// (2) and metadata (3), a map of strings. It reads the wire format itself,
// so the SDK needs no module for the googleapis types.
func decodeErrorInfo(b []byte) (reason, domain string, metadata map[string]string, ok bool) {
	metadata = map[string]string{}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", "", nil, false
		}
		b = b[n:]
		if typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return "", "", nil, false
			}
			b = b[n:]
			continue
		}
		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return "", "", nil, false
		}
		b = b[n:]
		switch num {
		case 1:
			reason = string(v)
		case 2:
			domain = string(v)
		case 3:
			key, value, entryOK := decodeMapEntry(v)
			if !entryOK {
				return "", "", nil, false
			}
			metadata[key] = value
		}
	}
	return reason, domain, metadata, true
}

// decodeMapEntry reads one entry of a map<string, string>: key (1), value (2).
func decodeMapEntry(b []byte) (key, value string, ok bool) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", "", false
		}
		b = b[n:]
		if typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return "", "", false
			}
			b = b[n:]
			continue
		}
		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return "", "", false
		}
		b = b[n:]
		switch num {
		case 1:
			key = string(v)
		case 2:
			value = string(v)
		}
	}
	return key, value, true
}
