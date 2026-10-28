package toshi

import (
	"errors"
	"fmt"

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
	// ReasonProductUnavailable: the product is off for the workspace. The
	// code is CodeFailedPrecondition, and Cause says which input turned it
	// off.
	ReasonProductUnavailable ProductReason = "PRODUCT_UNAVAILABLE"
)

// ProductCause is the input that turned a product off: a ProductError's
// Cause.
type ProductCause string

// The inputs that turn a product off.
const (
	// CauseBuild: the deployment's build does not include the product.
	CauseBuild ProductCause = "build"
	// CauseOperator: the deployment's config turns the product off.
	CauseOperator ProductCause = "operator"
	// CausePlan: the workspace's plan leaves the product out.
	CausePlan ProductCause = "plan"
	// CauseWorkspace: the workspace's settings turn the product off.
	CauseWorkspace ProductCause = "workspace"
)

// RefusalReason says why the API refused a call and named the reason: the
// reason of the refusal's ErrorInfo detail.
type RefusalReason string

// The reasons the API names on a refusal that is not a product's, which
// ProductError covers.
const (
	// ReasonDisplayInputNotEnabled: the Windows VM takes no display input.
	// The code is CodeFailedPrecondition.
	ReasonDisplayInputNotEnabled RefusalReason = "DISPLAY_INPUT_NOT_ENABLED"
	// ReasonImageBuildInProgress: the image already has a build running.
	// The code is CodeFailedPrecondition.
	ReasonImageBuildInProgress RefusalReason = "IMAGE_BUILD_IN_PROGRESS"
	// ReasonImageCheckFailed: the registry of the revision's image could
	// not be reached, refused the login or failed, so whether it serves the
	// digest is not known. The code is CodeFailedPrecondition.
	ReasonImageCheckFailed RefusalReason = "IMAGE_CHECK_FAILED"
	// ReasonImageInUse: a workload or a service revision still uses the
	// image. The code is CodeFailedPrecondition.
	ReasonImageInUse RefusalReason = "IMAGE_IN_USE"
	// ReasonImageNameTaken: another live image of the workspace has the
	// name. The code is CodeAlreadyExists.
	ReasonImageNameTaken RefusalReason = "IMAGE_NAME_TAKEN"
	// ReasonImageNotReady: a workspace image named by a job, a sandbox or a
	// service revision has no ready version. The code is
	// CodeFailedPrecondition.
	ReasonImageNotReady RefusalReason = "IMAGE_NOT_READY"
	// ReasonImageNotServed: the revision's registry answered that it does
	// not serve the revision's image digest. The code is
	// CodeFailedPrecondition.
	ReasonImageNotServed RefusalReason = "IMAGE_NOT_SERVED"
	// ReasonLastIdentity: the call would remove the caller's last identity.
	// The code is CodeFailedPrecondition.
	ReasonLastIdentity RefusalReason = "LAST_IDENTITY"
	// ReasonLastMember: the call would remove the workspace's last member.
	// The code is CodeFailedPrecondition.
	ReasonLastMember RefusalReason = "LAST_MEMBER"
	// ReasonLastOwner: the call would leave the workspace without an owner.
	// The code is CodeFailedPrecondition.
	ReasonLastOwner RefusalReason = "LAST_OWNER"
	// ReasonPermissionMissing: a role change names a permission the caller
	// does not hold where the role applies. The code is
	// CodePermissionDenied.
	ReasonPermissionMissing RefusalReason = "PERMISSION_MISSING"
	// ReasonPlanExcludesImages: the workspace's plan does not include
	// images. The code is CodeFailedPrecondition.
	ReasonPlanExcludesImages RefusalReason = "PLAN_EXCLUDES_IMAGES"
	// ReasonToshiLicensedWindows: a Windows job Toshi licenses opens no
	// display session. The code is CodeFailedPrecondition.
	ReasonToshiLicensedWindows RefusalReason = "TOSHI_LICENSED_WINDOWS"
)

// errorInfoDomain is the ErrorInfo domain of the API's refusals.
const errorInfoDomain = "toshi.run"

// ProductError is the API's refusal of a call for a product that is off for
// the workspace. errors.As finds it on the error of any call a Client makes,
// Wait, Watch and Execute included. It wraps the *connect.Error, so
// connect.CodeOf reads its code as before.
type ProductError struct {
	// Product is the product's ID, such as runners.
	Product string
	Reason  ProductReason
	// Cause is the input that turned the product off: CauseBuild,
	// CauseOperator, CausePlan or CauseWorkspace.
	Cause ProductCause
	// State is how far a product the deployment's config turns off has
	// wound down: no_new_work, draining or stopped; empty otherwise.
	State string
	Code  connect.Code
	Err   *connect.Error
}

// Error returns the API's message.
func (e *ProductError) Error() string { return e.Err.Error() }

// Unwrap returns the API's error.
func (e *ProductError) Unwrap() error { return e.Err }

// RefusalError is the API's refusal that names a reason: a rollback whose
// revision's image is gone, an image still in use, a plan without images, a
// last identity, and the rest of RefusalReason. errors.As finds it on the
// error of the call. It wraps the *connect.Error, so connect.CodeOf reads
// its code as before.
type RefusalError struct {
	Reason RefusalReason
	Code   connect.Code
	Err    *connect.Error
}

// Error returns the API's message.
func (e *RefusalError) Error() string { return e.Err.Error() }

// Unwrap returns the API's error.
func (e *RefusalError) Unwrap() error { return e.Err }

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
		if !ok || domain != errorInfoDomain {
			continue
		}
		switch r := ProductReason(reason); r {
		case ReasonProductUnavailable:
			return &ProductError{
				Product: metadata["product"],
				Reason:  r,
				Cause:   ProductCause(metadata["cause"]),
				State:   metadata["state"],
				Code:    cerr.Code(),
				Err:     cerr,
			}
		}
	}
	return err
}

// asRefusalError returns err as a *RefusalError when it is a refusal that
// names a reason this package knows, and err unchanged otherwise.
func asRefusalError(err error) error {
	var cerr *connect.Error
	if err == nil || !errors.As(err, &cerr) {
		return err
	}
	var already *RefusalError
	if errors.As(err, &already) {
		return err
	}
	for _, d := range cerr.Details() {
		if d.Type() != "google.rpc.ErrorInfo" {
			continue
		}
		reason, domain, _, ok := decodeErrorInfo(d.Bytes())
		if !ok || domain != errorInfoDomain {
			continue
		}
		switch r := RefusalReason(reason); r {
		case ReasonDisplayInputNotEnabled, ReasonImageBuildInProgress, ReasonImageCheckFailed,
			ReasonImageInUse, ReasonImageNameTaken, ReasonImageNotReady, ReasonImageNotServed,
			ReasonLastIdentity, ReasonLastMember, ReasonLastOwner, ReasonPermissionMissing,
			ReasonPlanExcludesImages, ReasonToshiLicensedWindows:
			return &RefusalError{Reason: r, Code: cerr.Code(), Err: cerr}
		}
	}
	return err
}

// asTypedError returns err as one of this package's typed errors when the
// API's ErrorInfo detail names a reason it knows, and err unchanged
// otherwise.
func asTypedError(err error) error {
	return asRefusalError(asProductError(err))
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
