package toshi

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	mathrand "math/rand/v2"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"
)

// retryClass says what one procedure's retries are.
type retryClass int

const (
	retryNone retryClass = iota
	retryRead
	retryKeyed
)

// procedureRetryClass names every procedure of the Workspace API's services.
// A missing procedure is treated as retryNone. A read is retried; so is a
// write that takes a request_key, which the interceptor fills and keeps for
// every try. OpenDisplaySession is the exception among the keyed writes: a
// retry returns the session with no token, so the caller opens another one
// itself.
var procedureRetryClass = map[string]retryClass{
	workspacev1connect.ArtifactServiceGetArtifactDownloadURLProcedure:          retryRead,
	workspacev1connect.ArtifactServiceGetArtifactProcedure:                     retryRead,
	workspacev1connect.ArtifactServiceListArtifactsProcedure:                   retryRead,
	workspacev1connect.ArtifactUploadServiceCompleteUploadProcedure:            retryNone,
	workspacev1connect.ArtifactUploadServiceReserveUploadProcedure:             retryNone,
	workspacev1connect.AuditEventServiceListAuditEventsProcedure:               retryRead,
	workspacev1connect.ChannelServiceCreateChannelConnectTokenProcedure:        retryNone,
	workspacev1connect.ChannelServiceCreateChannelProcedure:                    retryNone,
	workspacev1connect.ChannelServiceDeleteChannelProcedure:                    retryNone,
	workspacev1connect.ChannelServiceExportChannelLogProcedure:                 retryNone,
	workspacev1connect.ChannelServiceGetChannelPolicyProcedure:                 retryRead,
	workspacev1connect.ChannelServiceGetChannelProcedure:                       retryRead,
	workspacev1connect.ChannelServiceGetChannelRequestProcedure:                retryRead,
	workspacev1connect.ChannelServiceGetLinkProcedure:                          retryRead,
	workspacev1connect.ChannelServiceListChannelRequestsProcedure:              retryRead,
	workspacev1connect.ChannelServiceListChannelsProcedure:                     retryRead,
	workspacev1connect.ChannelServiceListForwardsProcedure:                     retryRead,
	workspacev1connect.ChannelServiceRevokeChannelConnectTokenProcedure:        retryNone,
	workspacev1connect.ChannelServiceReplayChannelMessageProcedure:             retryNone,
	workspacev1connect.ChannelServiceSendChannelMessageProcedure:               retryKeyed,
	workspacev1connect.ChannelServiceSetChannelPolicyProcedure:                 retryNone,
	workspacev1connect.ChannelServiceTailChannelMessagesProcedure:              retryNone,
	workspacev1connect.ConnectionServiceCreateConnectionProcedure:              retryKeyed,
	workspacev1connect.ConnectionServiceDeleteConnectionProcedure:              retryKeyed,
	workspacev1connect.ConnectionServiceGetConnectionDeliveryProcedure:         retryRead,
	workspacev1connect.ConnectionServiceGetConnectionProcedure:                 retryRead,
	workspacev1connect.ConnectionServiceListConnectionDeliveriesProcedure:      retryRead,
	workspacev1connect.ConnectionServiceListConnectionsProcedure:               retryRead,
	workspacev1connect.ConnectionServiceReplayConnectionDeliveryProcedure:      retryKeyed,
	workspacev1connect.ConnectionServiceRotateConnectionSecretProcedure:        retryKeyed,
	workspacev1connect.ConnectionServiceSetConnectionTransformProcedure:        retryNone,
	workspacev1connect.DiscoveryServiceDiscoverProcedure:                       retryRead,
	workspacev1connect.DisplayServiceCloseDisplaySessionProcedure:              retryNone,
	workspacev1connect.DisplayServiceGetDisplaySessionProcedure:                retryRead,
	workspacev1connect.DisplayServiceListDisplaySessionsProcedure:              retryRead,
	workspacev1connect.DisplayServiceOpenDisplaySessionProcedure:               retryNone,
	workspacev1connect.DisplayServiceSendDisplayKeysProcedure:                  retryNone,
	workspacev1connect.DisplayServiceSendDisplayPointerProcedure:               retryNone,
	workspacev1connect.DisplayServiceSetDisplayClipboardProcedure:              retryNone,
	workspacev1connect.DisplayServiceTakeDisplayScreenshotProcedure:            retryNone,
	workspacev1connect.FillerServiceDeleteFillerProcedure:                      retryNone,
	workspacev1connect.FillerServiceListFillerRunsProcedure:                    retryRead,
	workspacev1connect.FillerServiceListFillersProcedure:                       retryRead,
	workspacev1connect.FillerServicePutFillerProcedure:                         retryNone,
	workspacev1connect.FillerServiceSetFillerEnabledProcedure:                  retryNone,
	workspacev1connect.HookServiceActivateHookProcedure:                        retryNone,
	workspacev1connect.HookServiceDeleteHookVersionProcedure:                   retryNone,
	workspacev1connect.HookServiceListHooksProcedure:                           retryRead,
	workspacev1connect.HookServiceTestHookProcedure:                            retryNone,
	workspacev1connect.HookServiceUploadHookVersionProcedure:                   retryNone,
	workspacev1connect.HostJobServiceCancelHostJobProcedure:                    retryNone,
	workspacev1connect.HostJobServiceGetHostJobDebugAccessProcedure:            retryRead,
	workspacev1connect.HostJobServiceGetHostJobFailureProcedure:                retryRead,
	workspacev1connect.HostJobServiceGetHostJobProcedure:                       retryRead,
	workspacev1connect.HostJobServiceGetHostJobTelemetryProcedure:              retryRead,
	workspacev1connect.HostJobServiceListHostJobsProcedure:                     retryRead,
	workspacev1connect.HostJobServiceStopHostJobDebugProcedure:                 retryNone,
	workspacev1connect.HostServiceCheckCloudHostProcedure:                      retryNone,
	workspacev1connect.HostServiceCreateCloudHostProcedure:                     retryNone,
	workspacev1connect.HostServiceCreateEnrollmentTokenProcedure:               retryNone,
	workspacev1connect.HostServiceListHostAlertsProcedure:                      retryRead,
	workspacev1connect.HostServiceListHostStorageProcedure:                     retryRead,
	workspacev1connect.HostServiceListHostsProcedure:                           retryRead,
	workspacev1connect.HostServiceRetireHostProcedure:                          retryNone,
	workspacev1connect.HostServiceSetHostStateProcedure:                        retryNone,
	workspacev1connect.ProductServiceListCatalogEntriesProcedure:               retryRead,
	workspacev1connect.ProductServiceListProductsProcedure:                     retryRead,
	workspacev1connect.ProductServiceRemoveProductProcedure:                    retryNone,
	workspacev1connect.ProductServiceUpdateProductProcedure:                    retryNone,
	workspacev1connect.ProjectServiceClaimRepositoryProcedure:                  retryNone,
	workspacev1connect.ProjectServiceCreateEmbedTokenProcedure:                 retryNone,
	workspacev1connect.ProjectServiceCreateEnvironmentProcedure:                retryNone,
	workspacev1connect.ProjectServiceCreateProjectProcedure:                    retryNone,
	workspacev1connect.ProjectServiceDeleteEnvironmentProcedure:                retryNone,
	workspacev1connect.ProjectServiceGetProjectProcedure:                       retryRead,
	workspacev1connect.ProjectServiceListBranchRulesProcedure:                  retryRead,
	workspacev1connect.ProjectServiceListEmbedTokensProcedure:                  retryRead,
	workspacev1connect.ProjectServiceListProjectsProcedure:                     retryRead,
	workspacev1connect.ProjectServiceListRepositoriesProcedure:                 retryRead,
	workspacev1connect.ProjectServiceRevokeEmbedTokenProcedure:                 retryNone,
	workspacev1connect.ProjectServiceSetBranchRulesProcedure:                   retryNone,
	workspacev1connect.RecordingServiceCreateRecordingShareLinkProcedure:       retryNone,
	workspacev1connect.RecordingServiceGetRecordingProcedure:                   retryRead,
	workspacev1connect.RecordingServiceListRecordingShareLinksProcedure:        retryRead,
	workspacev1connect.RecordingServiceListRecordingsProcedure:                 retryRead,
	workspacev1connect.RecordingServiceRevokeRecordingShareLinkProcedure:       retryNone,
	workspacev1connect.RoleServiceAcceptInvitationProcedure:                    retryNone,
	workspacev1connect.RoleServiceCreateInvitationProcedure:                    retryNone,
	workspacev1connect.RoleServiceCreateRoleProcedure:                          retryNone,
	workspacev1connect.RoleServiceDeleteRoleProcedure:                          retryNone,
	workspacev1connect.RoleServiceGrantRoleProcedure:                           retryNone,
	workspacev1connect.RoleServiceListInvitationsProcedure:                     retryRead,
	workspacev1connect.RoleServiceListMembersProcedure:                         retryRead,
	workspacev1connect.RoleServiceListRoleGrantsProcedure:                      retryRead,
	workspacev1connect.RoleServiceListRolesProcedure:                           retryRead,
	workspacev1connect.RoleServiceRemoveMemberProcedure:                        retryNone,
	workspacev1connect.RoleServiceRevokeInvitationProcedure:                    retryNone,
	workspacev1connect.RoleServiceRevokeRoleProcedure:                          retryNone,
	workspacev1connect.RoleServiceUpdateRoleProcedure:                          retryNone,
	workspacev1connect.SecretServiceCreateSecretSetProcedure:                   retryNone,
	workspacev1connect.SecretServiceDeleteSecretProcedure:                      retryNone,
	workspacev1connect.SecretServiceDeleteSecretSetProcedure:                   retryNone,
	workspacev1connect.SecretServiceGrantSecretSetProcedure:                    retryNone,
	workspacev1connect.SecretServiceListSecretSetGrantsProcedure:               retryRead,
	workspacev1connect.SecretServiceListSecretSetsProcedure:                    retryRead,
	workspacev1connect.SecretServiceListSecretVersionsProcedure:                retryRead,
	workspacev1connect.SecretServiceListSecretsProcedure:                       retryRead,
	workspacev1connect.SecretServicePutSecretProcedure:                         retryNone,
	workspacev1connect.SecretServiceRevokeSecretSetProcedure:                   retryNone,
	workspacev1connect.StorageProviderServiceCheckStorageProviderProcedure:     retryNone,
	workspacev1connect.StorageProviderServiceCreateStorageProviderProcedure:    retryNone,
	workspacev1connect.StorageProviderServiceDeleteStorageProviderProcedure:    retryNone,
	workspacev1connect.StorageProviderServiceListStorageProvidersProcedure:     retryRead,
	workspacev1connect.TokenServiceCreateWorkspaceTokenProcedure:               retryNone,
	workspacev1connect.TokenServiceListTokensProcedure:                         retryRead,
	workspacev1connect.TokenServiceRevokeTokenProcedure:                        retryNone,
	workspacev1connect.TokenServiceRollWorkspaceTokenProcedure:                 retryNone,
	workspacev1connect.TokenServiceUpdateTokenProcedure:                        retryNone,
	workspacev1connect.TriggerServiceCreateTriggerProcedure:                    retryKeyed,
	workspacev1connect.TriggerServiceDeleteTriggerProcedure:                    retryKeyed,
	workspacev1connect.TriggerServiceGetTriggerProcedure:                       retryRead,
	workspacev1connect.TriggerServiceListTriggersProcedure:                     retryRead,
	workspacev1connect.TriggerServiceUpdateTriggerProcedure:                    retryKeyed,
	workspacev1connect.VolumeServiceAttachVolumeProcedure:                      retryNone,
	workspacev1connect.VolumeServiceDetachVolumeProcedure:                      retryNone,
	workspacev1connect.VolumeServiceListVolumesProcedure:                       retryRead,
	workspacev1connect.WebhookServiceCreateWebhookEndpointProcedure:            retryKeyed,
	workspacev1connect.WebhookServiceDeleteWebhookEndpointProcedure:            retryKeyed,
	workspacev1connect.WebhookServiceGetWebhookEndpointProcedure:               retryRead,
	workspacev1connect.WebhookServiceListWebhookDeliveriesProcedure:            retryRead,
	workspacev1connect.WebhookServiceListWebhookEndpointsProcedure:             retryRead,
	workspacev1connect.WebhookServiceReplayWebhookEventsProcedure:              retryKeyed,
	workspacev1connect.WebhookServiceRotateWebhookEndpointSecretProcedure:      retryKeyed,
	workspacev1connect.WebhookServiceUpdateWebhookEndpointProcedure:            retryKeyed,
	workspacev1connect.WorkloadServiceCancelExecutionProcedure:                 retryKeyed,
	workspacev1connect.WorkloadServiceCancelJobProcedure:                       retryNone,
	workspacev1connect.WorkloadServiceCreateJobProcedure:                       retryKeyed,
	workspacev1connect.WorkloadServiceCreatePreviewShareLinkProcedure:          retryNone,
	workspacev1connect.WorkloadServiceCreateSandboxProcedure:                   retryKeyed,
	workspacev1connect.WorkloadServiceCreateServiceProcedure:                   retryKeyed,
	workspacev1connect.WorkloadServiceDeleteJobProcedure:                       retryKeyed,
	workspacev1connect.WorkloadServiceDeleteSandboxProcedure:                   retryKeyed,
	workspacev1connect.WorkloadServiceDeleteServiceProcedure:                   retryKeyed,
	workspacev1connect.WorkloadServiceExecuteProcedure:                         retryNone,
	workspacev1connect.WorkloadServiceGetJobProcedure:                          retryRead,
	workspacev1connect.WorkloadServiceGetOperationProcedure:                    retryRead,
	workspacev1connect.WorkloadServiceGetSandboxProcedure:                      retryRead,
	workspacev1connect.WorkloadServiceGetServiceProcedure:                      retryRead,
	workspacev1connect.WorkloadServiceGetServiceRevisionProcedure:              retryRead,
	workspacev1connect.WorkloadServiceHoldServiceProcedure:                     retryKeyed,
	workspacev1connect.WorkloadServiceListEventsProcedure:                      retryRead,
	workspacev1connect.WorkloadServiceListJobsProcedure:                        retryRead,
	workspacev1connect.WorkloadServiceListPreviewShareLinksProcedure:           retryRead,
	workspacev1connect.WorkloadServiceListSandboxesProcedure:                   retryRead,
	workspacev1connect.WorkloadServiceListServiceActivationsProcedure:          retryRead,
	workspacev1connect.WorkloadServiceListServiceRevisionsProcedure:            retryRead,
	workspacev1connect.WorkloadServiceListServiceRunsProcedure:                 retryRead,
	workspacev1connect.WorkloadServiceListServicesProcedure:                    retryRead,
	workspacev1connect.WorkloadServiceReleaseServiceProcedure:                  retryKeyed,
	workspacev1connect.WorkloadServiceRevokePreviewShareLinkProcedure:          retryNone,
	workspacev1connect.WorkloadServiceRollbackServiceProcedure:                 retryKeyed,
	workspacev1connect.WorkloadServiceSetServiceModeProcedure:                  retryKeyed,
	workspacev1connect.WorkloadServiceStartServiceProcedure:                    retryKeyed,
	workspacev1connect.WorkloadServiceStopServiceProcedure:                     retryKeyed,
	workspacev1connect.WorkloadServiceUpdateJobProcedure:                       retryKeyed,
	workspacev1connect.WorkloadServiceUpdateSandboxProcedure:                   retryKeyed,
	workspacev1connect.WorkloadServiceUpdateServiceProcedure:                   retryKeyed,
	workspacev1connect.WorkloadServiceWaitJobProcedure:                         retryRead,
	workspacev1connect.WorkloadServiceWaitOperationProcedure:                   retryRead,
	workspacev1connect.WorkloadServiceWaitSandboxProcedure:                     retryRead,
	workspacev1connect.WorkloadServiceWaitServiceProcedure:                     retryRead,
	workspacev1connect.WorkloadServiceWakeServiceProcedure:                     retryKeyed,
	workspacev1connect.WorkloadServiceWatchEventsProcedure:                     retryNone,
	workspacev1connect.WorkloadServiceWatchGuestLogProcedure:                   retryNone,
	workspacev1connect.WorkloadServiceWatchJobsProcedure:                       retryNone,
	workspacev1connect.WorkloadServiceWatchOutputProcedure:                     retryNone,
	workspacev1connect.WorkloadServiceWatchSandboxesProcedure:                  retryNone,
	workspacev1connect.WorkloadServiceWatchServicesProcedure:                   retryNone,
	workspacev1connect.WorkloadServiceWatchTelemetryProcedure:                  retryNone,
	workspacev1connect.WorkspaceServiceCreateBillingPortalSessionProcedure:     retryNone,
	workspacev1connect.WorkspaceServiceCreateCheckoutSessionProcedure:          retryNone,
	workspacev1connect.WorkspaceServiceGetQueueTimesProcedure:                  retryRead,
	workspacev1connect.WorkspaceServiceGetUsageProcedure:                       retryRead,
	workspacev1connect.WorkspaceServiceGetVersionProcedure:                     retryRead,
	workspacev1connect.WorkspaceServiceGetWorkflowUsageProcedure:               retryRead,
	workspacev1connect.WorkspaceServiceGetWorkspaceBillingProcedure:            retryRead,
	workspacev1connect.WorkspaceServiceGetWorkspaceProcedure:                   retryRead,
	workspacev1connect.WorkspaceServiceGetWorkspaceStatementProcedure:          retryRead,
	workspacev1connect.WorkspaceServiceListEgressIPsProcedure:                  retryRead,
	workspacev1connect.WorkspaceServiceListPlansProcedure:                      retryRead,
	workspacev1connect.WorkspaceServiceListWorkspacesProcedure:                 retryRead,
	workspacev1connect.WorkspaceServiceSetWorkspaceAllowCacheWarmProcedure:     retryNone,
	workspacev1connect.WorkspaceServiceSetWorkspaceAllowOverflowProcedure:      retryNone,
	workspacev1connect.WorkspaceServiceSetWorkspaceCancelRunOnVmDeathProcedure: retryNone,
	workspacev1connect.WorkspaceServiceSetWorkspaceOwnHostsOnlyProcedure:       retryNone,
	workspacev1connect.WorkspaceServiceSetWorkspaceSpendLimitsProcedure:        retryNone,
}

// normalizeRetry fills zero backoff values with the defaults and leaves
// MaxAttempts as given, so a value of 1 or less still disables retries.
func normalizeRetry(policy RetryPolicy) RetryPolicy {
	defaults := DefaultRetryPolicy()
	if policy.InitialBackoff <= 0 {
		policy.InitialBackoff = defaults.InitialBackoff
	}
	if policy.MaxBackoff <= 0 {
		policy.MaxBackoff = defaults.MaxBackoff
	}
	return policy
}

// retryInterceptor retries a unary call when its procedure's class allows
// it, under the caller's context and with full-jitter backoff.
type retryInterceptor struct {
	policy RetryPolicy
}

func (r retryInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		class := procedureRetryClass[req.Spec().Procedure]
		if class == retryNone {
			return next(ctx, req)
		}
		if class == retryKeyed {
			fillRequestKey(requestMessage(req))
		}
		attempts := r.policy.MaxAttempts
		if attempts < 1 {
			attempts = 1
		}
		var last error
		for attempt := 0; attempt < attempts; attempt++ {
			resp, err := next(ctx, req)
			if err == nil {
				return resp, nil
			}
			last = err
			if attempt == attempts-1 || !retryable(err) {
				return nil, err
			}
			if err := sleepBackoff(ctx, r.policy, attempt); err != nil {
				return nil, err
			}
		}
		return nil, last
	}
}

func (r retryInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (r retryInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// retryable reports whether err is a dropped connection or an unavailable
// server.
func retryable(err error) bool {
	return connect.CodeOf(err) == connect.CodeUnavailable || errors.Is(err, io.ErrUnexpectedEOF)
}

// sleepBackoff waits a full-jitter delay for attempt, or returns the
// context's error when ctx ends first.
func sleepBackoff(ctx context.Context, policy RetryPolicy, attempt int) error {
	delay := backoff(policy, attempt)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctxError(ctx)
	case <-timer.C:
		return nil
	}
}

// backoff returns a random delay in [0, min(MaxBackoff, InitialBackoff times
// two to the attempt)].
func backoff(policy RetryPolicy, attempt int) time.Duration {
	limit := policy.InitialBackoff
	for i := 0; i < attempt; i++ {
		if limit >= policy.MaxBackoff {
			break
		}
		limit *= 2
		if limit <= 0 {
			limit = policy.MaxBackoff
			break
		}
	}
	if limit > policy.MaxBackoff || limit <= 0 {
		limit = policy.MaxBackoff
	}
	if limit <= 0 {
		return 0
	}
	return time.Duration(mathrand.Int64N(int64(limit) + 1))
}

// ctxError returns the context's error as a Connect error with its code.
func ctxError(ctx context.Context) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}
	code := connect.CodeCanceled
	if errors.Is(err, context.DeadlineExceeded) {
		code = connect.CodeDeadlineExceeded
	}
	return connect.NewError(code, err)
}

// fillRequestKey sets an empty request_key string field in place.
func fillRequestKey(msg proto.Message) {
	if msg == nil {
		return
	}
	m := msg.ProtoReflect()
	if !m.IsValid() {
		return
	}
	fd := m.Descriptor().Fields().ByName("request_key")
	if !isString(fd) || m.Get(fd).String() != "" {
		return
	}
	m.Set(fd, protoreflect.ValueOfString(randomKey()))
}

// maxReconnects is how many times a watch reconnects after a drop.
func (c *Client) maxReconnects() int {
	if c.retry.MaxAttempts <= 1 {
		return 0
	}
	return c.retry.MaxAttempts - 1
}

// randomKey returns 128 random bits as 32 hexadecimal characters.
func randomKey() string {
	var b [16]byte
	_, _ = cryptorand.Read(b[:])
	return hex.EncodeToString(b[:])
}
