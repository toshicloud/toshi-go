package toshi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
	workspacev1connect "github.com/toshicloud/toshi-api/workspacev1/workspacev1connect"
)

// Client holds a client for each service of the Workspace API, with this
// package's authentication, scope and retry, and the interceptors and options
// the caller adds.
type Client struct {
	// Workload holds the calls of WorkloadService: a job's, a sandbox's and
	// a service's, and the calls that span the three.
	Workload WorkloadClient
	// Project is the ProjectService client.
	Project workspacev1connect.ProjectServiceClient
	// Secret is the SecretService client.
	Secret workspacev1connect.SecretServiceClient
	// Webhook is the WebhookService client.
	Webhook workspacev1connect.WebhookServiceClient
	// Display is the DisplayService client.
	Display workspacev1connect.DisplayServiceClient
	// Channel is the ChannelService client, its server stream included.
	Channel ChannelClient
	// Recording is the RecordingService client.
	Recording workspacev1connect.RecordingServiceClient
	// Volume is the VolumeService client.
	Volume workspacev1connect.VolumeServiceClient
	// Artifact is the ArtifactService client.
	Artifact workspacev1connect.ArtifactServiceClient
	// ArtifactUpload is the ArtifactUploadService client.
	ArtifactUpload workspacev1connect.ArtifactUploadServiceClient
	// Workspace is the WorkspaceService client.
	Workspace workspacev1connect.WorkspaceServiceClient
	// HostJob is the HostJobService client.
	HostJob workspacev1connect.HostJobServiceClient
	// Host is the HostService client.
	Host workspacev1connect.HostServiceClient
	// Token is the TokenService client.
	Token workspacev1connect.TokenServiceClient
	// AuditEvent is the AuditEventService client.
	AuditEvent workspacev1connect.AuditEventServiceClient
	// Filler is the FillerService client.
	Filler workspacev1connect.FillerServiceClient
	// StorageProvider is the StorageProviderService client.
	StorageProvider workspacev1connect.StorageProviderServiceClient

	project     string
	environment string
	retry       RetryPolicy
}

// ChannelClient is the calls of ChannelService.
type ChannelClient interface {
	CreateChannel(context.Context, *connect.Request[workspacev1.CreateChannelRequest]) (*connect.Response[workspacev1.CreateChannelResponse], error)
	ListChannels(context.Context, *connect.Request[workspacev1.ListChannelsRequest]) (*connect.Response[workspacev1.ListChannelsResponse], error)
	GetChannel(context.Context, *connect.Request[workspacev1.GetChannelRequest]) (*connect.Response[workspacev1.GetChannelResponse], error)
	GetChannelPolicy(context.Context, *connect.Request[workspacev1.GetChannelPolicyRequest]) (*connect.Response[workspacev1.GetChannelPolicyResponse], error)
	SetChannelPolicy(context.Context, *connect.Request[workspacev1.SetChannelPolicyRequest]) (*connect.Response[workspacev1.SetChannelPolicyResponse], error)
	ListChannelRequests(context.Context, *connect.Request[workspacev1.ListChannelRequestsRequest]) (*connect.Response[workspacev1.ListChannelRequestsResponse], error)
	GetChannelRequest(context.Context, *connect.Request[workspacev1.GetChannelRequestRequest]) (*connect.Response[workspacev1.GetChannelRequestResponse], error)
	ExportChannelLog(context.Context, *connect.Request[workspacev1.ExportChannelLogRequest]) (*connect.ServerStreamForClient[workspacev1.ExportChannelLogResponse], error)
	CreateChannelConnectToken(context.Context, *connect.Request[workspacev1.CreateChannelConnectTokenRequest]) (*connect.Response[workspacev1.CreateChannelConnectTokenResponse], error)
	RevokeChannelConnectToken(context.Context, *connect.Request[workspacev1.RevokeChannelConnectTokenRequest]) (*connect.Response[workspacev1.RevokeChannelConnectTokenResponse], error)
	DeleteChannel(context.Context, *connect.Request[workspacev1.DeleteChannelRequest]) (*connect.Response[workspacev1.DeleteChannelResponse], error)
	ListForwards(context.Context, *connect.Request[workspacev1.ListForwardsRequest]) (*connect.Response[workspacev1.ListForwardsResponse], error)
	GetLink(context.Context, *connect.Request[workspacev1.GetLinkRequest]) (*connect.Response[workspacev1.GetLinkResponse], error)
}

// New builds a Client from opts, then from the environment. It returns an
// error when no server or no token is given, or when a file it names cannot
// be read or is empty.
func New(opts ...Option) (*Client, error) {
	cfg := &config{retry: DefaultRetryPolicy()}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	policy := normalizeRetry(cfg.retry)

	server := cfg.server
	if server == "" {
		server = os.Getenv("TOSHI_SERVER")
	}
	if server == "" {
		return nil, errors.New("toshi: no server: use WithServer or set TOSHI_SERVER")
	}

	token, err := resolveToken(cfg)
	if err != nil {
		return nil, err
	}

	caFile := cfg.caFile
	if caFile == "" {
		caFile = os.Getenv("TOSHI_CA_FILE")
	}
	httpClient := cfg.httpClient
	if httpClient != nil {
		if caFile != "" {
			return nil, errors.New("toshi: WithHTTPClient and a CA file cannot be combined: the client replaces the transport")
		}
	} else {
		httpClient, err = defaultHTTPClient(caFile)
		if err != nil {
			return nil, err
		}
	}

	api := headerClient{next: httpClient, token: token, userAgent: userAgent(cfg.product)}
	scope := scopeInterceptor{project: cfg.project, environment: cfg.environment}
	retry := retryInterceptor{policy: policy}
	interceptors := append(cfg.interceptors, scope, retry)
	options := append([]connect.ClientOption{connect.WithInterceptors(interceptors...)}, cfg.connectOptions...)

	client := &Client{
		Project:         workspacev1connect.NewProjectServiceClient(api, server, options...),
		Secret:          workspacev1connect.NewSecretServiceClient(api, server, options...),
		Webhook:         workspacev1connect.NewWebhookServiceClient(api, server, options...),
		Display:         workspacev1connect.NewDisplayServiceClient(api, server, options...),
		Channel:         workspacev1connect.NewChannelServiceClient(api, server, options...),
		Recording:       workspacev1connect.NewRecordingServiceClient(api, server, options...),
		Volume:          workspacev1connect.NewVolumeServiceClient(api, server, options...),
		Artifact:        workspacev1connect.NewArtifactServiceClient(api, server, options...),
		ArtifactUpload:  workspacev1connect.NewArtifactUploadServiceClient(api, server, options...),
		Workspace:       workspacev1connect.NewWorkspaceServiceClient(api, server, options...),
		HostJob:         workspacev1connect.NewHostJobServiceClient(api, server, options...),
		Host:            workspacev1connect.NewHostServiceClient(api, server, options...),
		Token:           workspacev1connect.NewTokenServiceClient(api, server, options...),
		AuditEvent:      workspacev1connect.NewAuditEventServiceClient(api, server, options...),
		Filler:          workspacev1connect.NewFillerServiceClient(api, server, options...),
		StorageProvider: workspacev1connect.NewStorageProviderServiceClient(api, server, options...),
		project:         cfg.project,
		environment:     cfg.environment,
		retry:           policy,
	}
	client.Workload = newWorkloadClient(client, workspacev1connect.NewWorkloadServiceClient(api, server, options...))
	return client, nil
}

// resolveToken returns the function that gives each request its token, by
// the documented precedence: the source, the option, the option's file,
// TOSHI_TOKEN, then TOSHI_TOKEN_FILE. A token file is read here, so a missing
// or empty one fails New.
func resolveToken(cfg *config) (func(context.Context) (string, error), error) {
	if cfg.tokenSource != nil {
		return sourceToken(cfg.tokenSource), nil
	}
	if token := strings.TrimSpace(cfg.token); token != "" {
		return fixedToken(token), nil
	}
	if cfg.tokenFile != "" {
		return newTokenFile(cfg.tokenFile)
	}
	if token := strings.TrimSpace(os.Getenv("TOSHI_TOKEN")); token != "" {
		return fixedToken(token), nil
	}
	if path := os.Getenv("TOSHI_TOKEN_FILE"); path != "" {
		return newTokenFile(path)
	}
	return nil, errors.New("toshi: no token: use WithTokenSource, WithToken or WithTokenFile, or set TOSHI_TOKEN or TOSHI_TOKEN_FILE")
}

// fixedToken returns a token function that always gives token.
func fixedToken(token string) func(context.Context) (string, error) {
	return func(context.Context) (string, error) { return token, nil }
}

// sourceToken returns a token function that asks source, trims its token and
// refuses an empty one.
func sourceToken(source func(context.Context) (string, error)) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		token, err := source(ctx)
		if err != nil {
			return "", fmt.Errorf("toshi: token source: %w", err)
		}
		token = strings.TrimSpace(token)
		if token == "" {
			return "", errors.New("toshi: token source: empty token")
		}
		return token, nil
	}
}

// tokenFile is a token file the client reads again when its modification
// time or size changes.
type tokenFile struct {
	path string

	mu      sync.Mutex
	token   string
	modTime time.Time
	size    int64
}

// newTokenFile reads the token file at path and returns its token function.
func newTokenFile(path string) (func(context.Context) (string, error), error) {
	f := &tokenFile{path: path, size: -1}
	if _, err := f.read(); err != nil {
		return nil, err
	}
	return func(context.Context) (string, error) { return f.read() }, nil
}

// read returns the file's token, reading the file again when it has changed
// since the last read. It stats the file before reading it, so a change made
// in between is read again next time. After a failure the next call reads the
// file again.
func (f *tokenFile) read() (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	info, err := os.Stat(f.path)
	if err != nil {
		f.size = -1
		return "", fmt.Errorf("toshi: read token file %s: %w", f.path, err)
	}
	if info.ModTime().Equal(f.modTime) && info.Size() == f.size {
		return f.token, nil
	}
	token, err := readTokenFile(f.path)
	if err != nil {
		f.size = -1
		return "", err
	}
	f.token, f.modTime, f.size = token, info.ModTime(), info.Size()
	return token, nil
}

// readTokenFile reads a token file, trims its contents and refuses an empty
// file.
func readTokenFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("toshi: read token file %s: %w", path, err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", fmt.Errorf("toshi: token file %s is empty", path)
	}
	return token, nil
}

// defaultHTTPClient returns an HTTP/2-capable client that trusts the system's
// roots and, when caFile is set, the certificates of that PEM bundle.
func defaultHTTPClient(caFile string) (*http.Client, error) {
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment}
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = base.Clone()
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		roots, err := rootCAs(caFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = roots
	}
	transport.TLSClientConfig = tlsConfig
	transport.ForceAttemptHTTP2 = true
	return &http.Client{Transport: transport}, nil
}

// rootCAs returns the system's pool with every certificate of the PEM bundle
// at path added. A bundle without a certificate is an error.
func rootCAs(path string) (*x509.CertPool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("toshi: read CA file %s: %w", path, err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	added := 0
	for rest := b; ; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("toshi: CA file %s: %w", path, err)
		}
		pool.AddCert(cert)
		added++
	}
	if added == 0 {
		return nil, fmt.Errorf("toshi: CA file %s holds no certificate", path)
	}
	return pool, nil
}
