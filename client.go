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

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"
)

// Client holds a client for each service of the Workspace API, with this
// package's authentication, scope and retry, and the interceptors and options
// the caller adds. Each service client is returned by a method; a Client built
// by New is the only way to get one, so every call goes through the client's
// authentication, scope and retry.
type Client struct {
	workload        *WorkloadClient
	project         workspacev1connect.ProjectServiceClient
	secret          workspacev1connect.SecretServiceClient
	webhook         workspacev1connect.WebhookServiceClient
	display         workspacev1connect.DisplayServiceClient
	endpoint        EndpointClient
	recording       workspacev1connect.RecordingServiceClient
	volume          workspacev1connect.VolumeServiceClient
	artifact        workspacev1connect.ArtifactServiceClient
	artifactUpload  workspacev1connect.ArtifactUploadServiceClient
	workspace       workspacev1connect.WorkspaceServiceClient
	hostJob         workspacev1connect.HostJobServiceClient
	host            workspacev1connect.HostServiceClient
	token           workspacev1connect.TokenServiceClient
	auditEvent      workspacev1connect.AuditEventServiceClient
	filler          workspacev1connect.FillerServiceClient
	storageProvider workspacev1connect.StorageProviderServiceClient
	product         workspacev1connect.ProductServiceClient
	discovery       workspacev1connect.DiscoveryServiceClient
	connection      workspacev1connect.ConnectionServiceClient
	account         workspacev1connect.AccountServiceClient
	image           workspacev1connect.ImageServiceClient
	role            workspacev1connect.RoleServiceClient
	trigger         workspacev1connect.TriggerServiceClient
	grant           workspacev1connect.GrantServiceClient
	domain          workspacev1connect.DomainServiceClient
	modelCredential workspacev1connect.ModelCredentialServiceClient

	retry RetryPolicy
}

// Workload holds the calls of WorkloadService: a job's, a sandbox's and
// a service's, and the calls that span the three.
func (c *Client) Workload() *WorkloadClient { return c.workload }

// Project is the ProjectService client.
func (c *Client) Project() workspacev1connect.ProjectServiceClient { return c.project }

// Secret is the SecretService client.
func (c *Client) Secret() workspacev1connect.SecretServiceClient { return c.secret }

// Webhook is the WebhookService client.
func (c *Client) Webhook() workspacev1connect.WebhookServiceClient { return c.webhook }

// Display is the DisplayService client.
func (c *Client) Display() workspacev1connect.DisplayServiceClient { return c.display }

// Endpoint is the EndpointService client, its server stream included.
func (c *Client) Endpoint() EndpointClient { return c.endpoint }

// Recording is the RecordingService client.
func (c *Client) Recording() workspacev1connect.RecordingServiceClient { return c.recording }

// Volume is the VolumeService client.
func (c *Client) Volume() workspacev1connect.VolumeServiceClient { return c.volume }

// Artifact is the ArtifactService client.
func (c *Client) Artifact() workspacev1connect.ArtifactServiceClient { return c.artifact }

// ArtifactUpload is the ArtifactUploadService client.
func (c *Client) ArtifactUpload() workspacev1connect.ArtifactUploadServiceClient {
	return c.artifactUpload
}

// Workspace is the WorkspaceService client.
func (c *Client) Workspace() workspacev1connect.WorkspaceServiceClient { return c.workspace }

// HostJob is the HostJobService client.
func (c *Client) HostJob() workspacev1connect.HostJobServiceClient { return c.hostJob }

// Host is the HostService client.
func (c *Client) Host() workspacev1connect.HostServiceClient { return c.host }

// Token is the TokenService client.
func (c *Client) Token() workspacev1connect.TokenServiceClient { return c.token }

// AuditEvent is the AuditEventService client.
func (c *Client) AuditEvent() workspacev1connect.AuditEventServiceClient { return c.auditEvent }

// Filler is the FillerService client.
func (c *Client) Filler() workspacev1connect.FillerServiceClient { return c.filler }

// StorageProvider is the StorageProviderService client.
func (c *Client) StorageProvider() workspacev1connect.StorageProviderServiceClient {
	return c.storageProvider
}

// Product is the ProductService client.
func (c *Client) Product() workspacev1connect.ProductServiceClient { return c.product }

// Discovery is the DiscoveryService client.
func (c *Client) Discovery() workspacev1connect.DiscoveryServiceClient { return c.discovery }

// Connection is the ConnectionService client.
func (c *Client) Connection() workspacev1connect.ConnectionServiceClient { return c.connection }

// Account is the AccountService client.
func (c *Client) Account() workspacev1connect.AccountServiceClient { return c.account }

// Image is the ImageService client.
func (c *Client) Image() workspacev1connect.ImageServiceClient { return c.image }

// Role is the RoleService client.
func (c *Client) Role() workspacev1connect.RoleServiceClient { return c.role }

// Trigger is the TriggerService client.
func (c *Client) Trigger() workspacev1connect.TriggerServiceClient { return c.trigger }

// Grant is the GrantService client.
func (c *Client) Grant() workspacev1connect.GrantServiceClient { return c.grant }

// Domain is the DomainService client.
func (c *Client) Domain() workspacev1connect.DomainServiceClient { return c.domain }

// ModelCredential is the ModelCredentialService client.
func (c *Client) ModelCredential() workspacev1connect.ModelCredentialServiceClient {
	return c.modelCredential
}

// EndpointClient is the calls of EndpointService.
type EndpointClient interface {
	CreateEndpoint(context.Context, *connect.Request[workspacev1.CreateEndpointRequest]) (*connect.Response[workspacev1.CreateEndpointResponse], error)
	ListEndpoints(context.Context, *connect.Request[workspacev1.ListEndpointsRequest]) (*connect.Response[workspacev1.ListEndpointsResponse], error)
	GetEndpoint(context.Context, *connect.Request[workspacev1.GetEndpointRequest]) (*connect.Response[workspacev1.GetEndpointResponse], error)
	GetEndpointPolicy(context.Context, *connect.Request[workspacev1.GetEndpointPolicyRequest]) (*connect.Response[workspacev1.GetEndpointPolicyResponse], error)
	SetEndpointPolicy(context.Context, *connect.Request[workspacev1.SetEndpointPolicyRequest]) (*connect.Response[workspacev1.SetEndpointPolicyResponse], error)
	ListEndpointRequests(context.Context, *connect.Request[workspacev1.ListEndpointRequestsRequest]) (*connect.Response[workspacev1.ListEndpointRequestsResponse], error)
	GetEndpointRequest(context.Context, *connect.Request[workspacev1.GetEndpointRequestRequest]) (*connect.Response[workspacev1.GetEndpointRequestResponse], error)
	ExportEndpointLog(context.Context, *connect.Request[workspacev1.ExportEndpointLogRequest]) (*connect.ServerStreamForClient[workspacev1.ExportEndpointLogResponse], error)
	CreateEndpointConnectToken(context.Context, *connect.Request[workspacev1.CreateEndpointConnectTokenRequest]) (*connect.Response[workspacev1.CreateEndpointConnectTokenResponse], error)
	RevokeEndpointConnectToken(context.Context, *connect.Request[workspacev1.RevokeEndpointConnectTokenRequest]) (*connect.Response[workspacev1.RevokeEndpointConnectTokenResponse], error)
	DeleteEndpoint(context.Context, *connect.Request[workspacev1.DeleteEndpointRequest]) (*connect.Response[workspacev1.DeleteEndpointResponse], error)
	ListForwards(context.Context, *connect.Request[workspacev1.ListForwardsRequest]) (*connect.Response[workspacev1.ListForwardsResponse], error)
	GetTunnel(context.Context, *connect.Request[workspacev1.GetTunnelRequest]) (*connect.Response[workspacev1.GetTunnelResponse], error)
	SendEndpointMessage(context.Context, *connect.Request[workspacev1.SendEndpointMessageRequest]) (*connect.Response[workspacev1.SendEndpointMessageResponse], error)
	TailEndpointMessages(context.Context, *connect.Request[workspacev1.TailEndpointMessagesRequest]) (*connect.ServerStreamForClient[workspacev1.TailEndpointMessagesResponse], error)
	ReplayEndpointMessage(context.Context, *connect.Request[workspacev1.ReplayEndpointMessageRequest]) (*connect.Response[workspacev1.ReplayEndpointMessageResponse], error)
}

// New builds a Client from opts, then from the environment. It returns an
// error when no server or no token is given, or when a file it names cannot
// be read or is empty.
func New(opts ...Option) (*Client, error) {
	cfg := &config{retry: DefaultRetryPolicy()}
	for _, opt := range opts {
		if opt != nil {
			opt.apply(cfg)
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
	interceptors := append(cfg.interceptors, scope, retry, refusalInterceptor{})
	options := append([]connect.ClientOption{connect.WithInterceptors(interceptors...)}, cfg.connectOptions...)

	client := &Client{
		project:         workspacev1connect.NewProjectServiceClient(api, server, options...),
		secret:          workspacev1connect.NewSecretServiceClient(api, server, options...),
		webhook:         workspacev1connect.NewWebhookServiceClient(api, server, options...),
		display:         workspacev1connect.NewDisplayServiceClient(api, server, options...),
		endpoint:        workspacev1connect.NewEndpointServiceClient(api, server, options...),
		recording:       workspacev1connect.NewRecordingServiceClient(api, server, options...),
		volume:          workspacev1connect.NewVolumeServiceClient(api, server, options...),
		artifact:        workspacev1connect.NewArtifactServiceClient(api, server, options...),
		artifactUpload:  workspacev1connect.NewArtifactUploadServiceClient(api, server, options...),
		workspace:       workspacev1connect.NewWorkspaceServiceClient(api, server, options...),
		hostJob:         workspacev1connect.NewHostJobServiceClient(api, server, options...),
		host:            workspacev1connect.NewHostServiceClient(api, server, options...),
		token:           workspacev1connect.NewTokenServiceClient(api, server, options...),
		auditEvent:      workspacev1connect.NewAuditEventServiceClient(api, server, options...),
		filler:          workspacev1connect.NewFillerServiceClient(api, server, options...),
		storageProvider: workspacev1connect.NewStorageProviderServiceClient(api, server, options...),
		product:         workspacev1connect.NewProductServiceClient(api, server, options...),
		discovery:       workspacev1connect.NewDiscoveryServiceClient(api, server, options...),
		connection:      workspacev1connect.NewConnectionServiceClient(api, server, options...),
		account:         workspacev1connect.NewAccountServiceClient(api, server, options...),
		image:           workspacev1connect.NewImageServiceClient(api, server, options...),
		role:            workspacev1connect.NewRoleServiceClient(api, server, options...),
		trigger:         workspacev1connect.NewTriggerServiceClient(api, server, options...),
		grant:           workspacev1connect.NewGrantServiceClient(api, server, options...),
		domain:          workspacev1connect.NewDomainServiceClient(api, server, options...),
		modelCredential: workspacev1connect.NewModelCredentialServiceClient(api, server, options...),
		retry:           policy,
	}
	workload := newWorkloadClient(client, workspacev1connect.NewWorkloadServiceClient(api, server, options...))
	client.workload = workload
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
