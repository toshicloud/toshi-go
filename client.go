//go:generate go run ./internal/gen

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
	services

	workload *WorkloadClient
	endpoint EndpointClient

	retry RetryPolicy
}

// Workload holds the calls of WorkloadService: a job's, a sandbox's and
// a service's, and the calls that span the three.
func (c *Client) Workload() *WorkloadClient { return c.workload }

// Endpoint is the EndpointService client, its server stream included.
func (c *Client) Endpoint() EndpointClient { return c.endpoint }

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
	interceptors := append(cfg.interceptors, scope, retry, refusalInterceptor{}, deprecationInterceptor{warn: cfg.warn})
	options := append([]connect.ClientOption{connect.WithInterceptors(interceptors...)}, cfg.connectOptions...)

	client := &Client{
		services: newServices(api, server, options...),
		endpoint: workspacev1connect.NewEndpointServiceClient(api, server, options...),
		retry:    policy,
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
