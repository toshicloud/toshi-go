package toshi

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"connectrpc.com/connect"

	workspacev1connect "github.com/toshicloud/toshi-api/workspacev1/workspacev1connect"
)

// Client holds the generated clients of the Workspace API's five services,
// with this package's authentication, scope and retry interceptors.
type Client struct {
	// Workspace is the WorkspaceService client.
	Workspace workspacev1connect.WorkspaceServiceClient
	// Projects is the ProjectService client.
	Projects workspacev1connect.ProjectServiceClient
	// Secrets is the SecretService client.
	Secrets workspacev1connect.SecretServiceClient
	// Webhooks is the WebhookService client.
	Webhooks workspacev1connect.WebhookServiceClient
	// Displays is the DisplayService client.
	Displays workspacev1connect.DisplayServiceClient

	project     string
	environment string
	retry       RetryPolicy
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

	auth := authInterceptor{token: token}
	scope := scopeInterceptor{project: cfg.project, environment: cfg.environment}
	retry := retryInterceptor{policy: policy}
	interceptors := connect.WithInterceptors(auth, scope, retry)

	return &Client{
		Workspace:   workspacev1connect.NewWorkspaceServiceClient(httpClient, server, interceptors),
		Projects:    workspacev1connect.NewProjectServiceClient(httpClient, server, interceptors),
		Secrets:     workspacev1connect.NewSecretServiceClient(httpClient, server, interceptors),
		Webhooks:    workspacev1connect.NewWebhookServiceClient(httpClient, server, interceptors),
		Displays:    workspacev1connect.NewDisplayServiceClient(httpClient, server, interceptors),
		project:     cfg.project,
		environment: cfg.environment,
		retry:       policy,
	}, nil
}

// resolveToken picks the token by the documented precedence: the option, the
// option's file, TOSHI_TOKEN, then TOSHI_TOKEN_FILE.
func resolveToken(cfg *config) (string, error) {
	if token := strings.TrimSpace(cfg.token); token != "" {
		return token, nil
	}
	if cfg.tokenFile != "" {
		return readTokenFile(cfg.tokenFile)
	}
	if token := strings.TrimSpace(os.Getenv("TOSHI_TOKEN")); token != "" {
		return token, nil
	}
	if path := os.Getenv("TOSHI_TOKEN_FILE"); path != "" {
		return readTokenFile(path)
	}
	return "", errors.New("toshi: no token: use WithToken or WithTokenFile, or set TOSHI_TOKEN or TOSHI_TOKEN_FILE")
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
