package toshi

import (
	"net/http"
	"time"
)

// Option configures the Client New returns.
type Option func(*config)

// config holds what an Option sets and what the environment fills.
type config struct {
	server      string
	token       string
	tokenFile   string
	caFile      string
	httpClient  *http.Client
	project     string
	environment string
	retry       RetryPolicy
}

// WithServer sets the control plane's base URL. Without it New reads
// TOSHI_SERVER.
func WithServer(url string) Option {
	return func(c *config) { c.server = url }
}

// WithToken sets the workspace API token. It wins over WithTokenFile,
// TOSHI_TOKEN and TOSHI_TOKEN_FILE.
func WithToken(token string) Option {
	return func(c *config) { c.token = token }
}

// WithTokenFile sets the file holding the token. Its contents are trimmed,
// and an empty file is an error.
func WithTokenFile(path string) Option {
	return func(c *config) { c.tokenFile = path }
}

// WithCAFile sets a PEM bundle trusted besides the system's roots. Without
// it New reads TOSHI_CA_FILE.
func WithCAFile(path string) Option {
	return func(c *config) { c.caFile = path }
}

// WithHTTPClient sets the HTTP client, replacing the default transport. It
// cannot be combined with a CA file, because the CA file would be ignored.
func WithHTTPClient(client *http.Client) Option {
	return func(c *config) { c.httpClient = client }
}

// WithScope sets the project and environment New fills into an empty request
// field. An empty value fills nothing.
func WithScope(project, environment string) Option {
	return func(c *config) {
		c.project = project
		c.environment = environment
	}
}

// WithRetryPolicy sets how often and how long calls are retried. A
// MaxAttempts of 1 or less disables retries, and zero backoff values take
// the defaults.
func WithRetryPolicy(policy RetryPolicy) Option {
	return func(c *config) { c.retry = policy }
}

// RetryPolicy says how often and how long a call is retried. MaxAttempts
// counts the first try; a value of 1 or less disables retries.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// DefaultRetryPolicy returns five attempts, a 200 ms initial backoff and a
// 10 s cap.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    5,
		InitialBackoff: 200 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
	}
}
