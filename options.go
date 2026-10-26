package toshi

import (
	"context"
	"net/http"
	"time"

	"connectrpc.com/connect"
)

// Option configures the Client New returns. It is sealed: only options of
// this package implement it, so an option can be added without breaking
// callers.
type Option interface {
	apply(*config)
}

// optionFunc is an Option that runs f.
type optionFunc func(*config)

// apply runs f.
func (f optionFunc) apply(c *config) { f(c) }

// config holds what an Option sets and what the environment fills.
type config struct {
	server         string
	tokenSource    func(context.Context) (string, error)
	token          string
	tokenFile      string
	caFile         string
	httpClient     *http.Client
	project        string
	environment    string
	retry          RetryPolicy
	product        string
	interceptors   []connect.Interceptor
	connectOptions []connect.ClientOption
}

// WithServer sets the control plane's base URL. Without it New reads
// TOSHI_SERVER.
func WithServer(url string) Option {
	return optionFunc(func(c *config) { c.server = url })
}

// WithTokenSource sets a function the client calls for the token of every
// HTTP request, with that request's context, so a token that changes is used
// without a new Client. It wins over WithToken, WithTokenFile, TOSHI_TOKEN and
// TOSHI_TOKEN_FILE, and New then reads none of them. The client calls it
// concurrently and once for each try of a call, so it must be safe for
// concurrent use and fast: when getting a token is slow, cache it and refresh
// it outside the call. The token is trimmed. An error or an empty token fails
// the call with CodeUnauthenticated before the request is sent, and the call
// is not retried.
func WithTokenSource(source func(context.Context) (string, error)) Option {
	return optionFunc(func(c *config) { c.tokenSource = source })
}

// WithToken sets the workspace API token. It wins over WithTokenFile,
// TOSHI_TOKEN and TOSHI_TOKEN_FILE; WithTokenSource wins over it.
func WithToken(token string) Option {
	return optionFunc(func(c *config) { c.token = token })
}

// WithTokenFile sets the file holding the token. Its contents are trimmed,
// and an empty file is an error. New reads the file, and before each request
// the client reads it again when its modification time or size has changed,
// so a rotated token is used without a new Client; a file that has become
// empty or unreadable fails the call with CodeUnauthenticated. It wins over
// TOSHI_TOKEN and TOSHI_TOKEN_FILE, which is read the same way;
// WithTokenSource and WithToken win over it.
func WithTokenFile(path string) Option {
	return optionFunc(func(c *config) { c.tokenFile = path })
}

// WithCAFile sets a PEM bundle trusted besides the system's roots. Without
// it New reads TOSHI_CA_FILE.
func WithCAFile(path string) Option {
	return optionFunc(func(c *config) { c.caFile = path })
}

// WithHTTPClient sets the HTTP client, replacing the default transport. It
// cannot be combined with a CA file, because the CA file would be ignored.
func WithHTTPClient(client *http.Client) Option {
	return optionFunc(func(c *config) { c.httpClient = client })
}

// WithScope sets the project and environment New fills into an empty request
// field. An empty value fills nothing.
func WithScope(project, environment string) Option {
	return optionFunc(func(c *config) {
		c.project = project
		c.environment = environment
	})
}

// WithRetryPolicy sets how often and how long calls are retried. A
// MaxAttempts of 1 or less disables retries, and zero backoff values take
// the defaults.
func WithRetryPolicy(policy RetryPolicy) Option {
	return optionFunc(func(c *config) { c.retry = policy })
}

// WithInterceptors adds interceptors to every service client. They wrap the
// client's own scope and retry interceptors, the first one outermost: they see
// one call for each unary method call, with any retries inside it, and each
// stream the client opens, so a watch that resumes shows as a new stream. They
// run before the scope is filled in, and they never see the Authorization
// header, which the client sets on each HTTP request below every interceptor.
// Calling WithInterceptors again adds to the list.
func WithInterceptors(interceptors ...connect.Interceptor) Option {
	return optionFunc(func(c *config) { c.interceptors = append(c.interceptors, interceptors...) })
}

// WithConnectOptions adds Connect client options to every service client,
// after the client's own. An interceptor added this way runs inside the
// retry, once for each try. Calling WithConnectOptions again adds to the
// list.
func WithConnectOptions(opts ...connect.ClientOption) Option {
	return optionFunc(func(c *config) { c.connectOptions = append(c.connectOptions, opts...) })
}

// WithUserAgent puts product, for example "myapp/1.2", in front of the
// User-Agent every request carries, which is otherwise
// "toshi-go/<Version> connect-go/<Connect version> (<Go version>)", for
// example "toshi-go/0.2.1 connect-go/1.21.1 (go1.26.0)". An empty product
// adds nothing.
func WithUserAgent(product string) Option {
	return optionFunc(func(c *config) { c.product = product })
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
