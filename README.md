# toshi-go

A Go client for the Toshi Workspace API, `toshi.workspace.v1`.

```sh
go get go.toshicloud.io/toshi
```

v0.1.0 is under github.com/toshicloud/toshi-go and later versions use
go.toshicloud.io/toshi.

## Using the client

<!-- ExampleNew -->
```go
import (
	"context"
	"fmt"
	"log"

	"go.toshicloud.io/api/workspacev1"
	"go.toshicloud.io/toshi"
)

ctx := context.Background()
client, err := toshi.New(toshi.WithScope("default", "production"))
if err != nil {
	log.Fatal(err)
}
jobs, err := client.Workload().Jobs().List(ctx, &workspacev1.ListJobsRequest{})
if err != nil {
	log.Fatal(err)
}
for _, job := range jobs.GetJobs() {
	fmt.Println(job.GetId(), job.GetStatus().GetState())
}
```

`Client` has one method per service. `Workload()` returns a
`*WorkloadClient`, written by this module: a job's calls under
`Workload().Jobs()`, a sandbox's under `Workload().Sandboxes()`, a service's
under `Workload().Services()`, and the calls that span the three on
`Workload()` itself (`WatchEvents`, `ListEvents`, `GetOperation`,
`WaitOperation`).
`Project()`, `Secret()`, `Webhook()`, `Display()`, `Channel()`, `Recording()`,
`Volume()`, `Artifact()`, `ArtifactUpload()`, `Workspace()`, `HostJob()`,
`Host()`, `Token()`, `AuditEvent()`, `Filler()`, `StorageProvider()`,
`Product()`, `Discovery()`, `Connection()`, `Hook()`, `Role()` and
`Trigger()` return the generated Connect clients of their service; `Channel()`
is `ChannelClient`, which spells out the service's calls.

`New` reads the server and token from `TOSHI_SERVER` and `TOSHI_TOKEN`
(or the file `TOSHI_TOKEN_FILE` names), and an extra CA bundle from
`TOSHI_CA_FILE`, unless options name them. The token is a workspace API
token. There is no default project or environment: set them with
`WithScope`, or on each request.

- Reads, and the writes that take a `request_key`, are retried when the
  server is unavailable or the connection drops, with capped, jittered
  backoff that stops when the context ends. An empty `request_key` is
  filled in before the first try and kept for every retry, so the server
  answers a retry with the first result. `RESOURCE_EXHAUSTED` and every
  other error go back to the caller.
- `OpenDisplaySession`, the `SecretService` writes and `Execute` are never
  retried.
- `Run` creates a job and follows it: write nothing, read `Stdout` and
  `Stderr` at the same time, and `Wait` returns nil for exit 0 or a
  `*JobExitError` for any other end.
- `Workload().Jobs().Wait`, `Workload().Sandboxes().Wait`,
  `Workload().Services().Wait` and `Workload().WaitOperation` ask again until
  the resource reaches the state or the operation is done, or the context
  ends.
- The watches, `Workload().Jobs().Watch`, `Workload().Sandboxes().Watch`,
  `Workload().Services().Watch`, `Workload().WatchEvents` and
  `Workload().Jobs().WatchOutput`, resume after a dropped connection from the
  last cursor or byte offset received. A cursor the server no longer keeps
  ends the watch with `OUT_OF_RANGE`.
- `Workload().Sandboxes().Execute` starts a command and returns a `Process`:
  write its standard input to `Stdin`, read `Stdout` and `Stderr` at the
  same time, and `Wait` returns an `*ExitError` for a non-zero exit or a
  cancelled command, or a `*StreamLostError` with the request key when the
  connection drops before the exit.

Execute needs HTTP/2, which the server offers over https.

## Tokens that change

`WithTokenSource` gives the client a function it calls for the token of
every request, so a token that is refreshed, for example from a secret
manager, is used without a new client. It wins over every other token
option. The client calls it concurrently, so it must be fast: cache the
token and refresh it outside the call. An error from it fails the call with
`UNAUTHENTICATED`, and the call is not retried.

<!-- ExampleWithTokenSource -->
```go
import (
	"context"
	"log"
	"sync/atomic"

	"go.toshicloud.io/toshi"
)

var token atomic.Value // Store each new token here, for example from a refresh loop.
token.Store("first-token")
client, err := toshi.New(toshi.WithTokenSource(func(context.Context) (string, error) {
	return token.Load().(string), nil
}))
if err != nil {
	log.Fatal(err)
}
_ = client // Use the client as in the other examples.
```

A token file, from `WithTokenFile` or `TOSHI_TOKEN_FILE`, is read again
before a request when its modification time or size has changed, so a
rotated token, such as a Kubernetes projected service account token, is
picked up without a new client.

## Lists

`List` returns one page, and passing its `next_page_token` back asks for the
next. The `All` calls yield every item across the pages:
`Workload().Jobs().All`, `Workload().Sandboxes().All`,
`Workload().Services().All`, `Workload().Sandboxes().AllPreviewShareLinks`,
`Workload().Services().AllRuns` and `Workload().AllEvents`. A loop that breaks
stops the pages, and an error ends the sequence. For a generated client's
`List` call, `toshi.Paginate` follows the pages the same way, given the
request, the method and a function that picks the items from a response.

<!-- ExampleJobsClient_All -->
```go
import (
	"context"
	"fmt"
	"log"

	"go.toshicloud.io/api/workspacev1"
	"go.toshicloud.io/toshi"
)

ctx := context.Background()
client, err := toshi.New(toshi.WithScope("default", "production"))
if err != nil {
	log.Fatal(err)
}
for job, err := range client.Workload().Jobs().All(ctx, &workspacev1.ListJobsRequest{}) {
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(job.GetId(), job.GetStatus().GetState())
}
```

## Products that are off

A call for a product that is off for the workspace, or that turns on an
outside product without accepting the access it asks for, fails with a
`*toshi.ProductError`. `errors.As` finds it on the error of any call, `Wait`,
the watches and `Execute` included, and `connect.CodeOf` reads its code as
before. It has the product's ID (`Product`), the `Reason`
(`ReasonProductNotInstalled`, `ReasonProductDisabled`,
`ReasonProductNotEntitled`, `ReasonProductTurnedOff` or
`ReasonAccessNotAccepted`), the setting that turns the product on
(`Feature`, such as `runners.enabled`), how far a product the deployment
disables has wound down (`State`) and, for `ReasonAccessNotAccepted`, the
access to accept (`Access`).

<!-- ExampleProductError -->
```go
ctx := context.Background()
client, err := toshi.New()
if err != nil {
	log.Fatal(err)
}
// Turning an outside product on installs it, and the API asks the
// caller to accept the access it wants first.
enable := &workspacev1.UpdateProductRequest{Id: "acme_ci", Enabled: proto.Bool(true)}
_, err = client.Product().UpdateProduct(ctx, connect.NewRequest(enable))
var refused *toshi.ProductError
if errors.As(err, &refused) && refused.Reason == toshi.ReasonAccessNotAccepted {
	fmt.Println(refused.Product, refused.Reason, refused.Access)
	// Accept exactly the access the product asks for.
	enable.AcceptedAccess = refused.Access
	_, err = client.Product().UpdateProduct(ctx, connect.NewRequest(enable))
}
if errors.As(err, &refused) {
	// Off for the workspace: say which product and why.
	fmt.Println(refused.Product, refused.Reason, refused.Feature)
	return
}
if err != nil {
	log.Fatal(err)
}
```

## Interceptors and the User-Agent

`WithInterceptors` adds Connect interceptors to every service client, for
logging, metrics or tracing;
[otelconnect](https://github.com/connectrpc/otelconnect-go) is one that fits.
They wrap the client's own scope and retry: an interceptor sees one call for
each unary method call, with the retries inside it, and one for each stream
the client opens (a watch that resumes opens a new one). It
runs before the scope is filled in and never sees the token, which the client
sets on each HTTP request below every interceptor.

<!-- ExampleWithInterceptors -->
```go
import (
	"context"
	"log"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"go.toshicloud.io/toshi"
)

logCalls := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		start := time.Now()
		resp, err := next(ctx, req)
		slog.InfoContext(ctx, "toshi call",
			"procedure", req.Spec().Procedure,
			"duration", time.Since(start),
			"err", err)
		return resp, err
	}
})
client, err := toshi.New(
	toshi.WithScope("default", "production"),
	toshi.WithInterceptors(logCalls),
	toshi.WithUserAgent("myapp/1.2"),
)
if err != nil {
	log.Fatal(err)
}
_ = client // Use the client as in the other examples.
```

`WithConnectOptions` passes other Connect client options, such as
`connect.WithSendMaxBytes`, to every service client after the client's own.
An interceptor passed that way runs inside the retry, once for each try.

Every request carries the User-Agent
`toshi-go/<version> connect-go/<version> (<Go version>)`, for example
`toshi-go/0.4.0 connect-go/1.21.1 (go1.26.0)`. `WithUserAgent("myapp/1.2")`
puts your product in front: `myapp/1.2 toshi-go/0.4.0 ...`. `toshi.Version`
is this module's version.
## Testing with toshitest

`toshitest` starts a fake Toshi inside a test: a TLS server speaking HTTP/2
that serves every service of the Workspace API, and a client wired to it. A
test describes what the calls answer, then calls the client as it would call a
real one. `toshitest.New(t)` closes the fake with `t.Cleanup`; the example
below runs without a test, so it passes nil and closes the fake itself.

<!-- toshitest.ExampleNew -->
```go
import (
	"context"
	"io"
	"log"
	"os"

	"go.toshicloud.io/api/workspacev1"
	"go.toshicloud.io/toshi/toshitest"
)

tt := toshitest.New(nil) // nil: the example closes the fake itself
defer tt.Close()

tt.Job("job-1").Stdout("deployed\n").ExitCode(0)
run, err := tt.Client().Run(context.Background(), &workspacev1.CreateJobRequest{})
if err != nil {
	log.Fatal(err)
}
if _, err := io.Copy(os.Stdout, run.Stdout()); err != nil {
	log.Print(err)
}
if err := run.Wait(); err != nil {
	log.Fatal(err)
}
// Output:
// deployed
```

`Job`, `Sandbox` and `Execute` set the state of a job, a sandbox or a command
and the output and exit code it answers. `On`, `OnServerStream` and `OnBidi`
replace the fake's answer for one procedure with a hook, typed at the call
site; a hook that does not fit its procedure fails the test at setup. `Calls`
returns what the client sent, in order. The package documentation lists what
the fake answers and what needs a hook.

## Generated code

The messages and the clients come from the Go module
[`go.toshicloud.io/api`](https://pkg.go.dev/go.toshicloud.io/api),
which holds the protos of `toshi.workspace.v1` and the code generated
from them:

- `go.toshicloud.io/api/workspacev1`: the messages;
- `go.toshicloud.io/api/workspacev1/workspacev1connect`: a
  [Connect](https://connectrpc.com/docs/go/getting-started) client for each
  service.

This module generates no copy of its own. Protobuf for Go refuses to
start a program that registers the same `.proto` file twice, so a program
that imports both modules links one copy. `go.mod` names the API module
version; moving to another is one pull request that changes `go.mod` and
the code that follows it.

## Upgrading

The API module moved to `go.toshicloud.io/api`; v0.3.0 is the first release
at the new path. Change the import prefix in your code with:

```sh
git grep -lz 'github\.com/toshicloud/toshi-api' -- '*.go' \
  | xargs -0 -r sed -i 's#github\.com/toshicloud/toshi-api#go.toshicloud.io/api#'
```

Every service field of `Client` is a method of the same name now, and so are
`Workload`'s fields. An exported field could be replaced, and a replaced
service client silently loses the client's authentication, scope and retry;
the methods hand out the client `New` built, so that cannot happen. One call
becomes `client.Workload().Jobs().Create(ctx, req)` where it was
`client.Workload.Jobs.Create(ctx, req)`.

`Option` is a sealed interface now, so it can grow without another break;
code that passes options compiles unchanged.

## Versions

The module's major version follows the API's: `v1` will serve
`toshi.workspace.v1`, and a `toshi.workspace.v2` gets a `/v2` module path.
Until a Toshi release serves the API, the module stays at `v0.x` and
requires a tagged API release.

## Licence

Apache License 2.0, see LICENSE.
