# Using the client

A Go client for the Toshi Workspace API, `toshi.workspace.v1`.

```sh
go get go.toshicloud.io/toshi
```

v0.1.0 is under github.com/toshicloud/toshi-go and later versions use
go.toshicloud.io/toshi.


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
`Workload()` itself (`WatchEvents`, `ListEvents`, `EmitEvent`, `GetOperation`,
`AnswerPrompt`, `HoldWorkload`, `ReleaseWorkload`, `WaitOperation`).
`HoldWorkload` and `ReleaseWorkload` are operator-only; their reason header
comes from an interceptor, as [Operator calls](#operator-calls) shows.
`Workload().Sandboxes().ExecuteCommand`
runs a command without standard input. `Workload().Services()` mints, lists
and revokes the share links of a service's provided names, keeps its alarms
(`SetAlarm`, `GetAlarm`, `ListAlarms`, `DeleteAlarm`, `PauseAlarm`,
`ResumeAlarm`, `RunAlarmNow`), and runs its recipe's sign-in programs
(`StartSignIn`, `AnswerSignIn`, `CancelSignIn`, `GetSignInStatus`).
`Project()`, `Secret()`, `Webhook()`, `Display()`, `Endpoint()`, `Recording()`,
`Volume()`, `Artifact()`, `ArtifactUpload()`, `Workspace()`, `HostJob()`,
`Host()`, `Token()`, `AuditEvent()`, `Filler()`, `StorageProvider()`,
`Product()`, `Discovery()`, `Connection()`, `Role()`, `Trigger()`,
`Grant()`, `Domain()`, `Account()` and `Image()` return the generated Connect clients of
their service; `Endpoint()` is `EndpointClient`, which spells out the service's
calls.

`New` reads the server and token from `TOSHI_SERVER` and `TOSHI_TOKEN`
(or the file `TOSHI_TOKEN_FILE` names), and an extra CA bundle from
`TOSHI_CA_FILE`, unless options name them. The token is a workspace API
token. There is no default project or environment: set them with
`WithScope`, or on each request.

`WithServer` sets the control plane's base URL, `WithCAFile` a PEM bundle
trusted besides the system's roots, and `WithHTTPClient` the HTTP client,
replacing the default transport; a client from `WithHTTPClient` cannot be
combined with a CA file, because the CA file would be ignored.

- Reads, and the writes that take a `request_key`, are retried when the
  server is unavailable or the connection drops, with capped, jittered
  backoff that stops when the context ends. An empty `request_key` is
  filled in before the first try and kept for every retry, so the server
  answers a retry with the first result. A `RESOURCE_EXHAUSTED` refusal
  that names a wait (a `google.rpc.RetryInfo` detail, else `Retry-After`),
  as the API's rate limit does, is retried the same way after that wait or
  the backoff, whichever is longer, never longer than `MaxBackoff`; so is an
  HTTP 429 with a `Retry-After`. Any other `RESOURCE_EXHAUSTED`, such as a
  plan's limit, and every other error go back to the caller.
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
- An event's payload says what happened: a trigger's `TriggerResult` names the
  key's `Reason` and its workload, and with `process_exited` or `setup_failed`
  it carries the sandbox's `ExitCode` and `Message`.
- `Workload().Sandboxes().Execute` starts a command and returns a `Process`:
  write its standard input to `Stdin`, read `Stdout` and `Stderr` at the
  same time, and `Wait` returns an `*ExitError` for a non-zero exit or a
  cancelled command, or a `*StreamLostError` with the request key when the
  connection drops before the exit.

Execute needs HTTP/2, which the server offers over https.

The policy behind the first bullet is `RetryPolicy`: `MaxAttempts`, which
counts the first try and disables retries at 1 or less, `InitialBackoff`
and `MaxBackoff`, where a zero value takes the default. `WithRetryPolicy`
sets it, and `DefaultRetryPolicy` returns the default — five attempts, a
200 ms initial backoff and a 10 s cap.

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
`Workload().Services().AllServiceShareLinks`, `Workload().Services().AllRuns`
and `Workload().AllEvents`. A loop that breaks
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

A call for a product that is off for the workspace fails with a
`*toshi.ProductError`. `errors.As` finds it on the error of any call, `Wait`,
the watches and `Execute` included, and `connect.CodeOf` reads its code as
before. It has the product's ID (`Product`), the `Reason`
(`ReasonProductUnavailable`), the input that turned the product off
(`Cause`: `CauseBuild`, `CauseOperator`, `CausePlan` or `CauseWorkspace`)
and, for a product the deployment turns off, how far it has wound down
(`State`: `no_new_work`, `draining` or `stopped`). `ProductReason` is the
type of `Reason`, the reason of the refusal's `ErrorInfo` detail, and
`ProductCause` that of `Cause`.

<!-- ExampleProductError -->
```go
ctx := context.Background()
client, err := toshi.New()
if err != nil {
	log.Fatal(err)
}
// Turn a product on for the workspace.
enable := &workspacev1.UpdateProductRequest{Id: "acme_ci", Enabled: proto.Bool(true)}
_, err = client.Product().UpdateProduct(ctx, connect.NewRequest(enable))
var refused *toshi.ProductError
if errors.As(err, &refused) {
	// Off for the workspace: which product, the input that turns it
	// off, and, for the deployment's config, how far it has wound down.
	fmt.Println(refused.Product, refused.Cause, refused.State)
	return
}
if err != nil {
	log.Fatal(err)
}
```

## Refusals with a reason

A refusal that is not a product's comes back as a `*toshi.RefusalError`,
which `errors.As` finds, with the API's `Code` as before and a `Reason` from
`RefusalReason`: `ReasonImageInUse` when an image a workload or a revision
still uses cannot be deleted, `ReasonPlanExcludesImages` when the plan has no
images, `ReasonImageNotReady` when a job, a sandbox or a service revision
names a workspace image with no ready version, `ReasonLastIdentity` when the
caller's last identity cannot be unlinked, and so on for images, volumes,
roles and display sessions.

`Workload().Services().Rollback` asks the revision's registry whether it
still serves the revision's image before it selects it, and refuses with
`ReasonImageNotServed` when the registry answered that it does not serve the
image, or `ReasonImageCheckFailed` when it could not say. A revision's
`ImageCheck` carries the last answer: its `Outcome`, the registry's `Status`
and `Message`, and when it was asked, `CheckedAt`. Reading a revision asks
no registry.

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

## Operator calls

An operator's token sends its reason in `Toshi-Operator-Reason` and the
workspace it acts on in `Toshi-Workspace` on every Workspace API call. The
client has no option or per-call argument for either: `WithInterceptors` sets
them for the whole client on each request, as below. A stream's headers are
set the same way in a full `connect.Interceptor`'s `WrapStreamingClient`, on
the `RequestHeader` of the connection it returns.

<!-- ExampleWithInterceptors_operatorCalls -->
```go
import (
	"context"
	"log"

	"connectrpc.com/connect"
	"go.toshicloud.io/toshi"
)

operator := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("Toshi-Operator-Reason", "abuse-1234")
		req.Header().Set("Toshi-Workspace", "acme")
		return next(ctx, req)
	}
})
client, err := toshi.New(
	toshi.WithScope("default", "production"),
	toshi.WithInterceptors(operator),
)
if err != nil {
	log.Fatal(err)
}
_ = client // Use the client as in the other examples.
```

