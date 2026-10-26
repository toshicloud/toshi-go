# toshi-go

A Go client for the Toshi Workspace API, `toshi.workspace.v1`.

```sh
go get go.toshicloud.io/toshi
```

v0.1.0 is under github.com/toshicloud/toshi-go and later versions use
go.toshicloud.io/toshi.

## Using the client

```go
import (
	"go.toshicloud.io/toshi"
	"github.com/toshicloud/toshi-api/workspacev1"
)

client, err := toshi.New(toshi.WithScope("default", "production"))
if err != nil {
	return err
}
jobs, err := client.Workload.Jobs.List(ctx, &workspacev1.ListJobsRequest{})
```

`Client` has one field per service. `Workload` is written by this module: a
job's calls under `Workload.Jobs`, a sandbox's under `Workload.Sandboxes`, a
service's under `Workload.Services`, and the calls that span the three on
`Workload` itself (`WatchEvents`, `ListEvents`, `GetOperation`,
`WaitOperation`).
`Project`, `Secret`, `Webhook`, `Display`, `Channel`, `Recording`, `Volume`,
`Artifact` and `ArtifactUpload` are the generated Connect clients of their
service; `Channel` is `ChannelClient`, which spells out the service's calls.

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
- `Workload.Jobs.Wait`, `Workload.Sandboxes.Wait`, `Workload.Services.Wait`
  and `Workload.WaitOperation` ask again until the resource reaches the
  state or the operation is done, or the context ends.
- The watches, `Workload.Jobs.Watch`, `Workload.Sandboxes.Watch`,
  `Workload.Services.Watch`, `Workload.WatchEvents` and
  `Workload.Jobs.WatchOutput`, resume after a dropped connection from the
  last cursor or byte offset received. A cursor the server no longer keeps
  ends the watch with `OUT_OF_RANGE`.
- `Workload.Sandboxes.Execute` starts a command and returns a `Process`:
  write its standard input to `Stdin`, read `Stdout` and `Stderr` at the
  same time, and `Wait` returns an `*ExitError` for a non-zero exit or a
  cancelled command, or a `*StreamLostError` with the request key when the
  connection drops before the exit.

Execute needs HTTP/2, which the server offers over https.

## Generated code

The messages and the clients come from the Go module
[`github.com/toshicloud/toshi-api`](https://github.com/toshicloud/toshi-api),
which holds the protos of `toshi.workspace.v1` and the code generated
from them:

- `github.com/toshicloud/toshi-api/workspacev1`: the messages;
- `github.com/toshicloud/toshi-api/workspacev1/workspacev1connect`: a
  [Connect](https://connectrpc.com/docs/go/getting-started) client for each
  service.

This module generates no copy of its own. Protobuf for Go refuses to
start a program that registers the same `.proto` file twice, so a program
that imports both modules links one copy. `go.mod` names the toshi-api
version; moving to another is one pull request that changes `go.mod` and
the code that follows it.

## Versions

The module's major version follows the API's: `v1` will serve
`toshi.workspace.v1`, and a `toshi.workspace.v2` gets a `/v2` module path.
Until a Toshi release serves the API, the module stays at `v0.x` and
requires a tagged toshi-api.

## Licence

Apache License 2.0, see LICENSE.
