# toshi-go

A Go client for the Toshi Workspace API, `toshi.workspace.v1`.

```sh
go get github.com/toshicloud/toshi-go
```

## Using the client

```go
client, err := toshi.New(toshi.WithScope("default", "production"))
if err != nil {
	return err
}
projects, err := client.Projects.ListProjects(ctx,
	connect.NewRequest(&workspacev1.ListProjectsRequest{}))
```

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
- `WaitWorkload` and `WaitOperation` ask again until the workload reaches
  the state or the operation is done, or the context ends.
- `WatchWorkloads`, `WatchEvents` and `WatchOutput` resume after a dropped
  connection from the last cursor or byte offset received. A cursor the
  server no longer keeps ends the watch with `OUT_OF_RANGE`.
- `Execute` starts a command and returns a `Process`: write its standard
  input to `Stdin`, read `Stdout` and `Stderr` at the same time, and
  `Wait` returns an `*ExitError` for a non-zero exit or a cancelled
  command, or a `*StreamLostError` with the request key when the
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
requires a pinned commit of toshi-api.

## Licence

Apache License 2.0, see LICENSE.
