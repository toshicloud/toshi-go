# Generated code, upgrading and versions


The messages and the clients come from the Go module
[`go.toshicloud.io/api`](https://pkg.go.dev/go.toshicloud.io/api),
which holds the protos of `toshi.workspace.v1` and the code generated
from them:

- `go.toshicloud.io/api/workspacev1`: the messages;
- `go.toshicloud.io/api/workspacev1/workspacev1connect`: a
  [Connect](https://connectrpc.com/docs/go/getting-started) client for each
  service.

This module generates no protobuf code of its own. Protobuf for Go refuses to
start a program that registers the same `.proto` file twice, so a program
that imports both modules links one copy. `go.mod` names the API module
version; moving to another is one pull request that changes `go.mod` and
the code that follows it. `go generate ./...` writes `services_gen.go`, the
accessor of each service, from the API's descriptors.

Generated code takes issues, not pull requests. Every generated file starts
with Go's `// Code generated ... DO NOT EDIT.` line and is never edited by
hand. The API module's files change in the protos of
[toshi-api](https://github.com/toshicloud/toshi-api), which generate them
again: open an issue there for a change to a message or a client. The
regenerated code reaches this module as a new version, taken with
`go get go.toshicloud.io/api@<version>` and `go mod tidy`, followed by
`go generate ./...` for this module's `services_gen.go` and `version.go`;
`scripts/check.sh` fails when they differ from what it writes.

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
Until a Toshi release serves the API, the module stays at `v0.x`. `go.mod`
names the API module version `go.toshicloud.io/api`, and `version.go` is
generated from that module's `VERSION` file by `go generate ./...`.

