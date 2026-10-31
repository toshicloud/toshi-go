<p align="center">
  <img src="docs/assets/toshi-logo.svg" alt="Toshi" width="120">
</p>

<h1 align="center">toshi-go</h1>

<p align="center">The Go SDK for the Toshi API.</p>

<p align="center">
  <img alt="licence" src="https://img.shields.io/badge/licence-Apache--2.0-blue">
  <img alt="go" src="https://img.shields.io/badge/go-1.26%2B-00ADD8">
  <img alt="status" src="https://img.shields.io/badge/status-early-orange">
</p>

<p align="center">
  <a href="docs/usage.md">Get started</a> ·
  <a href="docs/usage.md">Docs</a> ·
  <a href="https://github.com/toshicloud/toshi-go/issues">Issues</a>
</p>

A Go client for the Toshi Workspace API, `toshi.workspace.v1`, for programs that start jobs, sandboxes and services on Toshi, follow them and read their output. It retries safe calls, resumes watches and ships a fake Toshi for tests.

> **Early stage.** The module stays at v0.x until a Toshi release serves the API; the latest release is the one to use.
> Interfaces may change before 1.0.
> Read the [release notes](https://github.com/toshicloud/toshi-go/releases) when upgrading.

## Quick start

```sh
go get go.toshicloud.io/toshi
```

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

`New` reads the server and token from `TOSHI_SERVER` and `TOSHI_TOKEN`; the [usage guide](docs/usage.md) continues from there.

## Documentation

- [Usage](docs/usage.md): the client, options, retries, lists, errors, interceptors and operator calls
- [Testing with toshitest](docs/testing.md): the fake Toshi for your tests
- [Generated code, upgrading and versions](docs/upgrading.md)
- [Package reference](https://pkg.go.dev/go.toshicloud.io/toshi) on pkg.go.dev
- [The Toshi API](https://github.com/toshicloud/toshi-api) the client calls
- [Releases](https://github.com/toshicloud/toshi-go/releases)
- [Security](SECURITY.md) · [Licence](LICENSE)

## Status and support

Issues and questions go to [GitHub issues](https://github.com/toshicloud/toshi-go/issues).
Security reports follow [SECURITY.md](SECURITY.md).

Apache-2.0
