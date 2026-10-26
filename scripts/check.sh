#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

golangci_lint_version="v2.14.0"

go vet ./...

deps="$(go list -deps ./...)"
module="$(go list -m)"
if grep -qE '^github\.com/toshicloud/toshi-api(/|$)' <<<"$deps"; then
  echo "the SDK depends on the API module go.toshicloud.io/api; the old github.com path is not allowed" >&2
  exit 1
fi
other="$(grep -E '^(github\.com/toshicloud/|go\.toshicloud\.io/)' <<<"$deps" | grep -vE "^(go\.toshicloud\.io/api|${module//./\\.})(/|$)" || true)"
if [ -n "$other" ]; then
  echo "the SDK may depend only on the public API module go.toshicloud.io/api among toshicloud modules; it depends on:" >&2
  echo "$other" >&2
  exit 1
fi

go run "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$golangci_lint_version" run ./...
go test ./...
