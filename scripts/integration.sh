#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../pin.env
source "$root/pin.env"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

: "${TOSHI_SERVER_REPOSITORY:?set TOSHI_SERVER_REPOSITORY to the Toshi server repository}"

git clone --quiet "$TOSHI_SERVER_REPOSITORY" "$tmp/server"
git -C "$tmp/server" checkout --quiet --detach "$TOSHI_SERVER_REF"
(
  cd "$tmp/server"
  go build -o "$tmp/toshi" ./cmd/toshi
)

cd "$root"
echo "serving Toshi $TOSHI_SERVER_REF from $tmp/toshi"
TOSHI_INTEGRATION_BIN="$tmp/toshi" \
TOSHI_INTEGRATION_EXPECT_COMMIT="$TOSHI_SERVER_REF" \
  go test -count=1 -v ./integration/...
