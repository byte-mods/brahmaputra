#!/usr/bin/env bash
# Build, vet and run the Go driver's end-to-end suite against a live broker.
#
#   ./test.sh HOST PORT
#
# Runs under the race detector when cgo is available, because the group
# consumer shares state with its heartbeat goroutine and a data race there
# does not show up as a failed check.
set -euo pipefail
if [ $# -ne 2 ]; then
  echo "usage: $0 HOST PORT" >&2
  exit 2
fi
cd "$(dirname "$0")"
go vet ./...
race=()
if [ "$(go env CGO_ENABLED)" = "1" ] && command -v "$(go env CC)" >/dev/null 2>&1; then
  race=(-race)
fi
go run "${race[@]}" ./cmd/manualtest "$1:$2"
