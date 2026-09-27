#!/usr/bin/env bash
# Builds the Elixir driver and runs the end-to-end suite against a live broker.
#   ./test.sh HOST PORT
set -euo pipefail
if [ $# -lt 2 ]; then
  echo "usage: $0 HOST PORT" >&2
  exit 2
fi
cd "$(dirname "${BASH_SOURCE[0]}")"
# No dependencies, so Hex is never needed.
export MIX_ENV="${MIX_ENV:-dev}"
mix compile --warnings-as-errors
exec mix run e2e/manual_test.exs "$1" "$2"
