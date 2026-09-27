#!/usr/bin/env bash
# Builds the C client and runs its end-to-end suite against a live broker.
#   ./test.sh HOST PORT            (SANITIZE=1 ./test.sh ... for ASan/UBSan)
set -euo pipefail
if [ $# -ne 2 ]; then
  echo "usage: $0 HOST PORT" >&2
  exit 64
fi
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SANITIZE="${SANITIZE:-0}"
make -C "$HERE" SANITIZE="$SANITIZE" >/dev/null
if [ "$SANITIZE" = "1" ]; then BIN="$HERE/build-asan/manual_test"; else BIN="$HERE/build/manual_test"; fi
exec "$BIN" "$1" "$2"
