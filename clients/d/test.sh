#!/usr/bin/env bash
# Builds the D client and runs its end-to-end suite against a live broker.
#   ./test.sh HOST PORT
# Builds with plain ldc2 (warnings as errors); falls back to dub when ldc2
# is not on PATH but dub is.
set -euo pipefail
if [ $# -ne 2 ]; then
  echo "usage: $0 HOST PORT" >&2
  exit 64
fi
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$HERE/build/manual_test"
mkdir -p "$HERE/build"
if command -v "${DC:-ldc2}" >/dev/null 2>&1; then
  "${DC:-ldc2}" -w -O -I"$HERE/source" \
    "$HERE"/source/brahmaputra/*.d "$HERE/test/manual_test.d" \
    -of="$BIN" -od="$HERE/build/obj"
elif command -v dub >/dev/null 2>&1; then
  dub build --root="$HERE" --config=manual-test --build=release -q
else
  echo "neither ldc2 nor dub is installed" >&2
  exit 127
fi
exec "$BIN" "$1" "$2"
