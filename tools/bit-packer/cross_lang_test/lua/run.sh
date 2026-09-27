#!/usr/bin/env bash
# BitPacker Lua target: bench + edge conformance (Lua 5.4).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
LUA="${LUA:-$(command -v lua5.4 || command -v lua)}"

if [ -z "${BITPACKER:-}" ]; then
    TMP="$(mktemp -d)"
    trap 'rm -rf "$TMP"' EXIT
    (cd "$ROOT" && go build -o "$TMP/bitpacker" ./cmd/bitpacker)
    BITPACKER="$TMP/bitpacker"
fi

rm -rf "$HERE/gen"
mkdir -p "$HERE/gen"
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang lua --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$HERE/../edge/edge.buff" --lang lua --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$HERE/f32.buff" --lang lua --out "$HERE/gen" >/dev/null

"$LUA" "$HERE/test_lua.lua" "$HERE/gen/lua" "$HERE/.."
