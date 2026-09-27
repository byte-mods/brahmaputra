#!/usr/bin/env bash
# BitPacker Ruby target: bench + edge conformance.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

if [ -z "${BITPACKER:-}" ]; then
    TMP="$(mktemp -d)"
    trap 'rm -rf "$TMP"' EXIT
    (cd "$ROOT" && go build -o "$TMP/bitpacker" ./cmd/bitpacker)
    BITPACKER="$TMP/bitpacker"
fi

rm -rf "$HERE/gen"
mkdir -p "$HERE/gen"
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang ruby --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$HERE/../edge/edge.buff" --lang ruby --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$HERE/f32.buff" --lang ruby --out "$HERE/gen" >/dev/null

ruby -w "$HERE/test_ruby.rb" "$HERE/gen/ruby" "$HERE/.."
