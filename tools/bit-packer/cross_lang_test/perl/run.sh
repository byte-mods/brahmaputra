#!/usr/bin/env bash
# BitPacker Perl target: bench + edge conformance.
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
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang perl --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$HERE/../edge/edge.buff" --lang perl --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$HERE/f32.buff" --lang perl --out "$HERE/gen" >/dev/null

perl -w -I"$HERE/gen/perl" "$HERE/test_perl.pl" "$HERE/.."
