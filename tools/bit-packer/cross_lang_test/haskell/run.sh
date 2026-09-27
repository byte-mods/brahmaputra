#!/usr/bin/env bash
# Haskell conformance test for the BitPacker `haskell` target.
# Usage: [BITPACKER=/path/to/bitpacker] ./run.sh   (works from any cwd)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

if [ -z "${BITPACKER:-}" ]; then
    TMP="$(mktemp -d)"
    trap 'rm -rf "$TMP"' EXIT
    (cd "$ROOT" && go build -o "$TMP/bitpacker" ./cmd/bitpacker)
    BITPACKER="$TMP/bitpacker"
fi

rm -rf "$HERE/gen" "$HERE/build"
mkdir -p "$HERE/gen" "$HERE/build"
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang haskell --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$ROOT/cross_lang_test/edge/edge.buff" --lang haskell --out "$HERE/gen" >/dev/null

"${GHC:-ghc}" -v0 -O1 -Wall -Werror -outputdir "$HERE/build" -i"$HERE/gen/haskell" \
    -o "$HERE/build/test_haskell" "$HERE/TestHaskell.hs"
"$HERE/build/test_haskell" "$HERE"
