#!/usr/bin/env bash
# Crystal conformance test for the BitPacker `crystal` target.
# Usage: run.sh   (set BITPACKER to a built bitpacker binary to skip the build)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

if [ -z "${BITPACKER:-}" ]; then
  TMPD="$(mktemp -d)"
  trap 'rm -rf "$TMPD"' EXIT
  (cd "$ROOT" && go build -o "$TMPD/bitpacker" ./cmd/bitpacker)
  BITPACKER="$TMPD/bitpacker"
fi

rm -rf "$HERE/gen" "$HERE/build"
mkdir -p "$HERE/gen" "$HERE/build"
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang crystal --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$ROOT/cross_lang_test/edge/edge.buff" --lang crystal --out "$HERE/gen" >/dev/null
for f in bench_complex.cr edge.cr; do
  [ -f "$HERE/gen/crystal/$f" ] || { echo "crystal: generator did not produce gen/crystal/$f"; exit 1; }
done

export CRYSTAL_CACHE_DIR="${CRYSTAL_CACHE_DIR:-$HERE/build/cache}"
crystal build --no-color "$HERE/test.cr" -o "$HERE/build/test"
"$HERE/build/test" "$ROOT/cross_lang_test"
