#!/usr/bin/env bash
# Nim conformance test for the BitPacker `nim` target.
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
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang nim --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$ROOT/cross_lang_test/edge/edge.buff" --lang nim --out "$HERE/gen" >/dev/null
for f in bench_complex.nim edge.nim; do
  [ -f "$HERE/gen/nim/$f" ] || { echo "nim: generator did not produce gen/nim/$f"; exit 1; }
done

nim c --hints:off --verbosity:0 -d:release --nimcache:"$HERE/build/nimcache" -o:"$HERE/build/test" "$HERE/test.nim"
"$HERE/build/test" "$ROOT/cross_lang_test"
