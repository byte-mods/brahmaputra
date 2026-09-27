#!/usr/bin/env bash
# D conformance test for the BitPacker `d` target.
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
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang d --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$ROOT/cross_lang_test/edge/edge.buff" --lang d --out "$HERE/gen" >/dev/null
for f in bench_complex.d edge.d; do
  [ -f "$HERE/gen/d/$f" ] || { echo "d: generator did not produce gen/d/$f"; exit 1; }
done

ldc2 -w -dip1000 -O -of="$HERE/build/test" -od="$HERE/build" -I="$HERE/gen/d" \
  "$HERE/test.d" "$HERE/gen/d/bench_complex.d" "$HERE/gen/d/edge.d"
"$HERE/build/test" "$ROOT/cross_lang_test"
