#!/usr/bin/env bash
# TypeScript conformance test for the BitPacker `typescript` target.
# Usage: [BITPACKER=/path/to/bitpacker] ./run.sh   (works from any cwd)
# Uses `tsc` from PATH when it is TypeScript >= 5.8; otherwise installs the
# typescript package from npm into .cache/ (gitignored).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

for c in node go; do command -v "$c" >/dev/null 2>&1 || { echo "SKIP typescript: $c not installed"; exit 77; }; done
if ! command -v tsc >/dev/null 2>&1 && ! command -v npm >/dev/null 2>&1; then echo "SKIP typescript: neither tsc nor npm installed"; exit 77; fi

if [ -z "${BITPACKER:-}" ]; then
  TMPD="$(mktemp -d)"
  trap 'rm -rf "$TMPD"' EXIT
  (cd "$ROOT" && go build -o "$TMPD/bitpacker" ./cmd/bitpacker)
  BITPACKER="$TMPD/bitpacker"
fi

rm -rf "$HERE/gen" "$HERE/build"
mkdir -p "$HERE/gen"
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang typescript --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$ROOT/cross_lang_test/edge/edge.buff" --lang typescript --out "$HERE/gen" >/dev/null
for f in bench_complex.ts edge.ts; do
  [ -f "$HERE/gen/typescript/$f" ] || { echo "typescript: generator did not produce gen/typescript/$f"; exit 1; }
done

# tsconfig uses erasableSyntaxOnly, which needs TypeScript 5.8+.
TSC=""
if command -v tsc >/dev/null 2>&1; then
  v="$(tsc --version | sed 's/[^0-9.]//g')"
  major="${v%%.*}"; rest="${v#*.}"; minor="${rest%%.*}"
  if [ "$major" -gt 5 ] || { [ "$major" -eq 5 ] && [ "$minor" -ge 8 ]; }; then TSC="tsc"; fi
fi
if [ -z "$TSC" ]; then
  if [ ! -x "$HERE/.cache/node_modules/.bin/tsc" ]; then
    mkdir -p "$HERE/.cache"
    npm install --silent --no-audit --no-fund --prefix "$HERE/.cache" typescript@5 >/dev/null
  fi
  TSC="$HERE/.cache/node_modules/.bin/tsc"
fi

"$TSC" -p "$HERE/tsconfig.json"
node "$HERE/build/test.js" "$ROOT/cross_lang_test"
