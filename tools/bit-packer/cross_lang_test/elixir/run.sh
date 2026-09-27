#!/usr/bin/env bash
# Elixir conformance test for the BitPacker `elixir` target.
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
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang elixir --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$ROOT/cross_lang_test/edge/edge.buff" --lang elixir --out "$HERE/gen" >/dev/null

cd "$HERE"
elixirc --warnings-as-errors -o "$HERE/build" "$HERE/gen/elixir/bench_complex.ex" "$HERE/gen/elixir/edge.ex" >/dev/null
elixir -pa "$HERE/build" "$HERE/test_elixir.exs" "$HERE"
