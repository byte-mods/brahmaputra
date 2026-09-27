#!/usr/bin/env bash
# Erlang conformance test for the BitPacker `erlang` target.
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
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang erlang --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$ROOT/cross_lang_test/edge/edge.buff" --lang erlang --out "$HERE/gen" >/dev/null

erlc -Werror -I "$HERE/gen/erlang" -o "$HERE/build" \
    "$HERE/gen/erlang/bench_complex.erl" "$HERE/gen/erlang/edge.erl" "$HERE/test_erlang.erl"

cd "$HERE"
erl -noshell -noinput -pa "$HERE/build" -run test_erlang main "$HERE"
