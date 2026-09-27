#!/usr/bin/env bash
# BitPacker C target: bench + edge conformance, plain and under ASan/UBSan.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
CC="${CC:-cc}"

if [ -z "${BITPACKER:-}" ]; then
    TMP="$(mktemp -d)"
    trap 'rm -rf "$TMP"' EXIT
    (cd "$ROOT" && go build -o "$TMP/bitpacker" ./cmd/bitpacker)
    BITPACKER="$TMP/bitpacker"
fi

rm -rf "$HERE/gen"
mkdir -p "$HERE/gen"
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang c --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$HERE/../edge/edge.buff" --lang c --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$HERE/f32.buff" --lang c --out "$HERE/gen" >/dev/null

SRCS=("$HERE/test_c.c" "$HERE/gen/c/bench_complex.c" "$HERE/gen/c/edge.c" "$HERE/gen/c/f32.c")
FLAGS=(-std=c11 -Wall -Wextra -Wpedantic -Werror -I "$HERE/gen/c")

"$CC" "${FLAGS[@]}" -O2 -o "$HERE/gen/test_c" "${SRCS[@]}"
"$CC" "${FLAGS[@]}" -O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined \
    -fno-sanitize-recover=all -o "$HERE/gen/test_c_san" "${SRCS[@]}"

echo "== c (plain)"
"$HERE/gen/test_c" "$HERE/.."
echo "== c (address,undefined sanitizers)"
ASAN_OPTIONS=detect_leaks=1 "$HERE/gen/test_c_san" "$HERE/.."
