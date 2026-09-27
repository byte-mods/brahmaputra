#!/usr/bin/env bash
# OCaml conformance test for the BitPacker `ocaml` target.
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
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang ocaml --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$ROOT/cross_lang_test/edge/edge.buff" --lang ocaml --out "$HERE/gen" >/dev/null

# compile in build/ so no .cm* files land next to the sources
cp "$HERE/gen/ocaml/bench_complex.ml" "$HERE/gen/ocaml/edge.ml" "$HERE/test_ocaml.ml" "$HERE/build/"
(cd "$HERE/build" && ocamlfind ocamlopt -warn-error +a -o test_ocaml bench_complex.ml edge.ml test_ocaml.ml)
"$HERE/build/test_ocaml" "$HERE"
