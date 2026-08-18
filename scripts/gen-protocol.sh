#!/usr/bin/env bash
# Regenerate Rust message types from BitPacker schemas.
# Requires: tools/bitpacker(.exe) — build once from tools/bit-packer (Go):
#   cd tools/bit-packer && go build -o ../bitpacker ./cmd/bitpacker
#
# The generator names its output after the FIRST class in the schema and
# nests it under a `rust/` subdirectory; the files actually contain ALL
# message classes. We normalize them into crates/protocol/src/gen/ as
# `structs.rs` + `impls.rs`, which `gen/mod.rs` (checked in) include!()s.
set -euo pipefail
cd "$(dirname "$0")/.."

BITPACKER=tools/bitpacker
[[ -f tools/bitpacker.exe ]] && BITPACKER=tools/bitpacker.exe

GEN_DIR=crates/protocol/src/gen
TMP_DIR="$GEN_DIR/.tmp"
rm -rf "$TMP_DIR"

"$BITPACKER" --file schemas/protocol.buff --lang rust --out "$TMP_DIR" --sep

# *_structs.rs / *_impl.rs — one pair, containing every class.
mv "$TMP_DIR"/rust/*_structs.rs "$GEN_DIR/structs.rs"
mv "$TMP_DIR"/rust/*_impl.rs "$GEN_DIR/impls.rs"
rm -rf "$TMP_DIR"

echo "generated -> $GEN_DIR/{structs.rs,impls.rs}"
