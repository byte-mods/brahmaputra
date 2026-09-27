#!/usr/bin/env bash
# Rust target: generate fresh code (single-file and --sep), build, run checks.
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bp_init rust
bp_require cargo

bp_gen rust "$GEN/bench" "$BENCH_BUFF" || bp_finish
bp_gen rust "$GEN/bench_sep" "$BENCH_BUFF" --sep || bp_finish
bp_gen rust "$GEN/edge" "$EDGE_BUFF" || bp_finish
bp_gen rust "$GEN/edge_sep" "$EDGE_BUFF" --sep || bp_finish
cd "$HERE" || exit 1
export CARGO_TARGET_DIR="$GEN/target"
bp_step "cargo build" cargo build --quiet --offline --manifest-path "$HERE/Cargo.toml" || bp_finish
bp_run "$GEN/target/debug/crosstest" "$ROOT"
bp_finish
