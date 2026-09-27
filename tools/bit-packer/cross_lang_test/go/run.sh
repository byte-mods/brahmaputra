#!/usr/bin/env bash
# Go target: generate fresh code, build, run bench + edge checks.
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bp_init go
bp_require go

bp_gen go "$GEN/bench" "$BENCH_BUFF" --package bench || bp_finish
bp_gen go "$GEN/edge" "$EDGE_BUFF" --package edge || bp_finish
cd "$HERE" || exit 1
bp_step "go build" go build -o "$GEN/gotest" . || bp_finish
bp_step "go vet generated code" go vet ./gen/... || bp_finish
bp_run "$GEN/gotest" "$ROOT"
bp_finish
