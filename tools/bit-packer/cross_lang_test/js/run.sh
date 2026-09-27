#!/usr/bin/env bash
# JavaScript target: generate fresh code, run bench + edge checks under Node.
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bp_init js
bp_require node

bp_gen js "$GEN/bench" "$BENCH_BUFF" || bp_finish
bp_gen js "$GEN/edge" "$EDGE_BUFF" || bp_finish
bp_run node "$HERE/crosstest.js" "$ROOT" "$GEN"
bp_finish
