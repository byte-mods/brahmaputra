#!/usr/bin/env bash
# PHP target: generate fresh code, run bench + edge checks (64-bit PHP).
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bp_init php
bp_require php
[ "$(php -r 'echo PHP_INT_SIZE;')" = 8 ] || { echo "SKIP: generated PHP needs 64-bit PHP"; exit 77; }

bp_gen php "$GEN/bench" "$BENCH_BUFF" || bp_finish
bp_gen php "$GEN/edge" "$EDGE_BUFF" || bp_finish
for t in bench edge; do
    bp_run php -d error_reporting=E_ALL -d display_errors=stderr "$HERE/${t}_test.php" "$ROOT" "$GEN"
done
bp_finish
