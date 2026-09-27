#!/usr/bin/env bash
# Java target: generate fresh code, compile, run bench + edge checks.
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bp_init java
bp_require javac java

bp_gen java "$GEN/src" "$BENCH_BUFF" --package bench || bp_finish
mkdir -p "$GEN/src/java/bench" && mv "$GEN"/src/java/*.java "$GEN/src/java/bench/"
bp_gen java "$GEN/src" "$EDGE_BUFF" --package edge || bp_finish
mkdir -p "$GEN/src/java/edge" && mv "$GEN"/src/java/*.java "$GEN/src/java/edge/"
bp_step "javac" javac -encoding UTF-8 -d "$GEN/classes" \
    "$GEN"/src/java/bench/*.java "$GEN"/src/java/edge/*.java "$HERE/CrossTest.java" || bp_finish
bp_run java -cp "$GEN/classes" CrossTest "$ROOT"
bp_finish
