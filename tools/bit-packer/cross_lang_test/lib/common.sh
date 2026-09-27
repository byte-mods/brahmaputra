# Shared helpers for cross_lang_test/<lang>/run.sh (source it, don't run it).
#
#   . "$(dirname "$0")/../lib/common.sh"
#   bp_init go            # sets HERE, ROOT, GEN, BITPACKER; wipes and recreates gen/
#   bp_require go         # exit 77 (SKIP) if a tool is missing
#   bp_gen go "$GEN/bench" "$ROOT/../examples/bench_complex.buff" [--package x]
#   bp_run ./gen/prog "$ROOT"   # run a test program, tally its ok/FAIL lines
#   bp_finish             # print "<lang>: N passed, M failed" and exit
#
# A test program prints one line per check, "  ok   name" or
# "  FAIL name (detail)". bp_run counts those lines; a program that exits
# non-zero counts as one extra failure, so a crash can never pass.

set -uo pipefail

bp_init() {
    BP_LANG=$1
    HERE=$(cd "$(dirname "${BASH_SOURCE[1]}")" && pwd)
    ROOT=$(cd "$HERE/.." && pwd)
    BP_SRC=$(cd "$ROOT/.." && pwd)
    BENCH_BUFF="$BP_SRC/examples/bench_complex.buff"
    EDGE_BUFF="$ROOT/edge/edge.buff"
    GEN="$HERE/gen"
    BP_PASS=0
    BP_FAIL=0
    rm -rf "$GEN"
    mkdir -p "$GEN"
    if [ -z "${BITPACKER:-}" ]; then
        if ! command -v go >/dev/null 2>&1; then
            echo "SKIP: \$BITPACKER unset and no Go toolchain to build it" >&2
            exit 77
        fi
        BITPACKER="$GEN/bitpacker"
        (cd "$BP_SRC" && go build -o "$BITPACKER" ./cmd/bitpacker) || {
            echo "  FAIL build bitpacker"
            echo "$BP_LANG: 0 passed, 1 failed"
            exit 1
        }
    fi
    export BITPACKER
}

# bp_require tool... : exit 77 when a toolchain command is missing.
bp_require() {
    local t
    for t in "$@"; do
        if ! command -v "$t" >/dev/null 2>&1; then
            echo "SKIP: '$t' not found on PATH"
            exit 77
        fi
    done
}

# bp_gen lang outdir schema [extra bitpacker args]
bp_gen() {
    local lang=$1 out=$2 schema=$3
    shift 3
    if "$BITPACKER" --file "$schema" --lang "$lang" --out "$out" "$@" >"$GEN/bitpacker.log" 2>&1; then
        return 0
    fi
    cat "$GEN/bitpacker.log"
    bp_fail "generate $lang from $(basename "$schema")" "bitpacker exited non-zero"
    return 1
}

bp_ok()   { echo "  ok   $1"; BP_PASS=$((BP_PASS + 1)); }
bp_fail() { echo "  FAIL $1 (${2:-})"; BP_FAIL=$((BP_FAIL + 1)); }

# bp_step name cmd... : run a build step; on failure show its output and
# count one failure.
bp_step() {
    local name=$1
    shift
    local log="$GEN/step.log" status
    "$@" >"$log" 2>&1
    status=$?
    [ "$status" -eq 0 ] && return 0
    tail -n 40 "$log"
    bp_fail "$name" "exit status $status"
    return 1
}

# bp_run cmd... : run a test program and tally its ok/FAIL lines.
bp_run() {
    local out status p f
    out=$("$@" 2>&1)
    status=$?
    printf '%s\n' "$out"
    p=$(printf '%s\n' "$out" | grep -c '^  ok ' || true)
    f=$(printf '%s\n' "$out" | grep -c '^  FAIL ' || true)
    BP_PASS=$((BP_PASS + p))
    BP_FAIL=$((BP_FAIL + f))
    if [ "$status" -ne 0 ] && [ "$f" -eq 0 ]; then
        bp_fail "$(basename "$1") exited" "status $status"
    fi
    if [ "$status" -eq 0 ] && [ "$p" -eq 0 ]; then
        bp_fail "$(basename "$1") ran no checks" ""
    fi
}

bp_finish() {
    echo "$BP_LANG: $BP_PASS passed, $BP_FAIL failed"
    [ "$BP_FAIL" -eq 0 ] && [ "$BP_PASS" -gt 0 ]
    exit $?
}
