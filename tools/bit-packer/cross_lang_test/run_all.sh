#!/usr/bin/env bash
# BitPacker cross-language conformance runner.
#
#   ./run_all.sh              run every <lang>/run.sh found next to this script
#   ./run_all.sh go rust      run only these languages
#
# Builds the bitpacker compiler once from ../cmd/bitpacker (or uses
# $BITPACKER if already set), exports it as $BITPACKER, runs each language's
# run.sh, prints a summary table, then checks that every test_data_<lang>.bin
# written by this run is byte-identical to the committed test_data.bin.
# A run.sh that exits 77 means "toolchain missing" and is reported as SKIP
# (never as a pass). Exits non-zero if any language FAILs or any binary
# differs. See README.md for the per-language contract.
set -uo pipefail
cd "$(dirname "$0")" || exit 1
ROOT=$(pwd)
TIMEOUT=${BP_TIMEOUT:-1200}

WORK=$(mktemp -d "${TMPDIR:-/tmp}/bp-run-all.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

echo "═══════════════════════════════════════════════════"
echo "  BitPacker cross-language conformance"
echo "═══════════════════════════════════════════════════"

if [ -z "${BITPACKER:-}" ]; then
    echo "building bitpacker ..."
    if ! (cd .. && go build -o "$WORK/bitpacker" ./cmd/bitpacker); then
        echo "FAIL: could not build bitpacker from ../cmd/bitpacker"
        exit 1
    fi
    BITPACKER="$WORK/bitpacker"
fi
export BITPACKER
echo "bitpacker: $BITPACKER"

# Languages: the arguments, or every directory holding a run.sh.
LANGS=()
if [ $# -gt 0 ]; then
    LANGS=("$@")
else
    for f in */run.sh; do
        [ -f "$f" ] && LANGS+=("${f%/run.sh}")
    done
fi
[ ${#LANGS[@]} -gt 0 ] || { echo "no */run.sh found"; exit 1; }

# Only binaries written by this run are compared.
rm -f test_data_*.bin

RESULTS=()
NFAIL=0 NSKIP=0 NPASS=0
for lang in "${LANGS[@]}"; do
    script="$ROOT/$lang/run.sh"
    echo ""
    echo "─── $lang ───"
    if [ ! -f "$script" ]; then
        echo "  no $lang/run.sh"
        RESULTS+=("$lang|FAIL|no run.sh|0")
        NFAIL=$((NFAIL + 1))
        continue
    fi
    log="$WORK/$lang.log"
    start=$(date +%s)
    if command -v timeout >/dev/null 2>&1; then
        timeout "$TIMEOUT" bash "$script" >"$log" 2>&1
    else
        bash "$script" >"$log" 2>&1
    fi
    status=$?
    secs=$(( $(date +%s) - start ))
    cat "$log"
    summary=$(grep -E "^[A-Za-z0-9_+#.-]+: [0-9]+ passed, [0-9]+ failed" "$log" | tail -n 1)
    if [ "$status" -eq 77 ]; then
        reason=$(grep -m1 '^SKIP' "$log" || echo "toolchain missing")
        RESULTS+=("$lang|SKIP|$reason|$secs")
        NSKIP=$((NSKIP + 1))
    elif [ "$status" -eq 0 ] && [ -n "$summary" ] && echo "$summary" | grep -q ' 0 failed$'; then
        RESULTS+=("$lang|PASS|$summary|$secs")
        NPASS=$((NPASS + 1))
    else
        [ "$status" -eq 124 ] && summary="timed out after ${TIMEOUT}s"
        RESULTS+=("$lang|FAIL|${summary:-no summary line} (exit $status)|$secs")
        NFAIL=$((NFAIL + 1))
    fi
done

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Summary"
echo "═══════════════════════════════════════════════════"
printf '  %-12s %-5s %6s  %s\n' LANGUAGE RESULT TIME DETAIL
for r in "${RESULTS[@]}"; do
    IFS='|' read -r lang res detail secs <<<"$r"
    printf '  %-12s %-5s %5ss  %s\n' "$lang" "$res" "$secs" "$detail"
done

echo ""
echo "─── Binary compatibility (vs committed test_data.bin) ───"
MISMATCH=0
if command -v python3 >/dev/null 2>&1; then
    if python3 make_test_data.py --check >/dev/null; then
        echo "  ok   test_data.bin matches make_test_data.py"
    else
        echo "  FAIL test_data.bin differs from make_test_data.py"
        MISMATCH=$((MISMATCH + 1))
    fi
fi
found=0
for f in test_data_*.bin; do
    [ -f "$f" ] || continue
    found=$((found + 1))
    if cmp -s "$f" test_data.bin; then
        echo "  ok   $f"
    else
        echo "  FAIL $f differs from test_data.bin"
        MISMATCH=$((MISMATCH + 1))
    fi
done
[ "$found" -gt 0 ] || echo "  (no test_data_<lang>.bin written)"

echo ""
echo "Results: $NPASS passed, $NFAIL failed, $NSKIP skipped; $MISMATCH binary mismatch(es)"
[ "$NFAIL" -eq 0 ] && [ "$MISMATCH" -eq 0 ]
