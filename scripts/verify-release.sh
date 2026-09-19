#!/usr/bin/env bash
# Run every shell live suite and retain individual logs, including failures.
# Build both debug and release binaries first. Override SUITES for a subset.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1
OUT_DIR="${OUT_DIR:-$ROOT/bench/results/release-verification}"
mkdir -p "$OUT_DIR"
SUITES="${SUITES:-verify-m1 verify-m3 verify-m4 verify-m5 verify-m6 verify-replication verify-retention verify-failures verify-transport-parity verify-reassignment verify-admin-and-security verify-transactions verify-compaction verify-jbod verify-chaos}"
printf 'suite,exit_code\n' > "$OUT_DIR/results.csv"
failed=0
for suite in $SUITES; do
    if [[ ! "$suite" =~ ^verify-[a-z0-9-]+$ || ! -f "scripts/$suite.sh" ]]; then
        printf 'Unknown suite: %s\n' "$suite" >&2
        exit 2
    fi
    printf 'Running %s\n' "$suite"
    # The M3 gate holds a follower briefly so fast hosts cannot finish
    # every tiny write before the harness observes an in-flight request.
    if env STORM_GATE="${STORM_GATE:-1}" bash "scripts/$suite.sh" > "$OUT_DIR/$suite.log" 2>&1; then
        result=0
    else
        result=$?
    fi
    printf '%s,%s\n' "$suite" "$result" >> "$OUT_DIR/results.csv"
    printf '%s: exit %s (log: %s/%s.log)\n' "$suite" "$result" "$OUT_DIR" "$suite"
    if (( result != 0 )); then failed=1; fi
done
exit "$failed"
