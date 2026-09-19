#!/usr/bin/env bash
# Finite release comparison matrix, executed sequentially to avoid
# benchmark-on-benchmark interference. Requires Docker and the tools used
# by the underlying harnesses. Existing application containers are untouched.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
BASE="${OUT_DIR:-$ROOT/bench/results/release-benchmarks}"
mkdir -p "$BASE"
printf 'scenario,exit_code\n' > "$BASE/results.csv"
failed=0
check_counts() {
    local directory="$1" expected="$2" file actual checked=0
    while IFS= read -r file; do
        case "${file##*/}" in
            kafka*produce*) actual="$(sed -n 's/^\([0-9]*\) records sent,.*/\1/p' "$file" | tail -1)" ;;
            kafka*consume*) actual="$(awk -F', *' 'NF>=6 && $5 ~ /^[0-9]+$/ { n=$5 } END { print n }' "$file")" ;;
            *produce*) actual="$(sed -n 's/^produced \([0-9]*\) records.*/\1/p' "$file" | tail -1)" ;;
            *consume*) actual="$(sed -n 's/^consumed \([0-9]*\) records.*/\1/p' "$file" | tail -1)" ;;
        esac
        if [[ "$actual" != "$expected" ]]; then
            printf 'Count mismatch: %s expected=%s actual=%s\n' "$file" "$expected" "$actual" >&2
            return 1
        fi
        checked=$((checked + 1))
    done < <(find "$directory" -type f \( -name '*produce*.txt' -o -name '*consume*.txt' \) ! -name '*stats*')
    (( checked > 0 ))
}
run() {
    local scenario="$1" expected="$2" result
    shift 2
    mkdir -p "$BASE/$scenario"
    printf 'Running %s\n' "$scenario"
    if env OUT_DIR="$BASE/$scenario" "$@" > "$BASE/$scenario/run.log" 2>&1; then
        if check_counts "$BASE/$scenario" "$expected" >> "$BASE/$scenario/run.log" 2>&1; then
            result=0
        else
            result=1
            failed=1
        fi
    else
        result=$?
        failed=1
    fi
    printf '%s,%s\n' "$scenario" "$result" >> "$BASE/results.csv"
    printf '%s: exit %s\n' "$scenario" "$result"
}

# The first harness builds the release image from this working tree.
run replicated 500000 env SKIP_BUILD=0 PER_CLIENT=500000 LEVELS='1 2 4' \
    bash scripts/bench-replicated-vs-kafka.sh
# Never compare an old image if the initial build failed.
(( failed == 0 )) || exit 1

run concurrency 1000000 env PER_CLIENT=1000000 LEVELS='1 2 4 8' \
    bash scripts/bench-matched.sh
run large-records 2000 env RECORDS=2000 bash scripts/bench-three-way.sh
for codec in none lz4 gzip snappy zstd; do
    run "codec-$codec" 500000 env SKIP_BUILD=1 RECORDS=500000 COMPRESSION="$codec" ACKS=1 \
        bash scripts/bench-vs-kafka.sh
done
for acks in 0 all; do
    run "acks-$acks" 500000 env SKIP_BUILD=1 RECORDS=500000 COMPRESSION=none ACKS="$acks" \
        bash scripts/bench-vs-kafka.sh
done
run idempotent 50000 env SKIP_BUILD=1 RECORDS=50000 COMPRESSION=none ACKS=all IDEMPOTENT=true \
    bash scripts/bench-vs-kafka.sh
run rate-limited 50000 env SKIP_BUILD=1 PER_CLIENT=50000 LEVELS=1 RATE=10000 \
    bash scripts/bench-replicated-vs-kafka.sh
exit "$failed"
