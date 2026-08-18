#!/usr/bin/env bash
# Recompute the client-measured throughput column of a matched-benchmark
# run from the raw client output it saved, and rebuild the report.
#
# Split out from bench-matched.sh so a parsing correction does not cost a
# fresh multi-hour run: every client's stdout is kept, so the derived
# numbers can always be rebuilt from it.
#
# The subtlety this exists for: kafka-producer-perf-test prints a progress
# line every few seconds AND a final summary line. Only the summary — the
# one carrying latency percentiles — covers the whole run, so a naive
# "sum every records/sec" counts the interim lines too and overstates the
# rate several-fold.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS="${RESULTS:-$ROOT/bench/results/matched}"
CSV="$RESULTS/levels.csv"
[[ -f "$CSV" ]] || { echo "no run at $CSV" >&2; exit 1; }

# Sum the final per-client rate across one level's client output files.
sum_client_rates() {
  local kind="$1"; shift
  local file total=0 value
  for file in "$@"; do
    [[ -f "$file" ]] || continue
    case "$kind" in
      kafka-produce)
        # The summary line is the one with percentiles; take the last match.
        value="$(sed -n 's/.*records sent, \([0-9.]*\) records\/sec.*99.9th.*/\1/p' "$file" | tail -1)" ;;
      kafka-consume)
        value="$(awk -F', *' 'FNR>1 && NF>=6 { rate=$6 } END { print rate }' "$file")" ;;
      brahmaputra)
        value="$(sed -n 's/.*-> \([0-9.]*\) msgs\/sec.*/\1/p' "$file" | tail -1)" ;;
    esac
    total="$(awk -v t="$total" -v v="${value:-0}" 'BEGIN { printf "%.4f", t + v }')"
  done
  awk -v t="$total" 'BEGIN { printf "%.0f", t }'
}

TMP="$RESULTS/levels.recomputed.csv"
head -1 "$CSV" > "$TMP"
tail -n +2 "$CSV" | while IFS=, read -r system phase clients records seconds wall client cpu cpu_max mem mem_max; do
  case "$system" in
    kafka) kind="kafka-$phase" ;;
    *)     kind="brahmaputra" ;;
  esac
  fixed="$(sum_client_rates "$kind" "$RESULTS/$system-$phase-$clients"-*.txt)"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$system" "$phase" "$clients" "$records" "$seconds" "$wall" \
    "$fixed" "$cpu" "$cpu_max" "$mem" "$mem_max" >> "$TMP"
done
mv "$TMP" "$CSV"

echo "recomputed client rates in $CSV"
column -t -s, "$CSV" 2>/dev/null || cat "$CSV"
