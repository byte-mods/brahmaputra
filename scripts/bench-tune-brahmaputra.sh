#!/usr/bin/env bash
# Producer tuning sweep for Brahmaputra, against one long-lived broker in
# Docker (same image and limits as scripts/bench-vs-kafka.sh). Finds the
# best client configuration before the head-to-head comparison, so the
# published number is a tuned one rather than a default one.
#
# Requires that scripts/bench-vs-kafka.sh has built brahmaputra-bench:latest.

set -Eeuo pipefail

RECORDS="${RECORDS:-200000}"
RECORD_SIZE="${RECORD_SIZE:-256}"
PARTITIONS="${PARTITIONS:-6}"
ACKS="${ACKS:-1}"
CPUS="${CPUS:-4}"
MEMORY="${MEMORY:-4g}"
NETWORK="${NETWORK:-brahma-bench}"
COMPRESSION="${COMPRESSION:-none}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/bench/results/tuning-brahmaputra.md"
mkdir -p "$(dirname "$OUT")"

docker_run() { MSYS_NO_PATHCONV=1 docker "$@"; }
stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
die() { printf '\n\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

cleanup() { docker_run rm -f bench-tune >/dev/null 2>&1 || true; }
trap cleanup EXIT

# In-container load, matching how Kafka's perf tools run (loopback, shared
# CPU budget with the broker) so tuning numbers transfer to the head-to-head.
cli() {
  docker_run exec bench-tune /usr/local/bin/brahmaputra-cli \
    --broker bench-tune:9092 "$@"
}

start_broker() {
  cleanup
  docker_run run -d --name bench-tune --network "$NETWORK" \
    --cpus "$CPUS" --memory "$MEMORY" \
    brahmaputra-bench:latest \
    --host bench-tune --port 9092 --data-dir /data \
    --default-partitions "$PARTITIONS" --segment-bytes 1073741824 >/dev/null \
    || die "cannot start broker"
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if cli metadata >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  die "broker never became ready"
}

rate_of() { sed -n 's/.*-> \([0-9.]*\) msgs\/sec.*/\1/p' <<<"$1" | tail -1; }

declare -a ROWS
run_case() {
  local label="$1" batch="$2" linger="$3" inflight="$4" compression="${5:-$COMPRESSION}"
  local topic="tune-$(date +%s)-$RANDOM"
  local out rate
  out="$(cli produce --topic "$topic" --count "$RECORDS" --value-size "$RECORD_SIZE" \
    --no-key --acks "$ACKS" --batch-size "$batch" --linger-ms "$linger" \
    --in-flight "$inflight" --compression "$compression" 2>&1)" || {
      printf '%s\n' "$out" >&2
      die "produce failed for $label"
    }
  rate="$(rate_of "$out")"
  printf '    %-46s %10s msgs/sec\n' "$label" "$rate"
  ROWS+=("| $label | $batch | $linger | $inflight | $compression | $rate |")
}

stage "Start one broker for the whole sweep"
start_broker

stage "Producer sweep: $RECORDS x ${RECORD_SIZE}B, acks=$ACKS, $PARTITIONS partitions"
run_case "baseline (batch 64K, linger 10, in-flight 512)"   65536   10    512
run_case "in-flight 4096"                                    65536   10    4096
run_case "in-flight 16384"                                   65536   10    16384
run_case "in-flight 16384 + linger 0"                        65536   0     16384
run_case "in-flight 16384 + batch 256K"                      262144  10    16384
run_case "in-flight 65536 + batch 256K + linger 5"           262144  5     65536
run_case "in-flight 16384 + batch 256K + lz4"                262144  10    16384  lz4

{
  printf '# Brahmaputra producer tuning\n\n'
  printf '%s records of %s B, acks=%s, %s partitions, RF=1, %s CPUs / %s.\n\n' \
    "$RECORDS" "$RECORD_SIZE" "$ACKS" "$PARTITIONS" "$CPUS" "$MEMORY"
  printf '| Case | batch.size | linger.ms | in-flight records | compression | msgs/sec |\n'
  printf '|---|---|---|---|---|---|\n'
  printf '%s\n' "${ROWS[@]}"
} > "$OUT"

stage "Tuning complete"
cat "$OUT"
