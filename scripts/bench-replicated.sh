#!/usr/bin/env bash
# Measure the produce path production actually runs: RF=3 with acks=all.
#
# Every throughput number this project has published so far is single-node,
# RF=1, acks=1 — the configuration nobody deploys. That leaves the cost of
# replication, which is the dominant cost of a durable write, entirely
# unmeasured. This script measures it, on one host, against the same
# workload at RF=1/acks=1 so the difference is attributable.
#
# It deliberately does NOT compare against Kafka. A three-node cluster on
# one machine is not a capacity benchmark — the replicas share a disk and a
# NIC, so the absolute numbers are lower than any real deployment. What it
# does measure honestly is the *ratio*: what fraction of single-node
# throughput survives when a write has to reach three brokers before it is
# acknowledged.
#
#   bash scripts/bench-replicated.sh [records] [record-size]
set -uo pipefail

cd "$(dirname "$0")/.."

RECORDS="${1:-200000}"
RECORD_SIZE="${2:-256}"
PARTITIONS=6
NODE_COUNT=3
CLUSTER_ID="bench-replicated"

SERVER_EXE="./target/release/brahmaputra-server"
CLI_EXE="./target/release/brahmaputra-cli"
for exe in "$SERVER_EXE" "$CLI_EXE"; do
  [[ -x "$exe" ]] || { echo "missing $exe; run: cargo build --release" >&2; exit 2; }
done

ROOT="$(mktemp -d -t brahmaputra-bench-rep.XXXXXX)"
declare -A PID
DATA_PORT=([1]=19301 [2]=19302 [3]=19303)
CONTROL_PORT=([1]=19311 [2]=19312 [3]=19313)

cleanup() {
  local node
  for node in "${!PID[@]}"; do
    kill -9 "${PID[$node]}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

start_node() {
  local node="$1"
  local args=(
    --host 127.0.0.1
    --port "${DATA_PORT[$node]}"
    --data-dir "$ROOT/node-$node"
    --node-id "$node"
    --cluster-id "$CLUSTER_ID"
    --control-port "${CONTROL_PORT[$node]}"
    --http-port 0
    --heartbeat-interval-ms 500
    --session-timeout-ms 3000
  )
  local peer
  for peer in $(seq 1 "$NODE_COUNT"); do
    args+=(--controller-peer "$peer=127.0.0.1:${CONTROL_PORT[$peer]}")
  done
  RUST_LOG=brahmaputra=warn "$SERVER_EXE" "${args[@]}" \
    >"$ROOT/node-$node.stdout" 2>"$ROOT/node-$node.stderr" &
  PID[$node]=$!
}

wait_for_port() {
  local port="$1" deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      exec 3<&- 3>&-
      return 0
    fi
    sleep 0.2
  done
  echo "port $port never opened" >&2
  return 1
}

# Produce and report msgs/sec, parsed from the CLI's own summary line so the
# timing includes acknowledgement rather than just the local send.
run_produce() {
  local topic="$1" acks="$2"
  local output
  output="$("$CLI_EXE" --broker "127.0.0.1:${DATA_PORT[1]}" produce \
    --topic "$topic" \
    --count "$RECORDS" \
    --value-size "$RECORD_SIZE" \
    --acks "$acks" \
    --batch-size 65536 \
    --linger-ms 5 \
    --compression none \
    --in-flight 4096 \
    2>&1)" || { echo "produce failed: $output" >&2; return 1; }
  printf '%s\n' "$output" | grep -oE '[0-9]+ msgs/sec' | grep -oE '^[0-9]+'
}

echo "==> Starting a $NODE_COUNT-node cluster"
for node in $(seq 1 "$NODE_COUNT"); do
  start_node "$node"
done
for node in $(seq 1 "$NODE_COUNT"); do
  wait_for_port "${CONTROL_PORT[$node]}"
done

curl -s -X POST "http://127.0.0.1:${CONTROL_PORT[1]}/api/v1/controller/bootstrap" >/dev/null
sleep 3
for node in $(seq 1 "$NODE_COUNT"); do
  wait_for_port "${DATA_PORT[$node]}"
done
sleep 2

CONTROLLER="http://127.0.0.1:${CONTROL_PORT[1]}"

echo "==> Creating topics"
# RF=3 with min.insync.replicas=2: an acknowledged write exists on at least
# two brokers before the client hears about it. This is the durable
# configuration, and the one that has never been measured.
"$CLI_EXE" --controller "$CONTROLLER" topic create \
  --name bench-rf3 --partitions "$PARTITIONS" --replication-factor 3 \
  --config min.insync.replicas=2 >/dev/null 2>&1

# The control: same workload, no replication, leader-only acknowledgement.
"$CLI_EXE" --controller "$CONTROLLER" topic create \
  --name bench-rf1 --partitions "$PARTITIONS" --replication-factor 1 >/dev/null 2>&1

sleep 3

echo "==> $RECORDS records x $RECORD_SIZE B, $PARTITIONS partitions"
echo

echo "-- RF=1, acks=1 (the published configuration) --"
RF1="$(run_produce bench-rf1 1)"
echo "   ${RF1:-0} msgs/sec"

echo "-- RF=3, acks=all, min.insync.replicas=2 (the durable one) --"
RF3="$(run_produce bench-rf3 all)"
echo "   ${RF3:-0} msgs/sec"

echo
if [[ -n "${RF1:-}" && -n "${RF3:-}" && "$RF1" -gt 0 ]]; then
  awk -v rf1="$RF1" -v rf3="$RF3" 'BEGIN {
    printf "==> Replication keeps %.0f%% of single-node throughput ", (rf3 / rf1) * 100
    printf "(%.2fx slower)\n", rf1 / rf3
  }'
fi

echo
echo "==> Verifying the replicated topic actually replicated"
for node in $(seq 1 "$NODE_COUNT"); do
  count="$(find "$ROOT/node-$node" -name '*.log' -path '*bench-rf3*' 2>/dev/null | wc -l)"
  echo "   node $node holds $count bench-rf3 partition log(s)"
done

TOTAL="$("$CLI_EXE" --broker "127.0.0.1:${DATA_PORT[1]}" offsets --topic bench-rf3 2>/dev/null \
  | grep -oE "latest=[0-9]+" | grep -oE "[0-9]+" | awk '{s+=$1} END {print s+0}')"
echo "   log end offsets sum to $TOTAL (produced $RECORDS)"
if [[ "$TOTAL" == "$RECORDS" ]]; then
  echo "   OK: every acknowledged record is on the log"
else
  echo "   MISMATCH: expected $RECORDS"
fi

echo
echo "Artifacts: $ROOT"
trap - EXIT
cleanup
