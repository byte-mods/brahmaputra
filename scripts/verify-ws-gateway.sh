#!/usr/bin/env bash
# Live verification of the WebSocket gateway with real processes: a
# broker, a gateway, and ws-loadgen opening many authenticated sockets
# that publish continuously.
#
#   scripts/verify-ws-gateway.sh                    # defaults below
#   CONNECTIONS=50000 RATE=2 DURATION=60 scripts/verify-ws-gateway.sh
#
# It asserts that every connection is established, no publish fails,
# every message is acknowledged, and the broker saw only the gateway's
# small producer pool (not one connection per socket) and batched
# produce requests. It prints the gateway's resident memory per socket.
#
# Each process needs a file-descriptor limit above CONNECTIONS (the
# gateway and the load generator run as separate processes, so each
# needs its own). On a stock box: `ulimit -n 1048576` as root, and for
# more than ~28k sockets per source address list several in SOURCE_IPS.
set -euo pipefail
cd "$(dirname "$0")/.."

CONNECTIONS="${CONNECTIONS:-10000}"
RATE="${RATE:-1}"                  # messages/s per socket
DURATION="${DURATION:-20}"
PARTITIONS="${PARTITIONS:-16}"
SOURCE_IPS="${SOURCE_IPS:-127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5}"
READ_BUFFER="${READ_BUFFER:-1024}"
BROKER_PORT="${BROKER_PORT:-19700}"
GW_PORT="${GW_PORT:-19790}"
SECRET="verify-ws-gateway-secret-0123456789"

limit=$(ulimit -n)
if (( limit < CONNECTIONS + 200 )); then
  echo "file-descriptor limit $limit is below CONNECTIONS=$CONNECTIONS; lowering the test to $((limit - 500))" >&2
  CONNECTIONS=$((limit - 500))
fi

cargo build --release --locked -p brahmaputra-server -p brahmaputra-ws-gateway >/dev/null

WORK="$(mktemp -d "${TMPDIR:-/tmp}/brahmaputra-ws-gateway.XXXXXX")"
PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

wait_port() {
  for _ in $(seq 1 100); do
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && return 0
    sleep 0.1
  done
  echo "nothing listening on $1" >&2
  return 1
}

./target/release/brahmaputra-server --data-dir "$WORK/data" \
  --default-partitions "$PARTITIONS" --port "$BROKER_PORT" \
  --http-port $((BROKER_PORT + 1)) >"$WORK/broker.log" 2>&1 &
PIDS+=($!)
wait_port "$BROKER_PORT"

RUST_LOG=warn ./target/release/brahmaputra-ws-gateway \
  --listen "127.0.0.1:$GW_PORT" --http-listen "127.0.0.1:$((GW_PORT + 1))" \
  --broker "127.0.0.1:$BROKER_PORT" --jwt-secret "$SECRET" \
  --default-topic load --read-buffer-bytes "$READ_BUFFER" >"$WORK/gateway.log" 2>&1 &
PIDS+=($!)
wait_port $((GW_PORT + 1))
for _ in $(seq 1 50); do
  curl -sf "http://127.0.0.1:$((GW_PORT + 1))/readyz" >/dev/null && break
  sleep 0.1
done

echo "== $CONNECTIONS sockets, $RATE msg/s each, ${DURATION}s =="
started=$(date +%s)
./target/release/ws-loadgen --url "ws://127.0.0.1:$GW_PORT/ws" --secret "$SECRET" \
  --connections "$CONNECTIONS" --ramp 3000 --rate "$RATE" --duration "$DURATION" \
  --source-ips "$SOURCE_IPS" --metrics "http://127.0.0.1:$((GW_PORT + 1))/metrics" \
  | tee "$WORK/loadgen.txt"

elapsed=$(( $(date +%s) - started ))
broker_metrics="$(curl -s "http://127.0.0.1:$((BROKER_PORT + 1))/metrics")"
records=$(awk '/^brahmaputra_produce_records_total /{print $2}' <<<"$broker_metrics")
requests=$(awk '/^brahmaputra_produce_requests_total /{print $2}' <<<"$broker_metrics")
port_hex=$(printf '%04X' "$BROKER_PORT")
broker_conns=$({ cat /proc/net/tcp; cat /proc/net/tcp6 2>/dev/null || true; } | awk -v p=":$port_hex" '$2 ~ p"$" && $4=="01"' | wc -l)

echo
echo "== broker side =="
echo "records stored          $records"
echo "produce requests        $requests ($(( records / (requests > 0 ? requests : 1) )) records/request, $(( requests / (elapsed > 0 ? elapsed : 1) ))/s)"
echo "connections to broker   $broker_conns (for $CONNECTIONS client sockets)"

fail=0
grep -q "connections established $CONNECTIONS\$" "$WORK/loadgen.txt" || { echo "FAIL: not every socket connected" >&2; fail=1; }
grep -q '^error frames            0$' "$WORK/loadgen.txt" || { echo "FAIL: publishes were refused" >&2; fail=1; }
grep -q '^closed by gateway       0$' "$WORK/loadgen.txt" || { echo "FAIL: the gateway dropped sockets" >&2; fail=1; }
(( broker_conns <= 8 )) || { echo "FAIL: broker holds $broker_conns connections" >&2; fail=1; }
# Batching is what keeps the broker's request rate flat as sockets grow:
# at a high message rate, many records must share each request. (At a low
# rate the linger timer, not batch size, decides and ratios near 1 are fine.)
if (( records / (elapsed > 0 ? elapsed : 1) >= 5000 )) && (( records < requests * 5 )); then
  echo "FAIL: produce requests are not batched ($records records in $requests requests)" >&2
  fail=1
fi
(( fail == 0 )) && echo "PASS"
exit $fail
