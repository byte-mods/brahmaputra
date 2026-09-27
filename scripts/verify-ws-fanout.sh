#!/usr/bin/env bash
# Live verification of the WebSocket gateway's fan-out (subscriptions),
# with real processes: a broker, GATEWAYS gateway instances, and ws-loadgen
# holding SUBSCRIBERS sockets spread across them, all subscribed to one
# price topic, while it writes TICK_RATE ticks/s straight into Brahmaputra.
#
#   scripts/verify-ws-fanout.sh
#   SUBSCRIBERS=50000 GATEWAYS=4 TICK_RATE=50 scripts/verify-ws-fanout.sh
#
# It asserts that every socket subscribes, every tick reaches every socket
# (or is reported to it as skipped: none are silently lost), each gateway
# instance reads the topic from the broker once, the broker sees a handful
# of connections however many sockets there are, and the gateways' memory
# per subscribed socket stays within budget. It prints the latency from
# the broker write to the socket.
set -euo pipefail
cd "$(dirname "$0")/.."

SUBSCRIBERS="${SUBSCRIBERS:-10000}"
GATEWAYS="${GATEWAYS:-2}"
TICK_RATE="${TICK_RATE:-20}"          # ticks/s written to the broker
SYMBOLS="${SYMBOLS:-100}"
DURATION="${DURATION:-20}"
PARTITIONS="${PARTITIONS:-16}"
SOURCE_IPS="${SOURCE_IPS:-127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5}"
BROKER_PORT="${BROKER_PORT:-19720}"
GW_PORT="${GW_PORT:-19730}"
MAX_BYTES_PER_SUB="${MAX_BYTES_PER_SUB:-12288}"
SECRET="verify-ws-fanout-secret-0123456789"

limit=$(ulimit -n)
if (( limit < SUBSCRIBERS + 200 )); then
  echo "file-descriptor limit $limit is below SUBSCRIBERS=$SUBSCRIBERS; lowering the test to $((limit - 500))" >&2
  SUBSCRIBERS=$((limit - 500))
fi

cargo build --release --locked -p brahmaputra-server -p brahmaputra-ws-gateway >/dev/null

WORK="$(mktemp -d "${TMPDIR:-/tmp}/brahmaputra-ws-fanout.XXXXXX")"
PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 100); do
    curl -sf "$1" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "$1 never became ready" >&2
  return 1
}

./target/release/brahmaputra-server --data-dir "$WORK/data" \
  --default-partitions "$PARTITIONS" --port "$BROKER_PORT" \
  --http-port $((BROKER_PORT + 1)) >"$WORK/broker.log" 2>&1 &
PIDS+=($!)
wait_ready "http://127.0.0.1:$((BROKER_PORT + 1))/metrics"

urls=()
metrics=()
for ((i = 0; i < GATEWAYS; i++)); do
  port=$((GW_PORT + 2 * i))
  RUST_LOG=warn ./target/release/brahmaputra-ws-gateway \
    --listen "127.0.0.1:$port" --http-listen "127.0.0.1:$((port + 1))" \
    --broker "127.0.0.1:$BROKER_PORT" --jwt-secret "$SECRET" \
    --allow-subscribe 'prices.*' >"$WORK/gateway-$i.log" 2>&1 &
  PIDS+=($!)
  wait_ready "http://127.0.0.1:$((port + 1))/readyz"
  urls+=("ws://127.0.0.1:$port/ws")
  metrics+=("http://127.0.0.1:$((port + 1))/metrics")
done
join() { local IFS=,; echo "$*"; }

echo "== $SUBSCRIBERS subscribers on $GATEWAYS gateways, $TICK_RATE ticks/s for ${DURATION}s =="
./target/release/ws-loadgen --url "$(join "${urls[@]}")" --metrics "$(join "${metrics[@]}")" \
  --secret "$SECRET" --connections "$SUBSCRIBERS" --ramp 4000 --source-ips "$SOURCE_IPS" \
  --subscribe prices.us --broker "127.0.0.1:$BROKER_PORT" \
  --tick-rate "$TICK_RATE" --symbols "$SYMBOLS" --duration "$DURATION" \
  | tee "$WORK/loadgen.txt"

echo
echo "== gateway side =="
fail=0
for ((i = 0; i < GATEWAYS; i++)); do
  m="$(curl -s "${metrics[$i]}")"
  feeds=$(awk '/^ws_feeds_active /{print $2}' <<<"$m")
  fetched=$(awk '/^ws_feed_records_total /{print $2}' <<<"$m")
  delivered=$(awk '/^ws_records_delivered_total /{print $2}' <<<"$m")
  echo "gateway $i: feeds $feeds, records read from broker $fetched, frames delivered $delivered"
  (( feeds <= 1 )) || { echo "FAIL: gateway $i runs $feeds feeds for one topic" >&2; fail=1; }
done
port_hex=$(printf '%04X' "$BROKER_PORT")
broker_conns=$({ cat /proc/net/tcp; cat /proc/net/tcp6 2>/dev/null || true; } | awk -v p=":$port_hex" '$2 ~ p"$" && $4=="01"' | wc -l)
echo "connections to broker   $broker_conns (for $SUBSCRIBERS sockets on $GATEWAYS gateways)"

field() { awk -v k="$1" 'index($0, k) == 1 { print $NF }' "$WORK/loadgen.txt"; }
[[ "$(field 'subscribed sockets')" == "$SUBSCRIBERS" ]] || { echo "FAIL: not every socket subscribed" >&2; fail=1; }
[[ "$(field 'deliveries missing')" == "0" ]] || { echo "FAIL: deliveries were lost" >&2; fail=1; }
[[ "$(field 'closed by gateway')" == "0" ]] || { echo "FAIL: the gateway dropped sockets" >&2; fail=1; }
ticks=$(awk '/^ticks written to broker/{print $5}' "$WORK/loadgen.txt")
(( ticks > 0 )) || { echo "FAIL: no ticks were written" >&2; fail=1; }
per_sub=$(awk '/^gateway bytes\/conn/{print $3}' "$WORK/loadgen.txt")
if [[ -n "$per_sub" ]] && (( per_sub > MAX_BYTES_PER_SUB )); then
  echo "FAIL: $per_sub bytes per subscribed socket (budget $MAX_BYTES_PER_SUB)" >&2
  fail=1
fi
# Per gateway: 2 producers, 1 health check, 1 feed; plus the tick producer.
(( broker_conns <= GATEWAYS * 4 + 2 )) || { echo "FAIL: broker holds $broker_conns connections" >&2; fail=1; }
(( fail == 0 )) && echo "PASS"
exit $fail
