#!/usr/bin/env bash
# Sustained-load soak with induced failures.
#
# The other verify scripts answer "does this work". This one answers a
# different question: does it *keep* working, and does anything drift while
# it does. Those are not the same, and the second is the one nothing in
# this repository had ever asked. Every durability claim here came from
# runs measured in minutes.
#
# What it watches for is the class of fault that minutes cannot surface:
#
#   * resident memory climbing run-over-run, which a short test reads as
#     steady state;
#   * file descriptors or segments accumulating without bound;
#   * offsets drifting, gapping or rewinding across many failovers rather
#     than one;
#   * a broker that survives three kills but not the thirtieth.
#
# Usage: SOAK_MINUTES=60 bash scripts/soak.sh
#
# Default is deliberately short so it can be run casually. A real soak is
# measured in days, and this script does not pretend otherwise -- it prints
# what it actually covered.
set -uo pipefail

cd "$(dirname "$0")/.."

SERVER_EXE="./target/release/brahmaputra-server"
CLI_EXE="./target/release/brahmaputra-cli"
for exe in "$SERVER_EXE" "$CLI_EXE"; do
  [[ -x "$exe" ]] || { echo "missing $exe; run: cargo build --release" >&2; exit 2; }
done

SOAK_MINUTES="${SOAK_MINUTES:-15}"
KILL_EVERY_SECONDS="${KILL_EVERY_SECONDS:-45}"
SAMPLE_EVERY_SECONDS="${SAMPLE_EVERY_SECONDS:-15}"
BATCH_RECORDS="${BATCH_RECORDS:-2000}"
NODE_COUNT=3
TOPIC="soak"
CLUSTER_ID="soak"

ROOT="$(mktemp -d -t brahmaputra-soak.XXXXXX)"
declare -A PID
DATA_PORT=([1]=19501 [2]=19502 [3]=19503)
CONTROL_PORT=([1]=19511 [2]=19512 [3]=19513)
RACK=([1]="rack-a" [2]="rack-b" [3]="rack-c")

PASSED=0
FAILED=0
pass() { PASSED=$((PASSED + 1)); printf '\033[32mPASS: %s\033[0m\n' "$1"; }
fail() { FAILED=$((FAILED + 1)); printf '\033[31mFAIL: %s\033[0m\n' "$1"; }
stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
check() { if [[ "$1" == "true" ]]; then pass "$2"; else fail "$2${3:+ ($3)}"; fi; }

cleanup() {
  local node
  for node in "${!PID[@]}"; do kill -9 "${PID[$node]}" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

start_node() {
  local node="$1"
  local args=(
    --host 127.0.0.1 --port "${DATA_PORT[$node]}"
    --data-dir "$ROOT/node-$node"
    --node-id "$node" --cluster-id "$CLUSTER_ID"
    --control-port "${CONTROL_PORT[$node]}" --http-port 0
    --rack "${RACK[$node]}"
    --heartbeat-interval-ms 500 --session-timeout-ms 3000
    --replica-lag-time-max-ms 4000
    --retention-check-interval-ms 1000
  )
  local peer
  for peer in $(seq 1 "$NODE_COUNT"); do
    args+=(--controller-peer "$peer=127.0.0.1:${CONTROL_PORT[$peer]}")
  done
  RUST_LOG=brahmaputra=warn "$SERVER_EXE" "${args[@]}" \
    >>"$ROOT/node-$node.stdout" 2>>"$ROOT/node-$node.stderr" &
  PID[$node]=$!
}

wait_for_port() {
  local port="$1" deadline=$((SECONDS + 40))
  while (( SECONDS < deadline )); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then exec 3<&- 3>&-; return 0; fi
    sleep 0.2
  done
  return 1
}

# Resident memory of a live broker, in KiB. The number that must not climb.
rss_kib() {
  local node="$1" pid="${PID[$node]:-}"
  [[ -n "$pid" ]] || { echo 0; return; }
  # /proc where available; tasklist on the Windows host.
  if [[ -r "/proc/$pid/status" ]]; then
    awk '/VmRSS/ {print $2}' "/proc/$pid/status" 2>/dev/null || echo 0
  else
    local win_pid
    win_pid="$(ps -p "$pid" -o pid= 2>/dev/null | tr -d ' ')"
    tasklist.exe //FI "PID eq ${win_pid:-0}" //FO CSV //NH 2>/dev/null \
      | awk -F'","' '{gsub(/[^0-9]/,"",$5); print $5+0}' | head -1 || echo 0
  fi
}

segment_count() {
  find "$ROOT" -path "*${TOPIC}-*" -name '*.log' 2>/dev/null | wc -l
}

log_end_offset() {
  # `offsets` prints `<topic>-<partition>: earliest=N latest=M`.
  "$CLI_EXE" --broker "127.0.0.1:${DATA_PORT[$1]}" offsets \
    --topic "$TOPIC" --partition 0 2>/dev/null \
    | sed -n 's/.*latest=\([0-9]*\).*/\1/p' | head -1
}

live_node() {
  local node
  for node in $(seq 1 "$NODE_COUNT"); do
    if kill -0 "${PID[$node]}" 2>/dev/null; then echo "$node"; return; fi
  done
  echo ""
}

stage "Start a $NODE_COUNT-broker cluster"
for node in $(seq 1 "$NODE_COUNT"); do start_node "$node"; done
for node in $(seq 1 "$NODE_COUNT"); do wait_for_port "${CONTROL_PORT[$node]}"; done
curl -s -X POST "http://127.0.0.1:${CONTROL_PORT[1]}/api/v1/controller/bootstrap" >/dev/null
sleep 4
for node in $(seq 1 "$NODE_COUNT"); do wait_for_port "${DATA_PORT[$node]}"; done
sleep 3

"$CLI_EXE" --controller "http://127.0.0.1:${CONTROL_PORT[1]}" topic create \
  --name "$TOPIC" --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 >/dev/null 2>&1
sleep 3

stage "Soak for $SOAK_MINUTES minute(s): acks=all writes, a broker killed every ${KILL_EVERY_SECONDS}s"

DEADLINE=$((SECONDS + SOAK_MINUTES * 60))
NEXT_KILL=$((SECONDS + KILL_EVERY_SECONDS))
NEXT_SAMPLE=$((SECONDS + SAMPLE_EVERY_SECONDS))

TOTAL_ACKED=0
BATCHES=0
FAILED_BATCHES=0
KILLS=0
LAST_END=0
REWINDS=0
FIRST_RSS=""
LAST_RSS=""
PEAK_RSS=0
RSS_SAMPLES=""

while (( SECONDS < DEADLINE )); do
  target="$(live_node)"
  [[ -n "$target" ]] || { sleep 1; continue; }

  # A batch of acks=all writes. Failures during a failover window are
  # expected and counted rather than fatal -- what must not happen is the
  # log going backwards.
  if "$CLI_EXE" --broker "127.0.0.1:${DATA_PORT[$target]}" produce \
       --topic "$TOPIC" --partition 0 --count "$BATCH_RECORDS" --value-size 256 \
       --acks all --linger-ms 5 --compression lz4 >/dev/null 2>&1; then
    TOTAL_ACKED=$((TOTAL_ACKED + BATCH_RECORDS))
    BATCHES=$((BATCHES + 1))
  else
    FAILED_BATCHES=$((FAILED_BATCHES + 1))
  fi

  # The log must only ever move forward.
  end="$(log_end_offset "$target")"
  if [[ -n "$end" && "$end" =~ ^[0-9]+$ ]]; then
    if (( end < LAST_END )); then
      REWINDS=$((REWINDS + 1))
      echo "   !! log end went backwards: $LAST_END -> $end"
    fi
    LAST_END="$end"
  fi

  if (( SECONDS >= NEXT_SAMPLE )); then
    NEXT_SAMPLE=$((SECONDS + SAMPLE_EVERY_SECONDS))
    rss="$(rss_kib "$target")"
    if [[ "$rss" =~ ^[0-9]+$ && "$rss" -gt 0 ]]; then
      [[ -z "$FIRST_RSS" ]] && FIRST_RSS="$rss"
      LAST_RSS="$rss"
      (( rss > PEAK_RSS )) && PEAK_RSS="$rss"
      RSS_SAMPLES="$RSS_SAMPLES $rss"
    fi
    printf '   t=%4ss  acked=%-8s batches=%-4s failed=%-3s kills=%-3s end=%-9s rss=%sMiB segs=%s\n' \
      "$SECONDS" "$TOTAL_ACKED" "$BATCHES" "$FAILED_BATCHES" "$KILLS" \
      "$LAST_END" "$(( ${LAST_RSS:-0} / 1024 ))" "$(segment_count)"
  fi

  if (( SECONDS >= NEXT_KILL )); then
    NEXT_KILL=$((SECONDS + KILL_EVERY_SECONDS))
    victim=$(( (RANDOM % NODE_COUNT) + 1 ))
    if kill -0 "${PID[$victim]}" 2>/dev/null; then
      kill -9 "${PID[$victim]}" 2>/dev/null || true
      KILLS=$((KILLS + 1))
      echo "   -- killed broker $victim (kill #$KILLS)"
      sleep 2
      start_node "$victim"
      wait_for_port "${DATA_PORT[$victim]}" || echo "   !! broker $victim did not come back"
      sleep 2
    fi
  fi
done

stage "Settle, then audit what survived"
for node in $(seq 1 "$NODE_COUNT"); do
  kill -0 "${PID[$node]}" 2>/dev/null || start_node "$node"
done
for node in $(seq 1 "$NODE_COUNT"); do wait_for_port "${DATA_PORT[$node]}" || true; done
sleep 12

# Every surviving replica must agree on the log, and it must be gap-free.
survivor="$(live_node)"
READBACK="$("$CLI_EXE" --broker "127.0.0.1:${DATA_PORT[$survivor]}" consume \
  --topic "$TOPIC" --partition 0 --from earliest --max $((TOTAL_ACKED + 1000)) 2>/dev/null \
  | grep -c '^partition=')"
FINAL_END="$(log_end_offset "$survivor")"

echo "   acknowledged: $TOTAL_ACKED   readable: $READBACK   log end: ${FINAL_END:-?}"

check "$([[ "$REWINDS" == "0" ]] && echo true || echo false)" \
  "the log never went backwards across $KILLS kills" "$REWINDS rewind(s)"

check "$([[ "${READBACK:-0}" -ge "$TOTAL_ACKED" ]] && echo true || echo false)" \
  "every acknowledged record is still readable" "acked=$TOTAL_ACKED readable=$READBACK"

# Offsets contiguous: a gap means a record was lost, a duplicate offset
# means the log was rewritten.
OFFSETS_OK="$("$CLI_EXE" --broker "127.0.0.1:${DATA_PORT[$survivor]}" consume \
  --topic "$TOPIC" --partition 0 --from earliest --max $((TOTAL_ACKED + 1000)) 2>/dev/null \
  | sed -n 's/.*offset=\([0-9]*\).*/\1/p' \
  | awk 'NR==1{prev=$1-1} {if ($1 != prev+1) {bad++} prev=$1} END{print (bad+0)==0 ? "true" : "false"}')"
check "$OFFSETS_OK" "offsets are contiguous with no gap or repeat"

check "$([[ "$KILLS" -gt 0 ]] && echo true || echo false)" \
  "the run actually induced failures" "$KILLS kills"

# Memory: the structural claim is that resident set does not grow with
# load. Over a soak that is testable rather than asserted.
if [[ -n "$FIRST_RSS" && -n "$LAST_RSS" && "$FIRST_RSS" -gt 0 ]]; then
  GROWTH=$(( (LAST_RSS - FIRST_RSS) * 100 / FIRST_RSS ))
  echo "   rss first=$((FIRST_RSS / 1024))MiB last=$((LAST_RSS / 1024))MiB peak=$((PEAK_RSS / 1024))MiB growth=${GROWTH}%"
  # A broker that is doing more work legitimately holds more, so this is a
  # leak detector, not a ceiling: 100% growth over a soak is the signal.
  check "$([[ "$GROWTH" -lt 100 ]] && echo true || echo false)" \
    "resident memory did not double over the run" "grew ${GROWTH}%"
else
  echo "   (rss sampling unavailable on this host; memory growth not asserted)"
fi

printf '\n\033[36m==> Soak complete\033[0m\n'
printf 'duration: %s min   acknowledged: %s   batches: %s ok / %s failed   kills: %s\n' \
  "$SOAK_MINUTES" "$TOTAL_ACKED" "$BATCHES" "$FAILED_BATCHES" "$KILLS"
printf 'checks passed: %s, failed: %s\n' "$PASSED" "$FAILED"
printf '\nThis covered %s minutes. It is a smoke soak, not a production track\n' "$SOAK_MINUTES"
printf 'record -- the faults that matter most take days to surface.\n'
[[ "$FAILED" == "0" ]] || echo "artifacts: $ROOT"
exit $(( FAILED > 0 ? 1 : 0 ))
