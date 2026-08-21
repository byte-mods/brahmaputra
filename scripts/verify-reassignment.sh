#!/usr/bin/env bash
# Partition reassignment and rack-aware placement, against a live cluster.
#
# These are the two things that decide whether a cluster can be operated
# for years rather than merely started once:
#
#   * Reassignment is what lets a cluster be grown, shrunk or rebalanced.
#     Without it a partition lives on whichever brokers it was created on,
#     forever.
#   * Rack awareness is what makes a replication factor mean what an
#     operator thinks it means. Three replicas in one rack survive exactly
#     as much as one replica does.
#
# The assertions are about the *system*: which brokers hold real log files,
# whether every record survives the move, and whether the partition is
# readable throughout.
set -uo pipefail

cd "$(dirname "$0")/.."

SERVER_EXE="./target/release/brahmaputra-server"
CLI_EXE="./target/release/brahmaputra-cli"
for exe in "$SERVER_EXE" "$CLI_EXE"; do
  [[ -x "$exe" ]] || { echo "missing $exe; run: cargo build --release" >&2; exit 2; }
done

NODE_COUNT=4
RECORDS=500
CLUSTER_ID="verify-reassign"
TOPIC="reassign-me"

ROOT="$(mktemp -d -t brahmaputra-reassign.XXXXXX)"
declare -A PID
DATA_PORT=([1]=19401 [2]=19402 [3]=19403 [4]=19404)
CONTROL_PORT=([1]=19411 [2]=19412 [3]=19413 [4]=19414)
# Four brokers across three racks: broker 4 shares rack "a" with broker 1,
# so RF=3 has exactly one way to span all three racks.
RACK=([1]="rack-a" [2]="rack-b" [3]="rack-c" [4]="rack-a")

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
    --host 127.0.0.1
    --port "${DATA_PORT[$node]}"
    --data-dir "$ROOT/node-$node"
    --node-id "$node"
    --cluster-id "$CLUSTER_ID"
    --control-port "${CONTROL_PORT[$node]}"
    --http-port 0
    --rack "${RACK[$node]}"
    --heartbeat-interval-ms 500
    --session-timeout-ms 3000
    --replica-lag-time-max-ms 4000
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
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then exec 3<&- 3>&-; return 0; fi
    sleep 0.2
  done
  echo "port $port never opened" >&2
  return 1
}

# Which brokers hold a real, non-empty log for a partition — the ground
# truth, read off disk rather than from what the metadata claims.
holders_on_disk() {
  local partition="$1" node holders=""
  for node in $(seq 1 "$NODE_COUNT"); do
    local dir="$ROOT/node-$node/${TOPIC}-${partition}"
    if [[ -d "$dir" ]] && find "$dir" -name '*.log' -size +0c 2>/dev/null | grep -q .; then
      holders="$holders $node"
    fi
  done
  echo "$holders" | tr -s ' ' | sed 's/^ //'
}

metadata_json() {
  curl -s "http://127.0.0.1:${CONTROL_PORT[1]}/api/v1/controller/metadata" 2>/dev/null
}

# The metadata's replica list for a partition, sorted, space separated.
replicas_of() {
  local partition="$1"
  metadata_json | perl -MJSON::PP -0777 -e '
    my $d = eval { decode_json(<STDIN>) } or exit 0;
    my $t = $d->{topics}{$ARGV[0]} or exit 0;
    my $p = $t->{partitions}{$ARGV[1]} or exit 0;
    print join(" ", sort { $a <=> $b } @{$p->{replicas}});
  ' "$TOPIC" "$partition" 2>/dev/null
}

racks_of() {
  local list="$1" broker out=""
  for broker in $list; do out="$out ${RACK[$broker]}"; done
  echo "$out" | tr ' ' '\n' | grep -v '^$' | sort -u | tr '\n' ' ' | sed 's/ $//'
}

read_all() {
  local partition="$1"
  "$CLI_EXE" --broker "127.0.0.1:${DATA_PORT[1]}" consume \
    --topic "$TOPIC" --partition "$partition" --from earliest --max "$((RECORDS + 100))" 2>/dev/null \
    | grep -c '^partition=' || true
}

stage "Launch $NODE_COUNT brokers across racks a, b, c, a"
for node in $(seq 1 "$NODE_COUNT"); do start_node "$node"; done
for node in $(seq 1 "$NODE_COUNT"); do wait_for_port "${CONTROL_PORT[$node]}"; done
curl -s -X POST "http://127.0.0.1:${CONTROL_PORT[1]}/api/v1/controller/bootstrap" >/dev/null
sleep 4
for node in $(seq 1 "$NODE_COUNT"); do wait_for_port "${DATA_PORT[$node]}"; done
sleep 3

CONTROLLER="http://127.0.0.1:${CONTROL_PORT[1]}"

stage "Rack-aware placement: RF=3 must span three racks, not three brokers"
"$CLI_EXE" --controller "$CONTROLLER" topic create \
  --name "$TOPIC" --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 >/dev/null 2>&1
sleep 3

all_spread="true"
for partition in 0 1 2; do
  reps="$(replicas_of "$partition")"
  distinct="$(racks_of "$reps" | wc -w)"
  if [[ "$distinct" != "3" ]]; then
    all_spread="false"
    echo "   partition $partition replicas [$reps] cover racks: $(racks_of "$reps")"
  fi
done
check "$all_spread" "every RF=3 partition spans all three racks"

# Leadership concentrated in one rack means that rack takes every write.
leader_racks="$(metadata_json | perl -MJSON::PP -0777 -e '
  my $d = eval { decode_json(<STDIN>) } or exit 0;
  my $t = $d->{topics}{$ARGV[0]} or exit 0;
  print join(" ", map { $t->{partitions}{$_}{leader} } sort keys %{$t->{partitions}});
' "$TOPIC" 2>/dev/null)"
distinct_leader_racks="$(racks_of "$leader_racks" | wc -w)"
check "$([[ "$distinct_leader_racks" -ge 2 ]] && echo true || echo false)" \
  "leadership is spread over more than one rack" "leaders [$leader_racks]"

stage "Produce $RECORDS records"
"$CLI_EXE" --broker "127.0.0.1:${DATA_PORT[1]}" produce \
  --topic "$TOPIC" --partition 0 --count "$RECORDS" --value-size 128 \
  --acks all --linger-ms 5 --compression none 2>&1 | tail -1 | sed "s|^|   |"
sleep 2

BEFORE_COUNT="$(read_all 0)"
check "$([[ "$BEFORE_COUNT" == "$RECORDS" ]] && echo true || echo false)" \
  "all $RECORDS records readable before the move" "read $BEFORE_COUNT"

BEFORE_REPLICAS="$(replicas_of 0)"
BEFORE_HOLDERS="$(holders_on_disk 0)"
echo "   metadata replicas: [$BEFORE_REPLICAS]"
echo "   brokers with data: [$BEFORE_HOLDERS]"

stage "Reassign partition 0 to a set that excludes one current replica"
# Target: keep two current replicas, swap the third for whichever broker is
# not currently holding it. That is the realistic operation — draining one
# broker — rather than moving everything at once.
NEWCOMER=""
for node in $(seq 1 "$NODE_COUNT"); do
  if ! grep -qw "$node" <<<"$BEFORE_REPLICAS"; then NEWCOMER="$node"; break; fi
done
DEPARTING="$(awk '{print $NF}' <<<"$BEFORE_REPLICAS")"
TARGET="$(tr ' ' '\n' <<<"$BEFORE_REPLICAS" | grep -vw "$DEPARTING" | tr '\n' ',' )$NEWCOMER"
echo "   $BEFORE_REPLICAS  ->  ${TARGET//,/ }   (broker $DEPARTING out, broker $NEWCOMER in)"

"$CLI_EXE" --controller "$CONTROLLER" topic reassign \
  --name "$TOPIC" --partition 0 --replicas "$TARGET" 2>&1 | sed 's/^/   /'

# Immediately after the request, before any catch-up: the union must be in
# force, so no existing copy has been dropped.
DURING="$(replicas_of 0)"
union_ok="true"
for broker in $BEFORE_REPLICAS; do
  grep -qw "$broker" <<<"$DURING" || union_ok="false"
done
grep -qw "$NEWCOMER" <<<"$DURING" || union_ok="false"
check "$union_ok" "durability never dips: every old replica is kept while the new one catches up" \
  "during=[$DURING]"

stage "Wait for the reassignment to complete"
deadline=$((SECONDS + 90))
completed="false"
while (( SECONDS < deadline )); do
  now="$(replicas_of 0)"
  want="$(tr ',' ' ' <<<"$TARGET" | tr ' ' '\n' | sort -n | tr '\n' ' ' | sed 's/ $//')"
  if [[ "$now" == "$want" ]]; then completed="true"; break; fi
  sleep 2
done
check "$completed" "the replica set narrows to the target once it is caught up" \
  "replicas=[$(replicas_of 0)] target=[${TARGET//,/ }]"

stage "The data actually moved"
AFTER_HOLDERS="$(holders_on_disk 0)"
echo "   brokers with data: [$AFTER_HOLDERS]"
check "$(grep -qw "$NEWCOMER" <<<"$AFTER_HOLDERS" && echo true || echo false)" \
  "the new broker $NEWCOMER holds a real log for the partition" "holders=[$AFTER_HOLDERS]"

AFTER_COUNT="$(read_all 0)"
check "$([[ "$AFTER_COUNT" == "$RECORDS" ]] && echo true || echo false)" \
  "all $RECORDS records still readable after the move" "read $AFTER_COUNT"

stage "The partition still accepts writes on its new replicas"
"$CLI_EXE" --broker "127.0.0.1:${DATA_PORT[1]}" produce \
  --topic "$TOPIC" --partition 0 --value "after-the-move" --acks all >/dev/null 2>&1
sleep 2
FINAL_COUNT="$(read_all 0)"
check "$([[ "$FINAL_COUNT" -gt "$RECORDS" ]] && echo true || echo false)" \
  "acks=all still succeeds on the reassigned partition" "count=$FINAL_COUNT"

stage "The departed broker is no longer asked to hold the partition"
FINAL_REPLICAS="$(replicas_of 0)"
check "$(grep -qw "$DEPARTING" <<<"$FINAL_REPLICAS" && echo false || echo true)" \
  "broker $DEPARTING has been drained from the replica set" "replicas=[$FINAL_REPLICAS]"


# Draining must also free the disk, or a cluster can be rebalanced but
# never reclaims space -- which is half the reason to rebalance.
deadline=$((SECONDS + 60))
drained="false"
while (( SECONDS < deadline )); do
  if ! grep -qw "$DEPARTING" <<<"$(holders_on_disk 0)"; then drained="true"; break; fi
  sleep 2
done
check "$drained" "broker $DEPARTING released the partition's disk" \
  "holders=[$(holders_on_disk 0)]"
printf '\n\033[36m==> Reassignment verification complete\033[0m\n'
printf 'checks passed: %s, failed: %s\n' "$PASSED" "$FAILED"
[[ "$FAILED" == "0" ]] || echo "artifacts: $ROOT"
exit $(( FAILED > 0 ? 1 : 0 ))
