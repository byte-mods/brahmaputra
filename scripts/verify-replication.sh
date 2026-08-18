#!/usr/bin/env bash
# Focused replication verification (DESIGN.md §5, Blueprint 04): one RF=3
# partition on a three-node cluster, taken through leader loss, follower
# resync on restart, and min.insync.replicas enforcement.
#
# This is the narrow, fast-running complement to scripts/verify-m3.sh: it
# answers "does replication elect, resync and converge?" without the 64
# concurrent producer processes and 10-minute downtime scenario, so it is
# usable as a routine check.
#
# Requires Git Bash on Windows.

set -Eeuo pipefail

SESSION_TIMEOUT_MS="${SESSION_TIMEOUT_MS:-5000}"
HEARTBEAT_INTERVAL_MS="${HEARTBEAT_INTERVAL_MS:-500}"
REPLICA_LAG_TIME_MAX_MS="${REPLICA_LAG_TIME_MAX_MS:-10000}"
REJOIN_TIMEOUT_SECONDS="${REJOIN_TIMEOUT_SECONDS:-90}"
WAIT_SECONDS="${WAIT_SECONDS:-90}"
RECORDS="${RECORDS:-200}"
# tcp | quic — the whole cluster (clients and inter-broker replication)
# uses whichever is set.
TRANSPORT="${TRANSPORT:-tcp}"
NODE_COUNT=5
TOPIC="replication-focus"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-repl.XXXXXX")"
CLUSTER_ID="repl-$(perl -e 'printf "%08x", time')"
SERVER_EXE="$ROOT/target/debug/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/debug/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/debug/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/debug/brahmaputra-cli"

CHECKS=0
declare -A DATA_PORT CONTROL_PORT NODE_PID

stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
pass() { CHECKS=$((CHECKS + 1)); printf '\033[32mPASS: %s\033[0m\n' "$1"; }
info() { printf '     %s\n' "$1"; }
die() { printf '\n\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

assert_eq() {
  local actual="$1" expected="$2" description="$3"
  [[ "$actual" == "$expected" ]] || die "$description (expected=$expected actual=$actual)"
  pass "$description"
}

cleanup() {
  local node
  for node in "${!NODE_PID[@]}"; do
    kill -9 "${NODE_PID[$node]}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [[ "${KEEP_ARTIFACTS:-0}" != "1" && -d "$WORK_DIR" && "$WORK_DIR" == "$TEMP_ROOT"/brahmaputra-repl.* ]]; then
    rm -rf "$WORK_DIR"
  else
    printf 'artifacts: %s\n' "$WORK_DIR"
  fi
}
trap cleanup EXIT

allocate_port() {
  perl -MIO::Socket::INET -e '
    my $s = IO::Socket::INET->new(LocalAddr => "127.0.0.1", LocalPort => 0, Listen => 1, Proto => "tcp") or die $!;
    print $s->sockport;
  '
}

peers() {
  local node out=()
  for node in $(seq 1 "$NODE_COUNT"); do
    out+=(--controller-peer "$node=127.0.0.1:${CONTROL_PORT[$node]}")
  done
  printf '%s\n' "${out[@]}"
}

start_node() {
  local node="$1" restart="${2:-0}" generation="${3:-0}"
  local dir="$WORK_DIR/node-$node"
  mkdir -p "$dir/data"
  local -a peer_args
  mapfile -t peer_args < <(peers)
  local stdout="$dir/server.$generation.stdout.log" stderr="$dir/server.$generation.stderr.log"
  "$SERVER_EXE" \
    --host 127.0.0.1 --port "${DATA_PORT[$node]}" \
    --data-dir "$dir/data" \
    --node-id "$node" --cluster-id "$CLUSTER_ID" \
    --control-port "${CONTROL_PORT[$node]}" \
    "${peer_args[@]}" \
    --heartbeat-interval-ms "$HEARTBEAT_INTERVAL_MS" \
    --session-timeout-ms "$SESSION_TIMEOUT_MS" \
    --replica-lag-time-max-ms "$REPLICA_LAG_TIME_MAX_MS" \
    --offsets-topic-partitions 1 \
    --segment-bytes 1048576 \
    --transport "$TRANSPORT" \
    > "$stdout" 2> "$stderr" &
  NODE_PID[$node]=$!
}

kill_node() {
  local node="$1"
  kill -9 "${NODE_PID[$node]}" 2>/dev/null || true
  wait "${NODE_PID[$node]}" 2>/dev/null || true
  unset 'NODE_PID[$node]'
}

controller_get() {
  curl -sf --max-time 5 "http://127.0.0.1:${CONTROL_PORT[$1]}$2"
}

controller_post() {
  curl -sf --max-time 10 -X POST -H 'content-type: application/json' \
    ${3:+--data "$3"} "http://127.0.0.1:${CONTROL_PORT[$1]}$2"
}

wait_until() {
  local description="$1" timeout_seconds="$2"
  shift 2
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if "$@"; then return 0; fi
    sleep 0.5
  done
  die "timed out waiting for $description"
}

http_ready() { controller_get "$1" /api/v1/controller/metadata >/dev/null 2>&1; }
all_http_ready() {
  local node
  for node in $(seq 1 "$NODE_COUNT"); do http_ready "$node" || return 1; done
}

# Partition view from node $1: "leader isr_csv leader_epoch replicas_csv"
partition_view() {
  controller_get "$1" /api/v1/controller/metadata 2>/dev/null \
    | perl -0777 -ne '
      use JSON::PP;
      my $image = eval { decode_json($_) } or exit 1;
      my $topic = $image->{topics}{"'"$TOPIC"'"} or exit 1;
      my $p = $topic->{partitions}{"0"} or exit 1;
      printf "%s %s %s %s\n", $p->{leader}, join(",", @{$p->{isr}}),
        $p->{leader_epoch}, join(",", @{$p->{replicas}});
    '
}

live_brokers() {
  controller_get "$1" /api/v1/controller/metadata 2>/dev/null \
    | perl -0777 -ne '
      use JSON::PP;
      my $image = eval { decode_json($_) } or exit 1;
      print join(",", sort { $a <=> $b } grep { $image->{brokers}{$_}{alive} } keys %{$image->{brokers}}), "\n";
    '
}

isr_size_is() {
  local observer="$1" want="$2" view isr
  view="$(partition_view "$observer")" || return 1
  isr="$(printf '%s' "$view" | awk '{print $2}')"
  [[ -n "$isr" ]] || return 1
  (( $(printf '%s' "$isr" | tr ',' '\n' | grep -c .) == want ))
}

leader_is_not() {
  local observer="$1" avoid="$2" view leader
  view="$(partition_view "$observer")" || return 1
  leader="$(printf '%s' "$view" | awk '{print $1}')"
  [[ -n "$leader" && "$leader" != "$avoid" && "$leader" != "-1" ]]
}

broker_address() { printf '127.0.0.1:%s' "${DATA_PORT[$1]}"; }

# The brokers assigned to the partition, one per line. Only these hold the
# log; the rest of the cluster just keeps the controller quorum alive.
replica_nodes() { printf '%s' "$REPLICAS" | tr ',' '\n'; }

# Sum of committed record bytes, as a checksum per replica data dir.
log_digest() {
  local node="$1"
  find "$WORK_DIR/node-$node/data/$TOPIC-0" -name '*.log' -exec cat {} + 2>/dev/null \
    | cksum | awk '{print $1 "/" $2}'
}

values_file() {
  local count="$1" tag="$2"
  local path="$WORK_DIR/values-$tag.txt"
  perl -e '
    my ($n, $label) = @ARGV;
    printf "%s-%06d\n", $label, $_ for 1 .. $n;
  ' "$count" "$tag" > "$path"
  printf '%s' "$path"
}

produce_file() {
  local node="$1" path="$2" acks="${3:-all}"
  "$CLI_EXE" --transport "$TRANSPORT" --broker "$(broker_address "$node")" \
    produce --topic "$TOPIC" \
    --partition 0 --file "$path" --acks "$acks" --timeout-ms 15000
}

consume_all() {
  local node="$1"
  "$CLI_EXE" --transport "$TRANSPORT" --broker "$(broker_address "$node")" \
    consume --topic "$TOPIC" \
    --partition 0 --from earliest --max 100000
}

# ------------------------------------------------------------------ bring up

stage "Start a five-node combined cluster (quorum survives two broker losses)"
for node in $(seq 1 "$NODE_COUNT"); do
  DATA_PORT[$node]="$(allocate_port)"
  CONTROL_PORT[$node]="$(allocate_port)"
done
for node in $(seq 1 "$NODE_COUNT"); do start_node "$node" 0 0; done
wait_until "controller HTTP endpoints" 60 all_http_ready
controller_post 1 /api/v1/controller/bootstrap >/dev/null || die "bootstrap failed"

all_brokers_live() { [[ "$(live_brokers 1)" == "1,2,3,4,5" ]]; }
wait_until "all five brokers to register" 60 all_brokers_live
pass "five combined nodes registered with the controller quorum"

stage "Create one RF=3 partition with min.insync.replicas=2"
controller_post 1 /api/v1/controller/command \
  "$(printf '{"type":"create_topic","name":"%s","partitions":1,"replication_factor":3,"configs":{"min.insync.replicas":"2"}}' "$TOPIC")" \
  >/dev/null || die "topic creation failed"
wait_until "full ISR on the new partition" 60 isr_size_is 1 3
read -r LEADER ISR EPOCH REPLICAS <<<"$(partition_view 1)"
pass "partition has leader $LEADER, ISR [$ISR], replicas [$REPLICAS], leader epoch $EPOCH"

stage "acks=all replicates every record to all three replicas"
FIRST="$(values_file "$RECORDS" first)"
produce_file "$LEADER" "$FIRST" all >/dev/null || die "acks=all produce failed"
sleep 1
DIGEST_LEADER="$(log_digest "$LEADER")"
for node in $(replica_nodes); do
  [[ "$(log_digest "$node")" == "$DIGEST_LEADER" ]] \
    || die "replica $node log differs from leader $LEADER ($(log_digest "$node") vs $DIGEST_LEADER)"
done
pass "all three replica logs are byte-identical after acks=all ($DIGEST_LEADER)"
assert_eq "$(consume_all "$LEADER" | grep -c '^partition=')" "$RECORDS" \
  "a consumer reads exactly the $RECORDS acknowledged records"

# ------------------------------------------------------------- leader failure

stage "Hard-kill the leader: a surviving ISR member takes over"
OLD_LEADER="$LEADER"
kill_node "$OLD_LEADER"
SURVIVOR="$(replica_nodes | grep -v "^$OLD_LEADER$" | head -1)"
wait_until "leadership to move off broker $OLD_LEADER" "$WAIT_SECONDS" \
  leader_is_not "$SURVIVOR" "$OLD_LEADER"
read -r NEW_LEADER ISR EPOCH REPLICAS <<<"$(partition_view "$SURVIVOR")"
pass "controller elected broker $NEW_LEADER from the ISR (epoch $EPOCH, ISR [$ISR])"

SECOND="$(values_file 50 second)"
produce_file "$NEW_LEADER" "$SECOND" all >/dev/null \
  || die "acks=all produce failed on the new leader with two replicas left"
pass "acks=all still succeeds with two of three replicas (min.insync.replicas=2)"
assert_eq "$(consume_all "$NEW_LEADER" | grep -c '^partition=')" "$((RECORDS + 50))" \
  "no acknowledged record was lost across the failover"

# -------------------------------------------------------------- resync on return

stage "Restart the dead broker: it must catch up and re-enter the ISR"
start_node "$OLD_LEADER" 1 1
wait_until "broker $OLD_LEADER HTTP to come back" 60 http_ready "$OLD_LEADER"
REJOIN_STARTED="$SECONDS"
if ! wait_until "broker $OLD_LEADER to re-enter the ISR" "$REJOIN_TIMEOUT_SECONDS" \
    isr_size_is "$SURVIVOR" 3; then
  die "broker $OLD_LEADER never rejoined the ISR"
fi
REJOIN_SECONDS=$((SECONDS - REJOIN_STARTED))
read -r LEADER_NOW ISR EPOCH REPLICAS <<<"$(partition_view "$SURVIVOR")"
pass "broker $OLD_LEADER rejoined the ISR in ${REJOIN_SECONDS}s (ISR [$ISR])"
assert_eq "$LEADER_NOW" "$NEW_LEADER" "leadership stayed with broker $NEW_LEADER after the rejoin"

sleep 1
DIGEST_NEW_LEADER="$(log_digest "$NEW_LEADER")"
assert_eq "$(log_digest "$OLD_LEADER")" "$DIGEST_NEW_LEADER" \
  "the restarted broker's log is byte-identical to the leader's"

THIRD="$(values_file 50 third)"
produce_file "$NEW_LEADER" "$THIRD" all >/dev/null \
  || die "acks=all produce failed after the ISR healed"
sleep 1
for node in $(replica_nodes); do
  [[ "$(log_digest "$node")" == "$(log_digest "$NEW_LEADER")" ]] \
    || die "replica $node diverged after the rejoin"
done
pass "post-rejoin writes replicate to all three replicas again"
assert_eq "$(consume_all "$NEW_LEADER" | grep -c '^partition=')" "$((RECORDS + 100))" \
  "the partition serves every record produced across the whole run"

# ------------------------------------------------------- min.insync enforcement

stage "Drop below min.insync.replicas: acks=all must refuse to lose data"
THIRD_NODE="$(replica_nodes | grep -v "^$NEW_LEADER$" | tail -1)"
kill_node "$THIRD_NODE"
OTHER="$(replica_nodes | grep -v -e "^$NEW_LEADER$" -e "^$THIRD_NODE$" | head -1)"
kill_node "$OTHER"
info "killed replicas $THIRD_NODE and $OTHER; three of five controller nodes remain, so metadata writes continue"
wait_until "ISR to shrink to the lone leader" "$WAIT_SECONDS" isr_size_is "$NEW_LEADER" 1
FOURTH="$(values_file 5 fourth)"
if produce_file "$NEW_LEADER" "$FOURTH" all >"$WORK_DIR/underreplicated.out" 2>&1; then
  die "acks=all succeeded with only one replica in the ISR"
fi
grep -qi "replica" "$WORK_DIR/underreplicated.out" \
  || die "expected a not-enough-replicas error, got: $(cat "$WORK_DIR/underreplicated.out")"
pass "acks=all was rejected while the ISR was below min.insync.replicas"

start_node "$THIRD_NODE" 1 1
wait_until "broker $THIRD_NODE HTTP to come back" 60 http_ready "$THIRD_NODE"
wait_until "the ISR to recover to two replicas" "$REJOIN_TIMEOUT_SECONDS" isr_size_is "$NEW_LEADER" 2
produce_file "$NEW_LEADER" "$FOURTH" all >/dev/null \
  || die "acks=all did not resume once the ISR recovered"
pass "acks=all resumed as soon as the ISR met min.insync.replicas again"

stage "Replication verification complete"
printf 'checks passed: %s\n' "$CHECKS"
