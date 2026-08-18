#!/usr/bin/env bash
# Chaos run: continuous produce against a five-node cluster while brokers
# are killed and restarted at random, then a full audit of what survived.
#
# The other suites stage one failure at a time and check the result. This
# one refuses to be that tidy: it keeps writing with acks=all while the
# cluster is repeatedly broken underneath, and afterwards asserts the only
# invariant that must hold no matter the order events happened in —
#
#   every record the broker acknowledged is still readable, exactly once,
#   from every surviving replica, and the replicas agree byte for byte.
#
# A record whose produce failed or timed out is *not* required to be
# present: the client was told it failed. What is forbidden is an
# acknowledgement that turns out to be a lie.
#
# Requires Git Bash on Windows.

set -Eeuo pipefail

ROUNDS="${ROUNDS:-6}"
RECORDS_PER_ROUND="${RECORDS_PER_ROUND:-150}"
SESSION_TIMEOUT_MS="${SESSION_TIMEOUT_MS:-5000}"
HEARTBEAT_INTERVAL_MS="${HEARTBEAT_INTERVAL_MS:-500}"
REPLICA_LAG_TIME_MAX_MS="${REPLICA_LAG_TIME_MAX_MS:-10000}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
TRANSPORT="${TRANSPORT:-tcp}"
NODE_COUNT=5
TOPIC="chaos"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-chaos.XXXXXX")"
CLUSTER_ID="chaos-$(perl -e 'printf "%08x", time')"
SERVER_EXE="$ROOT/target/debug/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/debug/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/debug/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/debug/brahmaputra-cli"

CHECKS=0
ACKED_FILE="$WORK_DIR/acked.txt"
declare -A DATA_PORT CONTROL_PORT NODE_PID NODE_GENERATION

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
  if [[ "${KEEP_ARTIFACTS:-0}" != "1" && -d "$WORK_DIR" && "$WORK_DIR" == "$TEMP_ROOT"/brahmaputra-chaos.* ]]; then
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
  local node="$1"
  local generation="${NODE_GENERATION[$node]:-0}"
  generation=$((generation + 1))
  NODE_GENERATION[$node]="$generation"
  local dir="$WORK_DIR/node-$node"
  mkdir -p "$dir/data"
  local -a peer_args
  mapfile -t peer_args < <(peers)
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
    --segment-bytes 4194304 \
    --transport "$TRANSPORT" \
    > "$dir/server.$generation.out" 2> "$dir/server.$generation.err" &
  NODE_PID[$node]=$!
}

kill_node() {
  local node="$1"
  kill -9 "${NODE_PID[$node]}" 2>/dev/null || true
  wait "${NODE_PID[$node]}" 2>/dev/null || true
  unset 'NODE_PID[$node]'
}

node_running() { [[ -n "${NODE_PID[$1]:-}" ]]; }

controller_get() { curl -sf --max-time 5 "http://127.0.0.1:${CONTROL_PORT[$1]}$2"; }
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

# "leader isr_csv replicas_csv" as seen by an observer node.
partition_view() {
  controller_get "$1" /api/v1/controller/metadata 2>/dev/null \
    | perl -0777 -ne '
      use JSON::PP;
      my $image = eval { decode_json($_) } or exit 1;
      my $topic = $image->{topics}{"'"$TOPIC"'"} or exit 1;
      my $p = $topic->{partitions}{"0"} or exit 1;
      printf "%s %s %s\n", $p->{leader}, join(",", @{$p->{isr}}), join(",", @{$p->{replicas}});
    '
}

live_observer() {
  local node
  for node in $(seq 1 "$NODE_COUNT"); do
    if node_running "$node" && http_ready "$node"; then
      printf '%s' "$node"
      return 0
    fi
  done
  return 1
}

isr_at_least() {
  local observer="$1" want="$2" view isr
  view="$(partition_view "$observer")" || return 1
  isr="$(printf '%s' "$view" | awk '{print $2}')"
  [[ -n "$isr" ]] || return 1
  (( $(printf '%s' "$isr" | tr ',' '\n' | grep -c .) >= want ))
}

# Leadership has moved to a broker that is actually running.
leader_is_live() {
  local observer="$1" view leader
  view="$(partition_view "$observer")" || return 1
  leader="$(printf '%s' "$view" | awk '{print $1}')"
  [[ -n "$leader" && "$leader" != "-1" ]] || return 1
  node_running "$leader"
}

broker_address() { printf '127.0.0.1:%s' "${DATA_PORT[$1]}"; }

cli() {
  local node="$1"
  shift
  "$CLI_EXE" --transport "$TRANSPORT" --broker "$(broker_address "$node")" "$@"
}

# Produce one round, recording only the values the broker acknowledged.
produce_round() {
  local node="$1" round="$2"
  local path="$WORK_DIR/round-$round.txt"
  perl -e '
    my ($n, $r) = @ARGV;
    printf "chaos-%03d-%06d\n", $r, $_ for 1 .. $n;
  ' "$RECORDS_PER_ROUND" "$round" > "$path"

  if cli "$node" produce --topic "$TOPIC" --partition 0 --file "$path" \
      --acks all --timeout-ms 10000 > "$WORK_DIR/produce-$round.out" 2>&1; then
    cat "$path" >> "$ACKED_FILE"
    printf '%s' "$RECORDS_PER_ROUND"
  else
    # A failed produce may still have appended some records; the client was
    # told nothing succeeded, so none of them are *required* to be present.
    # Recording none of them keeps the audit strictly about broken promises.
    printf '0'
  fi
}

log_digest() {
  find "$WORK_DIR/node-$1/data/$TOPIC-0" -name '*.log' -exec cat {} + 2>/dev/null \
    | cksum | awk '{print $1 "/" $2}'
}

# ------------------------------------------------------------------ set up

stage "Start a five-node cluster and one RF=3 partition (min.insync.replicas=2)"
: > "$ACKED_FILE"
for node in $(seq 1 "$NODE_COUNT"); do
  DATA_PORT[$node]="$(allocate_port)"
  CONTROL_PORT[$node]="$(allocate_port)"
done
for node in $(seq 1 "$NODE_COUNT"); do start_node "$node"; done
wait_until "controller HTTP endpoints" 60 all_http_ready
controller_post 1 /api/v1/controller/bootstrap >/dev/null || die "bootstrap failed"

all_registered() {
  controller_get 1 /api/v1/controller/metadata 2>/dev/null \
    | perl -0777 -ne '
      use JSON::PP;
      my $image = eval { decode_json($_) } or exit 1;
      exit(scalar(grep { $image->{brokers}{$_}{alive} } keys %{$image->{brokers}}) == 5 ? 0 : 1);
    '
}
wait_until "all five brokers to register" 60 all_registered
controller_post 1 /api/v1/controller/command \
  "$(printf '{"type":"create_topic","name":"%s","partitions":1,"replication_factor":3,"configs":{"min.insync.replicas":"2"}}' "$TOPIC")" \
  >/dev/null || die "topic creation failed"
wait_until "full ISR" 60 isr_at_least 1 3
read -r LEADER ISR REPLICAS <<<"$(partition_view 1)"
pass "partition ready: leader $LEADER, ISR [$ISR], replicas [$REPLICAS]"
replica_nodes() { printf '%s' "$REPLICAS" | tr ',' '\n'; }

# -------------------------------------------------------------------- chaos

stage "Chaos: $ROUNDS rounds of produce, each with a broker killed or restarted"
TOTAL_ACKED=0
DOWN=""
for round in $(seq 1 "$ROUNDS"); do
  observer="$(live_observer)" || die "no live node to observe the cluster"

  # Alternate: break something, then heal it. Only ever take one replica
  # down at a time, so acks=all with min.insync.replicas=2 stays satisfiable
  # and the controller quorum (5 nodes) never loses majority.
  if [[ -z "$DOWN" ]]; then
    victim="$(replica_nodes | shuf -n 1)"
    kill_node "$victim"
    DOWN="$victim"
    info "round $round: killed replica $victim"
  else
    start_node "$DOWN"
    info "round $round: restarted replica $DOWN"
    wait_until "broker $DOWN to come back" 60 http_ready "$DOWN"
    DOWN=""
  fi

  observer="$(live_observer)" || die "no live node to observe the cluster"
  wait_until "ISR to hold at least min.insync.replicas" "$WAIT_SECONDS" \
    isr_at_least "$observer" 2
  # Produce only once leadership has landed on a node that is actually up.
  # Sending at a leader the controller has not fenced yet just burns the
  # request timeout, which is realistic but makes the run needlessly slow.
  wait_until "leadership to sit on a live broker" "$WAIT_SECONDS" \
    leader_is_live "$observer"

  # Produce through a live replica; a dead one would just fail to connect.
  #
  # Deliberately not `... | head -1`: under `set -o pipefail` head exits as
  # soon as it has its line, the upstream loop dies of SIGPIPE, and the
  # assignment fails — so `set -e` aborted the whole run silently, with no
  # failed assertion and no message. That is what made this suite look like
  # a broker fault for so long. Reading from a process substitution and
  # breaking keeps the exit status ours.
  writer=""
  while read -r candidate; do
    if node_running "$candidate"; then
      writer="$candidate"
      break
    fi
  done < <(replica_nodes)
  [[ -n "$writer" ]] || die "no live replica to produce through"
  acked="$(produce_round "$writer" "$round")"
  TOTAL_ACKED=$((TOTAL_ACKED + acked))
  info "round $round: $acked/$RECORDS_PER_ROUND records acknowledged"
done

# Heal the cluster and let it converge before auditing.
if [[ -n "$DOWN" ]]; then
  start_node "$DOWN"
  wait_until "broker $DOWN to come back" 60 http_ready "$DOWN"
fi
observer="$(live_observer)" || die "no live node to observe the cluster"
wait_until "the ISR to heal to all three replicas" "$WAIT_SECONDS" isr_at_least "$observer" 3
pass "cluster healed to a full ISR after $ROUNDS rounds of chaos"

# ------------------------------------------------------------------- audit

stage "Audit: every acknowledged record must still be there, exactly once"
(( TOTAL_ACKED > 0 )) || die "no produce round was acknowledged; the run proved nothing"
info "$TOTAL_ACKED records were acknowledged across the run"

read -r LEADER ISR REPLICAS <<<"$(partition_view "$observer")"
cli "$LEADER" consume --topic "$TOPIC" --partition 0 --from earliest --max 100000 \
  | sed -n 's/^partition=[0-9]* offset=[0-9]* key=[^ ]* value=\(.*\)$/\1/p' \
  | sort > "$WORK_DIR/read-back.txt"
sort "$ACKED_FILE" > "$WORK_DIR/acked-sorted.txt"

MISSING="$(comm -23 "$WORK_DIR/acked-sorted.txt" "$WORK_DIR/read-back.txt" | wc -l | tr -d ' ')"
assert_eq "$MISSING" "0" "no acknowledged record was lost across $ROUNDS failures"

DUPLICATED="$(( $(wc -l < "$WORK_DIR/read-back.txt") - $(sort -u "$WORK_DIR/read-back.txt" | wc -l) ))"
assert_eq "$DUPLICATED" "0" "no record was duplicated across $ROUNDS failures"

# Offsets must be contiguous: a gap would mean the log itself is damaged.
GAPS="$(cli "$LEADER" consume --topic "$TOPIC" --partition 0 --from earliest --max 100000 \
  | sed -n 's/^partition=[0-9]* offset=\([0-9]*\).*$/\1/p' \
  | awk 'NR == 1 { expected = $1 } { if ($1 != expected) { gaps++ } expected = $1 + 1 } END { print gaps + 0 }')"
assert_eq "$GAPS" "0" "offsets are contiguous with no holes"

# And every replica agrees byte for byte.
sleep 2
DIGEST="$(log_digest "$LEADER")"
for node in $(replica_nodes); do
  node_running "$node" || continue
  [[ "$(log_digest "$node")" == "$DIGEST" ]] \
    || die "replica $node diverged from leader $LEADER ($(log_digest "$node") vs $DIGEST)"
done
pass "every surviving replica's log is byte-identical to the leader's ($DIGEST)"

# The cluster still works.
FINAL="$WORK_DIR/final.txt"
perl -e 'printf "final-%06d\n", $_ for 1 .. 50' > "$FINAL"
cli "$LEADER" produce --topic "$TOPIC" --partition 0 --file "$FINAL" --acks all >/dev/null \
  || die "the cluster does not accept writes after the chaos run"
pass "the cluster still accepts acks=all writes after the chaos run"

stage "Chaos verification complete ($TRANSPORT)"
printf 'checks passed: %s\n' "$CHECKS"
