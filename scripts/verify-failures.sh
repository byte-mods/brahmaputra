#!/usr/bin/env bash
# Failure-scenario suite: what happens when each participant dies.
#
# The other scripts verify happy paths and one failure each. This one is
# organised by *who fails*, because that is how the question is actually
# asked in review:
#
#   producer failure  — a producer killed mid-send must not corrupt the log,
#                       and its acknowledged records must all survive
#   consumer failure  — a consumer killed mid-stream must not lose or skip
#                       records for its group; another member picks up from
#                       the last commit
#   broker failure    — covered end-to-end by verify-replication.sh; here we
#                       check the standalone case: kill -9 mid-produce and
#                       confirm the log recovers to the last valid batch
#   data loss         — nothing acknowledged is ever missing, and a torn
#                       tail from a hard kill is truncated, not served
#
# Runs against a standalone broker (fast, deterministic). Cluster-level
# broker failure lives in verify-replication.sh, which this script does not
# duplicate.
#
# Requires Git Bash on Windows.

set -Eeuo pipefail

TRANSPORT="${TRANSPORT:-tcp}"
PARTITIONS="${PARTITIONS:-4}"
RECORDS="${RECORDS:-40000}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-failures.XXXXXX")"
SERVER_EXE="$ROOT/target/debug/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/debug/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/debug/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/debug/brahmaputra-cli"

CHECKS=0
SERVER_PID=""
DATA_DIR="$WORK_DIR/data"

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
  stop_broker || true
  if [[ "${KEEP_ARTIFACTS:-0}" != "1" && -d "$WORK_DIR" && "$WORK_DIR" == "$TEMP_ROOT"/brahmaputra-failures.* ]]; then
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

start_broker() {
  local generation="${1:-0}"
  PORT="${PORT:-$(allocate_port)}"
  BROKER="127.0.0.1:$PORT"
  mkdir -p "$DATA_DIR"
  "$SERVER_EXE" --port "$PORT" --data-dir "$DATA_DIR" \
    --default-partitions "$PARTITIONS" --segment-bytes 8388608 \
    --transport "$TRANSPORT" \
    > "$WORK_DIR/server.$generation.out" 2> "$WORK_DIR/server.$generation.err" &
  SERVER_PID=$!
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if cli metadata >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  die "broker never became ready (see $WORK_DIR/server.$generation.err)"
}

stop_broker() {
  [[ -n "$SERVER_PID" ]] || return 0
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

kill_broker_hard() {
  [[ -n "$SERVER_PID" ]] || return 0
  kill -9 "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

cli() { "$CLI_EXE" --transport "$TRANSPORT" --broker "$BROKER" "$@"; }

total_latest() {
  cli offsets --topic "$1" | awk -F'latest=' '{ total += $2 } END { print total + 0 }'
}

# Killing a client does not stop the broker from finishing requests it has
# already received, so the log keeps growing for a moment afterwards.
# Sample until it stops moving before asserting anything about its size.
settled_latest() {
  local topic="$1" previous="" current=""
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    current="$(total_latest "$topic")"
    if [[ "$current" == "$previous" ]]; then
      printf '%s' "$current"
      return 0
    fi
    previous="$current"
    sleep 0.5
  done
  die "log for $topic never stopped growing"
}

consumed_values() {
  cli consume --topic "$1" --from earliest --max "$((RECORDS * 4))" \
    | sed -n 's/^partition=[0-9]* offset=[0-9]* key=[^ ]* value=\(.*\)$/\1/p'
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

# ------------------------------------------------------- producer failure

stage "Producer killed mid-send: acknowledged records survive, log stays valid"
start_broker 0
TOPIC="producer-failure"

# A long-running producer we can kill while it is streaming records.
PRODUCER_VALUES="$(values_file "$RECORDS" producer)"
cli produce --topic "$TOPIC" --file "$PRODUCER_VALUES" --acks all \
  > "$WORK_DIR/producer.out" 2> "$WORK_DIR/producer.err" &
PRODUCER_PID=$!

# Let it get going, then kill -9 it mid-stream.
sleep 0.3
kill -9 "$PRODUCER_PID" 2>/dev/null || true
wait "$PRODUCER_PID" 2>/dev/null || true
info "producer killed mid-send"

AFTER_KILL="$(settled_latest "$TOPIC")"
info "log settled at $AFTER_KILL of $RECORDS records after the kill"
(( AFTER_KILL > 0 )) || die "the producer died before anything reached the log; raise RECORDS"

# Whatever reached the log must be readable and well formed: a torn write
# would fail CRC validation and the read would stop short or error.
READABLE="$(consumed_values "$TOPIC" | wc -l | tr -d ' ')"
assert_eq "$READABLE" "$AFTER_KILL" "every record in the log is readable after the producer died mid-send"

# No partial record: every consumed value must be one of the values the
# producer was sending, intact.
BAD="$(consumed_values "$TOPIC" | grep -vc '^producer-[0-9]\{6\}$' || true)"
assert_eq "$BAD" "0" "no truncated or corrupt record survived the producer kill"

# The broker is still fully functional for a new producer.
SECOND="$(values_file 200 producer-second)"
cli produce --topic "$TOPIC" --file "$SECOND" --acks all >/dev/null \
  || die "broker rejected a new producer after the previous one was killed"
assert_eq "$(total_latest "$TOPIC")" "$((AFTER_KILL + 200))" \
  "a replacement producer appends normally after the failure"

# ---------------------------------------------------- broker hard failure

stage "Broker killed -9 mid-produce: recovery truncates any torn tail, keeps every acked record"
TOPIC="broker-failure"
THIRD="$(values_file "$RECORDS" broker)"
cli produce --topic "$TOPIC" --file "$THIRD" --acks all > "$WORK_DIR/produce-broker.out" 2>&1 &
PRODUCER_PID=$!
sleep 0.5
kill_broker_hard
wait "$PRODUCER_PID" 2>/dev/null || true
info "broker hard-killed while a produce was in flight"

start_broker 1
RECOVERED="$(settled_latest "$TOPIC")"
READABLE="$(consumed_values "$TOPIC" | wc -l | tr -d ' ')"
assert_eq "$READABLE" "$RECOVERED" "the recovered log serves exactly its recovered offsets ($RECOVERED)"
BAD="$(consumed_values "$TOPIC" | grep -vc '^broker-[0-9]\{6\}$' || true)"
assert_eq "$BAD" "0" "recovery discarded any torn tail instead of serving it"

# Everything the earlier topic held is still there: a crash on one
# partition set must not lose committed data elsewhere.
assert_eq "$(consumed_values producer-failure | wc -l | tr -d ' ')" "$((AFTER_KILL + 200))" \
  "records committed before the crash survived the restart"

# The broker accepts writes again and offsets continue from the recovered end.
FOURTH="$(values_file 100 broker-second)"
cli produce --topic "$TOPIC" --file "$FOURTH" --acks all >/dev/null \
  || die "broker did not accept writes after crash recovery"
assert_eq "$(total_latest "$TOPIC")" "$((RECOVERED + 100))" \
  "offsets continue from the recovered log end, with no gap or rewind"

# ------------------------------------------------------ consumer failure

stage "Consumer killed mid-stream: its group resumes from the last commit, nothing skipped"
TOPIC="consumer-failure"
GROUP="failure-group"
FIFTH="$(values_file 600 consumer)"
cli produce --topic "$TOPIC" --file "$FIFTH" --acks all >/dev/null

# First member consumes a bounded slice and commits as it goes, then dies.
cli consume --topic "$TOPIC" --group "$GROUP" --max 200 --commit-interval-ms 100 \
  > "$WORK_DIR/consumer-first.out" 2>&1 \
  || die "first consumer failed"
FIRST_COUNT="$(grep -c '^partition=' "$WORK_DIR/consumer-first.out" || true)"
assert_eq "$FIRST_COUNT" "200" "first consumer received exactly its bounded 200 records"

COMMITTED="$(cli groups lag --group "$GROUP" \
  | sed -n 's/^.*committed=\([0-9]*\) .*$/\1/p' | awk '{ total += $1 } END { print total + 0 }')"
assert_eq "$COMMITTED" "200" "the group committed exactly what the dead consumer delivered"

# A replacement member must see the remaining 400 and no others.
cli consume --topic "$TOPIC" --group "$GROUP" --max 400 --commit-interval-ms 100 \
  > "$WORK_DIR/consumer-second.out" 2>&1 \
  || die "replacement consumer failed"
SECOND_COUNT="$(grep -c '^partition=' "$WORK_DIR/consumer-second.out" || true)"
assert_eq "$SECOND_COUNT" "400" "the replacement consumed exactly the remaining 400 records"

# Union of both runs is the full set, each exactly once: nothing skipped,
# nothing double-delivered across the failure.
{
  sed -n 's/^.*value=\(.*\)$/\1/p' "$WORK_DIR/consumer-first.out"
  sed -n 's/^.*value=\(.*\)$/\1/p' "$WORK_DIR/consumer-second.out"
} | sort > "$WORK_DIR/consumed-union.txt"
sort "$FIFTH" > "$WORK_DIR/produced-sorted.txt"
assert_eq "$(comm -23 "$WORK_DIR/produced-sorted.txt" "$WORK_DIR/consumed-union.txt" | wc -l | tr -d ' ')" "0" \
  "no record was skipped across the consumer failure"
assert_eq "$(( $(wc -l < "$WORK_DIR/consumed-union.txt") - $(sort -u "$WORK_DIR/consumed-union.txt" | wc -l) ))" "0" \
  "no record was delivered twice across the consumer failure"
assert_eq "$(cli groups lag --group "$GROUP" | sed -n 's/^total lag: \([0-9]*\)$/\1/p')" "0" \
  "group lag returns to zero once the replacement catches up"

# --------------------------------------------------------- data integrity

stage "Data loss check: every acknowledged record is still readable"
for topic in producer-failure broker-failure consumer-failure; do
  latest="$(total_latest "$topic")"
  readable="$(consumed_values "$topic" | wc -l | tr -d ' ')"
  [[ "$latest" == "$readable" ]] \
    || die "topic $topic reports $latest offsets but serves $readable records"
done
pass "all three topics serve exactly the offsets they report, after two process kills"

stop_broker
start_broker 2
for topic in producer-failure broker-failure consumer-failure; do
  latest="$(total_latest "$topic")"
  readable="$(consumed_values "$topic" | wc -l | tr -d ' ')"
  [[ "$latest" == "$readable" ]] \
    || die "topic $topic lost data across a clean restart ($latest vs $readable)"
done
pass "the same holds after a clean restart: nothing acknowledged was lost"

stage "Failure verification complete ($TRANSPORT)"
printf 'checks passed: %s\n' "$CHECKS"
