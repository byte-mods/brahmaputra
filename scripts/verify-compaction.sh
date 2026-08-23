#!/usr/bin/env bash
# Live verification of log compaction: what survives, what is deleted, and
# what a consumer sees while it happens.
#
# Compaction is the one background process that *removes* records a producer
# successfully wrote, so every check here is made against what a consumer can
# actually read rather than against file sizes or an exit code:
#
#   tombstone   a null value reaches the consumer as one, distinct from empty
#   supersede   only the newest record for a key survives
#   offsets     nothing is renumbered, and the earliest offset is still readable
#   horizon     the tombstone goes too, once delete.retention.ms has passed
#   restart     the result survives a kill -9 and a reopen
#
# Requires Git Bash on Windows.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-compact.XXXXXX")"
SERVER_EXE="$ROOT/target/release/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/release/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/release/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/release/brahmaputra-cli"

CHECKS=0
SERVER_PID=""
PORT=""
BROKER=""

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
  [[ -n "$SERVER_PID" ]] && kill -9 "$SERVER_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  if [[ "${KEEP_ARTIFACTS:-0}" != "1" && "$WORK_DIR" == "$TEMP_ROOT"/brahmaputra-compact.* ]]; then
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

stop_broker() {
  [[ -n "$SERVER_PID" ]] || return 0
  kill -9 "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

# Start a broker on its own data directory with whatever compaction policy
# this stage is about. Standalone mode has no controller, so per-topic
# configuration is not reachable — these are the broker-wide defaults every
# topic inherits, which is the same code path a topic config resolves into.
start_broker() {
  local name="$1"
  shift
  # Stop whatever the previous stage left running. Without this each stage
  # leaks a broker, and the cleanup trap's `wait` never returns because
  # those children are still alive.
  stop_broker
  PORT="$(allocate_port)"
  BROKER="127.0.0.1:$PORT"
  mkdir -p "$WORK_DIR/$name"
  "$SERVER_EXE" --port "$PORT" --data-dir "$WORK_DIR/$name" --default-partitions 1 \
    --retention-check-interval-ms 300 "$@" \
    >>"$WORK_DIR/$name.out" 2>>"$WORK_DIR/$name.err" &
  SERVER_PID=$!
  local deadline=$((SECONDS + 40))
  until "$CLI_EXE" --broker "$BROKER" metadata >/dev/null 2>&1; do
    (( SECONDS < deadline )) || die "broker never became ready: $(tail -3 "$WORK_DIR/$name.err")"
    sleep 0.3
  done
}

# Restart on the same directory, which is what makes durability checkable.
restart_broker() {
  local name="$1"
  shift
  stop_broker
  "$SERVER_EXE" --port "$PORT" --data-dir "$WORK_DIR/$name" --default-partitions 1 \
    --retention-check-interval-ms 300 "$@" \
    >>"$WORK_DIR/$name.out" 2>>"$WORK_DIR/$name.err" &
  SERVER_PID=$!
  local deadline=$((SECONDS + 40))
  until "$CLI_EXE" --broker "$BROKER" metadata >/dev/null 2>&1; do
    (( SECONDS < deadline )) || die "broker never restarted: $(tail -3 "$WORK_DIR/$name.err")"
    sleep 0.3
  done
}

cli() { "$CLI_EXE" --broker "$BROKER" "$@"; }

# `key=value` for every record a consumer can read, in offset order.
pairs() {
  cli consume --topic "$1" --from earliest --max 2000 2>/dev/null \
    | sed -n 's/.*key=\([^ ]*\) value=\(.*\)$/\1=\2/p' | tr '\n' ',' | sed 's/,$//'
}

values_for() {
  cli consume --topic "$1" --from earliest --max 2000 2>/dev/null \
    | sed -n "s/.*key=$2 value=\\(.*\\)\$/\\1/p" | tr '\n' ',' | sed 's/,$//'
}

offsets_of() {
  cli consume --topic "$1" --from earliest --max 2000 2>/dev/null \
    | sed -n 's/.*offset=\([0-9]*\) .*/\1/p' | tr '\n' ',' | sed 's/,$//'
}

count() {
  cli consume --topic "$1" --from earliest --max 2000 --quiet 2>/dev/null \
    | sed -n 's/consumed \([0-9]*\) records.*/\1/p'
}

# ------------------------------------------------------ tombstone on the wire

stage "a tombstone is a record with a null value, distinct from an empty one"

start_broker shapes

cli produce --topic shapes --key k-set --value hello >/dev/null
cli produce --topic shapes --key k-empty --value "" >/dev/null
cli produce --topic shapes --key k-gone --tombstone >/dev/null

assert_eq "$(values_for shapes k-set)" "hello" "an ordinary value round-trips"
assert_eq "$(values_for shapes k-empty)" "" "an empty value round-trips as empty"
assert_eq "$(values_for shapes k-gone)" "null" \
  "a tombstone reaches the consumer as a null value, so the deletion is an event it can see"
assert_eq "$(count shapes)" "3" \
  "and all three are ordinary records on a topic that is not compacted"

# A tombstone must survive a restart like any other record: it is the
# deletion, and losing it would resurrect the key.
restart_broker shapes
assert_eq "$(values_for shapes k-gone)" "null" "a tombstone is durable"

# ------------------------------------------------------------ superseding

stage "only the newest record for a key survives compaction"

start_broker ledger --cleanup-policy compact --segment-bytes 1024 \
  --min-cleanable-dirty-ratio 0.0

# 80 records over 2 keys. Compaction runs while they arrive, which is the
# realistic case — the assertion is about what is left, not about catching
# it in the act.
WRITTEN=80
for i in $(seq 1 40); do
  cli produce --topic ledger --key account-a --value "a$i" >/dev/null
  cli produce --topic ledger --key account-b --value "b$i" >/dev/null
done

DEADLINE=$((SECONDS + 30))
while (( SECONDS < DEADLINE )); do
  [[ "$(count ledger)" -lt 20 ]] && break
  sleep 0.3
done
AFTER="$(count ledger)"
[[ "$AFTER" -lt 20 ]] \
  || die "compaction did not remove the bulk of the superseded records ($WRITTEN -> $AFTER)"
pass "$WRITTEN records over 2 keys compacted down to $AFTER"

# The active segment is never compacted, so the tail keeps a few superseded
# records. What must be true is that the *newest* value per key is there.
assert_eq "$(values_for ledger account-a | sed 's/.*,//')" "a40" \
  "the surviving value for the first key is the newest one"
assert_eq "$(values_for ledger account-b | sed 's/.*,//')" "b40" \
  "and for the second"

# ------------------------------------------------------- offsets preserved

stage "compaction leaves gaps rather than renumbering anything"

OFFSETS="$(offsets_of ledger)"
FIRST="${OFFSETS%%,*}"
LAST="${OFFSETS##*,}"
assert_eq "$LAST" "$((WRITTEN - 1))" \
  "the newest record still sits at the offset it was written at"
info "the oldest surviving record is at offset $FIRST"

# Reading from the beginning must return the oldest record that still
# exists. Compaction removing the record at offset 0 does not make offset 0
# out of range — only retention and DeleteRecords do that.
assert_eq "$(cli consume --topic ledger --from earliest --max 1 --quiet 2>/dev/null \
  | sed -n 's/consumed \([0-9]*\) records.*/\1/p')" "1" \
  "reading from the earliest offset still works after the record at it was removed"
assert_eq "$(cli offsets --topic ledger --partition 0 \
  | sed -n 's/.*earliest=\([0-9]*\).*/\1/p')" "0" \
  "and the reported log start offset has not moved"

# --------------------------------------------------------------- durability

stage "a compacted log reopens as exactly what it was"

BEFORE_KILL="$(pairs ledger)"
restart_broker ledger --cleanup-policy compact --segment-bytes 1024 \
  --min-cleanable-dirty-ratio 0.0
assert_eq "$(pairs ledger)" "$BEFORE_KILL" \
  "a kill -9 and a reopen change nothing about what a consumer reads"

# ---------------------------------------------------------- tombstone deletes

stage "a tombstone deletes its key, and then itself"

start_broker registry --cleanup-policy compact --segment-bytes 1024 \
  --min-cleanable-dirty-ratio 0.0 --delete-retention-ms 600000

for i in $(seq 1 20); do
  cli produce --topic registry --key doomed --value "d$i" >/dev/null
  cli produce --topic registry --key kept --value "k$i" >/dev/null
done
cli produce --topic registry --key doomed --tombstone >/dev/null
# Enough afterwards that the tombstone is not stranded in the active
# segment, which compaction never touches.
for i in $(seq 21 45); do
  cli produce --topic registry --key kept --value "k$i" >/dev/null
done

# Within the grace period the deletion is all that is left of the key — a
# consumer that has not caught up can still see that it happened.
DEADLINE=$((SECONDS + 30))
while (( SECONDS < DEADLINE )); do
  [[ "$(values_for registry doomed)" == "null" ]] && break
  sleep 0.3
done
assert_eq "$(values_for registry doomed)" "null" \
  "every value for the deleted key is gone, and the tombstone remains to say so"

# Past the grace period the tombstone goes too, and the key stops occupying
# the log at all. New writes are what give the cleaner something to do — as
# in Kafka, a tombstone is dropped while cleaning the range it sits in.
restart_broker registry --cleanup-policy compact --segment-bytes 1024 \
  --min-cleanable-dirty-ratio 0.0 --delete-retention-ms 0
for i in $(seq 46 70); do
  cli produce --topic registry --key kept --value "k$i" >/dev/null
done
DEADLINE=$((SECONDS + 30))
while (( SECONDS < DEADLINE )); do
  [[ -z "$(values_for registry doomed)" ]] && break
  sleep 0.3
done
assert_eq "$(values_for registry doomed)" "" \
  "once delete.retention.ms has passed, the tombstone is removed and the key is gone"
assert_eq "$(values_for registry kept | sed 's/.*,//')" "k70" \
  "and the key that was never deleted still holds its newest value"

# ------------------------------------------------------------ config echo

stage "the compaction settings are reported back"

CONFIGS="$(cli describe-configs --type topic --name registry 2>/dev/null)"
for setting in delete.retention.ms min.cleanable.dirty.ratio min.compaction.lag.ms max.compaction.lag.ms; do
  grep -q "$setting" <<<"$CONFIGS" || die "describe-configs does not report $setting"
done
pass "describe-configs reports every compaction setting"
grep -qE '^delete\.retention\.ms +0 ' <<<"$CONFIGS" \
  || die "describe-configs does not reflect the delete.retention.ms in force: $CONFIGS"
pass "and reflects the value actually in force"

printf '\n\033[32mAll %d checks passed.\033[0m\n' "$CHECKS"
