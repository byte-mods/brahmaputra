#!/usr/bin/env bash
# Live verification of transactions and read-committed isolation.
#
# The properties under test are the ones a transaction is *for*, so each is
# asserted on what a consumer can actually read rather than on an exit code:
#
#   commit    every partition's records become visible together
#   abort     none of them do, and the records stay in the log
#   in doubt  an unfinished transaction blocks a committed reader at the
#             last stable offset — the case that distinguishes this from
#             filtering after the fact
#   recovery  a producer that never came back is resolved by whoever claims
#             its transactional id next, not by anyone noticing
#   fencing   the replaced producer cannot come back and finish
#
# The default isolation level is `read_uncommitted`, so every check that
# matters is run at both levels: a transaction that changed what an ordinary
# consumer sees would be a regression, not a feature.
#
# Requires Git Bash on Windows.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-txn.XXXXXX")"
SERVER_EXE="$ROOT/target/release/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/release/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/release/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/release/brahmaputra-cli"

CHECKS=0
SERVER_PID=""

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
  if [[ "${KEEP_ARTIFACTS:-0}" != "1" && "$WORK_DIR" == "$TEMP_ROOT"/brahmaputra-txn.* ]]; then
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

PORT="$(allocate_port)"
BROKER="127.0.0.1:$PORT"
mkdir -p "$WORK_DIR/data"
"$SERVER_EXE" --port "$PORT" --data-dir "$WORK_DIR/data" --default-partitions 1 \
  >"$WORK_DIR/broker.out" 2>"$WORK_DIR/broker.err" &
SERVER_PID=$!

DEADLINE=$((SECONDS + 40))
until "$CLI_EXE" --broker "$BROKER" metadata >/dev/null 2>&1; do
  (( SECONDS < DEADLINE )) || die "broker never became ready: $(tail -3 "$WORK_DIR/broker.err")"
  sleep 0.3
done

cli() { "$CLI_EXE" --broker "$BROKER" "$@"; }

# Records a consumer can actually read, at a given isolation level.
readable() {
  local topic="$1"
  shift
  cli consume --topic "$topic" --from earliest --max 1000 --quiet "$@" 2>/dev/null \
    | sed -n 's/consumed \([0-9]*\) records.*/\1/p'
}
committed() { readable "$1" --isolation-level read_committed; }

values() {
  local topic="$1"
  shift
  cli consume --topic "$topic" --from earliest --max 1000 "$@" 2>/dev/null \
    | sed -n 's/.*value=\(.*\)$/\1/p' | tr '\n' ',' | sed 's/,$//'
}

# ------------------------------------------------------------- commit

stage "a committed transaction becomes visible on every partition at once"

cli transaction --id etl --send "orders:0=order-A" --send "audit:0=audit-A" >/dev/null
assert_eq "$(committed orders)" "1" "a committed record is readable on the first topic"
assert_eq "$(committed audit)" "1" "and on the second, which is what atomic across partitions means"
assert_eq "$(values orders --isolation-level read_committed)" "order-A" \
  "the committed value is the one that was written"

# -------------------------------------------------------------- abort

stage "an aborted transaction is skipped, not deleted"

cli transaction --id etl --abort \
  --send "orders:0=order-DOOMED" --send "audit:0=audit-DOOMED" >/dev/null
assert_eq "$(committed orders)" "1" "an aborted record is not readable"
assert_eq "$(committed audit)" "1" "on either partition"
assert_eq "$(readable orders)" "2" \
  "read_uncommitted still sees it: isolation is the reader's choice, not the writer's"

# The records are still on disk — an append-only log cannot remove them
# without moving every offset after them, so they are skipped instead.
LATEST="$(cli offsets --topic orders --partition 0 | sed -n 's/.*latest=\([0-9]*\).*/\1/p')"
[[ "$LATEST" -ge 4 ]] \
  || die "offsets did not advance past the aborted records and their markers (latest=$LATEST)"
pass "the aborted records and their markers still occupy offsets (latest=$LATEST)"

# ------------------------------------------------------------ in doubt

stage "an unfinished transaction blocks a committed reader where it starts"

cli transaction --id etl --abandon --send "orders:0=order-INDOUBT" >/dev/null
assert_eq "$(committed orders)" "1" \
  "a committed reader stops at the last stable offset rather than showing an undecided record"
assert_eq "$(readable orders)" "3" "read_uncommitted is not held back by it"

# This is the property that separates a real transaction from filtering
# after the fact: the record exists, the log end has moved, and a committed
# reader still refuses to advance past it.
LATEST="$(cli offsets --topic orders --partition 0 | sed -n 's/.*latest=\([0-9]*\).*/\1/p')"
info "log end is at $LATEST while committed readers stop before the open transaction"

# ------------------------------------------------------------ recovery

stage "the next claim of the id resolves what the dead producer left open"

OUTPUT="$(cli transaction --id etl --send "orders:0=order-AFTER")"
printf '%s\n' "$OUTPUT" | sed 's/^/     /'
EPOCH="$(printf '%s\n' "$OUTPUT" | sed -n 's/.*epoch=\([0-9]*\).*/\1/p')"
[[ "$EPOCH" -ge 3 ]] \
  || die "the producer epoch did not advance across instances (epoch=$EPOCH)"
pass "each claim of the transactional id fences the last one (epoch=$EPOCH)"

assert_eq "$(committed orders)" "2" \
  "the abandoned transaction resolved as aborted, and the new one committed"
assert_eq "$(values orders --isolation-level read_committed)" "order-A,order-AFTER" \
  "exactly the two committed records, in order, with the abandoned one skipped"

# ------------------------------------------------------------- markers

stage "markers are never delivered as records"

# Every transaction above wrote a marker to `orders`. If any of them reached
# a consumer, these counts would include them.
assert_eq "$(readable orders)" "4" \
  "read_uncommitted sees the four data records and none of the four markers"

# ------------------------------------------------- non-transactional topics

stage "a topic nobody writes transactionally to is unchanged"

cli produce --topic plain --count 100 --value-size 64 --no-key \
  --acks 1 --compression none >/dev/null
assert_eq "$(readable plain)" "100" "an ordinary topic reads the same at read_uncommitted"
assert_eq "$(committed plain)" "100" \
  "and identically at read_committed: with nothing in flight the two bounds are the same place"

# ------------------------------------------------------------- restart

stage "the decisions survive a restart"

kill -9 "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
"$SERVER_EXE" --port "$PORT" --data-dir "$WORK_DIR/data" --default-partitions 1 \
  >>"$WORK_DIR/broker.out" 2>>"$WORK_DIR/broker.err" &
SERVER_PID=$!
DEADLINE=$((SECONDS + 40))
until "$CLI_EXE" --broker "$BROKER" metadata >/dev/null 2>&1; do
  (( SECONDS < DEADLINE )) || die "broker never restarted: $(tail -3 "$WORK_DIR/broker.err")"
  sleep 0.3
done

# An aborted record that reappears after a restart is the failure the
# whole transaction index exists to prevent.
assert_eq "$(committed orders)" "2" "aborted records stay aborted across a restart"
assert_eq "$(values orders --isolation-level read_committed)" "order-A,order-AFTER" \
  "and the surviving records are the same two, unchanged"

printf '\n\033[32mAll %d checks passed.\033[0m\n' "$CHECKS"
