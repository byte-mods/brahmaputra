#!/usr/bin/env bash
# Live retention verification (DESIGN.md §4.4, Blueprint 01): time-based and
# size-based segment deletion on a running broker, the log start offset that
# results, and how consumers behave once their position falls off the log.
#
# Runs standalone brokers with tiny segments so retention is observable in
# seconds. Requires Git Bash on Windows.

set -Eeuo pipefail

SEGMENT_BYTES="${SEGMENT_BYTES:-4096}"
RETENTION_MS="${RETENTION_MS:-4000}"
RETENTION_BYTES="${RETENTION_BYTES:-16384}"
CHECK_INTERVAL_MS="${CHECK_INTERVAL_MS:-200}"
RECORD_SIZE="${RECORD_SIZE:-512}"
RECORD_COUNT="${RECORD_COUNT:-200}"
WAIT_SECONDS="${WAIT_SECONDS:-60}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-retention.XXXXXX")"
SERVER_EXE="$ROOT/target/debug/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/debug/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/debug/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/debug/brahmaputra-cli"

CHECKS=0
SERVER_PID=""

stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
pass() { CHECKS=$((CHECKS + 1)); printf '\033[32mPASS: %s\033[0m\n' "$1"; }
die() { printf '\n\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

assert_eq() {
  local actual="$1" expected="$2" description="$3"
  [[ "$actual" == "$expected" ]] || die "$description (expected=$expected actual=$actual)"
  pass "$description"
}

assert_true() {
  local description="$1"
  shift
  "$@" || die "$description"
  pass "$description"
}

cleanup() {
  stop_broker || true
  if [[ -d "$WORK_DIR" && "$WORK_DIR" == "$TEMP_ROOT"/brahmaputra-retention.* ]]; then
    rm -rf "$WORK_DIR"
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
  local name="$1"
  shift
  PORT="$(allocate_port)"
  BROKER="127.0.0.1:$PORT"
  DATA_DIR="$WORK_DIR/$name"
  mkdir -p "$DATA_DIR"
  "$SERVER_EXE" --port "$PORT" --data-dir "$DATA_DIR" --default-partitions 1 \
    --segment-bytes "$SEGMENT_BYTES" \
    --retention-check-interval-ms "$CHECK_INTERVAL_MS" \
    "$@" > "$WORK_DIR/$name.out" 2> "$WORK_DIR/$name.err" &
  SERVER_PID=$!
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if "$CLI_EXE" --broker "$BROKER" metadata >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  die "broker $name never became ready (see $WORK_DIR/$name.err)"
}

stop_broker() {
  [[ -n "$SERVER_PID" ]] || return 0
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

# Random payloads: the producer compresses batches with LZ4, so a constant
# payload would shrink to nothing and never roll a segment.
random_values_file() {
  local count="$1" path="$WORK_DIR/values-$count.txt"
  [[ -f "$path" ]] && { printf '%s' "$path"; return; }
  perl -e '
    my ($count, $size) = @ARGV;
    my @alphabet = ("A".."Z", "a".."z", "0".."9");
    for my $i (1 .. $count) {
      print join("", map { $alphabet[int(rand(@alphabet))] } 1 .. $size), "\n";
    }
  ' "$count" "$RECORD_SIZE" > "$path"
  printf '%s' "$path"
}

produce() {
  local topic="$1" count="$2" path
  path="$(random_values_file "$count")"
  "$CLI_EXE" --broker "$BROKER" produce --topic "$topic" --file "$path" >/dev/null
}

earliest_offset() {
  "$CLI_EXE" --broker "$BROKER" offsets --topic "$1" \
    | sed -n 's/^.*earliest=\([0-9-]*\) .*$/\1/p' | head -1
}

latest_offset() {
  "$CLI_EXE" --broker "$BROKER" offsets --topic "$1" \
    | sed -n 's/^.*latest=\([0-9-]*\).*$/\1/p' | head -1
}

segment_count() {
  find "$DATA_DIR/$1-0" -name '*.log' 2>/dev/null | wc -l | tr -d ' '
}

log_bytes() {
  find "$DATA_DIR/$1-0" -name '*.log' -printf '%s\n' 2>/dev/null \
    | awk '{ total += $1 } END { print total + 0 }'
}

wait_until() {
  local description="$1" timeout_seconds="$2"
  shift 2
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if "$@"; then return 0; fi
    sleep 0.25
  done
  die "timed out waiting for $description"
}

# ---------------------------------------------------------------- time-based

stage "Time-based retention deletes sealed segments past --retention-ms"
start_broker time-retention --retention-ms "$RETENTION_MS"
TOPIC="retention-time"
produce "$TOPIC" "$RECORD_COUNT"

SEGMENTS_BEFORE="$(segment_count "$TOPIC")"
EARLIEST_BEFORE="$(earliest_offset "$TOPIC")"
LATEST="$(latest_offset "$TOPIC")"
assert_true "produced $RECORD_COUNT records across several segments (got $SEGMENTS_BEFORE)" \
  test "$SEGMENTS_BEFORE" -gt 2
assert_eq "$EARLIEST_BEFORE" "0" "log starts at offset 0 before retention runs"
assert_eq "$LATEST" "$RECORD_COUNT" "log end offset equals the produced count"

# Nothing ages out while the records are younger than the retention window.
sleep 1
assert_eq "$(earliest_offset "$TOPIC")" "0" "no segment is deleted before its records age out"

fewer_segments_than() {
  (( $(segment_count "$TOPIC") < $1 ))
}
wait_until "aged segments to be deleted" "$WAIT_SECONDS" fewer_segments_than "$SEGMENTS_BEFORE"

SEGMENTS_AFTER="$(segment_count "$TOPIC")"
EARLIEST_AFTER="$(earliest_offset "$TOPIC")"
assert_true "segment count dropped ($SEGMENTS_BEFORE -> $SEGMENTS_AFTER)" \
  test "$SEGMENTS_AFTER" -lt "$SEGMENTS_BEFORE"
assert_true "log start offset advanced past deleted data (0 -> $EARLIEST_AFTER)" \
  test "$EARLIEST_AFTER" -gt 0
assert_eq "$(latest_offset "$TOPIC")" "$LATEST" "retention never moves the log end offset"

# Retention only ever deletes *sealed* segments, so what survives is
# whatever the active segment still holds — often nothing, sometimes the
# last few records. Either is correct; what must hold is that the log
# start never passes the log end.
SURVIVING=$((LATEST - EARLIEST_AFTER))
assert_true "the log start stopped at or before the log end ($EARLIEST_AFTER of $LATEST, $SURVIVING records left in the active segment)" \
  test "$EARLIEST_AFTER" -le "$LATEST"

produce "$TOPIC" 20
assert_eq "$(latest_offset "$TOPIC")" "$((RECORD_COUNT + 20))" "the log keeps accepting writes after retention emptied it"
FIRST_OFFSET="$("$CLI_EXE" --broker "$BROKER" consume --topic "$TOPIC" --from earliest --max 1 \
  | sed -n 's/^partition=[0-9]* offset=\([0-9]*\).*$/\1/p')"
assert_eq "$FIRST_OFFSET" "$EARLIEST_AFTER" "reading from earliest starts at the surviving log start"
CONSUMED="$("$CLI_EXE" --broker "$BROKER" consume --topic "$TOPIC" --from earliest --max 200 \
  | grep -c '^partition=' || true)"
assert_eq "$CONSUMED" "$((SURVIVING + 20))" "every surviving record plus every new one is readable"
stop_broker

# ---------------------------------------------------------------- size-based

stage "Size-based retention keeps the log under --retention-bytes"
start_broker size-retention --retention-bytes "$RETENTION_BYTES"
TOPIC="retention-size"
produce "$TOPIC" "$RECORD_COUNT"

within_byte_budget() {
  (( $(log_bytes "$TOPIC") <= RETENTION_BYTES + SEGMENT_BYTES ))
}
wait_until "log to be trimmed under the byte budget" "$WAIT_SECONDS" within_byte_budget

BYTES_AFTER="$(log_bytes "$TOPIC")"
EARLIEST_SIZE="$(earliest_offset "$TOPIC")"
assert_true "log trimmed to $BYTES_AFTER bytes (budget $RETENTION_BYTES + one active segment)" \
  test "$BYTES_AFTER" -le "$((RETENTION_BYTES + SEGMENT_BYTES))"
assert_true "log start offset advanced under size retention (0 -> $EARLIEST_SIZE)" \
  test "$EARLIEST_SIZE" -gt 0
assert_eq "$(latest_offset "$TOPIC")" "$RECORD_COUNT" "size retention never moves the log end offset"

# Retention survives restart: recovery must adopt the surviving log start.
stop_broker
start_broker size-retention --retention-bytes "$RETENTION_BYTES"
assert_eq "$(earliest_offset "$TOPIC")" "$EARLIEST_SIZE" "log start offset survives restart"
assert_eq "$(latest_offset "$TOPIC")" "$RECORD_COUNT" "log end offset survives restart"
stop_broker

# ------------------------------------------------------------------- default

stage "A group whose committed offset fell off the log resumes at the new start"
start_broker group-retention --retention-ms "$RETENTION_MS"
TOPIC="retention-group"
produce "$TOPIC" 40
# Commit a position inside the data that is about to be deleted.
"$CLI_EXE" --broker "$BROKER" consume --topic "$TOPIC" --group survivors --max 5 \
  --commit-interval-ms 200 >/dev/null
COMMITTED="$("$CLI_EXE" --broker "$BROKER" groups lag --group survivors \
  | sed -n 's/^.*committed=\([0-9]*\) .*$/\1/p' | head -1)"
assert_eq "$COMMITTED" "5" "the group committed offset 5 before retention ran"

start_offset_at_least() {
  (( $(earliest_offset "$TOPIC") >= $1 ))
}
wait_until "the committed offset to fall off the log" "$WAIT_SECONDS" start_offset_at_least 6
NEW_START="$(earliest_offset "$TOPIC")"
assert_true "log start advanced past the committed offset (5 -> $NEW_START)" \
  test "$NEW_START" -gt 5

produce "$TOPIC" 20
RESUMED="$("$CLI_EXE" --broker "$BROKER" consume --topic "$TOPIC" --group survivors --max 20 \
  --commit-interval-ms 200)"
RESUMED_FIRST="$(printf '%s\n' "$RESUMED" | sed -n 's/^partition=[0-9]* offset=\([0-9]*\).*$/\1/p' | head -1)"
RESUMED_COUNT="$(printf '%s\n' "$RESUMED" | grep -c '^partition=' || true)"
assert_eq "$RESUMED_COUNT" "20" "the group read the surviving records instead of failing"
assert_true "the group restarted at the new log start ($RESUMED_FIRST >= $NEW_START)" \
  test "$RESUMED_FIRST" -ge "$NEW_START"
stop_broker

stage "Retention is off by default"
start_broker no-retention
TOPIC="retention-off"
produce "$TOPIC" "$RECORD_COUNT"
sleep 2
assert_eq "$(earliest_offset "$TOPIC")" "0" "nothing is deleted when no retention flag is set"
stop_broker

stage "Retention verification complete"
printf 'checks passed: %s\n' "$CHECKS"
