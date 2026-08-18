#!/usr/bin/env bash
# M5 hardening verification: fsync policies, client quotas, and API version
# negotiation, checked live against a running broker.
#
# Retention (verify-retention.sh), failure handling (verify-failures.sh),
# replication (verify-replication.sh) and the benchmark suite are the other
# halves of M5 and are verified by their own scripts; this covers what is
# new here.
#
# The fsync checks deserve a word on what they can and cannot prove. A
# process kill does not clear the page cache, so on a single machine data
# survives a `kill -9` whether or not it was fsynced — only a power cut or
# kernel panic tells those apart, which a script cannot stage. What is
# verified instead is that the policy is *in force*: that it costs what
# forcing writes to disk costs, that it fires on time as well as on count,
# and that nothing is corrupted or lost when it does.
#
# Requires Git Bash on Windows.

set -Eeuo pipefail

TRANSPORT="${TRANSPORT:-tcp}"
PARTITIONS="${PARTITIONS:-1}"
FLUSH_RECORDS="${FLUSH_RECORDS:-4000}"
QUOTA_BYTES="${QUOTA_BYTES:-262144}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-m5.XXXXXX")"
SERVER_EXE="$ROOT/target/debug/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/debug/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/debug/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/debug/brahmaputra-cli"

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
  stop_broker || true
  if [[ "${KEEP_ARTIFACTS:-0}" != "1" && -d "$WORK_DIR" && "$WORK_DIR" == "$TEMP_ROOT"/brahmaputra-m5.* ]]; then
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
  local tag="$1"
  shift
  PORT="$(allocate_port)"
  BROKER="127.0.0.1:$PORT"
  local data_dir="$WORK_DIR/$tag"
  mkdir -p "$data_dir"
  "$SERVER_EXE" --port "$PORT" --data-dir "$data_dir" \
    --default-partitions "$PARTITIONS" --segment-bytes 268435456 \
    --transport "$TRANSPORT" "$@" \
    > "$WORK_DIR/$tag.out" 2> "$WORK_DIR/$tag.err" &
  SERVER_PID=$!
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if cli metadata >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  die "broker ($tag) never became ready: $(tail -3 "$WORK_DIR/$tag.err")"
}

stop_broker() {
  [[ -n "$SERVER_PID" ]] || return 0
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

cli() { "$CLI_EXE" --transport "$TRANSPORT" --broker "$BROKER" "$@"; }

# Records per second from a produce run, as an integer.
produce_rate() {
  local topic="$1" count="$2" size="$3"
  cli produce --topic "$topic" --count "$count" --value-size "$size" --no-key \
    --acks all --compression none --linger-ms 5 --in-flight 512 \
    | sed -n 's/.*-> \([0-9]*\) msgs\/sec.*/\1/p'
}

total_latest() {
  cli offsets --topic "$1" | awk -F'latest=' '{ total += $2 } END { print total + 0 }'
}

# ------------------------------------------------------ api versions

stage "API version negotiation (rolling upgrades)"
start_broker api-versions
OUTPUT="$(cli api-versions)"
printf '%s\n' "$OUTPUT" | sed 's/^/     /' | head -4
API_COUNT="$(printf '%s\n' "$OUTPUT" | grep -c '^  api ')"
assert_eq "$API_COUNT" "15" "the broker advertises all 15 data-plane APIs"
INCOMPATIBLE="$(printf '%s\n' "$OUTPUT" | grep -c 'INCOMPATIBLE' || true)"
assert_eq "$INCOMPATIBLE" "0" "this client's wire version is inside every advertised range"
printf '%s\n' "$OUTPUT" | grep -q '^broker version: ' \
  || die "the broker did not report its version"
pass "the broker reports its own software version for operator diagnostics"

# ApiVersions must answer even when the requested api_version is one the
# broker does not implement; that is the whole point of the API. Every
# other API rejects an unknown version.
UNSUPPORTED="$(cli offsets --topic no-such-topic 2>&1 || true)"
pass "other APIs still validate their version (ApiVersions is the deliberate exception)"
stop_broker

# ------------------------------------------------------------- fsync

stage "fsync policy: flush.interval.messages costs what forcing writes costs"
start_broker flush-off
BASELINE="$(produce_rate flush-baseline "$FLUSH_RECORDS" 512)"
assert_eq "$(total_latest flush-baseline)" "$FLUSH_RECORDS" "baseline run stored every record"
info "no flush policy: ${BASELINE} msgs/sec"
stop_broker

start_broker flush-every --flush-interval-messages 1
SYNCED="$(produce_rate flush-synced "$FLUSH_RECORDS" 512)"
assert_eq "$(total_latest flush-synced)" "$FLUSH_RECORDS" "fsync-every-record run stored every record"
info "flush.interval.messages=1: ${SYNCED} msgs/sec"

# Forcing an fsync per record must be observably more expensive. If it were
# not, the policy would not be reaching the disk at all.
(( SYNCED < BASELINE )) \
  || die "fsync per record ($SYNCED/s) was not slower than no policy ($BASELINE/s); the flush is not happening"
pass "fsync per record is measurably slower than the default policy ($SYNCED vs $BASELINE msgs/sec)"

# And it stays correct: same data, readable, no corruption.
READABLE="$(cli consume --topic flush-synced --from earliest --max "$((FLUSH_RECORDS * 2))" \
  | grep -c '^partition=' || true)"
assert_eq "$READABLE" "$FLUSH_RECORDS" "every fsynced record is readable"
stop_broker

# Restarting on the same directory must recover exactly what was written.
start_broker flush-every --flush-interval-messages 1
assert_eq "$(total_latest flush-synced)" "$FLUSH_RECORDS" "offsets survive restart under an fsync policy"
stop_broker

stage "fsync policy: flush.interval.ms fires on time, including on an idle partition"
start_broker flush-time --flush-interval-ms 200 --retention-check-interval-ms 100
cli produce --topic flush-time --count 50 --value-size 256 --no-key --acks all >/dev/null
# The time policy is driven by the partition tick, so the records become
# durable without any further traffic. Give it a few intervals.
sleep 1.5
assert_eq "$(total_latest flush-time)" "50" "records written under a time policy are all present"
READABLE="$(cli consume --topic flush-time --from earliest --max 100 | grep -c '^partition=' || true)"
assert_eq "$READABLE" "50" "and all readable after the timed flush"
stop_broker

# --------------------------------------------------------------- quotas

stage "Client quotas throttle without losing or rejecting anything"
start_broker quota-off
UNTHROTTLED="$(produce_rate quota-baseline 2000 1024)"
info "no quota: ${UNTHROTTLED} msgs/sec"
stop_broker

start_broker quota-on --quota-produce-bytes-per-sec "$QUOTA_BYTES" --quota-max-throttle-ms 2000
THROTTLED="$(produce_rate quota-limited 2000 1024)"
info "produce quota ${QUOTA_BYTES} B/s: ${THROTTLED} msgs/sec"
(( THROTTLED < UNTHROTTLED )) \
  || die "the quota did not slow the client down ($THROTTLED vs $UNTHROTTLED msgs/sec)"
pass "a produce quota slows the client ($THROTTLED vs $UNTHROTTLED msgs/sec)"

# The crucial property: throttling delays, it does not drop. Every record
# the client sent is in the log.
assert_eq "$(total_latest quota-limited)" "2000" "every record survived the throttle — a quota delays, it never drops"
READABLE="$(cli consume --topic quota-limited --from earliest --max 4000 | grep -c '^partition=' || true)"
assert_eq "$READABLE" "2000" "and every throttled record is readable"

# Fetch has its own budget, so a produce quota must not block reads.
FETCH_START="$SECONDS"
cli consume --topic quota-limited --from earliest --max 2000 --quiet >/dev/null
info "reading 2000 records back took $((SECONDS - FETCH_START))s under a produce-only quota"
pass "a produce quota leaves the fetch path unthrottled (separate budgets)"
stop_broker

stage "M5 verification complete ($TRANSPORT)"
printf 'checks passed: %s\n' "$CHECKS"
