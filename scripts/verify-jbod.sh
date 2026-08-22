#!/usr/bin/env bash
# Live verification of JBOD: several log directories per broker, and what
# happens when one of them fails.
#
# The claim being tested is a blast-radius claim, so every assertion is
# about what survives:
#
#   placement   partitions spread evenly, and the mapping is rebuilt from
#               the disks themselves across a restart
#   isolation   a disk that fails at runtime takes only its own partitions;
#               the others keep serving reads *and writes*
#   diagnosis   the failed disk is reported as failed, with a reason, and
#               with the partitions that were on it
#   honesty     a disk already broken at startup makes its partitions
#               unavailable rather than silently re-creating them empty
#
# Simulating a disk failure without a disk: the health probe writes a small
# file into each directory, so occupying that path with a *directory* makes
# the write fail exactly as a read-only or dead filesystem would — and does
# it without touching the segment files the broker holds open, which is what
# makes this work on Windows too.
#
# Requires Git Bash on Windows.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-jbod.XXXXXX")"
SERVER_EXE="$ROOT/target/release/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/release/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/release/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/release/brahmaputra-cli"

# The broker probes each directory on this cadence; give it two.
PROBE_GRACE="${PROBE_GRACE:-12}"
DISKS=3
PARTITIONS=6

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
  if [[ "${KEEP_ARTIFACTS:-0}" != "1" && "$WORK_DIR" == "$TEMP_ROOT"/brahmaputra-jbod.* ]]; then
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
DISK_ARGS=()
for index in $(seq 1 "$DISKS"); do
  mkdir -p "$WORK_DIR/disk$index"
  DISK_ARGS+=(--data-dir "$WORK_DIR/disk$index")
done

start_broker() {
  "$SERVER_EXE" --port "$PORT" --default-partitions "$PARTITIONS" --http-port 0 \
    "${DISK_ARGS[@]}" \
    >>"$WORK_DIR/broker.out" 2>>"$WORK_DIR/broker.err" &
  SERVER_PID=$!
  local deadline=$((SECONDS + 40))
  until "$CLI_EXE" --broker "$BROKER" metadata >/dev/null 2>&1; do
    (( SECONDS < deadline )) || die "broker never became ready: $(tail -3 "$WORK_DIR/broker.err")"
    sleep 0.3
  done
}

stop_broker() {
  [[ -n "$SERVER_PID" ]] || return 0
  kill -9 "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

cli() { "$CLI_EXE" --broker "$BROKER" "$@"; }

# Partitions of `t` sitting on one disk, read off the filesystem rather than
# from anything the broker says.
on_disk() {
  ls "$WORK_DIR/disk$1" 2>/dev/null | grep '^t-' | sed 's/^t-//' | sort -n | tr '\n' ' ' | sed 's/ $//'
}

latest() {
  cli offsets --topic t --partition "$1" 2>/dev/null \
    | sed -n 's/.*latest=\([0-9]*\).*/\1/p'
}

# ---------------------------------------------------------- placement

stage "partitions spread across the disks"

start_broker
cli produce --topic t --count 6000 --value-size 200 --no-key \
  --acks 1 --compression none >/dev/null

TOTAL=0
for index in $(seq 1 "$DISKS"); do
  COUNT=$(on_disk "$index" | wc -w)
  info "disk$index: partitions $(on_disk "$index")"
  TOTAL=$((TOTAL + COUNT))
  assert_eq "$COUNT" "$((PARTITIONS / DISKS))" \
    "disk$index holds its even share of the partitions"
done
assert_eq "$TOTAL" "$PARTITIONS" "every partition landed on exactly one disk"

DIR_LINES="$(cli describe-log-dirs --topic t | grep -c '^broker ')"
assert_eq "$DIR_LINES" "$DISKS" "DescribeLogDirs reports every configured directory"

# ------------------------------------------------------------ restart

stage "placement survives a restart, rebuilt from the disks"

BEFORE="$(for index in $(seq 1 "$DISKS"); do on_disk "$index"; echo; done)"
stop_broker
start_broker
AFTER="$(for index in $(seq 1 "$DISKS"); do on_disk "$index"; echo; done)"
assert_eq "$AFTER" "$BEFORE" "no partition moved disks across the restart"

RESTORED=0
for partition in $(seq 0 $((PARTITIONS - 1))); do
  RESTORED=$((RESTORED + $(latest "$partition")))
done
assert_eq "$RESTORED" "6000" "every record is still readable after the restart"

# ---------------------------------------------------------- isolation

stage "a disk that fails takes only its own partitions"

VICTIM_DISK=2
VICTIM="$(on_disk "$VICTIM_DISK" | awk '{print $1}')"
SURVIVOR="$(on_disk 1 | awk '{print $1}')"
[[ -n "$VICTIM" && -n "$SURVIVOR" ]] || die "could not pick a victim and a survivor partition"
info "failing disk$VICTIM_DISK (holds partition $VICTIM); partition $SURVIVOR is on disk1"

# Occupy the probe path so the next write into that directory fails.
mkdir "$WORK_DIR/disk$VICTIM_DISK/.brahmaputra-health"
sleep "$PROBE_GRACE"

# The survivor must still serve reads *and* writes. A broker that went
# read-only, or that answered stale offsets from memory, would pass a
# read-only check and still be broken.
assert_eq "$(latest "$SURVIVOR")" "1000" "a partition on a healthy disk still reads"
ACKED="$(cli produce --topic t --partition "$SURVIVOR" --value still-working 2>&1 | tail -1)"
[[ "$ACKED" == *"acked"* ]] || die "a partition on a healthy disk stopped accepting writes ($ACKED)"
pass "a partition on a healthy disk still accepts writes"
assert_eq "$(latest "$SURVIVOR")" "1001" "and the write landed"

# The casualty must be refused as unavailable, never as a missing topic —
# a client that read it as "unknown topic" would conclude the topic had
# been deleted.
VICTIM_ERROR="$(cli offsets --topic t --partition "$VICTIM" 2>&1 | tail -1 || true)"
[[ "$VICTIM_ERROR" == *"log directory"*"offline"* ]] \
  || die "a partition on the failed disk was not reported as offline ($VICTIM_ERROR)"
pass "a partition on the failed disk is refused as unavailable"

# Other partitions on the *same* failed disk go with it, and nothing else.
STILL_UP=0
for partition in $(seq 0 $((PARTITIONS - 1))); do
  if [[ -n "$(latest "$partition")" ]]; then
    STILL_UP=$((STILL_UP + 1))
  fi
done
assert_eq "$STILL_UP" "$((PARTITIONS - PARTITIONS / DISKS))" \
  "exactly the partitions on the failed disk are unavailable; the rest are not"

# The broker itself is still up. With one data directory this is precisely
# what would not be true.
cli metadata >/dev/null 2>&1 || die "the broker died with the disk"
pass "the broker is still serving"

# --------------------------------------------------------- diagnosis

stage "the failed disk is reported, with a reason"

REPORT="$(cli describe-log-dirs --topic t)"
printf '%s\n' "$REPORT" | grep '^broker ' | sed 's/^/     /'
OFFLINE_LINES="$(printf '%s\n' "$REPORT" | grep -c 'OFFLINE' || true)"
assert_eq "$OFFLINE_LINES" "1" "exactly one directory is reported offline"
printf '%s\n' "$REPORT" | grep 'OFFLINE' | grep -q ':' \
  || die "the offline directory was reported without a reason"
pass "the offline directory is reported with the error that took it down"

grep -q "log directory taken offline" "$WORK_DIR/broker.out" \
  || die "the broker did not log the directory going offline"
pass "the broker logged the failure"

# ---------------------------------------------- honesty across a restart

stage "a disk broken at startup makes its partitions unavailable, not empty"

stop_broker
# Replace the directory with a file: it can no longer be created or read,
# which is what a failed mount looks like from the process's side.
rm -rf "$WORK_DIR/disk$VICTIM_DISK"
printf 'not a directory' >"$WORK_DIR/disk$VICTIM_DISK"
start_broker

# The decisive check. Without the recorded placement the broker cannot see
# what was on the dead disk, would place those partitions fresh on a healthy
# one, and would report them as empty — indistinguishable from a partition
# that lost every record.
STARTUP_ERROR="$(cli offsets --topic t --partition "$VICTIM" 2>&1 | tail -1 || true)"
[[ "$STARTUP_ERROR" == *"log directory"*"offline"* ]] \
  || die "a partition on a disk broken at startup was not reported as offline ($STARTUP_ERROR)"
pass "a partition on a disk broken at startup is unavailable, not silently empty"

assert_eq "$(latest "$SURVIVOR")" "1001" \
  "the surviving disk's data is intact and unchanged"

printf '\n\033[32mAll %d checks passed.\033[0m\n' "$CHECKS"
