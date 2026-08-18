#!/usr/bin/env bash
# M1 live verification: real broker processes, real CLI/API traffic, concurrent
# producers, visible on-disk logs, and SIGKILL recovery while writes are active.
# Usage (Git Bash on Windows or a Unix shell): bash scripts/verify-m1.sh
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
export LC_ALL=C

PORT=${BRAHMAPUTRA_M1_PORT:-19092}
BROKER="127.0.0.1:$PORT"
PARTITIONS=3
CONCURRENT_PER_PRODUCER=${BRAHMAPUTRA_M1_CONCURRENT_RECORDS:-1500}
CRASH_RECORDS=${BRAHMAPUTRA_M1_CRASH_RECORDS:-500000}
POST_CRASH_RECORDS=25
PASS=0
FAIL=0
SPID=""
WRITER_PIDS=()

WORK=$(mktemp -d "${TMPDIR:-/tmp}/brahmaputra-m1.XXXXXX") || exit 1
DATA="$WORK/data"
SERVER_LOG="$WORK/server.log"

pass() {
  echo "PASS: $1"
  PASS=$((PASS + 1))
}

fail() {
  echo "FAIL: $1"
  FAIL=$((FAIL + 1))
}

check() { # check <name> <command> [args...]
  local name=$1
  shift
  if "$@"; then pass "$name"; else fail "$name"; fi
}

stop_process() {
  local pid=${1:-}
  local attempt
  [[ -n "$pid" ]] || return 0
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    for ((attempt = 0; attempt < 50; attempt++)); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.02
    done
    kill -9 "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  local pid
  for pid in "${WRITER_PIDS[@]:-}"; do stop_process "$pid"; done
  stop_process "$SPID"
  if [[ $FAIL -eq 0 ]]; then
    rm -rf "$WORK"
  else
    echo "Verification artifacts retained at: $WORK"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

resolve_binary() {
  local stem=$1
  local candidate
  # Cargo emits .exe under Git Bash on Windows. Also accept extensionless
  # binaries so the same script remains useful on Unix.
  for candidate in "./target/debug/$stem.exe" "./target/debug/$stem"; do
    if [[ -f "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  echo "cannot find target/debug/$stem(.exe)" >&2
  return 1
}

wait_for_broker() {
  local attempt
  for ((attempt = 0; attempt < 100; attempt++)); do
    if [[ -n "$SPID" ]] && ! kill -0 "$SPID" 2>/dev/null; then
      return 1
    fi
    if "$CLI" metadata --broker "$BROKER" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

start_broker() {
  echo "== starting broker (port $PORT, data $DATA) =="
  "$SERVER" --port "$PORT" --data-dir "$DATA" --default-partitions "$PARTITIONS" \
    >>"$SERVER_LOG" 2>&1 &
  SPID=$!
  if ! wait_for_broker; then
    echo "server failed to become ready:" >&2
    cat "$SERVER_LOG" >&2
    return 1
  fi
}

partition_has_nonempty_log() {
  local topic=$1
  local partition=$2
  local file
  for file in "$DATA/$topic-$partition"/*.log; do
    [[ -f "$file" && -s "$file" ]] && return 0
  done
  return 1
}

partition_log_bytes() {
  local topic=$1
  local partition=$2
  local total=0
  local size
  local file
  for file in "$DATA/$topic-$partition"/*.log; do
    if [[ -f "$file" ]]; then
      size=$(wc -c <"$file")
      total=$((total + size))
    fi
  done
  printf '%s\n' "$total"
}

latest_offset() {
  local topic=$1
  local partition=$2
  "$CLI" offsets --broker "$BROKER" --topic "$topic" --partition "$partition" \
    2>/dev/null | awk '
      {
        for (field = 1; field <= NF; field++) {
          if ($field ~ /^latest=/) {
            split($field, value, "=")
            print value[2] + 0
            exit
          }
        }
      }
    '
}

validate_offsets() {
  local path=$1
  local expected_partitions=$2
  # Whether every partition must have received data. A keyed producer
  # cannot satisfy that: murmur2 pins a key to one partition, which is the
  # guarantee that makes per-key ordering work, so N distinct keys reach at
  # most N partitions.
  local require_all=${3:-1}
  awk -v partitions="$expected_partitions" -v require_all="$require_all" '
    function invalid(message) {
      print "offset validation: " message > "/dev/stderr"
      bad = 1
    }
    {
      if (NF != 4) {
        invalid("line " NR " has " NF " fields")
        next
      }
      split($1, partition_field, "=")
      split($2, offset_field, "=")
      if (partition_field[1] != "partition" ||
          offset_field[1] != "offset" ||
          $3 !~ /^key=/ || $4 !~ /^value=/) {
        invalid("line " NR " has unexpected format")
        next
      }
      partition = partition_field[2] + 0
      offset = offset_field[2] + 0
      if (partition < 0 || partition >= partitions) {
        invalid("line " NR " has partition " partition)
      }
      if (!(partition in expected_offset)) {
        expected_offset[partition] = 0
      }
      if (offset != expected_offset[partition]) {
        message = "partition " partition " expected offset " expected_offset[partition] ", got " offset
        invalid(message)
      }
      expected_offset[partition]++
    }
    END {
      if (require_all) {
        for (partition = 0; partition < partitions; partition++) {
          if (!(partition in expected_offset)) {
            invalid("partition " partition " had no records")
          }
        }
      }
      exit bad
    }
  ' "$path"
}

validate_crash_prefix() {
  local path=$1
  local partition=$2
  awk -v partition="$partition" '
    function invalid(message) {
      print "prefix validation: " message > "/dev/stderr"
      bad = 1
    }
    {
      split($1, partition_field, "=")
      split($2, offset_field, "=")
      offset = offset_field[2] + 0
      if (NF != 4 || partition_field[2] + 0 != partition || offset != NR - 1) {
        invalid("bad partition/offset at line " NR)
      }
      if ($3 != "key=load-" (NR - 1)) {
        invalid("non-prefix key at line " NR ": " $3)
      }
      if ($4 !~ /^value=x+$/ || length($4) != 262) {
        invalid("unexpected 256-byte value at line " NR)
      }
    }
    END {
      if (NR == 0) invalid("recovered prefix is empty")
      exit bad
    }
  ' "$path"
}

validate_single_partition_offsets() {
  local path=$1
  local wanted_partition=$2
  awk -v wanted="$wanted_partition" '
    function invalid(message) {
      print "single-partition offset validation: " message > "/dev/stderr"
      bad = 1
    }
    {
      split($1, partition_field, "=")
      split($2, offset_field, "=")
      if (NF != 4 || partition_field[1] != "partition" ||
          partition_field[2] + 0 != wanted || offset_field[1] != "offset" ||
          offset_field[2] + 0 != NR - 1 || $3 !~ /^key=/ || $4 !~ /^value=/) {
        invalid("bad record at line " NR)
      }
    }
    END {
      if (NR == 0) invalid("partition had no records")
      exit bad
    }
  ' "$path"
}

echo "== building checked-out binaries =="
if ! cargo build --bins; then
  echo "build failed" >&2
  exit 1
fi
SERVER=$(resolve_binary brahmaputra-server) || exit 1
CLI=$(resolve_binary brahmaputra-cli) || exit 1

start_broker || exit 1

echo "== two gated, independent producer processes =="
INPUT_A="$WORK/producer-a.txt"
INPUT_B="$WORK/producer-b.txt"
EXPECTED="$WORK/concurrent-expected.txt"
EXPECTED_SORTED="$WORK/concurrent-expected.sorted"
ACTUAL_SORTED="$WORK/concurrent-actual.sorted"
CONSUMED="$WORK/concurrent-consumed.txt"
GATE="$WORK/start-producers"

for ((i = 0; i < CONCURRENT_PER_PRODUCER; i++)); do
  printf 'producer-a-%06d\n' "$i"
done >"$INPUT_A"
for ((i = 0; i < CONCURRENT_PER_PRODUCER; i++)); do
  printf 'producer-b-%06d\n' "$i"
done >"$INPUT_B"
awk '{ print "key=producer-a value=" $0 }' "$INPUT_A" >"$EXPECTED"
awk '{ print "key=producer-b value=" $0 }' "$INPUT_B" >>"$EXPECTED"
sort "$EXPECTED" >"$EXPECTED_SORTED"

(
  while [[ ! -f "$GATE" ]]; do sleep 0.01; done
  exec "$CLI" produce --broker "$BROKER" --topic concurrent-topic \
    --key producer-a --file "$INPUT_A"
) >"$WORK/producer-a.log" 2>&1 &
PRODUCER_A_PID=$!
(
  while [[ ! -f "$GATE" ]]; do sleep 0.01; done
  exec "$CLI" produce --broker "$BROKER" --topic concurrent-topic \
    --key producer-b --file "$INPUT_B"
) >"$WORK/producer-b.log" 2>&1 &
PRODUCER_B_PID=$!
WRITER_PIDS=("$PRODUCER_A_PID" "$PRODUCER_B_PID")

if kill -0 "$PRODUCER_A_PID" 2>/dev/null && kill -0 "$PRODUCER_B_PID" 2>/dev/null; then
  pass "two independent producer processes are staged concurrently"
else
  fail "two independent producer processes are staged concurrently"
fi
: >"$GATE"
wait "$PRODUCER_A_PID"; PRODUCER_A_STATUS=$?
wait "$PRODUCER_B_PID"; PRODUCER_B_STATUS=$?
WRITER_PIDS=()
check "producer A completed" test "$PRODUCER_A_STATUS" -eq 0
check "producer B completed" test "$PRODUCER_B_STATUS" -eq 0

if ! "$CLI" consume --broker "$BROKER" --topic concurrent-topic \
  --from earliest --max $((CONCURRENT_PER_PRODUCER * 2 + 1)) >"$CONSUMED"; then
  fail "concurrent topic consumed successfully"
else
  pass "concurrent topic consumed successfully"
fi
CONCURRENT_COUNT=$(awk 'END { print NR + 0 }' "$CONSUMED")
check "exact concurrent record count" test "$CONCURRENT_COUNT" -eq $((CONCURRENT_PER_PRODUCER * 2))
check "contiguous offsets in every partition that received records" \
  validate_offsets "$CONSUMED" "$PARTITIONS" 0
# Both producers send under one fixed key each, so murmur2 must place all of
# a key's records in a single partition — that placement *is* the per-key
# ordering guarantee. Two keys therefore touch at most two of the three
# partitions, and a partition left empty is correct, not a lost write.
check "each key landed in exactly one partition" \
  awk '{ split($1, p, "="); split($3, k, "="); seen[k[2] " " p[2]] = 1 }
       END { for (pair in seen) { split(pair, f, " "); count[f[1]]++ }
             for (key in count) if (count[key] != 1) {
               print key " spread across " count[key] " partitions" > "/dev/stderr"; bad = 1 }
             exit bad }' "$CONSUMED"
awk '{ print $3 " " $4 }' "$CONSUMED" | sort >"$ACTUAL_SORTED"
check "exact concurrent keys and values" cmp -s "$EXPECTED_SORTED" "$ACTUAL_SORTED"

"$CLI" offsets --broker "$BROKER" --topic concurrent-topic >"$WORK/concurrent-offsets.txt"
for ((partition = 0; partition < PARTITIONS; partition++)); do
  OBSERVED=$(awk -v wanted="partition=$partition" '$1 == wanted { count++ } END { print count + 0 }' "$CONSUMED")
  check "offset API matches concurrent-topic-$partition" \
    grep -Fqx "concurrent-topic-$partition: earliest=0 latest=$OBSERVED" "$WORK/concurrent-offsets.txt"
  # Only a partition a key actually hashed to holds records; the others are
  # legitimately empty (see the keyed-placement check above).
  if [[ "$OBSERVED" -gt 0 ]]; then
    check "concurrent-topic-$partition has a visible non-empty .log file" \
      partition_has_nonempty_log concurrent-topic "$partition"
  fi
done

echo "== SIGKILL while two partition logs are visibly growing =="
"$CLI" produce --broker "$BROKER" --topic crash-topic --partition 0 \
  --count "$CRASH_RECORDS" --value-size 256 >"$WORK/crash-producer-0.log" 2>&1 &
CRASH_PID_0=$!
"$CLI" produce --broker "$BROKER" --topic crash-topic --partition 1 \
  --count "$CRASH_RECORDS" --value-size 256 >"$WORK/crash-producer-1.log" 2>&1 &
CRASH_PID_1=$!
WRITER_PIDS=("$CRASH_PID_0" "$CRASH_PID_1")

KNOWN_PREFIX_0=0
KNOWN_PREFIX_1=0
VISIBLE_PREFIX_OBSERVED=0
for ((attempt = 0; attempt < 500; attempt++)); do
  if ! kill -0 "$CRASH_PID_0" 2>/dev/null || ! kill -0 "$CRASH_PID_1" 2>/dev/null; then
    break
  fi
  if partition_has_nonempty_log crash-topic 0 && partition_has_nonempty_log crash-topic 1; then
    KNOWN_PREFIX_0=$(latest_offset crash-topic 0)
    KNOWN_PREFIX_1=$(latest_offset crash-topic 1)
    KNOWN_PREFIX_0=${KNOWN_PREFIX_0:-0}
    KNOWN_PREFIX_1=${KNOWN_PREFIX_1:-0}
    if [[ $KNOWN_PREFIX_0 -gt 0 && $KNOWN_PREFIX_1 -gt 0 ]]; then
      VISIBLE_PREFIX_OBSERVED=1
      break
    fi
  fi
  sleep 0.02
done
check "both active writers have a broker-visible acknowledged prefix" \
  test "$VISIBLE_PREFIX_OBSERVED" -eq 1

PREVIOUS_0=$(partition_log_bytes crash-topic 0)
PREVIOUS_1=$(partition_log_bytes crash-topic 1)
GROWTH_OBSERVED=0
for ((attempt = 0; attempt < 500; attempt++)); do
  if ! kill -0 "$CRASH_PID_0" 2>/dev/null || ! kill -0 "$CRASH_PID_1" 2>/dev/null; then
    break
  fi
  BYTES_0=$(partition_log_bytes crash-topic 0)
  BYTES_1=$(partition_log_bytes crash-topic 1)
  if [[ $BYTES_0 -gt $PREVIOUS_0 && $BYTES_1 -gt $PREVIOUS_1 ]]; then
    GROWTH_OBSERVED=1
    break
  fi
  PREVIOUS_0=$BYTES_0
  PREVIOUS_1=$BYTES_1
  sleep 0.02
done
check "both writers are active while both .log files grow" test "$GROWTH_OBSERVED" -eq 1

if kill -0 "$SPID" 2>/dev/null; then
  kill -9 "$SPID" 2>/dev/null || true
  wait "$SPID" 2>/dev/null || true
  SPID=""
  pass "broker terminated with SIGKILL during active writes"
else
  fail "broker terminated with SIGKILL during active writes"
fi

# Writers must fail promptly when their live broker disappears. Bound cleanup
# so a broken client cannot hang this verification indefinitely.
for pid in "$CRASH_PID_0" "$CRASH_PID_1"; do
  for ((attempt = 0; attempt < 250; attempt++)); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.02
  done
  stop_process "$pid"
done
WRITER_PIDS=()

echo "== restart after SIGKILL and verify durable prefixes =="
start_broker || exit 1

"$CLI" consume --broker "$BROKER" --topic concurrent-topic --from earliest \
  --max $((CONCURRENT_PER_PRODUCER * 2 + 1)) >"$WORK/concurrent-after-restart.txt"
check "completed concurrent data is byte-identical after SIGKILL restart" \
  cmp -s "$CONSUMED" "$WORK/concurrent-after-restart.txt"

for ((partition = 0; partition < 2; partition++)); do
  PREFIX_FILE="$WORK/crash-prefix-$partition.txt"
  "$CLI" consume --broker "$BROKER" --topic crash-topic --partition "$partition" \
    --from earliest >"$PREFIX_FILE"
  PREFIX_COUNT=$(awk 'END { print NR + 0 }' "$PREFIX_FILE")
  if [[ $partition -eq 0 ]]; then
    KNOWN_PREFIX=$KNOWN_PREFIX_0
  else
    KNOWN_PREFIX=$KNOWN_PREFIX_1
  fi
  check "crash-topic-$partition retained its acknowledged prefix" \
    test "$PREFIX_COUNT" -ge "$KNOWN_PREFIX"
  check "crash-topic-$partition recovered a valid input prefix" \
    validate_crash_prefix "$PREFIX_FILE" "$partition"
done

echo "== append after recovery and prove offsets continue =="
for ((partition = 0; partition < 2; partition++)); do
  PREFIX_FILE="$WORK/crash-prefix-$partition.txt"
  PREFIX_COUNT=$(awk 'END { print NR + 0 }' "$PREFIX_FILE")
  TAIL_INPUT="$WORK/post-crash-$partition.txt"
  TAIL_EXPECTED="$WORK/post-crash-$partition.expected"
  FULL_FILE="$WORK/crash-full-$partition.txt"
  OFFSETS_FILE="$WORK/crash-offsets-$partition.txt"

  for ((i = 0; i < POST_CRASH_RECORDS; i++)); do
    printf 'post-%d-%03d\n' "$partition" "$i"
  done >"$TAIL_INPUT"
  awk -v key="post-$partition" '{ print "key=" key " value=" $0 }' \
    "$TAIL_INPUT" >"$TAIL_EXPECTED"

  "$CLI" produce --broker "$BROKER" --topic crash-topic --partition "$partition" \
    --key "post-$partition" --file "$TAIL_INPUT" >"$WORK/post-producer-$partition.log"
  "$CLI" consume --broker "$BROKER" --topic crash-topic --partition "$partition" \
    --from earliest >"$FULL_FILE"

  FULL_COUNT=$(awk 'END { print NR + 0 }' "$FULL_FILE")
  check "crash-topic-$partition exact count after continuation" \
    test "$FULL_COUNT" -eq $((PREFIX_COUNT + POST_CRASH_RECORDS))
  check "crash-topic-$partition offsets remain contiguous after continuation" \
    validate_single_partition_offsets "$FULL_FILE" "$partition"
  awk -v prefix="$PREFIX_COUNT" 'NR > prefix { print $3 " " $4 }' \
    "$FULL_FILE" >"$WORK/post-crash-$partition.actual"
  check "crash-topic-$partition appended exact new content" \
    cmp -s "$TAIL_EXPECTED" "$WORK/post-crash-$partition.actual"
  awk -v prefix="$PREFIX_COUNT" 'NR <= prefix' "$FULL_FILE" \
    >"$WORK/crash-prefix-$partition.after"
  check "crash-topic-$partition durable prefix stayed byte-identical" \
    cmp -s "$PREFIX_FILE" "$WORK/crash-prefix-$partition.after"

  "$CLI" offsets --broker "$BROKER" --topic crash-topic --partition "$partition" \
    >"$OFFSETS_FILE"
  check "crash-topic-$partition offset API reports the continued end" \
    grep -Fqx "crash-topic-$partition: earliest=0 latest=$FULL_COUNT" "$OFFSETS_FILE"
done

echo
echo "== RESULT: $PASS passed, $FAIL failed =="
exit $((FAIL > 0))
