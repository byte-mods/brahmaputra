#!/usr/bin/env bash
# TCP vs QUIC parity: run the same correctness checks over both transports
# and print a side-by-side matrix.
#
# Throughput is measured elsewhere (scripts/bench-three-way.sh). What is
# checked here is whether the transport changes *behaviour* — which is the
# only question that matters before offering it as a choice:
#
#   accuracy        every produced record is readable, byte-identical
#   no loss         nothing acknowledged is missing after a broker restart
#   ordering        records sharing a key keep their relative order
#   retry           an idempotent replay does not duplicate
#   zero copy       the bytes on disk are the bytes the producer sent
#   sync            replicas converge byte-for-byte (verify-replication.sh)
#   fault tolerance leader failover loses nothing (verify-replication.sh)
#
# Requires Git Bash on Windows.

set -Eeuo pipefail

RECORDS="${RECORDS:-500}"
PARTITIONS="${PARTITIONS:-4}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-parity.XXXXXX")"
SERVER_EXE="$ROOT/target/debug/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/debug/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/debug/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/debug/brahmaputra-cli"

SERVER_PID=""
declare -A RESULT

stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
ok() { printf '\033[32m  PASS\033[0m %s\n' "$1"; }
bad() { printf '\033[31m  FAIL\033[0m %s\n' "$1"; }
die() { printf '\n\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

cleanup() {
  stop_broker || true
  if [[ -d "$WORK_DIR" && "$WORK_DIR" == "$TEMP_ROOT"/brahmaputra-parity.* ]]; then
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
  local transport="$1" data_dir="$2"
  PORT="$(allocate_port)"
  BROKER="127.0.0.1:$PORT"
  TRANSPORT="$transport"
  mkdir -p "$data_dir"
  "$SERVER_EXE" --port "$PORT" --data-dir "$data_dir" \
    --default-partitions "$PARTITIONS" --segment-bytes 67108864 \
    --transport "$transport" \
    > "$data_dir/../server.$transport.out" 2> "$data_dir/../server.$transport.err" &
  SERVER_PID=$!
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if cli metadata >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  die "broker ($transport) never became ready"
}

stop_broker() {
  [[ -n "$SERVER_PID" ]] || return 0
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

cli() {
  "$CLI_EXE" --transport "$TRANSPORT" --broker "$BROKER" "$@"
}

record_result() {
  local transport="$1" check="$2" verdict="$3" detail="$4"
  RESULT["$transport/$check"]="$verdict|$detail"
  if [[ "$verdict" == "pass" ]]; then ok "$check — $detail"; else bad "$check — $detail"; fi
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

# ---------------------------------------------------------------- checks

check_accuracy_and_loss() {
  local transport="$1" topic="parity-accuracy"
  local produced consumed missing duplicated
  produced="$(values_file "$RECORDS" "accuracy")"
  cli produce --topic "$topic" --file "$produced" --acks all >/dev/null

  cli consume --topic "$topic" --from earliest --max "$((RECORDS * 2))" \
    | sed -n 's/^partition=[0-9]* offset=[0-9]* key=[^ ]* value=\(.*\)$/\1/p' \
    | sort > "$WORK_DIR/consumed-$transport.txt"
  sort "$produced" > "$WORK_DIR/produced-sorted.txt"

  missing="$(comm -23 "$WORK_DIR/produced-sorted.txt" "$WORK_DIR/consumed-$transport.txt" | wc -l | tr -d ' ')"
  duplicated="$(( $(wc -l < "$WORK_DIR/consumed-$transport.txt") - $(sort -u "$WORK_DIR/consumed-$transport.txt" | wc -l) ))"
  consumed="$(wc -l < "$WORK_DIR/consumed-$transport.txt" | tr -d ' ')"

  if [[ "$missing" == "0" && "$duplicated" == "0" && "$consumed" == "$RECORDS" ]]; then
    record_result "$transport" accuracy pass "$consumed/$RECORDS records, byte-identical, 0 lost, 0 duplicated"
  else
    record_result "$transport" accuracy fail "$consumed/$RECORDS consumed, $missing lost, $duplicated duplicated"
  fi
}

check_durability_across_restart() {
  local transport="$1" data_dir="$2" topic="parity-accuracy"
  local before after
  before="$(cli offsets --topic "$topic" | awk -F'latest=' '{ total += $2 } END { print total + 0 }')"
  stop_broker
  start_broker "$transport" "$data_dir"
  after="$(cli offsets --topic "$topic" | awk -F'latest=' '{ total += $2 } END { print total + 0 }')"
  local readable
  readable="$(cli consume --topic "$topic" --from earliest --max "$((RECORDS * 2))" | grep -c '^partition=' || true)"
  if [[ "$before" == "$after" && "$readable" == "$RECORDS" ]]; then
    record_result "$transport" durability pass "offsets survived restart ($after) and all $readable records re-read"
  else
    record_result "$transport" durability fail "before=$before after=$after readable=$readable"
  fi
}

check_key_ordering() {
  local transport="$1" topic="parity-ordering" path="$WORK_DIR/keyed.txt"
  # Same key for every record: Kafka semantics put them all on one
  # partition, in send order.
  perl -e 'printf "ordered-%04d\n", $_ for 1 .. 200' > "$path"
  cli produce --topic "$topic" --key order-key --file "$path" --acks all >/dev/null

  local partitions_used sequence
  partitions_used="$(cli consume --topic "$topic" --from earliest --max 400 \
    | sed -n 's/^partition=\([0-9]*\) .*$/\1/p' | sort -u | wc -l | tr -d ' ')"
  sequence="$(cli consume --topic "$topic" --from earliest --max 400 \
    | sed -n 's/^.*value=\(.*\)$/\1/p')"
  if [[ "$partitions_used" == "1" ]] && diff -q <(printf '%s\n' "$sequence") "$path" >/dev/null; then
    record_result "$transport" ordering pass "200 same-key records on 1 partition, in send order"
  else
    record_result "$transport" ordering fail "partitions=$partitions_used, order preserved=$(diff -q <(printf '%s\n' "$sequence") "$path" >/dev/null && echo yes || echo no)"
  fi
}

check_idempotent_retry() {
  local transport="$1" topic="parity-retry"
  local first second latest_before latest_after
  first="$(cli produce --topic "$topic" --partition 0 --value retry-me --acks all \
    --producer-id 7 --producer-epoch 0 --base-sequence 0 | sed -n 's/.*acked offset=\([0-9]*\).*/\1/p')"
  latest_before="$(cli offsets --topic "$topic" --partition 0 | sed -n 's/.*latest=\([0-9]*\).*/\1/p')"
  # Byte-identical replay of the same batch: the broker must return the
  # original offset and append nothing.
  second="$(cli produce --topic "$topic" --partition 0 --value retry-me --acks all \
    --producer-id 7 --producer-epoch 0 --base-sequence 0 | sed -n 's/.*acked offset=\([0-9]*\).*/\1/p')"
  latest_after="$(cli offsets --topic "$topic" --partition 0 | sed -n 's/.*latest=\([0-9]*\).*/\1/p')"

  if [[ "$first" == "$second" && "$latest_before" == "$latest_after" ]]; then
    record_result "$transport" retry pass "replay returned offset $second and appended nothing"
  else
    record_result "$transport" retry fail "first=$first second=$second latest $latest_before -> $latest_after"
  fi
}

check_zero_copy_bytes() {
  local transport="$1" data_dir="$2" topic="parity-bytes"
  # A value that survives round-tripping unchanged only if the broker
  # stores the producer's bytes rather than re-encoding them.
  local value="zero-copy-payload-0123456789-abcdefghij"
  cli produce --topic "$topic" --partition 0 --value "$value" --acks all >/dev/null
  local on_disk
  on_disk="$(grep -c "$value" "$data_dir/$topic-0/"*.log 2>/dev/null | head -1 || true)"
  local read_back
  read_back="$(cli consume --topic "$topic" --partition 0 --from earliest --max 1 \
    | sed -n 's/^.*value=\(.*\)$/\1/p')"
  if [[ "$read_back" == "$value" && "${on_disk:-0}" -ge 1 ]]; then
    record_result "$transport" zero_copy pass "payload found verbatim in the segment file and read back unchanged"
  else
    record_result "$transport" zero_copy fail "on_disk=${on_disk:-0} read_back=${read_back:-<none>}"
  fi
}

check_group_consumption() {
  local transport="$1" topic="parity-groups" group="parity-group"
  local first second
  first="$(cli consume --topic "$topic" --group "$group" --max 0 2>/dev/null; \
    cli produce --topic "$topic" --file "$(values_file 120 groups)" --acks all >/dev/null; \
    cli consume --topic "$topic" --group "$group" --max 60 --commit-interval-ms 200 \
      | grep -c '^partition=' || true)"
  second="$(cli consume --topic "$topic" --group "$group" --max 200 --commit-interval-ms 200 \
    | grep -c '^partition=' || true)"
  local lag_total
  lag_total="$(cli groups lag --group "$group" | sed -n 's/^total lag: \([0-9]*\)$/\1/p')"
  if [[ "$first" == "60" && "$second" == "60" && "$lag_total" == "0" ]]; then
    record_result "$transport" groups pass "bounded run took 60, resumed for the remaining 60, final lag 0"
  else
    record_result "$transport" groups fail "first=$first second=$second lag=$lag_total"
  fi
}

run_suite() {
  local transport="$1"
  local data_dir="$WORK_DIR/$transport/data"
  stage "Transport: $transport"
  start_broker "$transport" "$data_dir"
  check_accuracy_and_loss "$transport"
  check_durability_across_restart "$transport" "$data_dir"
  check_key_ordering "$transport"
  check_idempotent_retry "$transport"
  check_zero_copy_bytes "$transport" "$data_dir"
  check_group_consumption "$transport"
  stop_broker
}

TRANSPORTS=(tcp tcp-tls quic)
for transport in "${TRANSPORTS[@]}"; do
  run_suite "$transport"
done

stage "Transport parity matrix"
printf '\n| Check |'
for transport in "${TRANSPORTS[@]}"; do printf ' %s |' "$transport"; done
printf '\n|---|'
for _ in "${TRANSPORTS[@]}"; do printf '%s' '---|'; done
printf '\n'

FAILURES=0
for check in accuracy durability ordering retry zero_copy groups; do
  printf '| %s |' "$check"
  for transport in "${TRANSPORTS[@]}"; do
    entry="${RESULT["$transport/$check"]:-fail|not run}"
    verdict="${entry%%|*}"
    if [[ "$verdict" == "pass" ]]; then
      printf '%s' ' pass |'
    else
      FAILURES=$((FAILURES + 1))
      printf ' FAIL: %s |' "${entry#*|}"
    fi
  done
  printf '\n'
done
printf '\n'
if (( FAILURES > 0 )); then
  die "$FAILURES parity check(s) failed"
fi
printf '\033[32mEvery transport behaves identically across every check.\033[0m\n'
