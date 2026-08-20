#!/usr/bin/env bash
# M4 authoritative live verification (consumer groups). Requires Git Bash on
# Windows.
#
# Five combined nodes provide a five-member controller quorum. The internal
# __consumer_offsets topic is pinned to six RF=3 partitions so the group
# coordinator for any group can survive one hard-killed broker, and the data
# topic has six RF=3 partitions for the three-consumer split.

set -Eeuo pipefail

TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
HEARTBEAT_INTERVAL_MS="${HEARTBEAT_INTERVAL_MS:-500}"
SESSION_TIMEOUT_MS="${SESSION_TIMEOUT_MS:-5000}"
SEGMENT_BYTES="${SEGMENT_BYTES:-1048576}"
CLI_WALL_TIMEOUT_SECONDS="${CLI_WALL_TIMEOUT_SECONDS:-180}"
BUILD_WALL_TIMEOUT_SECONDS="${BUILD_WALL_TIMEOUT_SECONDS:-600}"
CONSUMER_WALL_TIMEOUT_SECONDS="${CONSUMER_WALL_TIMEOUT_SECONDS:-900}"
OFFSETS_TOPIC_PARTITIONS="${OFFSETS_TOPIC_PARTITIONS:-6}"
TOPIC_PARTITIONS="${TOPIC_PARTITIONS:-6}"
COMMIT_INTERVAL_MS="${COMMIT_INTERVAL_MS:-300}"

NODE_COUNT=5
TOPIC="consumer-groups-live"
OFFSETS_TOPIC="__consumer_offsets"
GROUP_SPREAD="m4-spread"
GROUP_FAILOVER="m4-failover"
GROUP_RESUME="m4-resume"
CHECKS=0
SUCCESS=0
CONSUMER_SEQUENCE=0

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-m4.XXXXXX")"
CLUSTER_ID="m4-live-$(perl -e 'printf "%08x%04x", time, int(rand(65536))')"
SERVER_EXE="$ROOT/target/debug/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/debug/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/debug/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/debug/brahmaputra-cli"

declare -A DATA_PORT CONTROL_PORT NODE_PID NODE_DATA NODE_STDOUT NODE_STDERR NODE_RESTARTS
declare -A CONSUMER_PID CONSUMER_LOG CONSUMER_ERR

stage() {
  printf '\n\033[36m==> %s\033[0m\n' "$1"
}

pass() {
  CHECKS=$((CHECKS + 1))
  printf '\033[32mPASS: %s\033[0m\n' "$1"
}

die() {
  printf '\n\033[31mFAIL: %s\033[0m\n' "$*" >&2
  return 1
}

assert() {
  local description="$1"
  shift
  if ! "$@"; then
    die "assertion failed: $description"
  fi
  pass "$description"
}

assert_eq() {
  local actual="$1" expected="$2" description="$3"
  if [[ "$actual" != "$expected" ]]; then
    die "assertion failed: $description (expected=$expected actual=$actual)"
  fi
  pass "$description"
}

nonce() {
  perl -e 'printf "%08x", int(rand(0xffffffff))'
}

work_dir_safe() {
  local parent base
  [[ -d "$WORK_DIR" && "$WORK_DIR" != / ]] || return 1
  parent="$(cd "$(dirname "$WORK_DIR")" && pwd -P)" || return 1
  base="${WORK_DIR##*/}"
  [[ "$parent" == "$TEMP_ROOT" && "$base" =~ ^brahmaputra-m4\.[[:alnum:]]{6}$ ]]
}

allocate_port() {
  perl -MIO::Socket::INET -e '
    my $s = IO::Socket::INET->new(LocalAddr => "127.0.0.1", LocalPort => 0, Listen => 1, Proto => "tcp") or die $!;
    print $s->sockport;
  '
}

wait_until() {
  local description="$1" timeout_seconds="$2" delay="$3"
  shift 3
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if "$@"; then
      return 0
    fi
    sleep "$delay"
  done
  die "timed out waiting for $description"
}

node_alive() {
  local node="$1" pid="${NODE_PID[$1]:-}"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

start_node() {
  local node="$1" restart="${2:-0}"
  local node_dir="$WORK_DIR/node-$node"
  local data_dir="$node_dir/data"
  mkdir -p "$data_dir"
  local generation="${NODE_RESTARTS[$node]:-0}"
  if (( restart )); then
    generation=$((generation + 1))
  fi
  NODE_RESTARTS[$node]="$generation"
  local suffix=""
  (( generation > 0 )) && suffix=".restart-$generation"
  local stdout="$node_dir/server${suffix}.stdout.log"
  local stderr="$node_dir/server${suffix}.stderr.log"
  local args=(
    --host 127.0.0.1
    --port "${DATA_PORT[$node]}"
    --data-dir "$data_dir"
    --segment-bytes "$SEGMENT_BYTES"
    --node-id "$node"
    --cluster-id "$CLUSTER_ID"
    --control-port "${CONTROL_PORT[$node]}"
    --heartbeat-interval-ms "$HEARTBEAT_INTERVAL_MS"
    --session-timeout-ms "$SESSION_TIMEOUT_MS"
    --offsets-topic-partitions "$OFFSETS_TOPIC_PARTITIONS"
  )
  local peer
  for peer in $(seq 1 "$NODE_COUNT"); do
    args+=(--controller-peer "$peer=127.0.0.1:${CONTROL_PORT[$peer]}")
  done
  RUST_LOG=brahmaputra=info "$SERVER_EXE" "${args[@]}" >"$stdout" 2>"$stderr" &
  local pid=$!
  NODE_PID[$node]="$pid"
  NODE_DATA[$node]="$data_dir"
  NODE_STDOUT[$node]="$stdout"
  NODE_STDERR[$node]="$stderr"
  printf 'started node %s: pid=%s data=%s control=%s\n' "$node" "$pid" "${DATA_PORT[$node]}" "${CONTROL_PORT[$node]}"
}

stop_node() {
  local node="$1" pid="${NODE_PID[$1]:-}" deadline
  [[ -z "$pid" ]] && return 0
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid" 2>/dev/null || true
    deadline=$((SECONDS + 3))
    while kill -0 "$pid" 2>/dev/null && (( SECONDS < deadline )); do sleep 0.05; done
    if kill -0 "$pid" 2>/dev/null; then
      taskkill.exe //PID "$pid" //T //F >/dev/null 2>&1 || true
      kill -9 "$pid" 2>/dev/null || true
      deadline=$((SECONDS + 3))
      while kill -0 "$pid" 2>/dev/null && (( SECONDS < deadline )); do sleep 0.05; done
    fi
  fi
  kill -0 "$pid" 2>/dev/null && { die "unable to hard-kill node $node process $pid"; return 1; }
  wait "$pid" 2>/dev/null || true
}

controller_get() {
  local node="$1" path="$2"
  curl --silent --show-error --fail --max-time 4 "http://127.0.0.1:${CONTROL_PORT[$node]}$path"
}

controller_post() {
  local node="$1" path="$2" body="${3:-}"
  if [[ -z "$body" ]]; then
    curl --silent --show-error --fail --max-time 20 -X POST "http://127.0.0.1:${CONTROL_PORT[$node]}$path"
  else
    curl --silent --show-error --fail --max-time 20 -X POST -H 'content-type: application/json' --data "$body" "http://127.0.0.1:${CONTROL_PORT[$node]}$path"
  fi
}

http_ready_probe() {
  local node="$1"
  node_alive "$node" && controller_get "$node" /api/v1/controller/raft >/dev/null 2>&1
}

wait_http_ready() {
  local node
  for node in "$@"; do
    wait_until "node $node controller HTTP readiness" 25 0.1 http_ready_probe "$node"
  done
}

raft_leader() {
  controller_get "$1" /api/v1/controller/raft 2>/dev/null |
    perl -MJSON::PP -0777 -e '$d=decode_json(<STDIN>); defined $d->{current_leader} or exit 1; print $d->{current_leader}'
}

wait_shared_leader() {
  local nodes_csv="$1" deadline=$((SECONDS + TIMEOUT_SECONDS))
  local -a nodes
  IFS=',' read -r -a nodes <<<"$nodes_csv"
  while (( SECONDS < deadline )); do
    local agreed="" leader="" ok=1 node
    for node in "${nodes[@]}"; do
      leader="$(raft_leader "$node" 2>/dev/null || true)"
      if [[ -z "$leader" ]]; then ok=0; break; fi
      if [[ -z "$agreed" ]]; then agreed="$leader"; elif [[ "$agreed" != "$leader" ]]; then ok=0; break; fi
    done
    if (( ok )) && [[ ",$nodes_csv," == *",$agreed,"* ]]; then
      printf '%s' "$agreed"
      return 0
    fi
    sleep 0.1
  done
  die "controllers $nodes_csv did not agree on a live Raft leader"
}

meta_check() {
  local mode="$1"
  shift
  perl -MJSON::PP -e '
    my ($mode, @a) = @ARGV;
    local $/;
    my $d = eval { decode_json(<STDIN>) } or exit 1;
    if ($mode eq "brokers_alive") {
      my @ids = split /,/, $a[0];
      exit((keys %{$d->{brokers}}) == @ids && !grep { !$d->{brokers}{$_}{alive} } @ids ? 0 : 1);
    }
    if ($mode eq "topic_exists") {
      exit(exists $d->{topics}{$a[0]} ? 0 : 1);
    }
    if ($mode eq "topic_ready") {
      my ($name, $partitions, $rf) = @a;
      my $t = $d->{topics}{$name} or exit 1;
      exit 1 unless keys %{$t->{partitions}} == $partitions;
      for my $p (values %{$t->{partitions}}) {
        exit 1 if $p->{leader} < 0 || @{$p->{replicas}} != $rf;
      }
      exit 0;
    }
    if ($mode eq "partition_leader_moved") {
      my ($name, $part, $dead) = @a;
      my $p = $d->{topics}{$name}{partitions}{$part} or exit 1;
      exit($p->{leader} >= 0 && $p->{leader} != $dead && !$d->{brokers}{$dead}{alive} ? 0 : 1);
    }
    exit 1;
  ' "$mode" "$@"
}

metadata_probe_all() {
  local nodes_csv="$1" mode="$2"
  shift 2
  local -a nodes
  IFS=',' read -r -a nodes <<<"$nodes_csv"
  local json node
  for node in "${nodes[@]}"; do
    json="$(controller_get "$node" /api/v1/controller/metadata 2>/dev/null || true)"
    [[ -n "$json" ]] || return 1
    printf '%s' "$json" | meta_check "$mode" "$@" || return 1
  done
}

wait_metadata() {
  local nodes_csv="$1" description="$2" mode="$3"
  shift 3
  wait_until "$description" "$TIMEOUT_SECONDS" 0.1 metadata_probe_all "$nodes_csv" "$mode" "$@"
}

broker_address() {
  printf '127.0.0.1:%s' "${DATA_PORT[$1]}"
}

CLI_STATUS=0
CLI_OUTPUT=""
cli_raw() {
  set +e
  CLI_OUTPUT="$(timeout --kill-after=5s "${CLI_WALL_TIMEOUT_SECONDS}s" "$CLI_EXE" "$@" 2>&1)"
  CLI_STATUS=$?
  set -e
}

cli() {
  cli_raw "$@"
  if (( CLI_STATUS != 0 )); then
    die "CLI exited $CLI_STATUS while running: $*\n$CLI_OUTPUT"
  fi
  printf '%s' "$CLI_OUTPUT"
}

# --- consumer-group helpers -------------------------------------------------

start_consumer() {
  local seed="$1" group="$2"
  CONSUMER_SEQUENCE=$((CONSUMER_SEQUENCE + 1))
  local id="$CONSUMER_SEQUENCE" dir="$WORK_DIR/consumers"
  mkdir -p "$dir"
  local out="$dir/consumer-$id.stdout.log" err="$dir/consumer-$id.stderr.log"
  timeout --kill-after=2s "${CONSUMER_WALL_TIMEOUT_SECONDS}s" "$CLI_EXE" \
    --broker "$(broker_address "$seed")" consume --topic "$TOPIC" --group "$group" \
    --follow --commit-interval-ms "$COMMIT_INTERVAL_MS" >"$out" 2>"$err" &
  CONSUMER_PID[$id]=$!
  CONSUMER_LOG[$id]="$out"
  CONSUMER_ERR[$id]="$err"
  LAST_CONSUMER_ID="$id"
  printf 'started consumer %s: group=%s pid=%s\n' "$id" "$group" "${CONSUMER_PID[$id]}"
}

consumer_log() {
  printf '%s' "${CONSUMER_LOG[$1]}"
}

stop_consumer() {
  local id="$1" pid="${CONSUMER_PID[$1]:-}" deadline
  [[ -z "$pid" ]] && return 0
  if kill -0 "$pid" 2>/dev/null; then
    # Let GNU timeout relay TERM to its CLI child before the hard fallback.
    kill -TERM "$pid" 2>/dev/null || true
    deadline=$((SECONDS + 3))
    while kill -0 "$pid" 2>/dev/null && (( SECONDS < deadline )); do sleep 0.05; done
    if kill -0 "$pid" 2>/dev/null; then
      taskkill.exe //PID "$pid" //T //F >/dev/null 2>&1 || true
      kill -9 "$pid" 2>/dev/null || true
      deadline=$((SECONDS + 3))
      while kill -0 "$pid" 2>/dev/null && (( SECONDS < deadline )); do sleep 0.05; done
    fi
  fi
  kill -0 "$pid" 2>/dev/null && { die "unable to terminate consumer $id process $pid"; return 1; }
  wait "$pid" 2>/dev/null || true
}

# The last `assignment:` line of each consumer log must show exactly `each`
# partitions, all logs together covering 0..TOPIC_PARTITIONS-1 disjointly.
assignment_split_probe() {
  local each="$1"
  shift
  perl -e '
    use strict; use warnings;
    my ($topic, $total, $each, @logs) = @ARGV;
    my (%seen, $covered);
    $covered = 0;
    for my $log (@logs) {
      open my $fh, q{<}, $log or exit 1;
      my $last;
      while (<$fh>) { $last = $1 if /^assignment: \Q$topic\E=\[([0-9,]+)\]\s*$/; }
      exit 1 unless defined $last;
      my @parts = split /,/, $last;
      exit 1 unless @parts == $each;
      for my $p (@parts) { exit 1 if $p >= $total || $seen{$p}++; }
      $covered += @parts;
    }
    exit(($covered == $total && keys(%seen) == $total) ? 0 : 1);
  ' "$TOPIC" "$TOPIC_PARTITIONS" "$each" "$@"
}

# Every value in the file must appear exactly once (mode=once) or at least
# once (mode=any) across the given consumer logs.
values_consumed_probe() {
  local mode="$1" values_file="$2"
  shift 2
  perl -e '
    use strict; use warnings;
    my ($mode, $values_file, @logs) = @ARGV;
    open my $vf, q{<}, $values_file or die $!;
    my %count = map { chomp; $_ => 0 } grep { /\S/ } <$vf>;
    for my $log (@logs) {
      open my $fh, q{<}, $log or next;
      while (<$fh>) {
        $count{$1}++ if /^partition=\d+ offset=\d+ key=.*? value=(\S+)\s*$/ && exists $count{$1};
      }
    }
    for my $n (values %count) {
      exit 1 if $mode eq "once" ? $n != 1 : $n < 1;
    }
    exit 0;
  ' "$mode" "$values_file" "$@"
}

records_seen_probe() {
  local minimum="$1"
  shift
  perl -e '
    use strict; use warnings;
    my ($min, @logs) = @ARGV;
    my $n = 0;
    for my $log (@logs) {
      open my $fh, q{<}, $log or next;
      $n += grep { /^partition=\d+ offset=\d+ key=/ } <$fh>;
    }
    exit($n >= $min ? 0 : 1);
  ' "$minimum" "$@"
}

# Redelivery is only legal inside the kill window: every value consumed more
# than once must have been consumed by the killed member before it died.
assert_dups_from_victim() {
  local description="$1" victim_log="$2"
  shift 2
  if ! perl -e '
    use strict; use warnings;
    my ($victim, @logs) = @ARGV;
    my %victim_values;
    open my $fh, q{<}, $victim or die "missing victim log $victim\n";
    while (<$fh>) { $victim_values{$1} = 1 if /^partition=\d+ offset=\d+ key=.*? value=(\S+)\s*$/; }
    close $fh;
    my %count;
    for my $log ($victim, @logs) {
      open my $lf, q{<}, $log or next;
      while (<$lf>) { $count{$1}++ if /^partition=\d+ offset=\d+ key=.*? value=(\S+)\s*$/; }
      close $lf;
    }
    for my $value (keys %count) {
      if ($count{$value} > 1 && !$victim_values{$value}) {
        die "duplicate $value was never consumed by the killed member\n";
      }
    }
    exit 0;
  ' "$victim_log" "$@"; then
    die "assertion failed: $description"
  fi
  pass "$description"
}

# None of the values in the file may appear in the log (used to prove a
# resumed consumer did not rewind to earliest).
assert_values_absent() {
  local description="$1" values_file="$2" log="$3"
  if ! perl -e '
    use strict; use warnings;
    my ($values_file, $log) = @ARGV;
    open my $vf, q{<}, $values_file or die $!;
    my %old = map { chomp; $_ => 1 } grep { /\S/ } <$vf>;
    open my $lf, q{<}, $log or exit 0;
    while (<$lf>) {
      if (/^partition=\d+ offset=\d+ key=.*? value=(\S+)\s*$/ && $old{$1}) {
        die "pre-failover record $1 was redelivered after the committed resume\n";
      }
    }
    exit 0;
  ' "$values_file" "$log"; then
    die "assertion failed: $description"
  fi
  pass "$description"
}

# A foreground consume run must print exactly the values in the file, each
# once, with every record offset >= min_offset.
assert_resume_run() {
  local description="$1" run_log="$2" values_file="$3" min_offset="$4"
  if ! perl -e '
    use strict; use warnings;
    my ($run, $values_file, $min_offset) = @ARGV;
    open my $vf, q{<}, $values_file or die $!;
    my %want = map { chomp; $_ => 1 } grep { /\S/ } <$vf>;
    my (%seen, $records);
    $records = 0;
    open my $rf, q{<}, $run or die "missing run log $run\n";
    while (<$rf>) {
      next unless /^partition=(\d+) offset=(\d+) key=.*? value=(\S+)\s*$/;
      my ($p, $o, $v) = ($1, $2, $3);
      die "run consumed offset $o below committed position $min_offset on partition $p\n" if $o < $min_offset;
      die "run consumed unexpected value $v\n" unless $want{$v};
      die "run consumed $v twice\n" if $seen{$v}++;
      $records++;
    }
    die "run consumed $records records, expected " . scalar(keys %want) . "\n" if $records != keys %want;
    exit 0;
  ' "$run_log" "$values_file" "$min_offset"; then
    die "assertion failed: $description"
  fi
  pass "$description"
}

new_values_file() {
  local path="$1" prefix="$2" count="$3"
  perl -e '
    use strict; use warnings;
    my ($path, $prefix, $count) = @ARGV;
    open my $out, q{>}, $path or die $!;
    for my $i (0 .. $count - 1) { printf {$out} "%s-%04d\n", $prefix, $i }
  ' "$path" "$prefix" "$count"
}

produce_values() {
  local seed="$1" file="$2" count="$3" topic="${4:-$TOPIC}" output
  output="$(cli --broker "$(broker_address "$seed")" produce --topic "$topic" \
    --file "$file" --acks all --timeout-ms 60000)"
  [[ "$output" == *"produced $count records"* ]] || die "produce failed: $output"
  printf '%s' "$output"
}

# crc32c(group_id) % offsets-partitions, matching the broker/client routing.
offsets_partition_for() {
  perl -e '
    my ($group, $mod) = @ARGV;
    sub crc32c {
      my ($data) = @_;
      my $crc = 0xffffffff;
      for my $byte (unpack(q{C*}, $data)) {
        $crc ^= $byte;
        for (1..8) {
          $crc = ($crc & 1) ? (($crc >> 1) ^ 0x82f63b78) : ($crc >> 1);
          $crc &= 0xffffffff;
        }
      }
      return ($crc ^ 0xffffffff) & 0xffffffff;
    }
    print crc32c($group) % $mod;
  ' "$1" "$OFFSETS_TOPIC_PARTITIONS"
}

partition_leader() {
  controller_get "$1" /api/v1/controller/metadata 2>/dev/null | perl -MJSON::PP -0777 -e '
    my $d = decode_json(<STDIN>);
    my $p = $d->{topics}{$ARGV[0]}{partitions}{$ARGV[1]} or exit 1;
    print $p->{leader};
  ' "$2" "$3"
}

# The server creates __consumer_offsets as soon as its first node registers,
# possibly with RF=1 when the rest of the cluster has not registered yet.
# Before any group traffic, pin it to the full RF=3 layout by deleting and
# recreating it until the metadata image converges.
pin_offsets_topic() {
  local nodes_csv="$1" controller="$2" deadline=$((SECONDS + TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    if metadata_probe_all "$nodes_csv" topic_ready "$OFFSETS_TOPIC" "$OFFSETS_TOPIC_PARTITIONS" 3; then
      return 0
    fi
    cli_raw --broker invalid-admin-broker --controller "http://127.0.0.1:${CONTROL_PORT[$controller]}" \
      topic delete --name "$OFFSETS_TOPIC"
    cli_raw --broker invalid-admin-broker --controller "http://127.0.0.1:${CONTROL_PORT[$controller]}" \
      topic create --name "$OFFSETS_TOPIC" --partitions "$OFFSETS_TOPIC_PARTITIONS" --replication-factor 3
    sleep 0.5
  done
  die "could not pin $OFFSETS_TOPIC to $OFFSETS_TOPIC_PARTITIONS RF=3 partitions"
}

show_diagnostics() {
  printf '\n\033[33mVerification diagnostics\033[0m\n' >&2
  printf 'Artifacts: %s\nCluster: %s\n' "$WORK_DIR" "$CLUSTER_ID" >&2
  local node id
  for node in $(seq 1 "$NODE_COUNT"); do
    [[ -n "${NODE_PID[$node]:-}" ]] || continue
    printf 'node %s pid=%s status=%s\n' "$node" "${NODE_PID[$node]}" "$(node_alive "$node" && echo running || echo stopped)" >&2
    for file in "${NODE_STDOUT[$node]}" "${NODE_STDERR[$node]}"; do
      printf '  tail %s\n' "$file" >&2
      [[ -f "$file" ]] && tail -n 100 "$file" >&2 || true
    done
  done
  for id in "${!CONSUMER_PID[@]}"; do
    printf 'consumer %s pid=%s status=%s\n' "$id" "${CONSUMER_PID[$id]}" \
      "$(kill -0 "${CONSUMER_PID[$id]}" 2>/dev/null && echo running || echo stopped)" >&2
    for file in "${CONSUMER_LOG[$id]}" "${CONSUMER_ERR[$id]}"; do
      printf '  tail %s\n' "$file" >&2
      [[ -f "$file" ]] && tail -n 60 "$file" >&2 || true
    done
  done
}

cleanup() {
  local status=$?
  trap - EXIT
  if (( ! SUCCESS )); then
    show_diagnostics || true
  fi
  local node id cleanup_failed=0
  for id in "${!CONSUMER_PID[@]}"; do
    stop_consumer "$id" || cleanup_failed=1
  done
  for node in $(seq 1 "$NODE_COUNT"); do stop_node "$node" || cleanup_failed=1; done
  if (( cleanup_failed )); then
    printf 'one or more child processes could not be terminated; retaining artifacts\n' >&2
    SUCCESS=0
    status=1
  fi
  if (( SUCCESS )); then
    if work_dir_safe; then
      rm -rf -- "$WORK_DIR"
    else
      printf 'refusing to remove unexpected temp path: %s\n' "$WORK_DIR" >&2
      status=1
    fi
  else
    printf '\033[33mVerification artifacts retained at: %s\033[0m\n' "$WORK_DIR" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

main() {
  work_dir_safe || die "mktemp returned an unsafe or unexpected work directory: $WORK_DIR"
  (( TIMEOUT_SECONDS >= 20 )) || die "TIMEOUT_SECONDS must be at least 20"
  (( HEARTBEAT_INTERVAL_MS >= 50 )) || die "HEARTBEAT_INTERVAL_MS must be at least 50"
  (( SESSION_TIMEOUT_MS > HEARTBEAT_INTERVAL_MS * 2 )) || die "SESSION_TIMEOUT_MS must exceed twice heartbeat"
  (( SEGMENT_BYTES >= 4096 )) || die "SEGMENT_BYTES must be at least 4096"
  (( OFFSETS_TOPIC_PARTITIONS >= 1 )) || die "OFFSETS_TOPIC_PARTITIONS must be positive"
  (( TOPIC_PARTITIONS >= 3 && TOPIC_PARTITIONS % 3 == 0 )) || die "TOPIC_PARTITIONS must be a positive multiple of three"
  (( COMMIT_INTERVAL_MS >= 100 )) || die "COMMIT_INTERVAL_MS must be at least 100"
  (( CLI_WALL_TIMEOUT_SECONDS >= 60 )) || die "CLI_WALL_TIMEOUT_SECONDS must be at least 60"
  (( CONSUMER_WALL_TIMEOUT_SECONDS >= 120 )) || die "CONSUMER_WALL_TIMEOUT_SECONDS must cover every consumer stage"
  (( BUILD_WALL_TIMEOUT_SECONDS >= 60 )) || die "BUILD_WALL_TIMEOUT_SECONDS must be at least 60"

  cd "$ROOT"
  stage "Build the actual combined server and CLI binaries"
  timeout --kill-after=10s "${BUILD_WALL_TIMEOUT_SECONDS}s" cargo build -p brahmaputra-server -p brahmaputra-cli
  assert "server binary exists" test -f "$SERVER_EXE"
  assert "CLI binary exists" test -f "$CLI_EXE"

  local node peer port
  declare -A used_ports=()
  for node in $(seq 1 "$NODE_COUNT"); do
    while :; do port="$(allocate_port)"; [[ -z "${used_ports[$port]:-}" ]] && break; done
    used_ports[$port]=1; DATA_PORT[$node]="$port"
    while :; do port="$(allocate_port)"; [[ -z "${used_ports[$port]:-}" ]] && break; done
    used_ports[$port]=1; CONTROL_PORT[$node]="$port"
  done
  (( ${#used_ports[@]} == NODE_COUNT * 2 )) || die "port allocation did not produce ten unique endpoints"
  pass "all ten data/control ports are unique"

  stage "Launch five combined nodes and bootstrap their fixed Raft quorum"
  for node in $(seq 1 "$NODE_COUNT"); do start_node "$node"; done
  wait_http_ready 1 2 3 4 5
  pass "all five controller HTTP endpoints became ready"
  local bootstrap controller_leader
  bootstrap="$(controller_post 1 /api/v1/controller/bootstrap)"
  printf '%s' "$bootstrap" | perl -MJSON::PP -0777 -e '$d=decode_json(<STDIN>); exists $d->{Ok} or die "bootstrap rejected\n"'
  pass "five-member controller quorum bootstrapped through HTTP"
  controller_leader="$(wait_shared_leader 1,2,3,4,5)"
  pass "all five controllers agreed on Raft leader $controller_leader"
  wait_metadata 1,2,3,4,5 "all five broker registrations" brokers_alive 1,2,3,4,5
  pass "all five combined nodes registered as live brokers"

  stage "Internal __consumer_offsets topic is auto-created and pinned to RF=3"
  wait_metadata 1,2,3,4,5 "server-created internal offsets topic" topic_exists "$OFFSETS_TOPIC"
  pass "the cluster auto-created $OFFSETS_TOPIC at startup"
  local metadata_output
  metadata_output="$(cli --broker "$(broker_address 1)" metadata)"
  [[ "$metadata_output" == *"topic \"$OFFSETS_TOPIC\" (error_code=0)"* ]] || die "metadata response omitted $OFFSETS_TOPIC: $metadata_output"
  pass "Metadata API exposes $OFFSETS_TOPIC to clients"
  pin_offsets_topic 1,2,3,4,5 "$controller_leader"
  pass "$OFFSETS_TOPIC converged to $OFFSETS_TOPIC_PARTITIONS RF=3 partitions before group traffic"

  stage "Create the six-partition RF=3 group test topic"
  local create_controller=1 create_output
  [[ "$create_controller" == "$controller_leader" ]] && create_controller=2
  create_output="$(cli --broker deliberately-invalid-broker-address --controller "http://127.0.0.1:${CONTROL_PORT[$create_controller]}" \
    topic create --name "$TOPIC" --partitions "$TOPIC_PARTITIONS" --replication-factor 3)"
  [[ "$create_output" == *"topic created name=\"$TOPIC\" partitions=$TOPIC_PARTITIONS replication_factor=3"* ]] || die "unexpected topic-create output: $create_output"
  pass "CLI created the configured RF=3 test topic through a nonleader controller"
  wait_metadata 1,2,3,4,5 "six-partition assignment with full leadership" topic_ready "$TOPIC" "$TOPIC_PARTITIONS" 3
  pass "all $TOPIC_PARTITIONS partitions have leaders and three replicas"

  stage "Three consumers in one group each own exactly two partitions"
  local run_nonce="$(nonce)" c1 c2 c3
  start_consumer 1 "$GROUP_SPREAD"; c1="$LAST_CONSUMER_ID"
  start_consumer 2 "$GROUP_SPREAD"; c2="$LAST_CONSUMER_ID"
  start_consumer 3 "$GROUP_SPREAD"; c3="$LAST_CONSUMER_ID"
  wait_until "three-way rebalance to settle into disjoint partition pairs" "$TIMEOUT_SECONDS" 0.5 \
    assignment_split_probe 2 "$(consumer_log "$c1")" "$(consumer_log "$c2")" "$(consumer_log "$c3")"
  pass "each of the three consumers owns exactly two partitions, disjoint and complete"

  local spread_values="$WORK_DIR/values-spread.txt"
  new_values_file "$spread_values" "spread-$run_nonce" 30
  produce_values 4 "$spread_values" 30 >/dev/null
  pass "produced 30 unique records across the six partitions"
  wait_until "every spread record consumed exactly once across the group" "$TIMEOUT_SECONDS" 0.5 \
    values_consumed_probe once "$spread_values" "$(consumer_log "$c1")" "$(consumer_log "$c2")" "$(consumer_log "$c3")"
  pass "all 30 records were consumed exactly once with a stable assignment"

  stage "Hard-kill one consumer mid-stream; survivors rebalance and resume its partitions"
  local pre_kill_values="$WORK_DIR/values-kill-pre.txt" post_kill_values="$WORK_DIR/values-kill-post.txt" all_kill_values="$WORK_DIR/values-kill-all.txt"
  new_values_file "$pre_kill_values" "kill-pre-$run_nonce" 36
  produce_values 5 "$pre_kill_values" 36 >/dev/null
  pass "produced 36 pre-kill records"
  wait_until "the group started draining the pre-kill batch" 60 0.2 \
    records_seen_probe 6 "$(consumer_log "$c1")" "$(consumer_log "$c2")" "$(consumer_log "$c3")"
  stop_consumer "$c2"
  pass "consumer $c2 was forcibly terminated mid-stream"
  new_values_file "$post_kill_values" "kill-post-$run_nonce" 24
  produce_values 1 "$post_kill_values" 24 >/dev/null
  pass "produced 24 more records into the rebalancing group"
  wait_until "surviving consumers rebalance to three partitions each" "$TIMEOUT_SECONDS" 0.5 \
    assignment_split_probe 3 "$(consumer_log "$c1")" "$(consumer_log "$c3")"
  pass "dead consumer's partitions were redistributed within the session timeout"
  cat "$pre_kill_values" "$post_kill_values" >"$all_kill_values"
  wait_until "every record consumed at least once across the group" "$TIMEOUT_SECONDS" 0.5 \
    values_consumed_probe any "$all_kill_values" "$(consumer_log "$c1")" "$(consumer_log "$c2")" "$(consumer_log "$c3")"
  pass "all 60 records were consumed at least once despite the mid-stream kill"
  assert_dups_from_victim "redelivered records all come from the killed member's uncommitted tail (no rewind past committed offsets)" \
    "$(consumer_log "$c2")" "$(consumer_log "$c1")" "$(consumer_log "$c3")"
  stop_consumer "$c1"
  stop_consumer "$c3"

  stage "Kill the coordinator broker; the group rejoins with committed offsets intact"
  local coordinator_partition coordinator fresh_consumer
  coordinator_partition="$(offsets_partition_for "$GROUP_FAILOVER")"
  coordinator="$(partition_leader "$controller_leader" "$OFFSETS_TOPIC" "$coordinator_partition")"
  [[ -n "$coordinator" ]] || die "cannot resolve the leader of $OFFSETS_TOPIC-$coordinator_partition"
  pass "group $GROUP_FAILOVER maps to $OFFSETS_TOPIC-$coordinator_partition led by broker $coordinator"
  start_consumer "$coordinator" "$GROUP_FAILOVER"; c1="$LAST_CONSUMER_ID"
  wait_until "single consumer to hold all six partitions" "$TIMEOUT_SECONDS" 0.5 \
    assignment_split_probe 6 "$(consumer_log "$c1")"
  pass "group stabilized on coordinator broker $coordinator"
  local failover_values="$WORK_DIR/values-failover.txt"
  new_values_file "$failover_values" "coord-$run_nonce" 18
  produce_values 2 "$failover_values" 18 >/dev/null
  wait_until "pre-failover records consumed exactly once" "$TIMEOUT_SECONDS" 0.5 \
    values_consumed_probe once "$failover_values" "$(consumer_log "$c1")"
  sleep 2 # at least two auto-commit intervals, so every position is committed
  pass "consumer positions were committed to $OFFSETS_TOPIC before the failover"
  stop_consumer "$c1"
  stop_node "$coordinator"
  node_alive "$coordinator" && die "coordinator $coordinator survived kill -9"
  pass "coordinator broker $coordinator was forcibly terminated"
  local live_nodes
  live_nodes="$(printf '1\n2\n3\n4\n5\n' | grep -v "^$coordinator$" | paste -sd, -)"
  wait_shared_leader "$live_nodes" >/dev/null
  wait_metadata "$live_nodes" "offsets partition failover away from broker $coordinator" \
    partition_leader_moved "$OFFSETS_TOPIC" "$coordinator_partition" "$coordinator"
  pass "controller moved $OFFSETS_TOPIC-$coordinator_partition to a surviving broker"
  start_consumer "${live_nodes%%,*}" "$GROUP_FAILOVER"; fresh_consumer="$LAST_CONSUMER_ID"
  wait_until "fresh consumer rejoins and takes all six partitions on the new coordinator" "$TIMEOUT_SECONDS" 0.5 \
    assignment_split_probe 6 "$(consumer_log "$fresh_consumer")"
  pass "group rejoined on the new coordinator after the old member's session expired"
  local post_failover_values="$WORK_DIR/values-post-failover.txt"
  new_values_file "$post_failover_values" "coord-post-$run_nonce" 6
  produce_values 3 "$post_failover_values" 6 >/dev/null
  wait_until "post-failover records consumed exactly once" "$TIMEOUT_SECONDS" 0.5 \
    values_consumed_probe once "$post_failover_values" "$(consumer_log "$fresh_consumer")"
  assert_values_absent "consumption resumed from committed offsets, not earliest (no pre-failover redelivery)" \
    "$failover_values" "$(consumer_log "$fresh_consumer")"
  stop_consumer "$fresh_consumer"

  stage "A bounded consume run resumes from the previous run's committed offsets"
  # Dedicated topic: the shared test topic carries records from every prior
  # scenario, and a brand-new group reading from earliest must see only the
  # records this scenario produces.
  local resume_topic="$TOPIC-resume"
  cli --broker deliberately-invalid-broker-address --controller "http://127.0.0.1:${CONTROL_PORT[$create_controller]}" \
    topic create --name "$resume_topic" --partitions "$TOPIC_PARTITIONS" --replication-factor 3 >/dev/null
  wait_metadata 2,3,4,5 "resume topic assignment with full leadership" topic_ready "$resume_topic" "$TOPIC_PARTITIONS" 3
  local resume_first_values="$WORK_DIR/values-resume-first.txt" resume_second_values="$WORK_DIR/values-resume-second.txt"
  local run_dir="$WORK_DIR/runs"
  mkdir -p "$run_dir"
  new_values_file "$resume_first_values" "resume-a-$run_nonce" 12
  produce_values 4 "$resume_first_values" 12 "$resume_topic" >/dev/null
  cli --broker "$(broker_address 5)" consume --topic "$resume_topic" --group "$GROUP_RESUME" \
    --max 12 --commit-interval-ms "$COMMIT_INTERVAL_MS" >"$run_dir/resume-first.log" 2>&1
  assert_resume_run "first bounded run consumed and committed the initial 12 records" \
    "$run_dir/resume-first.log" "$resume_first_values" 0
  new_values_file "$resume_second_values" "resume-b-$run_nonce" 6
  produce_values 2 "$resume_second_values" 6 "$resume_topic" >/dev/null
  cli --broker "$(broker_address 2)" consume --topic "$resume_topic" --group "$GROUP_RESUME" \
    --max 100 --commit-interval-ms "$COMMIT_INTERVAL_MS" >"$run_dir/resume-second.log" 2>&1
  assert_resume_run "second run resumed at the committed position (offset 2 per partition) and consumed only the 6 new records" \
    "$run_dir/resume-second.log" "$resume_second_values" 2

  stage "M4 live verification complete"
  printf 'offsets topic: %s partitions (RF=3), group coordinator for %s: %s-%s\nchecks passed: %s\n' \
    "$OFFSETS_TOPIC_PARTITIONS" "$GROUP_FAILOVER" "$OFFSETS_TOPIC" "$coordinator_partition" "$CHECKS"
  SUCCESS=1
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
