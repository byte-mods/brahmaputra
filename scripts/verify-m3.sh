#!/usr/bin/env bash
# M3 authoritative live verification. Requires Git Bash on Windows.
#
# Five combined nodes provide a five-member controller quorum. The tested
# partition is RF=3 on brokers 1/2/3; brokers 4/5 are controller witnesses,
# allowing real two-replica hard failures without sacrificing Raft quorum.

set -Eeuo pipefail

TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
HEARTBEAT_INTERVAL_MS="${HEARTBEAT_INTERVAL_MS:-500}"
SESSION_TIMEOUT_MS="${SESSION_TIMEOUT_MS:-5000}"
STORM_PROCESSES="${STORM_PROCESSES:-64}"
# On very fast loopback hosts every tiny write can finish before the harness
# samples it. Opt in to a deterministic in-flight window by briefly stopping
# one live ISR follower after three writes have been acknowledged.
STORM_GATE="${STORM_GATE:-0}"
EXTENDED_DOWNTIME_SECONDS="${EXTENDED_DOWNTIME_SECONDS:-600}"
EXTENDED_PRODUCE_INTERVAL_MS="${EXTENDED_PRODUCE_INTERVAL_MS:-1000}"
EXTENDED_BACKLOG_RECORDS="${EXTENDED_BACKLOG_RECORDS:-1024}"
EXTENDED_VALUE_BYTES="${EXTENDED_VALUE_BYTES:-32768}"
SEGMENT_BYTES="${SEGMENT_BYTES:-1048576}"
CLI_WALL_TIMEOUT_SECONDS="${CLI_WALL_TIMEOUT_SECONDS:-180}"
CAPTURE_WALL_TIMEOUT_SECONDS="${CAPTURE_WALL_TIMEOUT_SECONDS:-40}"
BUILD_WALL_TIMEOUT_SECONDS="${BUILD_WALL_TIMEOUT_SECONDS:-600}"

NODE_COUNT=5
TOPIC="replication-live"
PARTITION=0
CHECKS=0
SUCCESS=0
CAPTURE_SEQUENCE=0
LAST_CAPTURE_ID=""

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-m3.XXXXXX")"
CLUSTER_ID="m3-live-$(perl -e 'printf "%08x%04x", time, int(rand(65536))')"
SERVER_EXE="$ROOT/target/debug/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/debug/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/debug/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/debug/brahmaputra-cli"
ACK_JOURNAL="$WORK_DIR/acknowledged.tsv"
: >"$ACK_JOURNAL"

declare -A DATA_PORT CONTROL_PORT NODE_PID NODE_DATA NODE_STDOUT NODE_STDERR NODE_RESTARTS
declare -A CAP_PID CAP_OUT CAP_ERR CAP_VALUE
CURRENT_STORM_IDS=()
STORM_ACK_OFFSETS=()
STORM_ACK_VALUES=()
STORM_BLOCKER=""

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

work_dir_safe() {
  local parent base
  [[ -d "$WORK_DIR" && "$WORK_DIR" != / ]] || return 1
  parent="$(cd "$(dirname "$WORK_DIR")" && pwd -P)" || return 1
  base="${WORK_DIR##*/}"
  [[ "$parent" == "$TEMP_ROOT" && "$base" =~ ^brahmaputra-m3\.[[:alnum:]]{6}$ ]]
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

stop_capture() {
  local pid="$1" deadline
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
  kill -0 "$pid" 2>/dev/null && { die "unable to terminate capture process $pid"; return 1; }
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
    my ($mode, $topic, $part, @a) = @ARGV;
    local $/;
    my $d = eval { decode_json(<STDIN>) } or exit 1;
    my $p = $d->{topics}{$topic}{partitions}{$part};
    my $csv = sub { join q{,}, sort {$a <=> $b} map { 0+$_ } @_ };
    if ($mode eq "brokers_alive") {
      my @ids = split /,/, $a[0];
      exit((keys %{$d->{brokers}}) == @ids && !grep { !$d->{brokers}{$_}{alive} } @ids ? 0 : 1);
    }
    if ($mode eq "topic_full") {
      exit($p && @{$p->{replicas}} == $a[0] && @{$p->{isr}} == $a[1] ? 0 : 1);
    }
    if ($mode eq "kill_failover") {
      my ($dead, $count) = @a;
      exit($p && !$d->{brokers}{$dead}{alive} && $p->{leader} != $dead && @{$p->{isr}} == $count &&
           !grep({$_ == $dead} @{$p->{isr}}) && grep({$_ == $p->{leader}} @{$p->{isr}}) ? 0 : 1);
    }
    if ($mode eq "dead_isr") {
      my ($dead, $count) = @a;
      exit($p && !$d->{brokers}{$dead}{alive} && @{$p->{isr}} == $count && !grep({$_ == $dead} @{$p->{isr}}) ? 0 : 1);
    }
    if ($mode eq "leader_isr") {
      my ($leader, $wanted) = @a;
      exit($p && $p->{leader} == $leader && $csv->(@{$p->{isr}}) eq $wanted ? 0 : 1);
    }
    if ($mode eq "alive_isr") {
      my ($broker, $wanted) = @a;
      exit($p && $d->{brokers}{$broker}{alive} && $csv->(@{$p->{isr}}) eq $wanted ? 0 : 1);
    }
    if ($mode eq "two_dead_isr") {
      my ($leader, $dead_csv) = @a;
      my @dead = split /,/, $dead_csv;
      exit($p && $p->{leader} == $leader && @{$p->{isr}} == 1 && $p->{isr}[0] == $leader &&
           !grep({$d->{brokers}{$_}{alive}} @dead) ? 0 : 1);
    }
    if ($mode eq "topic_exists") {
      exit(exists $d->{topics}{$a[0]} ? 0 : 1);
    }
    exit 1;
  ' "$mode" "$TOPIC" "$PARTITION" "$@"
}

WAIT_METADATA=""
metadata_probe_all() {
  local nodes_csv="$1" mode="$2"
  shift 2
  local -a nodes
  IFS=',' read -r -a nodes <<<"$nodes_csv"
  local first="" json node
  for node in "${nodes[@]}"; do
    json="$(controller_get "$node" /api/v1/controller/metadata 2>/dev/null || true)"
    [[ -n "$json" ]] || return 1
    printf '%s' "$json" | meta_check "$mode" "$@" || return 1
    [[ -z "$first" ]] && first="$json"
  done
  WAIT_METADATA="$first"
}

wait_metadata() {
  local nodes_csv="$1" description="$2" mode="$3"
  shift 3
  wait_until "$description" "$TIMEOUT_SECONDS" 0.1 metadata_probe_all "$nodes_csv" "$mode" "$@"
}

partition_info() {
  printf '%s' "$1" | perl -MJSON::PP -0777 -e '
    $d=decode_json(<STDIN>); $p=$d->{topics}{$ARGV[0]}{partitions}{$ARGV[1]};
    print join q{|}, $p->{leader}, $p->{leader_epoch}, join(q{,}, @{$p->{replicas}}), join(q{,}, @{$p->{isr}});
  ' "$TOPIC" "$PARTITION"
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

acks_all_produce() {
  local seed="$1" value="$2" timeout_ms="${3:-30000}"
  cli --broker "$(broker_address "$seed")" produce --topic "$TOPIC" --partition "$PARTITION" \
    --value "$value" --acks all --timeout-ms "$timeout_ms"
}

explicit_produce() {
  local seed="$1" value="$2" producer_id="$3" producer_epoch="$4" base_sequence="$5"
  cli --broker "$(broker_address "$seed")" produce --topic "$TOPIC" --partition "$PARTITION" \
    --value "$value" --acks all --timeout-ms 30000 --producer-id "$producer_id" \
    --producer-epoch "$producer_epoch" --base-sequence "$base_sequence"
}

explicit_produce_raw() {
  local seed="$1" value="$2" producer_id="$3" producer_epoch="$4" base_sequence="$5"
  cli_raw --broker "$(broker_address "$seed")" produce --topic "$TOPIC" --partition "$PARTITION" \
    --value "$value" --acks all --timeout-ms 30000 --producer-id "$producer_id" \
    --producer-epoch "$producer_epoch" --base-sequence "$base_sequence"
}

parse_producer_identity() {
  printf '%s\n' "$1" | perl -ne '
    if (/^producer_id=(\d+) producer_epoch=(\d+)$/) { print "$1|$2"; $found=1; last }
    END { exit($found ? 0 : 1) }
  '
}

parse_explicit_ack() {
  printf '%s\n' "$1" | perl -ne '
    if (/^acked offset=(\d+) producer_id=(\d+) producer_epoch=(\d+) base_sequence=(\d+)$/) {
      print join(q{|},$1,$2,$3,$4); $found=1; last
    }
    END { exit($found ? 0 : 1) }
  '
}

get_offsets() {
  local output
  output="$(cli --broker "$(broker_address "$1")" offsets --topic "$TOPIC" --partition "$PARTITION")"
  printf '%s\n' "$output" | perl -e '
    my ($topic,$part)=@ARGV;
    while (<STDIN>) { if (/^\Q$topic\E-$part: earliest=(\d+) latest=(\d+)$/) { print "$1|$2"; $ok=1 } }
    END { exit($ok ? 0 : 1) }
  ' "$TOPIC" "$PARTITION"
}

start_capture() {
  local seed="$1" value="$2" label="$3"
  CAPTURE_SEQUENCE=$((CAPTURE_SEQUENCE + 1))
  local id="$CAPTURE_SEQUENCE" dir="$WORK_DIR/captures"
  mkdir -p "$dir"
  local stem
  stem="$(printf '%s-%04d' "$label" "$id")"
  local out="$dir/$stem.stdout.log" err="$dir/$stem.stderr.log"
  timeout --kill-after=2s "${CAPTURE_WALL_TIMEOUT_SECONDS}s" "$CLI_EXE" \
    --broker "$(broker_address "$seed")" produce --topic "$TOPIC" --partition "$PARTITION" \
    --value "$value" --acks all --timeout-ms 30000 >"$out" 2>"$err" &
  CAP_PID[$id]=$!
  CAP_OUT[$id]="$out"
  CAP_ERR[$id]="$err"
  CAP_VALUE[$id]="$value"
  LAST_CAPTURE_ID="$id"
}

start_storm() {
  local seed="$1" label="$2" nonce
  nonce="$(perl -e 'printf "%08x", int(rand(0xffffffff))')"
  CURRENT_STORM_IDS=()
  local i value
  for ((i=0; i<STORM_PROCESSES; i++)); do
    value="$(printf '%s-%s-%04d' "$label" "$nonce" "$i")"
    start_capture "$seed" "$value" "$label"
    CURRENT_STORM_IDS+=("$LAST_CAPTURE_ID")
    if [[ "$STORM_GATE" == "1" && "$i" == "2" ]]; then
      wait_until "three pre-gate storm acknowledgements" 30 0.02 storm_acked_at_least 3
      local candidate
      STORM_BLOCKER=""
      for candidate in $(printf '%s' "$replicas" | tr ',' '\n'); do
        if [[ "$candidate" != "$leader" ]] && node_alive "$candidate"; then
          STORM_BLOCKER="$candidate"
          break
        fi
      done
      [[ -n "$STORM_BLOCKER" ]] || die "no live ISR follower available for deterministic storm gate"
      kill -STOP "${NODE_PID[$STORM_BLOCKER]}"
    fi
  done
}

storm_acked_at_least() {
  local wanted="$1" acked=0 id
  for id in "${CURRENT_STORM_IDS[@]}"; do
    if grep -Eq '^acked offset=[0-9]+$' "${CAP_OUT[$id]}" 2>/dev/null; then
      acked=$((acked + 1))
    fi
  done
  (( acked >= wanted ))
}

resume_storm_blocker() {
  if [[ -n "$STORM_BLOCKER" ]] && node_alive "$STORM_BLOCKER"; then
    kill -CONT "${NODE_PID[$STORM_BLOCKER]}" 2>/dev/null || true
  fi
  STORM_BLOCKER=""
}

storm_armed_probe() {
  local acked=0 running=0 id pid
  for id in "${CURRENT_STORM_IDS[@]}"; do
    pid="${CAP_PID[$id]}"
    if kill -0 "$pid" 2>/dev/null; then
      running=$((running + 1))
    elif grep -Eq '^acked offset=[0-9]+$' "${CAP_OUT[$id]}" 2>/dev/null; then
      acked=$((acked + 1))
    fi
  done
  if (( acked >= 3 && running >= 2 )); then
    STORM_ARMED_ACKED="$acked"
    STORM_ARMED_RUNNING="$running"
    return 0
  fi
  return 1
}

wait_storm_armed() {
  wait_until "produce storm to have acknowledged and in-flight calls" 30 0.02 storm_armed_probe
}

complete_storm() {
  STORM_ACK_OFFSETS=()
  STORM_ACK_VALUES=()
  local failures=0 id pid status output offset active
  local deadline=$((SECONDS + CAPTURE_WALL_TIMEOUT_SECONDS + 5))
  while (( SECONDS < deadline )); do
    active=0
    for id in "${CURRENT_STORM_IDS[@]}"; do
      kill -0 "${CAP_PID[$id]}" 2>/dev/null && active=$((active + 1))
    done
    (( active == 0 )) && break
    sleep 0.1
  done
  for id in "${CURRENT_STORM_IDS[@]}"; do
    pid="${CAP_PID[$id]}"
    if kill -0 "$pid" 2>/dev/null; then
      stop_capture "$pid"
    fi
  done
  for id in "${CURRENT_STORM_IDS[@]}"; do
    pid="${CAP_PID[$id]}"
    set +e
    wait "$pid"
    status=$?
    set -e
    output="$(cat "${CAP_OUT[$id]}" "${CAP_ERR[$id]}" 2>/dev/null || true)"
    offset="$(printf '%s\n' "$output" | sed -n 's/^acked offset=\([0-9][0-9]*\)$/\1/p' | head -n1)"
    if (( status == 0 )) && [[ -n "$offset" ]]; then
      STORM_ACK_OFFSETS+=("$offset")
      STORM_ACK_VALUES+=("${CAP_VALUE[$id]}")
      printf '%s\t%s\n' "$offset" "${CAP_VALUE[$id]}" >>"$ACK_JOURNAL"
    else
      failures=$((failures + 1))
    fi
  done
  printf 'storm outcomes: acknowledged=%s failed-or-ambiguous=%s\n' "${#STORM_ACK_OFFSETS[@]}" "$failures"
  (( ${#STORM_ACK_OFFSETS[@]} >= 3 )) || die "storm retained fewer than three explicit acknowledgements"
  pass "produce storm retained at least three explicit acks across the hard kill"
}

assert_ack_continuity() {
  local seed="$1" description="$2"
  local offsets earliest latest consume_file
  offsets="$(get_offsets "$seed")"
  IFS='|' read -r earliest latest <<<"$offsets"
  assert_eq "$earliest" "0" "$description retains offset zero"
  (( latest > 0 )) || die "$description has an empty committed prefix"
  pass "$description exposes a nonempty committed prefix"
  consume_file="$WORK_DIR/consume-${description//[^A-Za-z0-9]/_}.log"
  cli --broker "$(broker_address "$seed")" consume --topic "$TOPIC" --partition "$PARTITION" \
    --from earliest --max "$latest" >"$consume_file"
  perl -e '
    my ($latest, $acks, $records) = @ARGV;
    open my $rf, q{<}, $records or die $!;
    my (@values, %counts);
    while (<$rf>) {
      /^partition=\d+ offset=(\d+) key=.*? value=(.*)$/ or next;
      die "offset gap/duplicate: expected ".scalar(@values)." got $1\n" if $1 != @values;
      push @values, $2; $counts{$2}++;
    }
    die "record count ".scalar(@values)." != latest $latest\n" if @values != $latest;
    open my $af, q{<}, $acks or die $!;
    while (<$af>) {
      chomp; my ($offset, $value) = split /\t/, $_, 2;
      die "ack $offset beyond latest $latest\n" if $offset >= $latest;
      die "ack value mismatch at $offset\n" if $values[$offset] ne $value;
      die "ack value not unique at $offset\n" if $counts{$value} != 1;
    }
  ' "$latest" "$ACK_JOURNAL" "$consume_file"
  pass "$description consumer returned exactly every committed record"
  pass "$description offsets are contiguous with no gaps or duplicates"
  pass "$description contains every explicitly acknowledged value exactly once at its acked offset"
  LATEST_CONTIGUOUS="$latest"
}

partition_dir() {
  printf '%s/%s-%s' "${NODE_DATA[$1]}" "$TOPIC" "$PARTITION"
}

read_high_watermark() {
  local path
  path="$(partition_dir "$1")/hwm"
  perl -e '
    use strict; use warnings;
    my $path = shift;
    if (!-f $path) { print 0; exit }
    open my $fh, q{<:raw}, $path or die $!;
    local $/; my $bytes = <$fh>; close $fh;
    if (length($bytes) < 8) { print 0; exit }
    my $hwm = unpack(q{q>}, substr($bytes,0,8));
    if ($hwm < 0) { print 0; exit }
    my ($generation,$offset) = (0,8);
    sub crc32c {
      my ($data)=@_; my $crc=0xffffffff;
      for my $byte (unpack(q{C*},$data)) {
        $crc ^= $byte;
        for (1..8) {
          $crc = ($crc & 1) ? (($crc >> 1) ^ 0x82f63b78) : ($crc >> 1);
          $crc &= 0xffffffff;
        }
      }
      return ($crc ^ 0xffffffff) & 0xffffffff;
    }
    while (length($bytes)-$offset >= 36) {
      my $record=substr($bytes,$offset,36);
      last if substr($record,0,4) ne q{HWMJ} || unpack(q{C},substr($record,4,1)) != 1;
      my $kind=unpack(q{C},substr($record,5,1));
      last if ($kind != 0 && $kind != 1) || substr($record,6,2) ne "\0\0";
      my $expected_crc=unpack(q{N},substr($record,32,4));
      last if crc32c(substr($record,0,32)) != $expected_crc;
      my $next_generation=unpack(q{Q>},substr($record,8,8));
      my $previous_hwm=unpack(q{q>},substr($record,16,8));
      my $next_hwm=unpack(q{q>},substr($record,24,8));
      last if $next_generation != $generation+1 || $previous_hwm != $hwm || $next_hwm < 0;
      last if ($kind == 0 && $next_hwm < $hwm) || ($kind == 1 && $next_hwm > $hwm);
      ($generation,$hwm,$offset)=($next_generation,$next_hwm,$offset+36);
    }
    print $hwm;
  ' "$path"
}

log_state() {
  local dir hwm layout
  dir="$(partition_dir "$1")"
  hwm="$(read_high_watermark "$1")"
  layout="$(perl -e '
    use strict; use warnings;
    my $dir = shift;
    if (!-d $dir) { print "0|0"; exit }
    my @files = sort glob("$dir/*.log");
    my ($expected, $count) = (0, 0);
    for my $file (@files) {
      open my $fh, q{<:raw}, $file or die $!;
      while (1) {
        my $read = read($fh, my $header, 12);
        last if !$read;
        die "truncated header $file\n" if $read != 12;
        my ($base, $length) = unpack(q{q>l>}, $header);
        die "invalid batch length $length\n" if $length < 23;
        read($fh, my $body, $length) == $length or die "truncated body $file\n";
        my $delta = unpack(q{l>}, substr($body, 11, 4));
        die "non-contiguous $file expected=$expected base=$base delta=$delta\n" if $base != $expected || $delta < 0;
        $expected = $base + $delta + 1; $count++;
      }
    }
    print join q{|}, $expected, $count;
  ' "$dir")"
  printf '%s|%s' "$hwm" "$layout"
}

log_hwm_is() {
  local state hwm
  state="$(log_state "$1")" || return 1
  IFS='|' read -r hwm _ <<<"$state"
  [[ "$hwm" == "$2" ]]
}

replicas_persisted_hwm() {
  local replicas_csv="$1" target="$2" node hwm
  local -a ids
  IFS=',' read -r -a ids <<<"$replicas_csv"
  for node in "${ids[@]}"; do
    hwm="$(read_high_watermark "$node" 2>/dev/null || true)"
    [[ -n "$hwm" ]] && (( hwm >= target )) || return 1
  done
}

inject_divergent_tail() {
  local dir minimum_base="$2"
  dir="$(partition_dir "$1")"
  perl -MDigest::SHA=sha256_hex -e '
    use strict; use warnings;
    my ($dir, $minimum_base) = @ARGV;
    my @files = sort glob("$dir/*.log"); @files or die "no log files\n";
    my ($expected, $last);
    $expected=0;
    for my $file (@files) {
      open my $fh, q{<:raw}, $file or die $!;
      while (1) {
        my $read=read($fh,my $header,12); last if !$read; die "truncated header\n" if $read != 12;
        my ($base,$length)=unpack(q{q>l>},$header); read($fh,my $body,$length)==$length or die "truncated body\n";
        my $delta=unpack(q{l>},substr($body,11,4)); die "non-contiguous\n" if $base != $expected || $delta < 0;
        $last=$header.$body; $expected=$base+$delta+1;
      }
    }
    defined $last or die "no batch to clone\n";
    my $delta=unpack(q{l>},substr($last,23,4));
    open my $out,q{>>:raw},$files[-1] or die $!;
    while (1) {
      my $clone = $last;
      my $base = $expected;
      substr($clone,0,8,pack(q{q>},$base));
      print {$out} $clone;
      $expected = $base+$delta+1;
      if ($base >= $minimum_base) {
        close $out;
        print join q{|}, $base, $expected, sha256_hex($clone);
        last;
      }
    }
  ' "$dir" "$minimum_base"
}

batch_hash_at() {
  local dir target="$2"
  dir="$(partition_dir "$1")"
  perl -MDigest::SHA=sha256_hex -e '
    use strict; use warnings; my ($dir,$target)=@ARGV;
    for my $file (sort glob("$dir/*.log")) {
      open my $fh,q{<:raw},$file or die $!;
      while (read($fh,my $header,12)) {
        my ($base,$length)=unpack(q{q>l>},$header); read($fh,my $body,$length)==$length or die "truncated\n";
        my $end=$base+unpack(q{l>},substr($body,11,4))+1;
        if ($target >= $base && $target < $end) { print sha256_hex($header.$body); exit 0 }
      }
    }
    exit 1;
  ' "$dir" "$target"
}

committed_digest() {
  local dir hwm="$2"
  dir="$(partition_dir "$1")"
  perl -MDigest::SHA -e '
    use strict; use warnings; my ($dir,$hwm)=@ARGV; my $sha=Digest::SHA->new(256); my ($expected,$bytes,$count)=(0,0,0);
    FILE: for my $file (sort glob("$dir/*.log")) {
      open my $fh,q{<:raw},$file or die $!;
      while ($expected < $hwm && read($fh,my $header,12)) {
        my ($base,$length)=unpack(q{q>l>},$header); read($fh,my $body,$length)==$length or die "truncated\n";
        my $end=$base+unpack(q{l>},substr($body,11,4))+1;
        die "non-contiguous committed image\n" if $base != $expected;
        die "HWM splits a batch\n" if $end > $hwm;
        $sha->add($header,$body); $bytes += 12+$length; $count++; $expected=$end;
      }
      last FILE if $expected == $hwm;
    }
    die "committed image ended $expected expected $hwm\n" if $expected != $hwm;
    print join q{|}, $sha->hexdigest, $bytes, $count;
  ' "$dir" "$hwm"
}

assert_prefix_identity() {
  local replicas_csv="$1" description="$2"
  local -a ids
  IFS=',' read -r -a ids <<<"$replicas_csv"
  local min_hwm="" node state hwm digest first=""
  for node in "${ids[@]}"; do
    state="$(log_state "$node")"; IFS='|' read -r hwm _ <<<"$state"
    [[ -z "$min_hwm" || "$hwm" -lt "$min_hwm" ]] && min_hwm="$hwm"
  done
  (( min_hwm > 0 )) || die "$description has non-positive cluster HWM"
  pass "$description has a positive cluster-wide persisted HWM"
  for node in "${ids[@]}"; do
    digest="$(committed_digest "$node" "$min_hwm")"
    if [[ -z "$first" ]]; then first="$digest"; elif [[ "$digest" != "$first" ]]; then die "$description differs on node $node: $digest != $first"; fi
  done
  pass "$description is byte-identical on every assigned replica through HWM $min_hwm"
  FINAL_DIGEST="$min_hwm|$first"
  printf 'committed prefix: hwm=%s sha256|bytes|batches=%s\n' "$min_hwm" "$first"
}

new_incompressible_file() {
  local path="$1" count="$2" length="$3"
  perl -e '
    use strict; use warnings; my ($path,$count,$length)=@ARGV;
    open my $random,q{<:raw},q{/dev/urandom} or die $!; open my $out,q{>:raw},$path or die $!;
    my $need=int(($length+1)/2);
    for (1..$count) { read($random,my $raw,$need)==$need or die "random read\n"; print {$out} substr(unpack(q{H*},$raw),0,$length),"\n" }
  ' "$path" "$count" "$length"
}

show_diagnostics() {
  printf '\n\033[33mVerification diagnostics\033[0m\n' >&2
  printf 'Artifacts: %s\nCluster: %s\n' "$WORK_DIR" "$CLUSTER_ID" >&2
  local node
  for node in $(seq 1 "$NODE_COUNT"); do
    [[ -n "${NODE_PID[$node]:-}" ]] || continue
    printf 'node %s pid=%s status=%s\n' "$node" "${NODE_PID[$node]}" "$(node_alive "$node" && echo running || echo stopped)" >&2
    for file in "${NODE_STDOUT[$node]}" "${NODE_STDERR[$node]}"; do
      printf '  tail %s\n' "$file" >&2
      [[ -f "$file" ]] && tail -n 100 "$file" >&2 || true
    done
  done
}

cleanup() {
  local status=$?
  trap - EXIT
  if (( ! SUCCESS )); then
    show_diagnostics || true
  fi
  local node id pid cleanup_failed=0
  for id in "${!CAP_PID[@]}"; do
    pid="${CAP_PID[$id]}"
    stop_capture "$pid" || cleanup_failed=1
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
  (( STORM_PROCESSES >= 8 )) || die "STORM_PROCESSES must be at least 8"
  (( EXTENDED_DOWNTIME_SECONDS >= 1 )) || die "EXTENDED_DOWNTIME_SECONDS must be positive"
  (( EXTENDED_PRODUCE_INTERVAL_MS >= 50 )) || die "EXTENDED_PRODUCE_INTERVAL_MS must be at least 50"
  (( EXTENDED_BACKLOG_RECORDS >= 1 && EXTENDED_VALUE_BYTES >= 1 )) || die "extended backlog settings must be positive"
  (( SEGMENT_BYTES >= 4096 )) || die "SEGMENT_BYTES must be at least 4096"
  (( CLI_WALL_TIMEOUT_SECONDS >= 130 )) || die "CLI_WALL_TIMEOUT_SECONDS must cover the 120-second bulk request"
  (( CAPTURE_WALL_TIMEOUT_SECONDS >= 35 )) || die "CAPTURE_WALL_TIMEOUT_SECONDS must cover the 30-second broker timeout"
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

  stage "Create one RF=3 partition with min.insync.replicas=2 through the CLI"
  local create_controller=1 create_output
  [[ "$create_controller" == "$controller_leader" ]] && create_controller=2
  create_output="$(cli --broker deliberately-invalid-broker-address --controller "http://127.0.0.1:${CONTROL_PORT[$create_controller]}" \
    topic create --name "$TOPIC" --partitions 1 --replication-factor 3 --config min.insync.replicas=2)"
  [[ "$create_output" == *"topic created name=\"$TOPIC\" partitions=1 replication_factor=3"* ]] || die "unexpected topic-create output: $create_output"
  pass "CLI created the configured RF=3 topic through a nonleader controller"
  wait_metadata 1,2,3,4,5 "RF=3 assignment and initial full ISR" topic_full 3 3
  local leader epoch replicas isr
  IFS='|' read -r leader epoch replicas isr <<<"$(partition_info "$WAIT_METADATA")"
  assert_eq "$(printf '%s' "$replicas" | tr ',' '\n' | sort -n | paste -sd, -)" "1,2,3" "deterministic placement selected brokers 1,2,3"
  local witnesses
  witnesses="$(perl -e '$r=",".$ARGV[0].","; @x=grep {index($r, ",$_,") < 0} 1..5; print join q{,},@x' "$replicas")"
  assert_eq "$(printf '%s' "$witnesses" | awk -F, '{print NF}')" "2" "partition has two non-replica controller witnesses"
  local witness_a="${witnesses%%,*}" witness_b="${witnesses##*,}"
  local sorted_replicas
  sorted_replicas="$(printf '%s' "$replicas" | tr ',' '\n' | sort -n | paste -sd, -)"

  stage "Idempotent producer allocation, deduplication, fencing, and failover recovery"
  local normal_before normal_before_latest normal_output normal_identity normal_pid normal_epoch normal_ack
  local normal_after normal_after_latest
  normal_before="$(get_offsets "$witness_a")"; IFS='|' read -r _ normal_before_latest <<<"$normal_before"
  normal_output="$(cli --broker "$(broker_address "$witness_a")" produce --topic "$TOPIC" --partition "$PARTITION" \
    --value "idempotent-client-$(perl -e 'printf "%08x", int(rand(0xffffffff))')" --acks all --timeout-ms 30000 --idempotent)"
  normal_identity="$(parse_producer_identity "$normal_output")" || die "normal idempotent produce omitted its allocated identity: $normal_output"
  IFS='|' read -r normal_pid normal_epoch <<<"$normal_identity"
  assert_eq "$normal_epoch" "0" "normal idempotent client allocated epoch zero through InitProducerId"
  normal_ack="$(printf '%s\n' "$normal_output" | sed -n 's/^acked offset=\([0-9][0-9]*\)$/\1/p' | head -n1)"
  [[ -n "$normal_ack" ]] || die "normal idempotent produce omitted its acknowledgement: $normal_output"
  assert_eq "$normal_ack" "$normal_before_latest" "normal idempotent client appended at the prior HWM"
  normal_after="$(get_offsets "$witness_b")"; IFS='|' read -r _ normal_after_latest <<<"$normal_after"
  assert_eq "$normal_after_latest" "$((normal_before_latest + 1))" "normal idempotent client advanced the committed log once"

  local idempotent_leader="$leader" init_output identity producer_id producer_epoch
  init_output="$(cli --broker "$(broker_address "$idempotent_leader")" producer init)"
  identity="$(parse_producer_identity "$init_output")" || die "cannot parse InitProducerId allocation: $init_output"
  IFS='|' read -r producer_id producer_epoch <<<"$identity"
  assert_eq "$producer_epoch" "0" "explicit producer identity allocation returned epoch zero"

  local sequence_zero_value="idempotent-seq0-$(perl -e 'printf "%08x", int(rand(0xffffffff))')"
  local sequence_before sequence_before_latest sequence_before_state sequence_before_leo
  sequence_before="$(get_offsets "$idempotent_leader")"; IFS='|' read -r _ sequence_before_latest <<<"$sequence_before"
  sequence_before_state="$(log_state "$idempotent_leader")"; IFS='|' read -r _ sequence_before_leo _ <<<"$sequence_before_state"
  local sequence_output sequence_tuple sequence_offset sequence_pid sequence_epoch sequence_base
  sequence_output="$(explicit_produce "$idempotent_leader" "$sequence_zero_value" "$producer_id" 0 0)"
  sequence_tuple="$(parse_explicit_ack "$sequence_output")" || die "cannot parse explicit sequence-zero acknowledgement: $sequence_output"
  IFS='|' read -r sequence_offset sequence_pid sequence_epoch sequence_base <<<"$sequence_tuple"
  assert_eq "$sequence_pid|$sequence_epoch|$sequence_base" "$producer_id|0|0" "sequence-zero acknowledgement echoed the explicit producer metadata"
  assert_eq "$sequence_offset" "$sequence_before_latest" "sequence zero appended at the prior HWM"
  local sequence_offsets sequence_latest sequence_state sequence_hwm sequence_leo sequence_digest
  sequence_offsets="$(get_offsets "$idempotent_leader")"; IFS='|' read -r _ sequence_latest <<<"$sequence_offsets"
  wait_until "the scheduled leader HWM checkpoint to reach $sequence_latest" 10 0.1 \
    log_hwm_is "$idempotent_leader" "$sequence_latest"
  sequence_state="$(log_state "$idempotent_leader")"; IFS='|' read -r sequence_hwm sequence_leo _ <<<"$sequence_state"
  assert_eq "$sequence_latest" "$((sequence_before_latest + 1))" "sequence zero advanced HWM exactly once"
  assert_eq "$sequence_leo" "$((sequence_before_leo + 1))" "sequence zero advanced on-disk LEO exactly once"
  assert_eq "$sequence_hwm" "$sequence_latest" "sequence-zero HWM reached its scheduled disk checkpoint"
  sequence_digest="$(committed_digest "$idempotent_leader" "$sequence_latest")"

  local replay_output replay_tuple replay_offsets replay_latest replay_state replay_digest
  replay_output="$(explicit_produce "$idempotent_leader" "$sequence_zero_value" "$producer_id" 0 0)"
  replay_tuple="$(parse_explicit_ack "$replay_output")" || die "cannot parse exact replay acknowledgement: $replay_output"
  assert_eq "$replay_tuple" "$sequence_tuple" "exact sequence replay returned the original offset and identity"
  replay_offsets="$(get_offsets "$idempotent_leader")"; IFS='|' read -r _ replay_latest <<<"$replay_offsets"
  replay_state="$(log_state "$idempotent_leader")"
  replay_digest="$(committed_digest "$idempotent_leader" "$replay_latest")"
  assert_eq "$replay_latest" "$sequence_latest" "exact sequence replay left HWM unchanged"
  assert_eq "$replay_state" "$sequence_state" "exact sequence replay left on-disk HWM, LEO, and batch count unchanged"
  assert_eq "$replay_digest" "$sequence_digest" "exact sequence replay left committed bytes and checksum unchanged"

  explicit_produce_raw "$idempotent_leader" "idempotent-conflict-$sequence_zero_value" "$producer_id" 0 0
  (( CLI_STATUS != 0 )) || die "same producer sequence with different content unexpectedly succeeded"
  [[ "$CLI_OUTPUT" == *"server error 12: out of order producer sequence"* ]] || die "wrong conflicting-sequence error: $CLI_OUTPUT"
  pass "same sequence with different content returned stable OutOfOrderSequence"
  local conflict_offsets conflict_latest conflict_state conflict_digest
  conflict_offsets="$(get_offsets "$idempotent_leader")"; IFS='|' read -r _ conflict_latest <<<"$conflict_offsets"
  conflict_state="$(log_state "$idempotent_leader")"
  conflict_digest="$(committed_digest "$idempotent_leader" "$conflict_latest")"
  assert_eq "$conflict_latest" "$sequence_latest" "conflicting replay left HWM unchanged"
  assert_eq "$conflict_state" "$sequence_state" "conflicting replay left on-disk LEO unchanged"
  assert_eq "$conflict_digest" "$sequence_digest" "conflicting replay left committed bytes and checksum unchanged"

  local bump_output bumped_identity bumped_id bumped_epoch
  bump_output="$(cli --broker "$(broker_address "$idempotent_leader")" producer init \
    --producer-id "$producer_id" --producer-epoch 0)"
  bumped_identity="$(parse_producer_identity "$bump_output")" || die "cannot parse producer epoch bump: $bump_output"
  IFS='|' read -r bumped_id bumped_epoch <<<"$bumped_identity"
  assert_eq "$bumped_id|$bumped_epoch" "$producer_id|1" "InitProducerId bumped the allocated identity to epoch one"

  local epoch_one_value="idempotent-epoch1-$(perl -e 'printf "%08x", int(rand(0xffffffff))')"
  local epoch_one_output epoch_one_tuple epoch_one_offset epoch_one_pid epoch_one_epoch epoch_one_base
  epoch_one_output="$(explicit_produce "$idempotent_leader" "$epoch_one_value" "$producer_id" 1 0)"
  epoch_one_tuple="$(parse_explicit_ack "$epoch_one_output")" || die "cannot parse epoch-one sequence-zero acknowledgement: $epoch_one_output"
  IFS='|' read -r epoch_one_offset epoch_one_pid epoch_one_epoch epoch_one_base <<<"$epoch_one_tuple"
  assert_eq "$epoch_one_pid|$epoch_one_epoch|$epoch_one_base" "$producer_id|1|0" "new epoch sequence zero durably established the partition fence"
  assert_eq "$epoch_one_offset" "$sequence_latest" "new epoch sequence zero appended at the prior HWM"
  local epoch_one_offsets epoch_one_latest epoch_one_state epoch_one_hwm epoch_one_leo epoch_one_digest
  epoch_one_offsets="$(get_offsets "$idempotent_leader")"; IFS='|' read -r _ epoch_one_latest <<<"$epoch_one_offsets"
  wait_until "the scheduled epoch-one HWM checkpoint to reach $epoch_one_latest" 10 0.1 \
    log_hwm_is "$idempotent_leader" "$epoch_one_latest"
  epoch_one_state="$(log_state "$idempotent_leader")"; IFS='|' read -r epoch_one_hwm epoch_one_leo _ <<<"$epoch_one_state"
  assert_eq "$epoch_one_latest" "$((sequence_latest + 1))" "new producer epoch advanced HWM exactly once"
  assert_eq "$epoch_one_hwm" "$epoch_one_latest" "new producer epoch is durable before testing the old-epoch fence"
  epoch_one_digest="$(committed_digest "$idempotent_leader" "$epoch_one_latest")"

  explicit_produce_raw "$idempotent_leader" "idempotent-stale-epoch" "$producer_id" 0 1
  (( CLI_STATUS != 0 )) || die "old producer epoch unexpectedly appended after durable epoch-one sequence zero"
  [[ "$CLI_OUTPUT" == *"server error 11: fenced producer epoch"* ]] || die "wrong old-epoch error: $CLI_OUTPUT"
  pass "old epoch returned stable FencedProducerEpoch after the new epoch became durable"
  local fenced_offsets fenced_latest fenced_state fenced_digest
  fenced_offsets="$(get_offsets "$idempotent_leader")"; IFS='|' read -r _ fenced_latest <<<"$fenced_offsets"
  fenced_state="$(log_state "$idempotent_leader")"
  fenced_digest="$(committed_digest "$idempotent_leader" "$fenced_latest")"
  assert_eq "$fenced_latest" "$epoch_one_latest" "fenced old epoch left HWM unchanged"
  assert_eq "$fenced_state" "$epoch_one_state" "fenced old epoch left on-disk LEO unchanged"
  assert_eq "$fenced_digest" "$epoch_one_digest" "fenced old epoch left committed bytes and checksum unchanged"
  wait_until "epoch-one HWM persistence on every replica" "$TIMEOUT_SECONDS" 0.02 \
    replicas_persisted_hwm "$sorted_replicas" "$epoch_one_latest"
  pass "every assigned replica persisted the epoch-one HWM before leader failure"

  stop_node "$idempotent_leader"
  node_alive "$idempotent_leader" && die "idempotence leader $idempotent_leader survived kill -9"
  pass "idempotence leader $idempotent_leader was forcibly terminated before replay"
  local idempotent_live
  idempotent_live="$(printf '1\n2\n3\n4\n5\n' | grep -v "^$idempotent_leader$" | paste -sd, -)"
  wait_shared_leader "$idempotent_live" >/dev/null
  wait_metadata "$idempotent_live" "idempotence leader fencing and ISR failover" kill_failover "$idempotent_leader" 2
  local idempotent_new_leader
  IFS='|' read -r idempotent_new_leader _ <<<"$(partition_info "$WAIT_METADATA")"
  [[ "$idempotent_new_leader" != "$idempotent_leader" ]] || die "idempotence replay did not move to a new leader"
  pass "idempotence replay target is failover leader $idempotent_new_leader"
  local failover_before_offsets failover_before_latest failover_before_state failover_before_digest
  failover_before_offsets="$(get_offsets "$idempotent_new_leader")"; IFS='|' read -r _ failover_before_latest <<<"$failover_before_offsets"
  failover_before_state="$(log_state "$idempotent_new_leader")"
  failover_before_digest="$(committed_digest "$idempotent_new_leader" "$failover_before_latest")"
  local failover_replay_output failover_replay_tuple failover_after_offsets failover_after_latest failover_after_state failover_after_digest
  failover_replay_output="$(explicit_produce "$idempotent_new_leader" "$epoch_one_value" "$producer_id" 1 0)"
  failover_replay_tuple="$(parse_explicit_ack "$failover_replay_output")" || die "cannot parse failover replay acknowledgement: $failover_replay_output"
  assert_eq "$failover_replay_tuple" "$epoch_one_tuple" "new leader replay returned the original epoch-one offset and identity"
  failover_after_offsets="$(get_offsets "$idempotent_new_leader")"; IFS='|' read -r _ failover_after_latest <<<"$failover_after_offsets"
  failover_after_state="$(log_state "$idempotent_new_leader")"
  failover_after_digest="$(committed_digest "$idempotent_new_leader" "$failover_after_latest")"
  assert_eq "$failover_after_latest" "$failover_before_latest" "new leader replay left HWM unchanged"
  assert_eq "$failover_after_state" "$failover_before_state" "new leader replay left on-disk LEO unchanged"
  assert_eq "$failover_after_digest" "$failover_before_digest" "new leader replay left committed bytes and checksum unchanged"
  start_node "$idempotent_leader" 1
  wait_http_ready "$idempotent_leader"
  wait_metadata 1,2,3,4,5 "idempotence old leader catch-up and full ISR" alive_isr "$idempotent_leader" "$sorted_replicas"
  IFS='|' read -r leader _ <<<"$(partition_info "$WAIT_METADATA")"
  assert_eq "$leader" "$idempotent_new_leader" "idempotence-killed leader rejoined as a follower"

  stage "RF=3 acks=all storm: hard-kill the leader with requests in flight"
  local first_leader="$leader"
  start_storm "$witness_a" storm-one
  wait_storm_armed
  pass "first storm had $STORM_ARMED_ACKED acknowledged and $STORM_ARMED_RUNNING in-flight calls at the kill point"
  stop_node "$first_leader"
  resume_storm_blocker
  node_alive "$first_leader" && die "leader $first_leader survived kill -9"
  pass "partition leader $first_leader was forcibly terminated mid-storm"
  local live_first="$(seq -s, 1 5 | perl -pe 's/(^|,)'"$first_leader"'(,|$)/$1/; s/,,/,/; s/^,|,$//g')"
  wait_shared_leader "$live_first" >/dev/null
  wait_metadata "$live_first" "first leader fencing and ISR failover" kill_failover "$first_leader" 2
  pass "controller elected a new leader from the surviving ISR"
  complete_storm
  local first_new_leader
  IFS='|' read -r first_new_leader _ <<<"$(partition_info "$WAIT_METADATA")"
  assert_ack_continuity "$witness_b" "first leader failover"
  pass "first acks=all failover preserved a committed prefix through latest offset $LATEST_CONTIGUOUS"
  start_node "$first_leader" 1
  wait_http_ready "$first_leader"
  wait_metadata 1,2,3,4,5 "first killed leader catch-up and ISR re-entry" alive_isr "$first_leader" "$(printf '%s' "$replicas" | tr ',' '\n' | sort -n | paste -sd, -)"
  IFS='|' read -r leader _ <<<"$(partition_info "$WAIT_METADATA")"
  assert_eq "$leader" "$first_new_leader" "first killed leader rejoined as a follower"

  stage "Follower stall plus ISR=2 leader kill and leader-epoch truncation"
  local second_leader="$leader" stalled surviving
  stalled="$(printf '%s' "$replicas" | tr ',' '\n' | grep -v "^$second_leader$" | head -n1)"
  surviving="$(printf '%s' "$replicas" | tr ',' '\n' | grep -v -e "^$second_leader$" -e "^$stalled$" | head -n1)"
  stop_node "$stalled"
  pass "follower $stalled was hard-stalled by process termination"
  local live_no_stalled
  live_no_stalled="$(printf '1\n2\n3\n4\n5\n' | grep -v "^$stalled$" | paste -sd, -)"
  wait_metadata "$live_no_stalled" "stalled follower fencing and ISR shrink" dead_isr "$stalled" 2
  pass "stalled follower left ISR before the second failure"
  start_storm "$witness_a" storm-two
  wait_storm_armed
  pass "ISR=2 storm had $STORM_ARMED_ACKED acknowledged and $STORM_ARMED_RUNNING in-flight calls at the kill point"
  stop_node "$second_leader"
  resume_storm_blocker
  pass "leader $second_leader was forcibly terminated while follower $stalled remained stalled"
  local dual_live
  dual_live="$(printf '1\n2\n3\n4\n5\n' | grep -v -e "^$stalled$" -e "^$second_leader$" | paste -sd, -)"
  local dual_controller_leader
  dual_controller_leader="$(wait_shared_leader "$dual_live")"
  pass "three surviving controllers retained quorum after dual failure (leader $dual_controller_leader)"
  wait_metadata "$dual_live" "sole surviving replica leadership" leader_isr "$surviving" "$surviving"
  pass "controller elected sole remaining in-sync replica $surviving"
  complete_storm
  assert_ack_continuity "$surviving" "leader failover during follower stall"
  pass "ISR=2 failover lost none of the acknowledged prefix through offset $LATEST_CONTIGUOUS"

  local injection new_state new_leo inject_base inject_end inject_hash
  new_state="$(log_state "$surviving")"; IFS='|' read -r _ new_leo _ <<<"$new_state"
  injection="$(inject_divergent_tail "$second_leader" "$new_leo")"
  IFS='|' read -r inject_base inject_end inject_hash <<<"$injection"
  (( inject_base >= new_leo )) || die "old leader injection offset $inject_base precedes new leader LEO $new_leo"
  pass "recorded a CRC-valid divergent old-leader batch at offset $inject_base"
  start_node "$stalled" 1
  wait_http_ready "$stalled"
  local recovered_two
  recovered_two="$(printf '%s\n%s\n' "$stalled" "$surviving" | sort -n | paste -sd, -)"
  wait_metadata "$dual_live,$stalled" "stalled follower catch-up and ISR re-entry" alive_isr "$stalled" "$recovered_two"
  pass "stalled follower caught up before re-entering ISR"
  local offsets authoritative_start authoritative_count authoritative_output
  offsets="$(get_offsets "$surviving")"; IFS='|' read -r _ authoritative_start <<<"$offsets"
  authoritative_count=$((inject_end - authoritative_start + 8)); (( authoritative_count < 8 )) && authoritative_count=8
  authoritative_output="$(cli --broker "$(broker_address "$surviving")" produce --topic "$TOPIC" --partition "$PARTITION" \
    --count "$authoritative_count" --value-size 128 --acks all --timeout-ms 30000)"
  [[ "$authoritative_output" == *"produced $authoritative_count records"* ]] || die "authoritative replacement produce failed"
  pass "new leader committed authoritative bytes across the divergent offset"
  start_node "$second_leader" 1
  wait_http_ready "$second_leader"
  wait_metadata 1,2,3,4,5 "old leader truncation, catch-up, and full ISR" alive_isr "$second_leader" "$sorted_replicas"
  local reconciled_hash leader_hash
  reconciled_hash="$(batch_hash_at "$second_leader" "$inject_base")"
  leader_hash="$(batch_hash_at "$surviving" "$inject_base")"
  assert_eq "$reconciled_hash" "$leader_hash" "rejoined old leader contains the authoritative raw batch"
  [[ "$reconciled_hash" != "$inject_hash" ]] || die "injected divergent batch survived epoch reconciliation"
  pass "leader-epoch reconciliation truncated and replaced the divergent batch"

  stage "Extended follower outage with continued acks=all production"
  IFS='|' read -r leader _ <<<"$(partition_info "$WAIT_METADATA")"
  local extended_down
  extended_down="$(printf '%s' "$replicas" | tr ',' '\n' | grep -v "^$leader$" | head -n1)"
  stop_node "$extended_down"
  local live_extended
  live_extended="$(printf '1\n2\n3\n4\n5\n' | grep -v "^$extended_down$" | paste -sd, -)"
  wait_metadata "$live_extended" "extended-down follower ISR removal" dead_isr "$extended_down" 2
  local down_state down_hwm
  down_state="$(log_state "$extended_down")"; IFS='|' read -r down_hwm _ <<<"$down_state"
  pass "follower $extended_down stopped at persisted HWM $down_hwm"
  local backlog_file="$WORK_DIR/extended-incompressible-records.txt" expected_bytes actual_bytes bulk_output
  new_incompressible_file "$backlog_file" "$EXTENDED_BACKLOG_RECORDS" "$EXTENDED_VALUE_BYTES"
  expected_bytes=$((EXTENDED_BACKLOG_RECORDS * EXTENDED_VALUE_BYTES))
  actual_bytes="$(wc -c <"$backlog_file")"
  (( actual_bytes >= expected_bytes )) || die "backlog file is too small"
  pass "generated incompressible backlog of at least $expected_bytes bytes"
  bulk_output="$(cli --broker "$(broker_address "$witness_a")" produce --topic "$TOPIC" --partition "$PARTITION" \
    --file "$backlog_file" --acks all --timeout-ms 120000)"
  [[ "$bulk_output" == *"produced $EXTENDED_BACKLOG_RECORDS records"* ]] || die "bulk backlog produce failed"
  pass "acks=all built a substantial catch-up backlog while one follower was down"
  local outage_start=$SECONDS outage_deadline=$((SECONDS + EXTENDED_DOWNTIME_SECONDS)) next_progress=$((SECONDS + 60)) continued=0 interval
  interval="$(perl -e 'printf "%.3f", $ARGV[0]/1000' "$EXTENDED_PRODUCE_INTERVAL_MS")"
  while (( SECONDS < outage_deadline )); do
    local value output ack
    value="extended-$continued-$(perl -e 'printf "%08x", int(rand(0xffffffff))')"
    output="$(acks_all_produce "$witness_b" "$value")"
    ack="$(printf '%s\n' "$output" | sed -n 's/^acked offset=\([0-9][0-9]*\)$/\1/p')"
    [[ -n "$ack" ]] || die "cannot parse extended-outage ack: $output"
    continued=$((continued + 1))
    if (( SECONDS >= next_progress )); then
      printf 'extended outage progress: %ss continued-acks=%s\n' "$((SECONDS-outage_start))" "$continued"
      next_progress=$((next_progress + 60))
    fi
    (( SECONDS < outage_deadline )) && sleep "$interval"
  done
  (( continued > 0 )) || die "no production during extended outage"
  pass "production continued throughout the configured ${EXTENDED_DOWNTIME_SECONDS}-second outage"
  offsets="$(get_offsets "$witness_a")"; local target_hwm; IFS='|' read -r _ target_hwm <<<"$offsets"
  local initial_lag=$((target_hwm - down_hwm))
  (( initial_lag > 0 )) || die "extended outage created no offset lag"
  pass "persisted offset lag grew to $initial_lag"
  start_node "$extended_down" 1
  wait_http_ready "$extended_down"
  local progress_hwm="" progress_probe
  progress_probe() {
    progress_hwm="$(read_high_watermark "$extended_down" 2>/dev/null || true)"
    [[ -n "$progress_hwm" ]] && (( progress_hwm > down_hwm && progress_hwm <= target_hwm ))
  }
  wait_until "restarted follower HWM progress" "$TIMEOUT_SECONDS" 0.02 progress_probe
  local intermediate_lag=$((target_hwm - progress_hwm))
  (( intermediate_lag >= 0 && intermediate_lag < initial_lag )) || die "invalid observed lag $intermediate_lag"
  pass "persisted catch-up lag visibly decreased from $initial_lag to $intermediate_lag"
  wait_metadata 1,2,3,4,5 "extended-down follower catch-up and ISR re-entry" alive_isr "$extended_down" "$sorted_replicas"
  local final_state final_hwm
  final_state="$(log_state "$extended_down")"; IFS='|' read -r final_hwm _ <<<"$final_state"
  (( final_hwm >= target_hwm )) || die "rejoined follower HWM $final_hwm below $target_hwm"
  pass "rejoined follower persisted the leader HWM"
  wait_until "post-catch-up HWM persistence on every replica" "$TIMEOUT_SECONDS" 0.02 \
    replicas_persisted_hwm "$sorted_replicas" "$target_hwm"
  pass "every assigned replica persisted the post-catch-up HWM"
  assert_prefix_identity "$sorted_replicas" "post-catch-up committed log prefix"

  stage "min.insync.replicas=2 with two of three assigned replicas down"
  IFS='|' read -r leader _ <<<"$(partition_info "$WAIT_METADATA")"
  local followers first_down second_down before_offsets before_latest before_state before_leo
  followers="$(printf '%s' "$replicas" | tr ',' '\n' | grep -v "^$leader$" | paste -sd, -)"
  first_down="${followers%%,*}"; second_down="${followers##*,}"
  before_offsets="$(get_offsets "$leader")"; IFS='|' read -r _ before_latest <<<"$before_offsets"
  before_state="$(log_state "$leader")"; IFS='|' read -r _ before_leo _ <<<"$before_state"
  stop_node "$first_down"
  local live_one_down
  live_one_down="$(printf '1\n2\n3\n4\n5\n' | grep -v "^$first_down$" | paste -sd, -)"
  wait_metadata "$live_one_down" "first min-ISR follower removal" dead_isr "$first_down" 2
  stop_node "$second_down"
  local min_survivors
  min_survivors="$(printf '1\n2\n3\n4\n5\n' | grep -v -e "^$first_down$" -e "^$second_down$" | paste -sd, -)"
  local min_controller_leader
  min_controller_leader="$(wait_shared_leader "$min_survivors")"
  pass "controller quorum remained live after two assigned replicas were killed (leader $min_controller_leader)"
  wait_metadata "$min_survivors" "sole-replica ISR" two_dead_isr "$leader" "$followers"
  pass "controller committed ISR=1 while two of three replicas were actually down"
  local proof="quorum-proof-$(perl -e 'printf "%08x", int(rand(0xffffffff))')" proof_output
  proof_output="$(cli --broker invalid-admin-broker --controller "http://127.0.0.1:${CONTROL_PORT[${min_survivors%%,*}]}" \
    topic create --name "$proof" --partitions 1 --replication-factor 1)"
  [[ "$proof_output" == *"topic created name=\"$proof\""* ]] || die "controller proof write failed"
  pass "surviving three-controller quorum committed a metadata write"
  cli_raw --broker "$(broker_address "$leader")" produce --topic "$TOPIC" --partition "$PARTITION" \
    --value must-not-append-below-min-isr --acks all --timeout-ms 2000
  (( CLI_STATUS != 0 )) || die "below-min-ISR produce unexpectedly succeeded"
  pass "acks=all produce failed below min.insync.replicas"
  [[ "$CLI_OUTPUT" == *"server error 10: not enough in-sync replicas"* ]] || die "wrong below-min-ISR error: $CLI_OUTPUT"
  pass "produce returned stable NotEnoughReplicas"
  offsets="$(get_offsets "$leader")"; local after_latest; IFS='|' read -r _ after_latest <<<"$offsets"
  local after_state after_leo
  after_state="$(log_state "$leader")"; IFS='|' read -r _ after_leo _ <<<"$after_state"
  assert_eq "$after_latest" "$before_latest" "rejected request did not advance HWM"
  assert_eq "$after_leo" "$before_leo" "rejected request did not append an uncommitted disk batch"
  start_node "$first_down" 1
  wait_http_ready "$first_down"
  local restored_two
  restored_two="$(printf '%s\n%s\n' "$leader" "$first_down" | sort -n | paste -sd, -)"
  wait_metadata "$min_survivors,$first_down" "one follower restores min ISR" alive_isr "$first_down" "$restored_two"
  pass "one returning follower caught up and restored min ISR"
  local recovery_value="min-isr-recovered-$(perl -e 'printf "%08x", int(rand(0xffffffff))')" recovery_output recovery_offset
  recovery_output="$(acks_all_produce "${min_survivors%%,*}" "$recovery_value")"
  recovery_offset="$(printf '%s\n' "$recovery_output" | sed -n 's/^acked offset=\([0-9][0-9]*\)$/\1/p')"
  [[ -n "$recovery_offset" ]] || die "recovery produce failed: $recovery_output"
  pass "acks=all production resumed after one follower returned"
  assert_eq "$recovery_offset" "$before_latest" "recovery append reused the unconsumed rejected offset"
  local recovery_record
  recovery_record="$(cli --broker "$(broker_address "$first_down")" consume --topic "$TOPIC" --partition "$PARTITION" --offset "$recovery_offset" --max 1)"
  [[ "$recovery_record" == *"offset=$recovery_offset"*"value=$recovery_value"* ]] || die "recovery record mismatch: $recovery_record"
  pass "recovery record is readable through the returning follower seed"
  start_node "$second_down" 1
  wait_http_ready "$second_down"
  wait_metadata 1,2,3,4,5 "final follower catch-up and full ISR" alive_isr "$second_down" "$sorted_replicas"
  local final_offsets final_latest
  final_offsets="$(get_offsets "$leader")"; IFS='|' read -r _ final_latest <<<"$final_offsets"
  wait_until "final HWM persistence on every replica" "$TIMEOUT_SECONDS" 0.02 \
    replicas_persisted_hwm "$sorted_replicas" "$final_latest"
  pass "every assigned replica persisted the final HWM"
  assert_prefix_identity "$sorted_replicas" "final committed log prefix"

  stage "M3 live verification complete"
  printf 'replicas=[%s] witnesses=[%s]\nfinal=%s\nchecks passed: %s\n' "$replicas" "$witnesses" "$FINAL_DIGEST" "$CHECKS"
  SUCCESS=1
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
