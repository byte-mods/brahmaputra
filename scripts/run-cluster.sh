#!/usr/bin/env bash
# Host a three-node Brahmaputra cluster on this machine, dashboard included.
#
#   scripts/run-cluster.sh start     # build if needed, start, form the quorum
#   scripts/run-cluster.sh status    # brokers, controller view, topics
#   scripts/run-cluster.sh logs 2    # tail node 2
#   scripts/run-cluster.sh stop      # stop all three, keep the data
#   scripts/run-cluster.sh destroy   # stop and delete the data directories
#
# Every node is a combined broker/controller: the three form one Raft quorum
# that owns the metadata, and each serves the data plane and the dashboard.
# That is the shape the README documents for production, only with the three
# nodes on one host and separated by port rather than by address.
#
# Ports are `base + node - 1`, so with the defaults:
#
#   node  data   controller  dashboard
#      1  9092        19092       8080
#      2  9093        19093       8081
#      3  9094        19094       8082
#
# Override a base when something else already holds it — a Kafka container
# on 9092 is the common one:
#
#   DATA_PORT_BASE=9192 HTTP_PORT_BASE=8090 scripts/run-cluster.sh start
#
# The resolved ports are written to data/cluster/cluster.env, which the
# other scripts source, so a shifted cluster stays usable without repeating
# the variables on every command.
#
# State lives under ./data/cluster, which is gitignored. `stop` leaves it in
# place, so a restart rejoins the same cluster with the same logs.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT/data/cluster"
ENV_FILE="$RUN_DIR/cluster.env"
SERVER_EXE="$ROOT/target/release/brahmaputra-server"
CLI_EXE="$ROOT/target/release/brahmaputra-cli"
NODE_COUNT=3

# A previous start's ports, so `status`, `logs` and the load script keep
# working without re-stating the overrides. An explicit environment
# variable still wins: the file is a default, not a lock.
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

DATA_PORT_BASE="${DATA_PORT_BASE:-9092}"
CONTROL_PORT_BASE="${CONTROL_PORT_BASE:-19092}"
HTTP_PORT_BASE="${HTTP_PORT_BASE:-8080}"
CLUSTER_ID="${CLUSTER_ID:-brahmaputra-local}"
# Auto-created topics get three partitions; anything created explicitly
# passes its own count.
DEFAULT_PARTITIONS="${DEFAULT_PARTITIONS:-3}"
# First boot only: the dashboard's initial login. The user store lives in
# the Raft metadata, so changing it later is a dashboard action, not a
# restart with a different value here.
ADMIN_USER="${BRAHMAPUTRA_ADMIN_USER:-admin}"
ADMIN_PASSWORD="${BRAHMAPUTRA_ADMIN_PASSWORD:-brahmaputra}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-60}"
STOP_TIMEOUT_SECONDS="${STOP_TIMEOUT_SECONDS:-15}"

data_port()    { echo $((DATA_PORT_BASE + $1 - 1)); }
control_port() { echo $((CONTROL_PORT_BASE + $1 - 1)); }
http_port()    { echo $((HTTP_PORT_BASE + $1 - 1)); }

stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
info()  { printf '     %s\n' "$1"; }
warn()  { printf '\033[33m     %s\033[0m\n' "$1"; }
die()   { printf '\n\033[31mfailed: %s\033[0m\n' "$*" >&2; exit 1; }

# Fills the global PEERS array. A function cannot return an array, and
# macOS ships bash 3.2, which has no `mapfile` to read one from a pipe.
PEERS=()
build_peers() {
  local node
  PEERS=()
  for node in $(seq 1 "$NODE_COUNT"); do
    PEERS+=(--controller-peer "$node=127.0.0.1:$(control_port "$node")")
  done
}

pid_file() { echo "$RUN_DIR/node-$1/server.pid"; }
log_file() { echo "$RUN_DIR/node-$1/server.log"; }

node_pid() {
  local file
  file="$(pid_file "$1")"
  [[ -f "$file" ]] || return 1
  local pid
  pid="$(cat "$file")"
  # A stale pid file outlives a killed broker; treat it as "not running"
  # rather than reporting a pid nothing answers on.
  kill -0 "$pid" 2>/dev/null || return 1
  echo "$pid"
}

controller_get() {
  curl -sf --max-time 5 "http://127.0.0.1:$(control_port "$1")$2"
}

# Who, if anyone, is listening on a port. Empty means free.
#
# `lsof` exits non-zero when nothing matches, which is the common case here
# and not an error, so the pipeline's status is discarded.
port_holder() {
  { lsof -nP -iTCP:"$1" -sTCP:LISTEN 2>/dev/null || true; } \
    | awk 'NR==2 {print $1 " (pid " $2 ")"}'
}

# Checked before starting anything, because a broker that loses this race
# exits with a bare "Address already in use" and the cluster then hangs
# waiting for a quorum member that is never coming.
preflight_ports() {
  local node port holder conflicts=0
  for node in $(seq 1 "$NODE_COUNT"); do
    # A port held by this cluster's own still-running node is not a
    # conflict; `start` is idempotent and leaves that node alone.
    if node_pid "$node" >/dev/null; then continue; fi
    for port in "$(data_port "$node")" "$(control_port "$node")" "$(http_port "$node")"; do
      holder="$(port_holder "$port")"
      if [[ -n "$holder" ]]; then
        warn "port $port is held by $holder"
        conflicts=$((conflicts + 1))
      fi
    done
  done
  (( conflicts == 0 )) && return 0
  die "$conflicts port(s) in use. Either stop the holder, or shift this cluster:
       DATA_PORT_BASE=9192 CONTROL_PORT_BASE=19192 HTTP_PORT_BASE=8090 $0 start"
}

write_env_file() {
  cat > "$ENV_FILE" <<EOF
# Written by run-cluster.sh; sourced by the other scripts so they find this
# cluster's ports. Delete it to go back to the defaults.
DATA_PORT_BASE=$DATA_PORT_BASE
CONTROL_PORT_BASE=$CONTROL_PORT_BASE
HTTP_PORT_BASE=$HTTP_PORT_BASE
CLUSTER_ID=$CLUSTER_ID
EOF
}

build_if_needed() {
  if [[ -x "$SERVER_EXE" && -x "$CLI_EXE" ]]; then return; fi
  stage "Building release binaries"
  (cd "$ROOT" && cargo build --release -p brahmaputra-server -p brahmaputra-cli)
}

start_node() {
  local node="$1"
  local dir="$RUN_DIR/node-$node"
  mkdir -p "$dir/data"

  local existing
  if existing="$(node_pid "$node")"; then
    info "node $node already running (pid $existing)"
    return
  fi

  build_peers

  # Node 1 carries --bootstrap. It forms the quorum on a first start and is
  # ignored once one exists, so it is safe to leave on every restart.
  local -a bootstrap=()
  [[ "$node" == 1 ]] && bootstrap=(--bootstrap)

  BRAHMAPUTRA_ADMIN_USER="$ADMIN_USER" \
  BRAHMAPUTRA_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
  "$SERVER_EXE" \
    --node-id "$node" --cluster-id "$CLUSTER_ID" \
    --host 127.0.0.1 \
    --port "$(data_port "$node")" \
    --control-port "$(control_port "$node")" \
    --http-port "$(http_port "$node")" \
    --data-dir "$dir/data" \
    --default-partitions "$DEFAULT_PARTITIONS" \
    "${PEERS[@]}" ${bootstrap[@]+"${bootstrap[@]}"} \
    >> "$(log_file "$node")" 2>&1 &

  echo $! > "$(pid_file "$node")"
  info "node $node started (pid $!) — data :$(data_port "$node")  controller :$(control_port "$node")  dashboard :$(http_port "$node")"
}

wait_ready() {
  local deadline=$((SECONDS + READY_TIMEOUT_SECONDS)) node ready
  while (( SECONDS < deadline )); do
    # A node that died is reported now, with its own last words. Waiting out
    # the full timeout on a broker that exited two seconds in tells you
    # nothing the log did not already say.
    for node in $(seq 1 "$NODE_COUNT"); do
      node_pid "$node" >/dev/null || die "node $node exited during startup:
$(tail -n 5 "$(log_file "$node")" | sed 's/^/       /')"
    done
    ready=1
    for node in $(seq 1 "$NODE_COUNT"); do
      controller_get "$node" /api/v1/controller/metadata >/dev/null 2>&1 || ready=0
    done
    (( ready == 1 )) && return 0
    sleep 0.5
  done
  die "cluster did not become ready within ${READY_TIMEOUT_SECONDS}s — see $RUN_DIR/node-*/server.log"
}

# Broker ids the controller has registered, so `status` reports the
# cluster's own view rather than what this script started.
live_brokers() {
  controller_get 1 /api/v1/controller/metadata 2>/dev/null | perl -0777 -ne '
    use JSON::PP;
    my $image = eval { decode_json($_) } or exit 1;
    my @ids = sort { $a <=> $b } keys %{ $image->{brokers} || {} };
    print join(",", @ids), "\n";
  '
}

wait_all_brokers_registered() {
  local deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    [[ "$(live_brokers 2>/dev/null || true)" == "1,2,3" ]] && return 0
    sleep 0.5
  done
  warn "not every broker registered yet: $(live_brokers || echo none)"
}

cmd_start() {
  build_if_needed
  mkdir -p "$RUN_DIR"
  preflight_ports

  stage "Starting a $NODE_COUNT-node cluster"
  local node
  for node in $(seq 1 "$NODE_COUNT"); do start_node "$node"; done

  stage "Waiting for the quorum"
  wait_ready
  wait_all_brokers_registered
  write_env_file
  info "brokers registered: $(live_brokers)"

  stage "Ready"
  info "dashboard   http://localhost:$(http_port 1)  (login: $ADMIN_USER / $ADMIN_PASSWORD)"
  info "brokers     127.0.0.1:$(data_port 1), 127.0.0.1:$(data_port 2), 127.0.0.1:$(data_port 3)"
  info "controller  http://127.0.0.1:$(control_port 1)"
  info "logs        $RUN_DIR/node-N/server.log"
}

# Waits for the process to actually be gone, escalating if it will not
# leave. `restart` depends on this: a node that starts while its previous
# incarnation still holds a controller session registers a newer broker
# epoch, the controller keeps rejecting the old one's heartbeats, and the
# new process fences itself out with "could not renew epoch within its
# session timeout". Stopping asynchronously makes that a coin flip.
wait_gone() {
  local pid="$1" deadline=$((SECONDS + STOP_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    kill -0 "$pid" 2>/dev/null || return 0
    sleep 0.2
  done
  kill -9 "$pid" 2>/dev/null || true
  # Even a killed process takes a moment to be reaped.
  while kill -0 "$pid" 2>/dev/null; do sleep 0.1; done
  return 1
}

cmd_stop() {
  stage "Stopping"
  local node pid
  for node in $(seq 1 "$NODE_COUNT"); do
    if pid="$(node_pid "$node")"; then
      kill "$pid" 2>/dev/null || true
      if wait_gone "$pid"; then
        info "node $node stopped (pid $pid)"
      else
        warn "node $node did not exit in ${STOP_TIMEOUT_SECONDS}s; killed (pid $pid)"
      fi
    else
      info "node $node not running"
    fi
    rm -f "$(pid_file "$node")"
  done
}

cmd_status() {
  local node pid
  printf '%-6s %-10s %-8s %-12s %s\n' node pid data controller dashboard
  for node in $(seq 1 "$NODE_COUNT"); do
    pid="$(node_pid "$node" || echo '-')"
    printf '%-6s %-10s %-8s %-12s %s\n' \
      "$node" "$pid" "$(data_port "$node")" "$(control_port "$node")" "$(http_port "$node")"
  done
  local brokers
  brokers="$(live_brokers || true)"
  if [[ -n "$brokers" ]]; then
    printf '\nbrokers registered with the controller: %s\n' "$brokers"
    printf 'topics (partitions):\n'
    controller_get 1 /api/v1/controller/metadata 2>/dev/null | perl -0777 -ne '
      use JSON::PP;
      my $image = eval { decode_json($_) } or exit 1;
      my $topics = $image->{topics} || {};
      printf("  %s (%d)\n", $_, scalar keys %{ $topics->{$_}{partitions} })
        for sort keys %$topics;
    ' || true
  else
    warn "controller not answering on 127.0.0.1:$(control_port 1)"
  fi
}

cmd_logs() {
  local node="${1:-1}"
  tail -f "$(log_file "$node")"
}

cmd_destroy() {
  cmd_stop
  stage "Deleting cluster state"
  # Guarded against a surprising rm: only the directory this script owns.
  if [[ -d "$RUN_DIR" && "$RUN_DIR" == "$ROOT/data/cluster" ]]; then
    rm -rf "$RUN_DIR"
    info "removed $RUN_DIR"
  fi
}

case "${1:-start}" in
  start)   cmd_start ;;
  stop)    cmd_stop ;;
  restart) cmd_stop; cmd_start ;;
  status)  cmd_status ;;
  logs)    shift; cmd_logs "$@" ;;
  destroy) cmd_destroy ;;
  *) die "usage: $0 {start|stop|restart|status|logs [node]|destroy}" ;;
esac
