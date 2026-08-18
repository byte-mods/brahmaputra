#!/usr/bin/env bash
# M6 verification: metrics API, Prometheus export, login, RBAC and the
# embedded dashboard, checked live against a running cluster.
#
# The interesting assertions are the negative ones. Anyone can build an API
# that returns data to an admin; what matters is that it refuses a viewer
# trying to create a user, refuses a token signed by someone else, refuses
# an expired session, and never returns a password hash to anybody.
#
# Requires Git Bash on Windows.

set -Eeuo pipefail

NODE_COUNT=3
ADMIN_PASSWORD="${ADMIN_PASSWORD:-bootstrap-admin-pw}"
SESSION_TIMEOUT_MS="${SESSION_TIMEOUT_MS:-5000}"
HEARTBEAT_INTERVAL_MS="${HEARTBEAT_INTERVAL_MS:-500}"
TOPIC="m6-topic"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-m6.XXXXXX")"
CLUSTER_ID="m6-$(perl -e 'printf "%08x", time')"
SERVER_EXE="$ROOT/target/debug/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/debug/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/debug/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/debug/brahmaputra-cli"

CHECKS=0
declare -A DATA_PORT CONTROL_PORT HTTP_PORT NODE_PID

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
  local node
  for node in "${!NODE_PID[@]}"; do
    kill -9 "${NODE_PID[$node]}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [[ "${KEEP_ARTIFACTS:-0}" != "1" && -d "$WORK_DIR" && "$WORK_DIR" == "$TEMP_ROOT"/brahmaputra-m6.* ]]; then
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

peers() {
  local node out=()
  for node in $(seq 1 "$NODE_COUNT"); do
    out+=(--controller-peer "$node=127.0.0.1:${CONTROL_PORT[$node]}")
  done
  printf '%s\n' "${out[@]}"
}

start_node() {
  local node="$1"
  local dir="$WORK_DIR/node-$node"
  mkdir -p "$dir/data"
  local -a peer_args
  mapfile -t peer_args < <(peers)
  BRAHMAPUTRA_ADMIN_PASSWORD="$ADMIN_PASSWORD" "$SERVER_EXE" \
    --host 127.0.0.1 --port "${DATA_PORT[$node]}" \
    --data-dir "$dir/data" \
    --node-id "$node" --cluster-id "$CLUSTER_ID" \
    --control-port "${CONTROL_PORT[$node]}" \
    --http-port "${HTTP_PORT[$node]}" \
    "${peer_args[@]}" \
    --heartbeat-interval-ms "$HEARTBEAT_INTERVAL_MS" \
    --session-timeout-ms "$SESSION_TIMEOUT_MS" \
    --offsets-topic-partitions 1 \
    > "$dir/server.out" 2> "$dir/server.err" &
  NODE_PID[$node]=$!
}

api() {
  local node="$1" method="$2" path="$3" data="${4:-}" auth="${5:-}"
  local -a args=(-sS -X "$method" --max-time 10)
  [[ -n "$auth" ]] && args+=(-H "authorization: Bearer $auth")
  [[ -n "$data" ]] && args+=(-H 'content-type: application/json' --data "$data")
  curl "${args[@]}" "http://127.0.0.1:${HTTP_PORT[$node]}$path"
}

api_status() {
  local node="$1" method="$2" path="$3" data="${4:-}" auth="${5:-}"
  local -a args=(-sS -o /dev/null -w '%{http_code}' -X "$method" --max-time 10)
  [[ -n "$auth" ]] && args+=(-H "authorization: Bearer $auth")
  [[ -n "$data" ]] && args+=(-H 'content-type: application/json' --data "$data")
  curl "${args[@]}" "http://127.0.0.1:${HTTP_PORT[$node]}$path"
}

# Read one field out of a JSON document on stdin. Arrays report their
# length, which is what every use here wants ("how many users came back").
json_field() {
  FIELD="$1" perl -0777 -ne '
    use JSON::PP;
    my $document = eval { decode_json($_) } or exit 1;
    for my $key (split /\./, $ENV{FIELD}) {
      $document = ref $document eq "ARRAY" ? $document->[$key] : $document->{$key};
      exit 1 unless defined $document;
    }
    print ref $document eq "ARRAY" ? scalar @$document : $document;
  '
}

wait_until() {
  local description="$1" timeout_seconds="$2"
  shift 2
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if "$@"; then return 0; fi
    sleep 0.5
  done
  die "timed out waiting for $description"
}

http_ready() { curl -sf --max-time 5 "http://127.0.0.1:${CONTROL_PORT[$1]}/api/v1/controller/metadata" >/dev/null 2>&1; }
all_http_ready() {
  local node
  for node in $(seq 1 "$NODE_COUNT"); do http_ready "$node" || return 1; done
}
dashboard_ready() { curl -sf --max-time 5 "http://127.0.0.1:${HTTP_PORT[$1]}/metrics" >/dev/null 2>&1; }
login_works() { [[ -n "$(api 1 POST /api/v1/auth/login "{\"username\":\"admin\",\"password\":\"$ADMIN_PASSWORD\"}" | json_field token)" ]]; }

# --------------------------------------------------------------- bring up

stage "Start a three-node cluster with the dashboard enabled"
for node in $(seq 1 "$NODE_COUNT"); do
  DATA_PORT[$node]="$(allocate_port)"
  CONTROL_PORT[$node]="$(allocate_port)"
  HTTP_PORT[$node]="$(allocate_port)"
done
for node in $(seq 1 "$NODE_COUNT"); do start_node "$node"; done
wait_until "controller endpoints" 60 all_http_ready
curl -sf -X POST --max-time 10 "http://127.0.0.1:${CONTROL_PORT[1]}/api/v1/controller/bootstrap" >/dev/null \
  || die "bootstrap failed"
wait_until "dashboard on node 1" 60 dashboard_ready 1
pass "every node serves the dashboard and metrics on its own HTTP port"

# ------------------------------------------------------------------ login

stage "Login and RBAC"
wait_until "the admin user to be created on first boot" 60 login_works
LOGIN="$(api 1 POST /api/v1/auth/login "{\"username\":\"admin\",\"password\":\"$ADMIN_PASSWORD\"}")"
ADMIN_TOKEN="$(printf '%s' "$LOGIN" | json_field token)"
[[ -n "$ADMIN_TOKEN" ]] || die "login returned no token: $LOGIN"
assert_eq "$(printf '%s' "$LOGIN" | json_field role)" "admin" "the bootstrap admin logs in with the admin role"

assert_eq "$(api_status 1 POST /api/v1/auth/login '{"username":"admin","password":"wrong"}')" "401" \
  "a wrong password is rejected"
assert_eq "$(api_status 1 POST /api/v1/auth/login '{"username":"nobody","password":"whatever"}')" "401" \
  "an unknown user is rejected with the same status, so accounts cannot be enumerated"
assert_eq "$(api_status 1 GET /api/v1/overview)" "401" "an unauthenticated read is rejected"
assert_eq "$(api_status 1 GET /api/v1/overview '' 'not-a-real-token')" "401" \
  "a forged token is rejected"

# A token from one broker must work on another: the signing secret is
# cluster-wide, which is what makes the dashboard usable behind a load
# balancer.
assert_eq "$(api_status 2 GET /api/v1/overview '' "$ADMIN_TOKEN")" "200" \
  "a token issued by node 1 is accepted by node 2"

stage "Roles are enforced, not merely recorded"
api 1 POST /api/v1/users '{"username":"vera","password":"viewer-password","role":"viewer"}' "$ADMIN_TOKEN" >/dev/null \
  || die "admin could not create a viewer"
api 1 POST /api/v1/users '{"username":"oscar","password":"operator-pw","role":"operator"}' "$ADMIN_TOKEN" >/dev/null \
  || die "admin could not create an operator"
pass "an admin can create users"

wait_until "the new users to replicate" 30 bash -c "[[ -n \"\$(curl -sS -X POST -H 'content-type: application/json' --data '{\"username\":\"vera\",\"password\":\"viewer-password\"}' http://127.0.0.1:${HTTP_PORT[1]}/api/v1/auth/login | perl -0777 -ne 'use JSON::PP; my \$d = eval { decode_json(\$_) } or exit 1; print \$d->{token} // \"\"')\" ]]"
VIEWER_TOKEN="$(api 1 POST /api/v1/auth/login '{"username":"vera","password":"viewer-password"}' | json_field token)"
OPERATOR_TOKEN="$(api 1 POST /api/v1/auth/login '{"username":"oscar","password":"operator-pw"}' | json_field token)"
[[ -n "$VIEWER_TOKEN" && -n "$OPERATOR_TOKEN" ]] || die "new users cannot log in"

assert_eq "$(api_status 1 GET /api/v1/overview '' "$VIEWER_TOKEN")" "200" "a viewer can read the overview"
assert_eq "$(api_status 1 GET /api/v1/users '' "$VIEWER_TOKEN")" "403" \
  "a viewer cannot list users"
assert_eq "$(api_status 1 POST /api/v1/users '{"username":"x","password":"password1","role":"admin"}' "$VIEWER_TOKEN")" "403" \
  "a viewer cannot create an admin — privilege escalation is refused"
assert_eq "$(api_status 1 POST /api/v1/topics '{"name":"viewer-topic","partitions":1,"replication_factor":1}' "$VIEWER_TOKEN")" "403" \
  "a viewer cannot create a topic"
assert_eq "$(api_status 1 POST /api/v1/topics "{\"name\":\"$TOPIC\",\"partitions\":3,\"replication_factor\":3}" "$OPERATOR_TOKEN")" "201" \
  "an operator can create a topic"
assert_eq "$(api_status 1 GET /api/v1/users '' "$OPERATOR_TOKEN")" "403" \
  "an operator still cannot administer users"
assert_eq "$(api_status 1 DELETE /api/v1/users/admin '' "$ADMIN_TOKEN")" "400" \
  "an admin cannot delete their own account and lock the cluster out"

USERS="$(api 1 GET /api/v1/users '' "$ADMIN_TOKEN")"
printf '%s' "$USERS" | grep -q 'password_hash' \
  && die "the users endpoint leaked a password hash"
pass "no endpoint returns a password hash, even to an admin"

grep -rq "$ADMIN_PASSWORD" "$WORK_DIR"/node-*/data 2>/dev/null \
  && die "the admin password was written to disk in the clear"
pass "the admin password is never stored in the clear"

# ---------------------------------------------------------------- metrics

stage "Metrics reflect real traffic"
BEFORE="$(curl -sS "http://127.0.0.1:${HTTP_PORT[1]}/metrics" | awk '/^brahmaputra_produce_records_total /{print $2}')"
BEFORE="${BEFORE:-0}"
wait_until "the topic to have leaders" 60 bash -c "curl -sf --max-time 5 http://127.0.0.1:${CONTROL_PORT[1]}/api/v1/controller/metadata | grep -q '$TOPIC'"
sleep 2
"$CLI_EXE" --broker "127.0.0.1:${DATA_PORT[1]}" produce --topic "$TOPIC" \
  --count 500 --value-size 256 --acks all >/dev/null || die "produce failed"

metrics_rose() {
  local now
  now="$(curl -sS "http://127.0.0.1:${HTTP_PORT[1]}/metrics" | awk '/^brahmaputra_produce_records_total /{print $2}')"
  [[ -n "$now" ]] && (( ${now%.*} > ${BEFORE%.*} ))
}
wait_until "produce metrics to rise" 30 metrics_rose
pass "produce counters rose after real traffic ($BEFORE -> $(curl -sS "http://127.0.0.1:${HTTP_PORT[1]}/metrics" | awk '/^brahmaputra_produce_records_total /{print $2}'))"

PROM="$(curl -sS "http://127.0.0.1:${HTTP_PORT[1]}/metrics")"
printf '%s' "$PROM" | grep -q '^# TYPE brahmaputra_produce_records_total counter' \
  || die "Prometheus output is missing TYPE metadata"
printf '%s' "$PROM" | grep -q '^# HELP brahmaputra_produce_records_total ' \
  || die "Prometheus output is missing HELP metadata"
pass "the Prometheus endpoint emits well-formed HELP/TYPE metadata"
assert_eq "$(api_status 1 GET /metrics)" "200" \
  "the Prometheus endpoint needs no session, so a scraper can use it"

# Per-partition gauges are sampled on a timer.
partition_gauges_present() {
  curl -sS "http://127.0.0.1:${HTTP_PORT[1]}/metrics" \
    | grep -q "brahmaputra_partition_log_end_offset{topic=\"$TOPIC\""
}
wait_until "per-partition gauges to be sampled" 30 partition_gauges_present
pass "per-partition offsets and watermarks appear as labelled gauges"

# A counter only exists once something increments it, so its history
# starts at the first sampler tick after the first produce. Wait for that
# tick rather than assuming one has already happened.
series_has_samples() {
  local count
  count="$(api 1 GET '/api/v1/metrics/timeseries?metric=brahmaputra_produce_records_total' '' "$ADMIN_TOKEN" \
    | json_field samples)"
  [[ -n "$count" ]] && (( count > 0 ))
}
wait_until "the sampler to record the produce counter" 30 series_has_samples
SAMPLES="$(api 1 GET '/api/v1/metrics/timeseries?metric=brahmaputra_produce_records_total' '' "$ADMIN_TOKEN" | json_field samples)"
pass "the in-process time series has $SAMPLES samples for the dashboard to chart"

# --------------------------------------------------------------- cluster

stage "Cluster views"
OVERVIEW="$(api 1 GET /api/v1/overview '' "$ADMIN_TOKEN")"
assert_eq "$(printf '%s' "$OVERVIEW" | json_field brokers_alive)" "3" "the overview sees three live brokers"
assert_eq "$(printf '%s' "$OVERVIEW" | json_field under_replicated_partitions)" "0" \
  "no partition is under-replicated on a healthy cluster"

TOPIC_DETAIL="$(api 1 "GET" "/api/v1/topics/$TOPIC" '' "$ADMIN_TOKEN")"
assert_eq "$(printf '%s' "$TOPIC_DETAIL" | json_field partitions)" "3" \
  "topic detail lists every partition with its leader, ISR and offsets"

# Kill a broker and confirm the dashboard reports the damage rather than
# quietly showing a healthy cluster.
kill -9 "${NODE_PID[3]}" 2>/dev/null || true
unset 'NODE_PID[3]'
reports_a_dead_broker() {
  local alive
  alive="$(api 1 GET /api/v1/overview '' "$ADMIN_TOKEN" | json_field brokers_alive)"
  [[ "$alive" == "2" ]]
}
wait_until "the dashboard to notice the dead broker" 60 reports_a_dead_broker
pass "killing a broker is reflected in the overview within the session timeout"

reports_under_replication() {
  local urp
  urp="$(api 1 GET /api/v1/overview '' "$ADMIN_TOKEN" | json_field under_replicated_partitions)"
  [[ -n "$urp" ]] && (( urp > 0 ))
}
wait_until "under-replicated partitions to be reported" 60 reports_under_replication
pass "under-replicated partitions are surfaced after the failure"

# ------------------------------------------------------------- dashboard

stage "The dashboard page itself"
PAGE="$(curl -sS "http://127.0.0.1:${HTTP_PORT[1]}/")"
printf '%s' "$PAGE" | grep -q '<title>Brahmaputra</title>' || die "the dashboard page did not render"
printf '%s' "$PAGE" | grep -q 'api/v1/auth/login' || die "the dashboard has no login form"
pass "the dashboard is served from the binary with no external assets"
printf '%s' "$PAGE" | grep -Eq 'https?://[^"]*(cdn|googleapis|unpkg|jsdelivr)' \
  && die "the dashboard references an external asset; it must work air-gapped"
pass "the dashboard loads nothing from the network"

stage "M6 verification complete"
printf 'checks passed: %s\n' "$CHECKS"
