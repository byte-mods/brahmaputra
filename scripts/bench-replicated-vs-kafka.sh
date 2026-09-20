#!/usr/bin/env bash
# Head-to-head at the durability setting people actually deploy:
# three brokers, RF=3, acks=all, min.insync.replicas=2 — Kafka against
# Brahmaputra, same container limits, same workload, same host.
#
# Why this script exists. Every published comparison in this repo is
# single-node, RF=1, acks=1, and `bench-replicated.sh` measures the
# replication cost for Brahmaputra *alone*. Neither answers the question a
# reader actually has: at the durable setting, which is faster? Carrying the
# RF=1 multiplier over to RF=3 would be dishonest — Kafka pays a replication
# cost too, and until this script ran nobody here had measured how much.
#
# So both systems are measured twice, on the same three-node cluster:
#
#   rf1  RF=1, acks=1                      the published configuration
#   rf3  RF=3, acks=all, min.insync=2      the durable one
#
# That yields three readings rather than one: each system's absolute
# throughput at RF=3, the ratio between them, and each system's *own*
# replication cost (rf1/rf3), which is the number that says whether a design
# replicates cheaply or expensively.
#
# Fairness rules, all of which matter:
#   - Every container of every system gets identical --cpus and --memory.
#   - Both clusters are three nodes on this one host. Shared disk and NIC
#     contention can affect them differently; results describe this host.
#   - Load generators run inside the broker containers and are spread
#     round-robin across all three, so sampled CPU covers broker plus client
#     for everyone and no single node carries all the client load.
#   - Clients are counted from their own reported rate as well as wall
#     clock, because a JVM perf tool takes seconds to start and the
#     Brahmaputra CLI takes milliseconds; wall clock alone flatters the
#     latter.
#   - Broker leases stay at their defaults (1 s heartbeat, 5 s session).
#     Expiry suspends the data plane until conditional re-registration;
#     the benchmark does not lengthen leases to conceal recovery failures.
#   - Each level's replication is verified, not assumed: Kafka must report
#     ISR=3 and Brahmaputra must have the partition logs on all three nodes
#     with log-end offsets summing to the records produced.
#   - Both consumers join a consumer group. kafka-consumer-perf-test always
#     does, and it pays for it — a measured 696 ms of group rebalance inside
#     a 2.4 s run. Letting the Brahmaputra CLI read partitions directly, as
#     an earlier revision of this script did, compared a coordinated
#     consumer against an uncoordinated one and overstated Brahmaputra's
#     consume rate several-fold.
#
#   ONLY=kafka|brahmaputra|both  bash scripts/bench-replicated-vs-kafka.sh
set -Eeuo pipefail

PER_CLIENT="${PER_CLIENT:-500000}"
RECORD_SIZE="${RECORD_SIZE:-256}"
PARTITIONS="${PARTITIONS:-6}"
GROUP_INITIAL_REBALANCE_DELAY_MS="${GROUP_INITIAL_REBALANCE_DELAY_MS:-0}"
LEVELS="${LEVELS:-1 2 4}"
BATCH_SIZE="${BATCH_SIZE:-65536}"
LINGER_MS="${LINGER_MS:-5}"
COMPRESSION="${COMPRESSION:-none}"
IN_FLIGHT="${IN_FLIGHT:-4096}"
# Offered records per second per client. Unset means "as fast as the broker
# accepts", which measures saturation throughput; set it below saturation to
# measure acknowledgement latency instead, since at saturation latency is
# just queue depth over throughput.
RATE="${RATE:-}"
CPUS="${CPUS:-4}"
MEMORY="${MEMORY:-4g}"
NODES=3
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.3.1}"
RUST_IMAGE="${RUST_IMAGE:-rust:1-bookworm}"
NETWORK="${NETWORK:-brahma-bench-rep}"
# Both clusters advertise static IPs rather than container names. Not a
# convenience: a Brahmaputra client pays roughly five seconds of one-time
# setup per *additional* broker it must reach when brokers advertise
# hostnames — ~10 s of dead time on a three-node cluster, which at these
# record counts would swamp the throughput being measured. Kafka shows no
# such penalty on the same Docker DNS. Removing name resolution from both
# sides measures the engines rather than that defect; the defect itself is
# measured separately and reported on its own.
SUBNET="${SUBNET:-172.31.77.0/24}"
KAFKA_IPS=(172.31.77.21 172.31.77.22 172.31.77.23)
BRAHMA_IPS=(172.31.77.11 172.31.77.12 172.31.77.13)
MAX_BYTES="${MAX_BYTES:-16777216}"
CLUSTER_ID="${CLUSTER_ID:-5L6g3nShT-eMCtK--X86sw}"
SKIP_BUILD="${SKIP_BUILD:-0}"
ONLY="${ONLY:-both}"
# Which Brahmaputra image to run. Overriding it with an image built from an
# older commit is how a before/after comparison is taken without touching
# the working tree: build that image once, then run with SKIP_BUILD=1.
BRAHMA_IMAGE="${BRAHMA_IMAGE:-brahmaputra-bench:latest}"

# See bench-matched.sh for why the broker heap and the client heap are set
# separately: KAFKA_HEAP_OPTS is read by every Kafka CLI tool, so one large
# container-wide heap would give each perf-test client a 2 GiB reservation
# and wall the container off from its own page cache.
KAFKA_BROKER_HEAP="${KAFKA_BROKER_HEAP:--Xmx2g -Xms2g}"
KAFKA_BROKER_GC="${KAFKA_BROKER_GC:--XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M -XX:MetaspaceSize=96m -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true}"
KAFKA_CLIENT_HEAP="${KAFKA_CLIENT_HEAP:--Xmx512m -Xms512m}"
KAFKA_CLIENT_GC="${KAFKA_CLIENT_GC:--XX:+UseG1GC -XX:MaxGCPauseMillis=20}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/bench/results}"
RESULTS="$OUT_DIR/replicated"
mkdir -p "$RESULTS"
REPORT="$OUT_DIR/replicated-vs-kafka.md"
CSV="$RESULTS/levels.csv"
NOTES="$RESULTS/verification.txt"

stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
info() { printf '    %s\n' "$1"; }
warn() { printf '\033[33m    ! %s\033[0m\n' "$1"; }
die() { printf '\n\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

# MSYS/Git Bash rewrites anything that looks like an absolute path; container
# paths must survive intact.
docker_run() { MSYS_NO_PATHCONV=1 docker "$@"; }
host_path() { if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else printf '%s' "$1"; fi; }

KAFKA_NODES=(bench-k1 bench-k2 bench-k3)
BRAHMA_NODES=(bench-b1 bench-b2 bench-b3)

cleanup() {
  if declare -F stop_resource_sampling >/dev/null; then stop_resource_sampling || true; fi
  # Capture diagnostics before removing this run's containers, on success
  # as well as failure. Client errors alone cannot explain lease loss.
  for node in "${KAFKA_NODES[@]}" "${BRAHMA_NODES[@]}"; do
    if docker_run inspect "$node" >/dev/null 2>&1; then
      docker_run logs "$node" > "$RESULTS/$node.log" 2>&1 || true
      docker_run inspect "$node" > "$RESULTS/$node-inspect.json" 2>/dev/null || true
    fi
  done
  docker_run rm -f "${KAFKA_NODES[@]}" "${BRAHMA_NODES[@]}" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup
docker_run network create --subnet "$SUBNET" "$NETWORK" >/dev/null 2>&1 || true
docker_run network inspect "$NETWORK" --format '{{range .IPAM.Config}}{{.Subnet}}{{end}}' 2>/dev/null \
  | grep -q "$SUBNET" || die "network $NETWORK exists with a different subnet; remove it first"

: > "$CSV"
printf 'system,config,phase,clients,records,seconds,msgs_per_sec,client_msgs_per_sec,cpu_avg,cpu_peak,mem_avg,mem_peak,lat_p50_ms,lat_p99_ms,lat_p999_ms,lat_max_ms\n' >> "$CSV"
: > "$NOTES"

# ------------------------------------------------------------- sampling
# Cgroup probes cover all three containers. Their overlapping observation
# window measures the cluster, including its clients, as one unit.

source "$ROOT/scripts/bench-resources.sh"
start_sampling() { start_resource_sampling "$@"; }
stop_sampling() { stop_resource_sampling; }
summarize_samples() { summarize_resource_samples "$1"; }

now_ms() { benchmark_now_ms; }
rate() { awk -v r="$1" -v ms="$2" 'BEGIN { if (ms > 0) printf "%.0f", r * 1000 / ms; else print 0 }'; }
wait_all() {
  local pid status=0 checks=0 deadline=$(( $(now_ms) + ${PHASE_TIMEOUT_SECONDS:-180} * 1000 ))
  for pid in "${pids[@]}"; do
    while kill -0 "$pid" 2>/dev/null; do
      # Read the monotonic clock approximately once a second; completion is
      # still observed every 50 ms without spawning a clock process each poll.
      if (( checks % 20 == 0 )) && (( $(now_ms) >= deadline )); then
        printf 'Benchmark phase exceeded %s seconds\n' "${PHASE_TIMEOUT_SECONDS:-180}" >&2
        for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
        return 1
      fi
      checks=$((checks + 1))
      sleep 0.05
    done
    wait "$pid" || status=1
  done
  return "$status"
}

# Acknowledgement-latency percentiles, in milliseconds.
#
# Both sides measure the same span — record admitted to record acked — but
# report it differently, so each is parsed from its own summary line rather
# than recomputed. Kafka prints whole milliseconds; Brahmaputra prints two
# decimals. With several clients per level the percentiles are averaged
# across them: a percentile of percentiles is not a percentile of the
# population, but it is the honest summary available without the raw
# samples, and it is applied identically to both systems.
latency_summary() {
    local kind="$1"; shift
    local file p50=0 p99=0 p999=0 max=0 n=0 value
    for file in "$@"; do
        [[ -f "$file" ]] || continue
        case "$kind" in
            kafka)
                value="$(sed -n 's/.*records sent.*, \([0-9.]*\) ms avg latency, \([0-9.]*\) ms max latency, \([0-9]*\) ms 50th, [0-9]* ms 95th, \([0-9]*\) ms 99th, \([0-9]*\) ms 99.9th.*/\3 \4 \5 \2/p' "$file" | tail -1)" ;;
            brahmaputra)
                value="$(sed -n 's/.*p50=\([0-9.]*\) p95=[0-9.]* p99=\([0-9.]*\) p99.9=\([0-9.]*\) max=\([0-9.]*\).*/\1 \2 \3 \4/p' "$file" | tail -1)" ;;
        esac
        [[ -n "$value" ]] || continue
        read -r a b c d <<<"$value"
        p50="$(awk -v x="$p50" -v y="$a" 'BEGIN { print x + y }')"
        p99="$(awk -v x="$p99" -v y="$b" 'BEGIN { print x + y }')"
        p999="$(awk -v x="$p999" -v y="$c" 'BEGIN { print x + y }')"
        max="$(awk -v x="$max" -v y="$d" 'BEGIN { print (y > x) ? y : x }')"
        n=$(( n + 1 ))
    done
    if (( n == 0 )); then
        printf 'NA,NA,NA,NA'
        return
    fi
    awk -v a="$p50" -v b="$p99" -v c="$p999" -v d="$max" -v n="$n" \
        'BEGIN { printf "%.2f,%.2f,%.2f,%.2f", a/n, b/n, c/n, d }'
}

sum_client_rates() {
  local kind="$1"; shift
  local file total=0 value
  for file in "$@"; do
    [[ -f "$file" ]] || continue
    case "$kind" in
      kafka-produce) value="$(sed -n 's/.*records sent, \([0-9.]*\) records\/sec.*99.9th.*/\1/p' "$file" | tail -1)" ;;
      kafka-consume) value="$(awk -F', *' 'FNR>1 && NF>=6 { rate=$6 } END { print rate }' "$file")" ;;
      brahmaputra)   value="$(sed -n 's/.*-> \([0-9.]*\) msgs\/sec.*/\1/p' "$file" | tail -1)" ;;
    esac
    total="$(awk -v t="$total" -v v="${value:-0}" 'BEGIN { printf "%.4f", t + v }')"
  done
  awk -v t="$total" 'BEGIN { printf "%.0f", t }'
}

record_level() {
  local system="$1" config="$2" phase="$3" clients="$4" records="$5" ms="$6" stats="$7" client_rate="$8"
  node "$ROOT/scripts/bench-resource-summary.cjs" --check-window "$stats.resources.json" "$ms"
  local latency="${9:-0,0,0,0}"
  local seconds throughput
  seconds="$(awk -v ms="$ms" 'BEGIN { printf "%.2f", ms / 1000 }')"
  throughput="$(rate "$records" "$ms")"
  read -r cpu cpu_max mem mem_max <<<"$(summarize_samples "$stats")"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$system" "$config" "$phase" "$clients" "$records" "$seconds" "$throughput" \
    "${client_rate:-0}" "$cpu" "$cpu_max" "$mem" "$mem_max" "$latency" >> "$CSV"
  info "$system/$config $phase x$clients: $throughput msgs/sec wall (${client_rate:-0} client), CPU ${cpu}%, ${mem} MiB"
  [[ "$phase" == "produce" ]] && info "  ack latency ms: p50/p99/p99.9/max = ${latency//,/ / }"
  return 0
}

# ---------------------------------------------------------------- build

if [[ "$SKIP_BUILD" != "1" && "$ONLY" != "kafka" ]]; then
  stage "Build a Linux release binary set in a cargo-cached builder container"
  docker_run volume create brahma-bench-cargo >/dev/null
  docker_run volume create brahma-bench-target >/dev/null
  docker_run run --rm \
    -v "$(host_path "$ROOT"):/src" \
    -v brahma-bench-cargo:/usr/local/cargo/registry \
    -v brahma-bench-target:/target \
    -e CARGO_TARGET_DIR=/target \
    -w /src "$RUST_IMAGE" \
    cargo build --release -j "${BUILD_JOBS:-2}" -p brahmaputra-server -p brahmaputra-cli \
    || die "release build failed"

  stage "Assemble the Brahmaputra runtime image"
  STAGE_DIR="$ROOT/bench/.stage"
  rm -rf "$STAGE_DIR"; mkdir -p "$STAGE_DIR"
  docker_run run --rm -v brahma-bench-target:/target -v "$(host_path "$STAGE_DIR"):/out" "$RUST_IMAGE" \
    bash -c 'cp /target/release/brahmaputra-server /target/release/brahmaputra-cli /out/'
  cp "$ROOT/bench/Dockerfile.brahmaputra" "$STAGE_DIR/Dockerfile"
  docker_run build -q -t brahmaputra-bench:latest "$(host_path "$STAGE_DIR")" >/dev/null || die "image build failed"
  rm -rf "$STAGE_DIR"
fi

# ---------------------------------------------------------------- kafka
# Three brokers, each also a controller: the KRaft equivalent of the
# three-node Brahmaputra cluster, with the internal topics replicated three
# ways so the control plane is as durable as the data plane.

start_kafka() {
  stage "Kafka: $NODES brokers, KRaft, RF=3-capable"
  local voters="" i
  for (( i = 1; i <= NODES; i++ )); do
    voters+="${voters:+,}$i@${KAFKA_IPS[$(( i - 1 ))]}:9093"
  done
  for (( i = 1; i <= NODES; i++ )); do
    docker_run run -d --name "bench-k$i" --network "$NETWORK" \
      --ip "${KAFKA_IPS[$(( i - 1 ))]}" \
      --cpus "$CPUS" --memory "$MEMORY" \
      -e CLUSTER_ID="$CLUSTER_ID" \
      -e KAFKA_NODE_ID="$i" \
      -e KAFKA_PROCESS_ROLES=broker,controller \
      -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
      -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${KAFKA_IPS[$(( i - 1 ))]}:9092" \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS="$voters" \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
      -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
      -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 \
      -e KAFKA_DEFAULT_REPLICATION_FACTOR=3 \
      -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS="$GROUP_INITIAL_REBALANCE_DELAY_MS" \
      -e KAFKA_NUM_PARTITIONS="$PARTITIONS" \
      -e KAFKA_LOG_SEGMENT_BYTES=1073741824 \
      -e KAFKA_NUM_NETWORK_THREADS=4 \
      -e KAFKA_NUM_IO_THREADS=8 \
      -e KAFKA_NUM_REPLICA_FETCHERS=4 \
      -e KAFKA_MESSAGE_MAX_BYTES="$MAX_BYTES" \
      -e KAFKA_REPLICA_FETCH_MAX_BYTES="$MAX_BYTES" \
      -e KAFKA_SOCKET_REQUEST_MAX_BYTES=104857600 \
      -e KAFKA_SOCKET_SEND_BUFFER_BYTES=1048576 \
      -e KAFKA_SOCKET_RECEIVE_BUFFER_BYTES=1048576 \
      -e KAFKA_QUEUED_MAX_REQUESTS=1000 \
      -e KAFKA_HEAP_OPTS="$KAFKA_BROKER_HEAP" \
      -e KAFKA_JVM_PERFORMANCE_OPTS="$KAFKA_BROKER_GC" \
      "$KAFKA_IMAGE" >/dev/null || die "cannot start bench-k$i"
  done
  local deadline=$(( $(benchmark_now_ms) + 240000 ))
  while (( $(benchmark_now_ms) < deadline )); do
    if docker_run exec bench-k1 /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server "$KAFKA_BOOTSTRAP" >/dev/null 2>&1; then
      sleep 5
      return 0
    fi
    sleep 3
  done
  docker_run logs --tail 40 bench-k1 >&2 || true
  die "Kafka cluster never became ready"
}

KAFKA_BOOTSTRAP="${KAFKA_IPS[0]}:9092,${KAFKA_IPS[1]}:9092,${KAFKA_IPS[2]}:9092"

kafka_topic() {
  local name="$1" rf="$2"
  local -a extra=()
  (( rf > 1 )) && extra=(--config min.insync.replicas=2)
  docker_run exec bench-k1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$PARTITIONS" --replication-factor "$rf" \
    --config max.message.bytes="$MAX_BYTES" "${extra[@]}" >/dev/null
}

# Assert the topic really is replicated three ways before its numbers are
# allowed to stand for RF=3: a topic that silently fell back to one replica
# would produce a flatteringly fast, and completely meaningless, result.
kafka_verify_isr() {
  local name="$1" expect="$2" described bad total deadline=$(( $(benchmark_now_ms) + 60000 ))
  while (( $(benchmark_now_ms) < deadline )); do
  described="$(docker_run exec bench-k1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic "$name" 2>/dev/null)" || described=''
  total="$(printf '%s\n' "$described" | grep -c 'Partition:' || true)"
  bad="$(printf '%s\n' "$described" | awk -v want="$expect" '
    /Partition:/ {
      n = 0;
      for (i = 1; i <= NF; i++) if ($i == "Isr:") { split($(i+1), a, ","); n = length(a) }
      if (n != want) bad++
    }
    END { print bad + 0 }')"
    if [[ "$total" == "$PARTITIONS" && "$bad" == 0 ]]; then
      printf 'kafka %s: %s/%s partitions at ISR=%s\n' \
        "$name" "$total" "$total" "$expect" >> "$NOTES"
      return 0
    fi
    sleep 1
  done
  die "kafka $name: $total partitions, $bad not at ISR=$expect"
}

run_kafka_level() {
  local config="$1" rf="$2" acks="$3" clients="$4"
  local index start elapsed node node_ip
  local -a pids=()
  for (( index = 0; index < clients; index++ )); do
    kafka_topic "$TOPIC-$config-$clients-$index" "$rf"
  done
  for (( index = 0; index < clients; index++ )); do
    kafka_verify_isr "$TOPIC-$config-$clients-$index" "$rf"
  done

  start_sampling "$RESULTS/kafka-$config-produce-$clients.stats" "${KAFKA_NODES[@]}"
  start="$(now_ms)"
  pids=()
  for (( index = 0; index < clients; index++ )); do
    node="${KAFKA_NODES[$(( index % NODES ))]}"
    docker_run exec \
      -e KAFKA_HEAP_OPTS="$KAFKA_CLIENT_HEAP" \
      -e KAFKA_JVM_PERFORMANCE_OPTS="$KAFKA_CLIENT_GC" \
      "$node" /opt/kafka/bin/kafka-producer-perf-test.sh \
      --topic "$TOPIC-$config-$clients-$index" --num-records "$PER_CLIENT" \
      --record-size "$RECORD_SIZE" --throughput "${RATE:--1}" \
      --producer-props bootstrap.servers="$KAFKA_BOOTSTRAP" "acks=$acks" \
        "batch.size=$BATCH_SIZE" "linger.ms=$LINGER_MS" \
        "compression.type=$COMPRESSION" "max.request.size=$MAX_BYTES" \
        "buffer.memory=268435456" "enable.idempotence=false" \
      > "$RESULTS/kafka-$config-produce-$clients-$index.txt" 2>&1 &
    pids+=($!)
  done
  wait_all || { stop_sampling; die "Kafka $config produce failed at $clients clients"; }
  elapsed=$(( $(now_ms) - start ))
  stop_sampling
  record_level kafka "$config" produce "$clients" $(( PER_CLIENT * clients )) "$elapsed" \
    "$RESULTS/kafka-$config-produce-$clients.stats" \
    "$(sum_client_rates kafka-produce "$RESULTS"/kafka-"$config"-produce-"$clients"-*.txt)" \
    "$(latency_summary kafka "$RESULTS"/kafka-"$config"-produce-"$clients"-*.txt)"

  for (( index = 0; index < clients; index++ )); do
    kafka_verify_isr "$TOPIC-$config-$clients-$index" "$rf"
  done

  start_sampling "$RESULTS/kafka-$config-consume-$clients.stats" "${KAFKA_NODES[@]}"
  start="$(now_ms)"
  pids=()
  for (( index = 0; index < clients; index++ )); do
    node="${KAFKA_NODES[$(( index % NODES ))]}"
    docker_run exec \
      -e KAFKA_HEAP_OPTS="$KAFKA_CLIENT_HEAP" \
      -e KAFKA_JVM_PERFORMANCE_OPTS="$KAFKA_CLIENT_GC" \
      "$node" /opt/kafka/bin/kafka-consumer-perf-test.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$TOPIC-$config-$clients-$index" \
      --messages "$PER_CLIENT" --group "rep-$config-$clients-$index-$RANDOM" \
      --timeout 300000 --fetch-size "$MAX_BYTES" \
      > "$RESULTS/kafka-$config-consume-$clients-$index.txt" 2>&1 &
    pids+=($!)
  done
  wait_all || { stop_sampling; die "Kafka $config consume failed at $clients clients"; }
  elapsed=$(( $(now_ms) - start ))
  stop_sampling
  record_level kafka "$config" consume "$clients" $(( PER_CLIENT * clients )) "$elapsed" \
    "$RESULTS/kafka-$config-consume-$clients.stats" \
    "$(sum_client_rates kafka-consume "$RESULTS"/kafka-"$config"-consume-"$clients"-*.txt)"
}

# ---------------------------------------------------------- brahmaputra

BRAHMA_CONTROLLER="http://${BRAHMA_IPS[0]}:19311"

start_brahmaputra() {
  stage "Brahmaputra: $NODES brokers, Raft control plane"
  local i peer
  local -a peers=()
  for (( peer = 1; peer <= NODES; peer++ )); do
    peers+=(--controller-peer "$peer=${BRAHMA_IPS[$(( peer - 1 ))]}:19311")
  done
  for (( i = 1; i <= NODES; i++ )); do
    local -a publish=()
    # Node 1's controller port is published so the host can POST the
    # one-time bootstrap; the image has no curl of its own.
    (( i == 1 )) && publish=(-p 127.0.0.1:19311:19311)
    docker_run run -d --name "bench-b$i" --network "$NETWORK" \
      --ip "${BRAHMA_IPS[$(( i - 1 ))]}" \
      --cpus "$CPUS" --memory "$MEMORY" "${publish[@]}" \
      "$BRAHMA_IMAGE" \
      --host "${BRAHMA_IPS[$(( i - 1 ))]}" --port 9092 --data-dir /data \
      --node-id "$i" --cluster-id brahma-rep \
      --control-port 19311 --http-port 0 \
      --default-partitions "$PARTITIONS" --segment-bytes 1073741824 \
      --group-initial-rebalance-delay-ms "$GROUP_INITIAL_REBALANCE_DELAY_MS" \
      "${peers[@]}" >/dev/null || die "cannot start bench-b$i"
  done

  local deadline=$(( $(benchmark_now_ms) + 60000 ))
  while (( $(benchmark_now_ms) < deadline )); do
    curl -fsS -X POST "http://127.0.0.1:19311/api/v1/controller/bootstrap" >/dev/null 2>&1 && break
    sleep 2
  done
  sleep 5

  deadline=$(( $(benchmark_now_ms) + 90000 ))
  while (( $(benchmark_now_ms) < deadline )); do
    if docker_run exec bench-b1 /usr/local/bin/brahmaputra-cli \
        --broker "${BRAHMA_IPS[0]}:9092" metadata >/dev/null 2>&1; then
      sleep 3
      return 0
    fi
    sleep 2
  done
  docker_run logs --tail 40 bench-b1 >&2 || true
  die "Brahmaputra cluster never became ready"
}

brahma_topic() {
  local name="$1" rf="$2"
  local -a extra=()
  (( rf > 1 )) && extra=(--config min.insync.replicas=2)
  docker_run exec bench-b1 /usr/local/bin/brahmaputra-cli \
    --controller "$BRAHMA_CONTROLLER" topic create \
    --name "$name" --partitions "$PARTITIONS" --replication-factor "$rf" \
    "${extra[@]}" >/dev/null 2>&1 || true
}

# The Brahmaputra counterpart of the Kafka ISR assertion: count how many
# nodes hold a log for the topic, and check the log-end offsets sum to the
# records produced. Both must hold or the RF=3 reading means nothing.
brahma_verify() {
  local name="$1" rf="$2" expected="$3" i holders=0 count total offsets deadline=$(( $(benchmark_now_ms) + 60000 ))
  while (( $(benchmark_now_ms) < deadline )); do
  holders=0
  for (( i = 1; i <= NODES; i++ )); do
    count="$(docker_run exec "bench-b$i" sh -c "find /data -name '*.log' -path '*$name*' 2>/dev/null | wc -l" | tr -d '[:space:]')"
    (( ${count:-0} > 0 )) && holders=$(( holders + 1 ))
  done
  if offsets="$(docker_run exec bench-b1 /usr/local/bin/brahmaputra-cli \
    --broker "${BRAHMA_IPS[0]}:9092" offsets --topic "$name" 2>/dev/null)"; then
    total="$(printf '%s\n' "$offsets" | grep -oE 'latest=[0-9]+' | grep -oE '[0-9]+' | awk '{s+=$1} END {print s+0}')" || total=0
    count="$(printf '%s\n' "$offsets" | grep -c 'latest=' || true)"
    if (( holders >= rf )) && [[ "$count" == "$PARTITIONS" && "$total" == "$expected" ]]; then
      printf 'brahmaputra %s: %s/%s nodes hold logs, %s partitions, offsets sum %s (expected %s)\n' \
        "$name" "$holders" "$rf" "$count" "$total" "$expected" >> "$NOTES"
      return 0
    fi
  fi
  sleep 1
  done
  die "brahmaputra $name: replication/offset verification did not converge in 60 seconds"
}

run_brahmaputra_level() {
  local config="$1" rf="$2" acks="$3" clients="$4"
  local index start elapsed node node_ip
  local -a pids=()
  for (( index = 0; index < clients; index++ )); do
    brahma_topic "$TOPIC-$config-$clients-$index" "$rf"
  done
  sleep 3

  start_sampling "$RESULTS/brahmaputra-$config-produce-$clients.stats" "${BRAHMA_NODES[@]}"
  start="$(now_ms)"
  pids=()
  for (( index = 0; index < clients; index++ )); do
    node="${BRAHMA_NODES[$(( index % NODES ))]}"; node_ip="${BRAHMA_IPS[$(( index % NODES ))]}"
    docker_run exec "$node" /usr/local/bin/brahmaputra-cli \
      --broker "$node_ip:9092" produce \
      --topic "$TOPIC-$config-$clients-$index" --count "$PER_CLIENT" \
      --value-size "$RECORD_SIZE" --no-key --acks "$acks" \
      --batch-size "$BATCH_SIZE" --linger-ms "$LINGER_MS" \
      --in-flight "$IN_FLIGHT" --compression "$COMPRESSION" --latency \
      ${RATE:+--rate "$RATE"} \
      > "$RESULTS/brahmaputra-$config-produce-$clients-$index.txt" 2>&1 &
    pids+=($!)
  done
  wait_all || { stop_sampling; die "Brahmaputra $config produce failed at $clients clients"; }
  elapsed=$(( $(now_ms) - start ))
  stop_sampling
  record_level brahmaputra "$config" produce "$clients" $(( PER_CLIENT * clients )) "$elapsed" \
    "$RESULTS/brahmaputra-$config-produce-$clients.stats" \
    "$(sum_client_rates brahmaputra "$RESULTS"/brahmaputra-"$config"-produce-"$clients"-*.txt)" \
    "$(latency_summary brahmaputra "$RESULTS"/brahmaputra-"$config"-produce-"$clients"-*.txt)"

  for (( index = 0; index < clients; index++ )); do
    brahma_verify "$TOPIC-$config-$clients-$index" "$rf" "$PER_CLIENT"
  done

  start_sampling "$RESULTS/brahmaputra-$config-consume-$clients.stats" "${BRAHMA_NODES[@]}"
  start="$(now_ms)"
  pids=()
  for (( index = 0; index < clients; index++ )); do
    node="${BRAHMA_NODES[$(( index % NODES ))]}"; node_ip="${BRAHMA_IPS[$(( index % NODES ))]}"
    docker_run exec "$node" /usr/local/bin/brahmaputra-cli \
      --broker "$node_ip:9092" consume \
      --topic "$TOPIC-$config-$clients-$index" \
      --group "rep-$config-$clients-$index-$RANDOM" \
      --auto-offset-reset earliest \
      --max "$PER_CLIENT" --quiet \
      > "$RESULTS/brahmaputra-$config-consume-$clients-$index.txt" 2>&1 &
    pids+=($!)
  done
  wait_all || { stop_sampling; die "Brahmaputra $config consume failed at $clients clients"; }
  elapsed=$(( $(now_ms) - start ))
  stop_sampling
  record_level brahmaputra "$config" consume "$clients" $(( PER_CLIENT * clients )) "$elapsed" \
    "$RESULTS/brahmaputra-$config-consume-$clients.stats" \
    "$(sum_client_rates brahmaputra "$RESULTS"/brahmaputra-"$config"-consume-"$clients"-*.txt)"
}

# ----------------------------------------------------------------- run

TOPIC="rep-$(date +%s)"

if [[ "$ONLY" == "both" || "$ONLY" == "kafka" ]]; then
  start_kafka
  for level in $LEVELS; do run_kafka_level rf3 3 all "$level"; done
  for level in $LEVELS; do run_kafka_level rf1 1 1 "$level"; done
  cleanup
fi

if [[ "$ONLY" == "both" || "$ONLY" == "brahmaputra" ]]; then
  start_brahmaputra
  for level in $LEVELS; do run_brahmaputra_level rf3 3 all "$level"; done
  for level in $LEVELS; do run_brahmaputra_level rf1 1 1 "$level"; done
  cleanup
fi

# -------------------------------------------------------------- report

peak() { awk -F, -v s="$1" -v c="$2" -v p="$3" '$1==s && $2==c && $3==p && $7+0 > b+0 { b=$7; row=$0 } END { print row }' "$CSV"; }
field() { printf '%s' "$1" | cut -d, -f"$2"; }
ratio() { awk -v a="$1" -v b="$2" 'BEGIN { if (b+0 > 0) printf "%.2fx", a/b; else print "n/a" }'; }

emit_head_to_head() {
  local phase="$1" krow brow
  krow="$(peak kafka rf3 "$phase")"
  brow="$(peak brahmaputra rf3 "$phase")"
  printf '### %s at RF=3, acks=all, min.insync.replicas=2\n\n' "$phase"
  printf '| Reading | Kafka | Brahmaputra |\n|---|---|---|\n'
  printf '| Clients at peak | %s | %s |\n' "$(field "$krow" 4)" "$(field "$brow" 4)"
  printf '| msgs/sec (wall clock) | %s | %s |\n' "$(field "$krow" 7)" "$(field "$brow" 7)"
  printf '| msgs/sec (client-measured) | %s | %s |\n' "$(field "$krow" 8)" "$(field "$brow" 8)"
  printf '| CPU %% avg, whole cluster | %s | %s |\n' "$(field "$krow" 9)" "$(field "$brow" 9)"
  printf '| Memory MiB avg, whole cluster | %s | %s |\n' "$(field "$krow" 11)" "$(field "$brow" 11)"
  printf '| Ratio | 1.00x | %s |\n\n' "$(ratio "$(field "$brow" 7)" "$(field "$krow" 7)")"
}

emit_replication_cost() {
  local phase="$1" system row1 row3
  printf '### %s: what replication costs each system\n\n' "$phase"
  printf '| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |\n|---|---|---|---|---|\n'
  for system in kafka brahmaputra; do
    row1="$(peak "$system" rf1 "$phase")"
    row3="$(peak "$system" rf3 "$phase")"
    awk -v s="$system" -v a="$(field "$row1" 7)" -v b="$(field "$row3" 7)" 'BEGIN {
      if (a + 0 > 0 && b + 0 > 0)
        printf "| %s | %s | %s | %.0f%% | %.2fx |\n", s, a, b, (b/a)*100, a/b;
      else printf "| %s | %s | %s | n/a | n/a |\n", s, a, b;
    }'
  done
  printf '\n'
}

{
  printf '# Replicated head-to-head: Kafka vs Brahmaputra at RF=3, acks=all\n\n'
  printf 'Three brokers per system on one host, %s CPUs and %s per container,\n' "$CPUS" "$MEMORY"
  printf '%s partitions, %s B records, %s records per client at %s concurrent\n' \
    "$PARTITIONS" "$RECORD_SIZE" "$PER_CLIENT" "$(printf '%s' "$LEVELS" | tr ' ' '/')"
  printf 'clients, batch.size=%s, linger.ms=%s, compression=%s. Kafka image `%s`.\n\n' \
    "$BATCH_SIZE" "$LINGER_MS" "$COMPRESSION" "$KAFKA_IMAGE"
  printf 'Producer idempotence is disabled on both systems.\n\n'
  printf 'Initial consumer-group rebalance delay is %s ms on both brokers.\n\n' "$GROUP_INITIAL_REBALANCE_DELAY_MS"
  printf 'Offered rate per producer: %s records/sec (unlimited means saturation).\n\n' "${RATE:-unlimited}"
  printf 'Both clusters share one host, disk and NIC. Contention can affect\n'
  printf 'the systems differently; these observations are not deployment\n'
  printf 'capacity estimates or evidence of a universal throughput ratio.\n\n'
  printf 'Load generators run inside the broker containers, spread round-robin\n'
  printf 'across all three, so sampled CPU and memory cover broker plus client\n'
  printf 'for both systems and no one node carries all the client work.\n\n'

  emit_head_to_head produce
  emit_head_to_head consume
  emit_replication_cost produce
  emit_replication_cost consume

  # Latency is the number a durable deployment actually feels: with
  # acks=all a caller waits for the record to reach the ISR, and no amount
  # of throughput hides a long tail.
  printf '### Acknowledgement latency, produce (milliseconds)\n\n'
  printf '| System | Config | Clients | p50 | p99 | p99.9 | max |\n'
  printf '|---|---|---|---|---|---|---|\n'
  awk -F, 'NR>1 && $3=="produce" { printf "| %s | %s | %s | %s | %s | %s | %s |\n", $1,$2,$4,$13,$14,$15,$16 }' "$CSV"
  printf '\nKafka reports whole milliseconds, Brahmaputra two decimals; each\n'
  printf 'is parsed from its own client summary rather than recomputed. Both\n'
  printf 'measure the same span: record admitted to record acknowledged.\n\n'

  printf '### Every level\n\n'
  printf '`NA` means no valid resource sample completed during the workload.\n\n'
  printf '| System | Config | Phase | Clients | Records | Seconds | msgs/sec | Client msgs/sec | CPU avg %% | CPU peak %% | Mem avg MiB | Mem peak MiB | p50 ms | p99 ms | p99.9 ms | max ms |\n'
  printf '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n'
  tail -n +2 "$CSV" | awk -F, '{ printf "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16 }'

  printf '\n### Replication actually happened\n\n```\n'
  cat "$NOTES"
  printf '```\n\nRaw client output, cgroup samples and CPU-time summaries: `bench/results/replicated/`.\n'
  resource_report "$RESULTS"
} > "$REPORT"

stage "Replicated head-to-head complete"
cat "$REPORT"
