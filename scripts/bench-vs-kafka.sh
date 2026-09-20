#!/usr/bin/env bash
# Head-to-head benchmark: Brahmaputra vs Apache Kafka, both in Docker on the
# same host, same kernel, same CPU/memory limits, same disk.
#
# What is (and is not) being compared
# -----------------------------------
# Kafka speaks its own protocol and Brahmaputra speaks its own, so each side
# is driven by its own client: Kafka by the bundled kafka-*-perf-test tools,
# Brahmaputra by brahmaputra-cli. The number reported is therefore
# "system + its own client", which is what a user actually experiences —
# client implementation differences are part of the result, not noise to be
# explained away.
#
# Both sides are tuned the same way: same record size, record count,
# partition count, acks, batch size, linger, compression, and replication
# factor 1 (single node). Both rely on the OS page cache with no per-batch
# fsync, which is Kafka's default and Brahmaputra's design (DESIGN.md §4.2).
#
# Usage:
#   bash scripts/bench-vs-kafka.sh                 # full run
#   RECORDS=200000 bash scripts/bench-vs-kafka.sh  # shorter run
#   SKIP_BUILD=1 bash scripts/bench-vs-kafka.sh    # reuse the built image

set -Eeuo pipefail

RECORDS="${RECORDS:-500000}"
RECORD_SIZE="${RECORD_SIZE:-256}"
PARTITIONS="${PARTITIONS:-6}"
GROUP_INITIAL_REBALANCE_DELAY_MS="${GROUP_INITIAL_REBALANCE_DELAY_MS:-0}"
BATCH_SIZE="${BATCH_SIZE:-65536}"
LINGER_MS="${LINGER_MS:-10}"
COMPRESSION="${COMPRESSION:-none}"       # none | lz4
ACKS="${ACKS:-1}"                        # 0 | 1 | all
IDEMPOTENT="${IDEMPOTENT:-false}"
CPUS="${CPUS:-4}"
MEMORY="${MEMORY:-4g}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.3.1}"
KAFKA_BROKER_HEAP="${KAFKA_BROKER_HEAP:--Xmx2g -Xms2g}"
KAFKA_CLIENT_HEAP="${KAFKA_CLIENT_HEAP:--Xmx512m -Xms512m}"
RUST_IMAGE="${RUST_IMAGE:-rust:1-bookworm}"
NETWORK="${NETWORK:-brahma-bench}"
WARMUP_RECORDS="${WARMUP_RECORDS:-20000}"
# Records the tuned client keeps in flight (see bench-tune-brahmaputra.sh).
IN_FLIGHT="${IN_FLIGHT:-4096}"
SKIP_BUILD="${SKIP_BUILD:-0}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/bench/results}"
mkdir -p "$OUT_DIR"
RESULTS="$OUT_DIR/raw"
mkdir -p "$RESULTS"

stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
info() { printf '    %s\n' "$1"; }
die() { printf '\n\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

cleanup() {
  if declare -F stop_resource_sampling >/dev/null; then stop_resource_sampling || true; fi
  docker rm -f bench-kafka bench-brahmaputra bench-client >/dev/null 2>&1 || true
}
trap cleanup EXIT

# MSYS/Git Bash rewrites arguments that look like absolute paths; disable it
# for docker invocations that pass container-side paths.
docker_run() { MSYS_NO_PATHCONV=1 docker "$@"; }

# ...but host paths (bind mounts, build contexts) must be handed to Docker in
# the platform's own form, which on Git Bash means converting back to Windows.
host_path() {
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -w "$1"
  else
    printf '%s' "$1"
  fi
}

# ---------------------------------------------------------------- build

if [[ "$SKIP_BUILD" != "1" ]]; then
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

docker_run network create "$NETWORK" >/dev/null 2>&1 || true
cleanup

# ------------------------------------------------------------- resources

# Cumulative CPU and dense memory observations also cover subsecond phases.
source "$ROOT/scripts/bench-resources.sh"
start_sampling() { start_resource_sampling "$2" "$1"; }
stop_sampling() { stop_resource_sampling; }
summarize_samples() { summarize_resource_samples "$1"; }

# On-disk size of a log directory, or empty when it cannot be read. Never
# fails the run: a missing measurement is reported as n/a, not a crash.
disk_bytes() {
  local container="$1" dir="$2" out=""
  out="$(docker_run exec "$container" du -sb "$dir" 2>/dev/null || true)"
  printf '%s' "$out" | awk 'NR==1 {print $1}'
}

# Kafka's log dir depends on the image's defaults and on how the entrypoint
# rewrote them, so find the directory that actually holds partition data
# rather than assuming a path.
kafka_log_dir() {
  local dir
  dir="$(docker_run exec bench-kafka sh -c \
    'find / -maxdepth 4 -type d -name "*-0" -path "*bench*" 2>/dev/null | head -1 | xargs -r dirname' \
    2>/dev/null || true)"
  dir="$(printf '%s' "$dir" | tr -d '\r')"
  printf '%s' "${dir:-/tmp/kraft-combined-logs}"
}

# ---------------------------------------------------------------- kafka

start_kafka() {
  stage "Start Kafka ($KAFKA_IMAGE), KRaft single node, ${CPUS} CPUs / ${MEMORY}"
  docker_run run -d --name bench-kafka --network "$NETWORK" \
    --cpus "$CPUS" --memory "$MEMORY" \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://bench-kafka:9092 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@bench-kafka:9093 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS="$GROUP_INITIAL_REBALANCE_DELAY_MS" \
    -e KAFKA_NUM_PARTITIONS="$PARTITIONS" \
    -e KAFKA_LOG_SEGMENT_BYTES=1073741824 \
    -e KAFKA_NUM_NETWORK_THREADS=4 \
    -e KAFKA_NUM_IO_THREADS=8 \
    -e KAFKA_HEAP_OPTS="$KAFKA_BROKER_HEAP" \
    "$KAFKA_IMAGE" >/dev/null || die "cannot start Kafka"
  # Kafka's image runs as uid 1000 and can only write its own default log
  # dir, so leave KAFKA_LOG_DIRS alone and read that path for disk usage.

  local deadline=$(( $(benchmark_now_ms) + 120000 ))
  while (( $(benchmark_now_ms) < deadline )); do
    if docker_run exec bench-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server bench-kafka:9092 >/dev/null 2>&1; then
      info "Kafka ready"
      return 0
    fi
    sleep 2
  done
  docker_run logs --tail 40 bench-kafka >&2 || true
  die "Kafka never became ready"
}

kafka_topic() {
  docker_run exec bench-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server bench-kafka:9092 --create --if-not-exists \
    --topic "$1" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
}

kafka_produce() {
  local topic="$1" records="$2" out="$3"
  # Match the CLI's count-mode payload exactly. Different compressibility
  # would otherwise turn a codec comparison into a payload comparison.
  docker_run exec bench-kafka sh -c \
    'head -c "$1" /dev/zero | tr "\000" x > /tmp/benchmark-payload.txt' sh "$RECORD_SIZE"
  docker_run exec -e KAFKA_HEAP_OPTS="$KAFKA_CLIENT_HEAP" bench-kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
    --topic "$topic" --num-records "$records" --payload-file /tmp/benchmark-payload.txt \
    --throughput -1 \
    --producer-props bootstrap.servers=bench-kafka:9092 \
      "acks=$ACKS" "batch.size=$BATCH_SIZE" "linger.ms=$LINGER_MS" \
      "compression.type=$COMPRESSION" "enable.idempotence=$IDEMPOTENT" > "$out" 2>&1
}

kafka_consume() {
  local topic="$1" records="$2" out="$3"
  docker_run exec -e KAFKA_HEAP_OPTS="$KAFKA_CLIENT_HEAP" bench-kafka /opt/kafka/bin/kafka-consumer-perf-test.sh \
    --bootstrap-server bench-kafka:9092 --topic "$topic" \
    --messages "$records" --group "bench-$RANDOM" --timeout 120000 \
    > "$out" 2>&1
}

# ---------------------------------------------------------- brahmaputra

start_brahmaputra() {
  stage "Start Brahmaputra (release build), ${CPUS} CPUs / ${MEMORY}"
  docker_run run -d --name bench-brahmaputra --network "$NETWORK" \
    --cpus "$CPUS" --memory "$MEMORY" \
    brahmaputra-bench:latest \
    --host bench-brahmaputra --port 9092 \
    --data-dir /data \
    --default-partitions "$PARTITIONS" \
    --group-initial-rebalance-delay-ms "$GROUP_INITIAL_REBALANCE_DELAY_MS" \
    --segment-bytes 1073741824 >/dev/null || die "cannot start Brahmaputra"

  local deadline=$(( $(benchmark_now_ms) + 60000 ))
  while (( $(benchmark_now_ms) < deadline )); do
    if brahma_cli metadata >/dev/null 2>&1; then
      info "Brahmaputra ready"
      return 0
    fi
    sleep 1
  done
  docker_run logs --tail 40 bench-brahmaputra >&2 || true
  die "Brahmaputra never became ready"
}

# Run the load generator inside the broker container, exactly as Kafka's
# perf tests run inside the Kafka container: same CPU budget, same
# loopback, no extra bridge hop for one side only.
brahma_cli() {
  docker_run exec bench-brahmaputra /usr/local/bin/brahmaputra-cli \
    --broker bench-brahmaputra:9092 "$@"
}

brahma_produce() {
  local topic="$1" records="$2" out="$3"
  local -a identity=()
  [[ "$IDEMPOTENT" == true ]] && identity=(--idempotent)
  brahma_cli produce --topic "$topic" \
    --count "$records" --value-size "$RECORD_SIZE" --no-key \
    --acks "$ACKS" --batch-size "$BATCH_SIZE" --linger-ms "$LINGER_MS" \
    --in-flight "$IN_FLIGHT" \
    --compression "$COMPRESSION" "${identity[@]}" > "$out" 2>&1
}

brahma_consume() {
  local topic="$1" records="$2" out="$3"
  brahma_cli consume --topic "$topic" --group "bench-$topic-$RANDOM" \
    --commit-interval-ms 0 --max "$records" --quiet \
    > "$out" 2>&1
}

# ---------------------------------------------------------------- report

# kafka-producer-perf-test prints a final summary line:
#   500000 records sent, 123456.7 records/sec (30.15 MB/sec), 12.34 ms avg latency, ...
parse_kafka_produce() {
  sed -n 's/.*records sent, \([0-9.]*\) records\/sec (\([0-9.]*\) MB\/sec).*/\1 \2/p' "$1" | tail -1
}

# kafka-consumer-perf-test prints a CSV data row; columns 4 and 6 are
# MB.sec and nMsg.sec.
parse_kafka_consume() {
  awk -F', *' 'NR>1 && NF>=6 { print $6, $4 }' "$1" | tail -1
}

# brahmaputra-cli prints: "produced N records (S B each) in T s -> R msgs/sec"
parse_brahma_produce() {
  local rate bytes_per_sec
  rate="$(sed -n 's/.*-> \([0-9.]*\) msgs\/sec.*/\1/p' "$1" | tail -1)"
  [[ -n "$rate" ]] || return 1
  bytes_per_sec="$(awk -v r="$rate" -v s="$RECORD_SIZE" 'BEGIN { printf "%.2f", r * s / 1048576 }')"
  printf '%s %s' "$rate" "$bytes_per_sec"
}

# brahmaputra-cli --quiet prints: "consumed N records (B B) in T s -> R msgs/sec, M MB/sec"
parse_brahma_consume() {
  sed -n 's/.*-> \([0-9.]*\) msgs\/sec, \([0-9.]*\) MB\/sec.*/\1 \2/p' "$1" | tail -1
}

REPORT="$OUT_DIR/report.md"

run_all() {
  local topic_prefix="bench-$(date +%s)"

  start_kafka
  kafka_topic "$topic_prefix-warm"
  kafka_topic "$topic_prefix-main"
  info "warmup"
  kafka_produce "$topic_prefix-warm" "$WARMUP_RECORDS" "$RESULTS/kafka-warmup.txt" || true
  info "produce $RECORDS x ${RECORD_SIZE}B"
  start_sampling bench-kafka "$RESULTS/kafka-produce-stats.txt"
  kafka_produce "$topic_prefix-main" "$RECORDS" "$RESULTS/kafka-produce.txt" \
    || { stop_sampling; cat "$RESULTS/kafka-produce.txt" >&2; die "Kafka produce failed"; }
  stop_sampling
  KAFKA_DISK="$(disk_bytes bench-kafka "$(kafka_log_dir)")"
  info "consume $RECORDS"
  start_sampling bench-kafka "$RESULTS/kafka-consume-stats.txt"
  kafka_consume "$topic_prefix-main" "$RECORDS" "$RESULTS/kafka-consume.txt" \
    || { stop_sampling; cat "$RESULTS/kafka-consume.txt" >&2; die "Kafka consume failed"; }
  stop_sampling
  docker_run rm -f bench-kafka >/dev/null

  start_brahmaputra
  info "warmup"
  brahma_produce "$topic_prefix-warm" "$WARMUP_RECORDS" "$RESULTS/brahma-warmup.txt" || true
  info "produce $RECORDS x ${RECORD_SIZE}B"
  start_sampling bench-brahmaputra "$RESULTS/brahma-produce-stats.txt"
  brahma_produce "$topic_prefix-main" "$RECORDS" "$RESULTS/brahma-produce.txt" \
    || { stop_sampling; cat "$RESULTS/brahma-produce.txt" >&2; die "Brahmaputra produce failed"; }
  stop_sampling
  BRAHMA_DISK="$(disk_bytes bench-brahmaputra /data)"
  info "consume $RECORDS"
  start_sampling bench-brahmaputra "$RESULTS/brahma-consume-stats.txt"
  brahma_consume "$topic_prefix-main" "$RECORDS" "$RESULTS/brahma-consume.txt" \
    || { stop_sampling; cat "$RESULTS/brahma-consume.txt" >&2; die "Brahmaputra consume failed"; }
  stop_sampling
  docker_run rm -f bench-brahmaputra >/dev/null
}

run_all

read -r KP_RATE KP_MB <<<"$(parse_kafka_produce "$RESULTS/kafka-produce.txt")"
read -r KC_RATE KC_MB <<<"$(parse_kafka_consume "$RESULTS/kafka-consume.txt")"
read -r BP_RATE BP_MB <<<"$(parse_brahma_produce "$RESULTS/brahma-produce.txt")"
read -r BC_RATE BC_MB <<<"$(parse_brahma_consume "$RESULTS/brahma-consume.txt")"

read -r KP_CPU_AVG KP_CPU_MAX KP_MEM_AVG KP_MEM_MAX <<<"$(summarize_samples "$RESULTS/kafka-produce-stats.txt")"
read -r KC_CPU_AVG KC_CPU_MAX KC_MEM_AVG KC_MEM_MAX <<<"$(summarize_samples "$RESULTS/kafka-consume-stats.txt")"
read -r BP_CPU_AVG BP_CPU_MAX BP_MEM_AVG BP_MEM_MAX <<<"$(summarize_samples "$RESULTS/brahma-produce-stats.txt")"
read -r BC_CPU_AVG BC_CPU_MAX BC_MEM_AVG BC_MEM_MAX <<<"$(summarize_samples "$RESULTS/brahma-consume-stats.txt")"

ratio() { awk -v a="$1" -v b="$2" 'BEGIN { if (b+0 == 0) print "n/a"; else printf "%.2fx", a / b }'; }

{
  printf '# Brahmaputra vs Kafka — head to head\n\n'
  printf 'Both systems in Docker on one host: %s CPUs, %s memory, replication\n' "$CPUS" "$MEMORY"
  printf 'factor 1, %s partitions, %s records of %s B, acks=%s, batch.size=%s,\n' \
    "$PARTITIONS" "$RECORDS" "$RECORD_SIZE" "$ACKS" "$BATCH_SIZE"
  printf 'linger.ms=%s, compression=%s. Kafka image `%s`.\n\n' "$LINGER_MS" "$COMPRESSION" "$KAFKA_IMAGE"
  printf 'Each system is driven by its own client (Kafka: kafka-*-perf-test;\n'
  printf 'Brahmaputra: brahmaputra-cli), so these are system+client numbers.\n\n'
  printf 'Both producers send identical repeated `x` payloads. These are\n'
  printf 'highly compressible; codec results do not represent high-entropy data.\n\n'
  printf 'Idempotence is `%s` on both producers.\n\n' "$IDEMPOTENT"
  printf 'Initial consumer-group rebalance delay is %s ms on both brokers.\n\n' "$GROUP_INITIAL_REBALANCE_DELAY_MS"
  printf '| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |\n'
  printf '|---|---|---|---|\n'
  printf '| Produce (msgs/sec) | %s | %s | %s |\n' "${KP_RATE:-n/a}" "${BP_RATE:-n/a}" "$(ratio "${BP_RATE:-0}" "${KP_RATE:-0}")"
  printf '| Produce (MB/sec) | %s | %s | %s |\n' "${KP_MB:-n/a}" "${BP_MB:-n/a}" "$(ratio "${BP_MB:-0}" "${KP_MB:-0}")"
  printf '| Consume (msgs/sec) | %s | %s | %s |\n' "${KC_RATE:-n/a}" "${BC_RATE:-n/a}" "$(ratio "${BC_RATE:-0}" "${KC_RATE:-0}")"
  printf '| Consume (MB/sec) | %s | %s | %s |\n' "${KC_MB:-n/a}" "${BC_MB:-n/a}" "$(ratio "${BC_MB:-0}" "${KC_MB:-0}")"

  printf '\n## Resource cost for the same stream\n\n'
  printf 'Broker-container cumulative CPU and 50 ms memory samples cover the\n'
  printf 'duration of each phase; disk is the on-disk size of the log\n'
  printf 'directory after producing %s records of %s B (%s MiB of payload).\n\n' \
    "$RECORDS" "$RECORD_SIZE" "$(awk -v r="$RECORDS" -v s="$RECORD_SIZE" 'BEGIN{printf "%.0f", r*s/1048576}')"
  printf '| Metric | Kafka | Brahmaputra |\n'
  printf '|---|---|---|\n'
  printf '| Produce CPU %% (avg / peak of one core-equivalent) | %s / %s | %s / %s |\n' \
    "$KP_CPU_AVG" "$KP_CPU_MAX" "$BP_CPU_AVG" "$BP_CPU_MAX"
  printf '| Produce memory MiB (avg / peak) | %s / %s | %s / %s |\n' \
    "$KP_MEM_AVG" "$KP_MEM_MAX" "$BP_MEM_AVG" "$BP_MEM_MAX"
  printf '| Consume CPU %% (avg / peak) | %s / %s | %s / %s |\n' \
    "$KC_CPU_AVG" "$KC_CPU_MAX" "$BC_CPU_AVG" "$BC_CPU_MAX"
  printf '| Consume memory MiB (avg / peak) | %s / %s | %s / %s |\n' \
    "$KC_MEM_AVG" "$KC_MEM_MAX" "$BC_MEM_AVG" "$BC_MEM_MAX"
  printf '| Log directory bytes after produce | %s | %s |\n' "${KAFKA_DISK:-n/a}" "${BRAHMA_DISK:-n/a}"
  printf '| Bytes on disk per record | %s | %s |\n' \
    "$(awk -v d="${KAFKA_DISK:-n/a}" -v r="$RECORDS" 'BEGIN{if(r && d ~ /^[0-9]+$/) printf "%.1f", d/r; else print "n/a"}')" \
    "$(awk -v d="${BRAHMA_DISK:-n/a}" -v r="$RECORDS" 'BEGIN{if(r && d ~ /^[0-9]+$/) printf "%.1f", d/r; else print "n/a"}')"
  printf '| Msgs/sec per CPU%% (produce) | %s | %s |\n' \
    "$(awk -v t="${KP_RATE:-0}" -v c="$KP_CPU_AVG" 'BEGIN{if(c+0>0) printf "%.0f", t/c; else print "n/a"}')" \
    "$(awk -v t="${BP_RATE:-0}" -v c="$BP_CPU_AVG" 'BEGIN{if(c+0>0) printf "%.0f", t/c; else print "n/a"}')"

  printf '\nRaw tool output, cgroup samples and CPU-time summaries: `bench/results/raw/`.\n'
  resource_report "$RESULTS"
} > "$REPORT"

stage "Benchmark complete"
cat "$REPORT"
