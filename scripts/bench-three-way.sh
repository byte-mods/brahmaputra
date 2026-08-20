#!/usr/bin/env bash
# Three-way benchmark: Apache Kafka, Brahmaputra over TCP, Brahmaputra over
# QUIC — same host, same container limits, same record size and count.
#
# Defaults to 1 MiB records, which is the interesting size for a transport
# comparison: one record is many packets, so loss recovery and stream
# multiplexing actually matter, and it sits right at Kafka's default
# message-size ceiling (both sides are raised to accept it).
#
# Throughput is reported alongside broker CPU, memory and on-disk bytes, so
# the comparison is cost-per-message rather than raw rate alone.

set -Eeuo pipefail

RECORDS="${RECORDS:-2000}"
RECORD_SIZE="${RECORD_SIZE:-1048576}"     # 1 MiB
PARTITIONS="${PARTITIONS:-6}"
BATCH_SIZE="${BATCH_SIZE:-2097152}"
LINGER_MS="${LINGER_MS:-5}"
COMPRESSION="${COMPRESSION:-none}"
ACKS="${ACKS:-1}"
IN_FLIGHT="${IN_FLIGHT:-64}"
CPUS="${CPUS:-4}"
MEMORY="${MEMORY:-4g}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.3.1}"
KAFKA_BROKER_HEAP="${KAFKA_BROKER_HEAP:--Xmx2g -Xms2g}"
KAFKA_BROKER_GC="${KAFKA_BROKER_GC:--XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M -XX:MetaspaceSize=96m -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true}"
NETWORK="${NETWORK:-brahma-bench}"
# One record is 1 MiB, so frames and fetches must be allowed past the
# defaults on both sides or the run is a config error, not a measurement.
MAX_BYTES="${MAX_BYTES:-16777216}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/bench/results}"
RESULTS="$OUT_DIR/three-way"
mkdir -p "$RESULTS"
REPORT="$OUT_DIR/three-way.md"

stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
info() { printf '    %s\n' "$1"; }
die() { printf '\n\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }
docker_run() { MSYS_NO_PATHCONV=1 docker "$@"; }

cleanup() { docker_run rm -f bench-kafka bench-tcp bench-quic >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
docker_run network create "$NETWORK" >/dev/null 2>&1 || true

# ------------------------------------------------------------- sampling

SAMPLER_PID=""
start_sampling() {
  local container="$1" out="$2"
  : > "$out"
  (
    while docker_run stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' "$container" \
        >> "$out" 2>/dev/null; do
      sleep 1
    done
  ) &
  SAMPLER_PID=$!
}
stop_sampling() {
  [[ -n "$SAMPLER_PID" ]] || return 0
  kill "$SAMPLER_PID" 2>/dev/null || true
  wait "$SAMPLER_PID" 2>/dev/null || true
  SAMPLER_PID=""
}
summarize_samples() {
  awk '
    {
      cpu = $1; sub("%", "", cpu); cpu += 0;
      mem = $2; unit = mem; sub("^[0-9.]+", "", unit);
      value = mem; sub("[A-Za-z]+$", "", value); value += 0;
      if (unit == "GiB") value *= 1024;
      else if (unit == "KiB") value /= 1024;
      else if (unit == "B") value /= 1048576;
      cpu_sum += cpu; mem_sum += value; n++;
      if (cpu > cpu_max) cpu_max = cpu;
      if (value > mem_max) mem_max = value;
    }
    END {
      if (n == 0) { print "n/a n/a n/a n/a"; exit }
      printf "%.1f %.1f %.0f %.0f\n", cpu_sum / n, cpu_max, mem_sum / n, mem_max;
    }
  ' "$1"
}
disk_bytes() {
  local out=""
  out="$(docker_run exec "$1" du -sb "$2" 2>/dev/null || true)"
  printf '%s' "$out" | awk 'NR==1 {print $1}'
}

# ---------------------------------------------------------------- kafka

start_kafka() {
  stage "Kafka: $RECORDS records of $RECORD_SIZE B"
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
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_NUM_PARTITIONS="$PARTITIONS" \
    -e KAFKA_LOG_SEGMENT_BYTES=1073741824 \
    -e KAFKA_NUM_NETWORK_THREADS=4 \
    -e KAFKA_NUM_IO_THREADS=8 \
    -e KAFKA_MESSAGE_MAX_BYTES="$MAX_BYTES" \
    -e KAFKA_REPLICA_FETCH_MAX_BYTES="$MAX_BYTES" \
    -e KAFKA_SOCKET_REQUEST_MAX_BYTES=104857600 \
    -e KAFKA_HEAP_OPTS="$KAFKA_BROKER_HEAP" \
    -e KAFKA_JVM_PERFORMANCE_OPTS="$KAFKA_BROKER_GC" \
    "$KAFKA_IMAGE" >/dev/null || die "cannot start Kafka"
  local deadline=$((SECONDS + 150))
  while (( SECONDS < deadline )); do
    if docker_run exec bench-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server bench-kafka:9092 >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  docker_run logs --tail 30 bench-kafka >&2 || true
  die "Kafka never became ready"
}

# ---------------------------------------------------------- brahmaputra

start_brahmaputra() {
  local name="$1" transport="$2"
  stage "Brahmaputra over ${transport}: $RECORDS records of $RECORD_SIZE B"
  docker_run run -d --name "$name" --network "$NETWORK" \
    --cpus "$CPUS" --memory "$MEMORY" \
    brahmaputra-bench:latest \
    --host "$name" --port 9092 --data-dir /data \
    --default-partitions "$PARTITIONS" --segment-bytes 1073741824 \
    --transport "$transport" >/dev/null || die "cannot start $name"
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if docker_run exec "$name" /usr/local/bin/brahmaputra-cli \
        --transport "$transport" --broker "$name:9092" metadata >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  docker_run logs --tail 30 "$name" >&2 || true
  die "$name never became ready"
}

brahma_produce() {
  local name="$1" transport="$2" topic="$3" records="$4" out="$5"
  docker_run exec "$name" /usr/local/bin/brahmaputra-cli \
    --transport "$transport" --broker "$name:9092" produce --topic "$topic" \
    --count "$records" --value-size "$RECORD_SIZE" --no-key \
    --acks "$ACKS" --batch-size "$BATCH_SIZE" --linger-ms "$LINGER_MS" \
    --in-flight "$IN_FLIGHT" --compression "$COMPRESSION" > "$out" 2>&1
}

brahma_consume() {
  local name="$1" transport="$2" topic="$3" records="$4" out="$5"
  docker_run exec "$name" /usr/local/bin/brahmaputra-cli \
    --transport "$transport" --broker "$name:9092" consume --topic "$topic" \
    --from earliest --max "$records" --quiet > "$out" 2>&1
}

# ------------------------------------------------------------- parsing

parse_rate() { sed -n 's/.*-> \([0-9.]*\) msgs\/sec.*/\1/p' "$1" | tail -1; }
parse_kafka_produce() {
  sed -n 's/.*records sent, \([0-9.]*\) records\/sec (\([0-9.]*\) MB\/sec).*/\1 \2/p' "$1" | tail -1
}
parse_kafka_consume() {
  awk -F', *' 'NR>1 && NF>=6 { print $6, $4 }' "$1" | tail -1
}
mb_per_sec() { awk -v r="$1" -v s="$RECORD_SIZE" 'BEGIN { printf "%.2f", r * s / 1048576 }'; }

# ----------------------------------------------------------------- run

TOPIC="threeway-$(date +%s)"

start_kafka
docker_run exec bench-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server bench-kafka:9092 --create --if-not-exists \
  --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1 \
  --config max.message.bytes="$MAX_BYTES" >/dev/null
info "produce"
start_sampling bench-kafka "$RESULTS/kafka-produce-stats.txt"
docker_run exec bench-kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic "$TOPIC" --num-records "$RECORDS" --record-size "$RECORD_SIZE" \
  --throughput -1 \
  --producer-props bootstrap.servers=bench-kafka:9092 "acks=$ACKS" \
    "batch.size=$BATCH_SIZE" "linger.ms=$LINGER_MS" "compression.type=$COMPRESSION" \
    "max.request.size=$MAX_BYTES" "buffer.memory=268435456" \
  > "$RESULTS/kafka-produce.txt" 2>&1 \
  || { stop_sampling; cat "$RESULTS/kafka-produce.txt" >&2; die "Kafka produce failed"; }
stop_sampling
KAFKA_DISK="$(disk_bytes bench-kafka /tmp/kraft-combined-logs)"
info "consume"
start_sampling bench-kafka "$RESULTS/kafka-consume-stats.txt"
docker_run exec bench-kafka /opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server bench-kafka:9092 --topic "$TOPIC" --messages "$RECORDS" \
  --group "threeway-$RANDOM" --timeout 180000 --fetch-size "$MAX_BYTES" \
  > "$RESULTS/kafka-consume.txt" 2>&1 \
  || { stop_sampling; cat "$RESULTS/kafka-consume.txt" >&2; die "Kafka consume failed"; }
stop_sampling
docker_run rm -f bench-kafka >/dev/null

run_brahmaputra() {
  local name="$1" transport="$2" tag="$3"
  start_brahmaputra "$name" "$transport"
  info "produce"
  start_sampling "$name" "$RESULTS/$tag-produce-stats.txt"
  brahma_produce "$name" "$transport" "$TOPIC" "$RECORDS" "$RESULTS/$tag-produce.txt" \
    || { stop_sampling; cat "$RESULTS/$tag-produce.txt" >&2; die "$tag produce failed"; }
  stop_sampling
  printf '%s' "$(disk_bytes "$name" /data)" > "$RESULTS/$tag-disk.txt"
  info "consume"
  start_sampling "$name" "$RESULTS/$tag-consume-stats.txt"
  brahma_consume "$name" "$transport" "$TOPIC" "$RECORDS" "$RESULTS/$tag-consume.txt" \
    || { stop_sampling; cat "$RESULTS/$tag-consume.txt" >&2; die "$tag consume failed"; }
  stop_sampling
  docker_run rm -f "$name" >/dev/null
}

run_brahmaputra bench-tcp tcp tcp
run_brahmaputra bench-quic quic quic

# -------------------------------------------------------------- report

read -r KP_RATE KP_MB <<<"$(parse_kafka_produce "$RESULTS/kafka-produce.txt")"
read -r KC_RATE KC_MB <<<"$(parse_kafka_consume "$RESULTS/kafka-consume.txt")"
TP_RATE="$(parse_rate "$RESULTS/tcp-produce.txt")"
TC_RATE="$(parse_rate "$RESULTS/tcp-consume.txt")"
QP_RATE="$(parse_rate "$RESULTS/quic-produce.txt")"
QC_RATE="$(parse_rate "$RESULTS/quic-consume.txt")"

read -r KP_CPU KP_CPU_MAX KP_MEM KP_MEM_MAX <<<"$(summarize_samples "$RESULTS/kafka-produce-stats.txt")"
read -r KC_CPU KC_CPU_MAX KC_MEM KC_MEM_MAX <<<"$(summarize_samples "$RESULTS/kafka-consume-stats.txt")"
read -r TP_CPU TP_CPU_MAX TP_MEM TP_MEM_MAX <<<"$(summarize_samples "$RESULTS/tcp-produce-stats.txt")"
read -r TC_CPU TC_CPU_MAX TC_MEM TC_MEM_MAX <<<"$(summarize_samples "$RESULTS/tcp-consume-stats.txt")"
read -r QP_CPU QP_CPU_MAX QP_MEM QP_MEM_MAX <<<"$(summarize_samples "$RESULTS/quic-produce-stats.txt")"
read -r QC_CPU QC_CPU_MAX QC_MEM QC_MEM_MAX <<<"$(summarize_samples "$RESULTS/quic-consume-stats.txt")"
TCP_DISK="$(cat "$RESULTS/tcp-disk.txt" 2>/dev/null || true)"
QUIC_DISK="$(cat "$RESULTS/quic-disk.txt" 2>/dev/null || true)"

{
  printf '# Kafka vs Brahmaputra (TCP) vs Brahmaputra (QUIC)\n\n'
  printf '%s records of %s B (%s MiB each), %s partitions, RF=1, acks=%s,\n' \
    "$RECORDS" "$RECORD_SIZE" "$(awk -v s="$RECORD_SIZE" 'BEGIN{printf "%.0f", s/1048576}')" \
    "$PARTITIONS" "$ACKS"
  printf 'batch.size=%s, linger.ms=%s, compression=%s. Each broker gets %s CPUs\n' \
    "$BATCH_SIZE" "$LINGER_MS" "$COMPRESSION" "$CPUS"
  printf 'and %s, and each system is driven by its own client from inside its\n' "$MEMORY"
  printf 'own container. Kafka image `%s`.\n\n' "$KAFKA_IMAGE"

  printf '| Metric | Kafka | Brahmaputra TCP | Brahmaputra QUIC |\n'
  printf '|---|---|---|---|\n'
  printf '| Produce msgs/sec | %s | %s | %s |\n' "${KP_RATE:-n/a}" "${TP_RATE:-n/a}" "${QP_RATE:-n/a}"
  printf '| Produce MB/sec | %s | %s | %s |\n' \
    "${KP_MB:-n/a}" "$(mb_per_sec "${TP_RATE:-0}")" "$(mb_per_sec "${QP_RATE:-0}")"
  printf '| Consume msgs/sec | %s | %s | %s |\n' "${KC_RATE:-n/a}" "${TC_RATE:-n/a}" "${QC_RATE:-n/a}"
  printf '| Consume MB/sec | %s | %s | %s |\n' \
    "${KC_MB:-n/a}" "$(mb_per_sec "${TC_RATE:-0}")" "$(mb_per_sec "${QC_RATE:-0}")"
  printf '| Produce CPU %% avg / peak | %s / %s | %s / %s | %s / %s |\n' \
    "$KP_CPU" "$KP_CPU_MAX" "$TP_CPU" "$TP_CPU_MAX" "$QP_CPU" "$QP_CPU_MAX"
  printf '| Produce memory MiB avg / peak | %s / %s | %s / %s | %s / %s |\n' \
    "$KP_MEM" "$KP_MEM_MAX" "$TP_MEM" "$TP_MEM_MAX" "$QP_MEM" "$QP_MEM_MAX"
  printf '| Consume CPU %% avg / peak | %s / %s | %s / %s | %s / %s |\n' \
    "$KC_CPU" "$KC_CPU_MAX" "$TC_CPU" "$TC_CPU_MAX" "$QC_CPU" "$QC_CPU_MAX"
  printf '| Consume memory MiB avg / peak | %s / %s | %s / %s | %s / %s |\n' \
    "$KC_MEM" "$KC_MEM_MAX" "$TC_MEM" "$TC_MEM_MAX" "$QC_MEM" "$QC_MEM_MAX"
  printf '| Log bytes on disk | %s | %s | %s |\n' \
    "${KAFKA_DISK:-n/a}" "${TCP_DISK:-n/a}" "${QUIC_DISK:-n/a}"
  printf '| Disk bytes per record | %s | %s | %s |\n' \
    "$(awk -v d="${KAFKA_DISK:-0}" -v r="$RECORDS" 'BEGIN{if(r) printf "%.0f", d/r; else print "n/a"}')" \
    "$(awk -v d="${TCP_DISK:-0}" -v r="$RECORDS" 'BEGIN{if(r) printf "%.0f", d/r; else print "n/a"}')" \
    "$(awk -v d="${QUIC_DISK:-0}" -v r="$RECORDS" 'BEGIN{if(r) printf "%.0f", d/r; else print "n/a"}')"
  printf '| Produce msgs/sec per CPU%% | %s | %s | %s |\n' \
    "$(awk -v t="${KP_RATE:-0}" -v c="$KP_CPU" 'BEGIN{if(c+0>0) printf "%.1f", t/c; else print "n/a"}')" \
    "$(awk -v t="${TP_RATE:-0}" -v c="$TP_CPU" 'BEGIN{if(c+0>0) printf "%.1f", t/c; else print "n/a"}')" \
    "$(awk -v t="${QP_RATE:-0}" -v c="$QP_CPU" 'BEGIN{if(c+0>0) printf "%.1f", t/c; else print "n/a"}')"
  printf '\nRaw output and per-second samples: `bench/results/three-way/`.\n'
} > "$REPORT"

stage "Three-way benchmark complete"
cat "$REPORT"
