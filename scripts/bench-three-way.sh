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
GROUP_INITIAL_REBALANCE_DELAY_MS="${GROUP_INITIAL_REBALANCE_DELAY_MS:-0}"
BATCH_SIZE="${BATCH_SIZE:-2097152}"
LINGER_MS="${LINGER_MS:-5}"
COMPRESSION="${COMPRESSION:-none}"
ACKS="${ACKS:-1}"
IN_FLIGHT="${IN_FLIGHT:-64}"
CPUS="${CPUS:-4}"
MEMORY="${MEMORY:-4g}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.3.1}"
KAFKA_BROKER_HEAP="${KAFKA_BROKER_HEAP:--Xmx2g -Xms2g}"
KAFKA_CLIENT_HEAP="${KAFKA_CLIENT_HEAP:--Xmx512m -Xms512m}"
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

cleanup() {
  if declare -F stop_resource_sampling >/dev/null; then stop_resource_sampling || true; fi
  docker_run rm -f bench-kafka bench-tcp bench-quic >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
docker_run network create "$NETWORK" >/dev/null 2>&1 || true

# ------------------------------------------------------------- sampling

source "$ROOT/scripts/bench-resources.sh"

# ------------------------------------------------------------- kafka tls
# Encrypted QUIC against plaintext Kafka compares a transport that encrypts
# with one that does not. Kafka therefore also serves an SSL listener on
# the same broker, from a self-signed certificate generated with the
# image's own keytool, and every QUIC row is paired with the Kafka-over-TLS
# row of the same phase. The plaintext listener and its rows are unchanged.
KAFKA_TLS_PASS="benchpass"
# Host paths handed to Docker must be in the platform's own form; on Git
# Bash that means converting back to Windows.
if ! command -v host_path >/dev/null 2>&1; then
  host_path() {
    if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else printf '%s' "$1"; fi
  }
fi
kafka_tls_material() {
  local dir="$1"
  mkdir -p "$dir"
  docker_run run --rm -v "$(host_path "$dir"):/certs" --entrypoint sh "$KAFKA_IMAGE" -c '
    set -e
    keytool -genkeypair -alias kafka -keyalg RSA -keysize 2048 -validity 3650 -storetype PKCS12 \
      -keystore /certs/kafka.p12 -storepass '"$KAFKA_TLS_PASS"' -keypass '"$KAFKA_TLS_PASS"' \
      -dname "CN=bench-kafka" -ext "SAN=dns:bench-kafka,dns:localhost" >/dev/null 2>&1
    keytool -exportcert -alias kafka -keystore /certs/kafka.p12 -storepass '"$KAFKA_TLS_PASS"' -rfc -file /certs/kafka.crt >/dev/null 2>&1
    keytool -importcert -noprompt -alias kafka -file /certs/kafka.crt -storetype PKCS12 \
      -keystore /certs/truststore.p12 -storepass '"$KAFKA_TLS_PASS"' >/dev/null 2>&1
    printf "%s" '"$KAFKA_TLS_PASS"' > /certs/keystore.creds
    printf "%s" '"$KAFKA_TLS_PASS"' > /certs/key.creds
    printf "security.protocol=SSL\nssl.truststore.location=/etc/kafka/secrets/truststore.p12\nssl.truststore.password=%s\nssl.truststore.type=PKCS12\n" '"$KAFKA_TLS_PASS"' > /certs/client-ssl.properties
  ' || die "cannot generate Kafka TLS material"
}
KAFKA_TLS_PRODUCER_PROPS="security.protocol=SSL ssl.truststore.location=/etc/kafka/secrets/truststore.p12 ssl.truststore.password=$KAFKA_TLS_PASS ssl.truststore.type=PKCS12"

start_sampling() { start_resource_sampling "$2" "$1"; }
stop_sampling() { stop_resource_sampling; }
summarize_samples() { summarize_resource_samples "$1"; }
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
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,SSL://:9094,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://bench-kafka:9092,SSL://bench-kafka:9094 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@bench-kafka:9093 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL \
    -e KAFKA_SSL_KEYSTORE_FILENAME=kafka.p12 \
    -e KAFKA_SSL_KEYSTORE_CREDENTIALS=keystore.creds \
    -e KAFKA_SSL_KEY_CREDENTIALS=key.creds \
    -e KAFKA_SSL_KEYSTORE_TYPE=PKCS12 \
    -e KAFKA_SSL_CLIENT_AUTH=none \
    -v "$(host_path "$RESULTS/kafka-tls"):/etc/kafka/secrets" \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS="$GROUP_INITIAL_REBALANCE_DELAY_MS" \
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
  local deadline=$(( $(benchmark_now_ms) + 150000 ))
  while (( $(benchmark_now_ms) < deadline )); do
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
    --group-initial-rebalance-delay-ms "$GROUP_INITIAL_REBALANCE_DELAY_MS" \
    --transport "$transport" >/dev/null || die "cannot start $name"
  local deadline=$(( $(benchmark_now_ms) + 60000 ))
  while (( $(benchmark_now_ms) < deadline )); do
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
    --group "bench-$topic-$RANDOM" --commit-interval-ms 0 \
    --max "$records" --quiet > "$out" 2>&1
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

mkdir -p "$RESULTS"
kafka_tls_material "$RESULTS/kafka-tls"
start_kafka
docker_run exec bench-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server bench-kafka:9092 --create --if-not-exists \
  --topic "$TOPIC-tls" --partitions "$PARTITIONS" --replication-factor 1 \
  --config max.message.bytes="$MAX_BYTES" >/dev/null
docker_run exec bench-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server bench-kafka:9092 --create --if-not-exists \
  --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1 \
  --config max.message.bytes="$MAX_BYTES" >/dev/null
info "produce"
start_sampling bench-kafka "$RESULTS/kafka-produce-stats.txt"
docker_run exec -e KAFKA_HEAP_OPTS="$KAFKA_CLIENT_HEAP" bench-kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic "$TOPIC" --num-records "$RECORDS" --record-size "$RECORD_SIZE" \
  --throughput -1 \
  --producer-props bootstrap.servers=bench-kafka:9092 "acks=$ACKS" \
    "batch.size=$BATCH_SIZE" "linger.ms=$LINGER_MS" "compression.type=$COMPRESSION" \
    "max.request.size=$MAX_BYTES" "buffer.memory=268435456" "enable.idempotence=false" \
  > "$RESULTS/kafka-produce.txt" 2>&1 \
  || { stop_sampling; cat "$RESULTS/kafka-produce.txt" >&2; die "Kafka produce failed"; }
stop_sampling
KAFKA_DISK="$(disk_bytes bench-kafka /tmp/kraft-combined-logs)"
info "consume"
start_sampling bench-kafka "$RESULTS/kafka-consume-stats.txt"
docker_run exec -e KAFKA_HEAP_OPTS="$KAFKA_CLIENT_HEAP" bench-kafka /opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server bench-kafka:9092 --topic "$TOPIC" --messages "$RECORDS" \
  --group "threeway-$RANDOM" --timeout 180000 --fetch-size "$MAX_BYTES" \
  > "$RESULTS/kafka-consume.txt" 2>&1 \
  || { stop_sampling; cat "$RESULTS/kafka-consume.txt" >&2; die "Kafka consume failed"; }
stop_sampling
info "produce over TLS"
start_sampling bench-kafka "$RESULTS/kafka-tls-produce-stats.txt"
docker_run exec -e KAFKA_HEAP_OPTS="$KAFKA_CLIENT_HEAP" bench-kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic "$TOPIC-tls" --num-records "$RECORDS" --record-size "$RECORD_SIZE" \
  --throughput -1 \
  --producer-props bootstrap.servers=bench-kafka:9094 "acks=$ACKS" \
    "batch.size=$BATCH_SIZE" "linger.ms=$LINGER_MS" "compression.type=$COMPRESSION" \
    "max.request.size=$MAX_BYTES" "buffer.memory=268435456" "enable.idempotence=false" $KAFKA_TLS_PRODUCER_PROPS \
  > "$RESULTS/kafka-tls-produce.txt" 2>&1 \
  || { stop_sampling; cat "$RESULTS/kafka-tls-produce.txt" >&2; die "Kafka TLS produce failed"; }
stop_sampling
info "consume over TLS"
start_sampling bench-kafka "$RESULTS/kafka-tls-consume-stats.txt"
docker_run exec -e KAFKA_HEAP_OPTS="$KAFKA_CLIENT_HEAP" bench-kafka /opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server bench-kafka:9094 --topic "$TOPIC-tls" --messages "$RECORDS" \
  --group "threeway-tls-$RANDOM" --timeout 180000 --fetch-size "$MAX_BYTES" \
  --consumer.config /etc/kafka/secrets/client-ssl.properties \
  > "$RESULTS/kafka-tls-consume.txt" 2>&1 \
  || { stop_sampling; cat "$RESULTS/kafka-tls-consume.txt" >&2; die "Kafka TLS consume failed"; }
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
run_brahmaputra bench-tls tcp-tls tls

# -------------------------------------------------------------- report

read -r KP_RATE KP_MB <<<"$(parse_kafka_produce "$RESULTS/kafka-produce.txt")"
read -r KC_RATE KC_MB <<<"$(parse_kafka_consume "$RESULTS/kafka-consume.txt")"
read -r SP_RATE SP_MB <<<"$(parse_kafka_produce "$RESULTS/kafka-tls-produce.txt")"
read -r SC_RATE SC_MB <<<"$(parse_kafka_consume "$RESULTS/kafka-tls-consume.txt")"
read -r SP_CPU SP_CPU_MAX SP_MEM SP_MEM_MAX <<<"$(summarize_samples "$RESULTS/kafka-tls-produce-stats.txt")"
read -r SC_CPU SC_CPU_MAX SC_MEM SC_MEM_MAX <<<"$(summarize_samples "$RESULTS/kafka-tls-consume-stats.txt")"
TP_RATE="$(parse_rate "$RESULTS/tcp-produce.txt")"
TC_RATE="$(parse_rate "$RESULTS/tcp-consume.txt")"
QP_RATE="$(parse_rate "$RESULTS/quic-produce.txt")"
QC_RATE="$(parse_rate "$RESULTS/quic-consume.txt")"
LP_RATE="$(parse_rate "$RESULTS/tls-produce.txt")"
LC_RATE="$(parse_rate "$RESULTS/tls-consume.txt")"
read -r LP_CPU LP_CPU_MAX LP_MEM LP_MEM_MAX <<<"$(summarize_samples "$RESULTS/tls-produce-stats.txt")"
read -r LC_CPU LC_CPU_MAX LC_MEM LC_MEM_MAX <<<"$(summarize_samples "$RESULTS/tls-consume-stats.txt")"
TLS_DISK="$(cat "$RESULTS/tls-disk.txt" 2>/dev/null || true)"

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
  printf 'Producer idempotence is disabled on both systems.\n\n'
  printf 'Initial consumer-group rebalance delay is %s ms on both brokers.\n\n' "$GROUP_INITIAL_REBALANCE_DELAY_MS"

  printf 'Kafka is also measured over its SSL listener, and Brahmaputra over TLS on\n'
  printf 'TCP (tcp-tls), the transport that matches it: kernel TCP with userspace\n'
  printf 'TLS. Encrypted QUIC and tcp-tls are each compared with Kafka over TLS;\n'
  printf 'plaintext Kafka is the comparator for plaintext TCP.\n\n'
  printf '| Metric | Kafka | Brahmaputra TCP | Brahmaputra QUIC | Kafka TLS | Brahmaputra TLS |\n'
  printf '|---|---|---|---|---|---|\n'
  printf '| Produce msgs/sec | %s | %s | %s | %s | %s |\n' "${KP_RATE:-n/a}" "${TP_RATE:-n/a}" "${QP_RATE:-n/a}" "${SP_RATE:-n/a}" "${LP_RATE:-n/a}"
  printf '| Produce MB/sec | %s | %s | %s | %s | %s |\n' \
    "${KP_MB:-n/a}" "$(mb_per_sec "${TP_RATE:-0}")" "$(mb_per_sec "${QP_RATE:-0}")" "${SP_MB:-n/a}" "$(mb_per_sec "${LP_RATE:-0}")"
  printf '| Consume msgs/sec | %s | %s | %s | %s | %s |\n' "${KC_RATE:-n/a}" "${TC_RATE:-n/a}" "${QC_RATE:-n/a}" "${SC_RATE:-n/a}" "${LC_RATE:-n/a}"
  printf '| Consume MB/sec | %s | %s | %s | %s | %s |\n' \
    "${KC_MB:-n/a}" "$(mb_per_sec "${TC_RATE:-0}")" "$(mb_per_sec "${QC_RATE:-0}")" "${SC_MB:-n/a}" "$(mb_per_sec "${LC_RATE:-0}")"
  printf '| Produce CPU %% avg / peak | %s / %s | %s / %s | %s / %s | %s / %s | %s / %s |\n' \
    "$KP_CPU" "$KP_CPU_MAX" "$TP_CPU" "$TP_CPU_MAX" "$QP_CPU" "$QP_CPU_MAX" "$SP_CPU" "$SP_CPU_MAX" "$LP_CPU" "$LP_CPU_MAX"
  printf '| Produce memory MiB avg / peak | %s / %s | %s / %s | %s / %s | %s / %s | %s / %s |\n' \
    "$KP_MEM" "$KP_MEM_MAX" "$TP_MEM" "$TP_MEM_MAX" "$QP_MEM" "$QP_MEM_MAX" "$SP_MEM" "$SP_MEM_MAX" "$LP_MEM" "$LP_MEM_MAX"
  printf '| Consume CPU %% avg / peak | %s / %s | %s / %s | %s / %s | %s / %s | %s / %s |\n' \
    "$KC_CPU" "$KC_CPU_MAX" "$TC_CPU" "$TC_CPU_MAX" "$QC_CPU" "$QC_CPU_MAX" "$SC_CPU" "$SC_CPU_MAX" "$LC_CPU" "$LC_CPU_MAX"
  printf '| Consume memory MiB avg / peak | %s / %s | %s / %s | %s / %s | %s / %s | %s / %s |\n' \
    "$KC_MEM" "$KC_MEM_MAX" "$TC_MEM" "$TC_MEM_MAX" "$QC_MEM" "$QC_MEM_MAX" "$SC_MEM" "$SC_MEM_MAX" "$LC_MEM" "$LC_MEM_MAX"
  printf '| Log bytes on disk | %s | %s | %s | (shared broker) | %s |\n' \
    "${KAFKA_DISK:-n/a}" "${TCP_DISK:-n/a}" "${QUIC_DISK:-n/a}" "${TLS_DISK:-n/a}"
  printf '| Disk bytes per record | %s | %s | %s | (shared broker) | %s |\n' \
    "$(awk -v d="${KAFKA_DISK:-n/a}" -v r="$RECORDS" 'BEGIN{if(r && d ~ /^[0-9]+$/) printf "%.0f", d/r; else print "n/a"}')" \
    "$(awk -v d="${TCP_DISK:-n/a}" -v r="$RECORDS" 'BEGIN{if(r && d ~ /^[0-9]+$/) printf "%.0f", d/r; else print "n/a"}')" \
    "$(awk -v d="${QUIC_DISK:-n/a}" -v r="$RECORDS" 'BEGIN{if(r && d ~ /^[0-9]+$/) printf "%.0f", d/r; else print "n/a"}')" \
    "$(awk -v d="${TLS_DISK:-n/a}" -v r="$RECORDS" 'BEGIN{if(r && d ~ /^[0-9]+$/) printf "%.0f", d/r; else print "n/a"}')"
  printf '| Produce msgs/sec per CPU%% | %s | %s | %s | %s | %s |\n' \
    "$(awk -v t="${KP_RATE:-0}" -v c="$KP_CPU" 'BEGIN{if(c+0>0) printf "%.1f", t/c; else print "n/a"}')" \
    "$(awk -v t="${TP_RATE:-0}" -v c="$TP_CPU" 'BEGIN{if(c+0>0) printf "%.1f", t/c; else print "n/a"}')" \
    "$(awk -v t="${QP_RATE:-0}" -v c="$QP_CPU" 'BEGIN{if(c+0>0) printf "%.1f", t/c; else print "n/a"}')" \
    "$(awk -v t="${SP_RATE:-0}" -v c="$SP_CPU" 'BEGIN{if(c+0>0) printf "%.1f", t/c; else print "n/a"}')" \
    "$(awk -v t="${LP_RATE:-0}" -v c="$LP_CPU" 'BEGIN{if(c+0>0) printf "%.1f", t/c; else print "n/a"}')"
  printf '\nRaw output, cgroup samples and CPU-time summaries: `bench/results/three-way/`.\n'
  resource_report "$RESULTS"
} > "$REPORT"

stage "Three-way benchmark complete"
cat "$REPORT"
