#!/usr/bin/env bash
# Resource-matched benchmark: Kafka, Brahmaputra/TCP and Brahmaputra/QUIC
# driven at increasing client concurrency until each reaches the same CPU
# envelope, so the comparison is throughput at equal resource draw rather
# than throughput at whatever load one client happens to offer.
#
# A single client can leave a fast broker idle, which reads as "similar
# throughput" when it really means "the client ran out of work to give".
# Each system is therefore driven at 1, 2, 4, ... concurrent clients inside
# its own container; the level whose CPU is closest to Kafka's is the
# matched point, and the peak across levels is the saturation point.
#
# Every system gets the same container limits, record size, partition count
# and durability setting. Clients run inside the broker container on both
# sides, so the sampled CPU covers broker plus client for everyone.

set -Eeuo pipefail

PER_CLIENT="${PER_CLIENT:-1000000}"       # records each client sends
RECORD_SIZE="${RECORD_SIZE:-256}"
PARTITIONS="${PARTITIONS:-6}"
LEVELS="${LEVELS:-1 2 4 8}"               # concurrent clients per level
BATCH_SIZE="${BATCH_SIZE:-65536}"
LINGER_MS="${LINGER_MS:-5}"
COMPRESSION="${COMPRESSION:-none}"
ACKS="${ACKS:-1}"
# Records the CLI keeps outstanding — the analogue of Kafka's
# `buffer.memory`, which the perf test sets to 256 MB. It must comfortably
# exceed `batch.size / record size`, or the buffer can never reach
# `batch.size` and every flush waits out `linger.ms` instead: at 256 B
# records a window of 64 caps the producer at a few thousand msgs/sec no
# matter how fast the broker is.
IN_FLIGHT="${IN_FLIGHT:-4096}"
CPUS="${CPUS:-4}"
MEMORY="${MEMORY:-4g}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.3.1}"
NETWORK="${NETWORK:-brahma-bench}"
MAX_BYTES="${MAX_BYTES:-16777216}"

# JVM tuning. Two separate settings, because `KAFKA_HEAP_OPTS` is read by
# `kafka-run-class.sh` — that is, by every Kafka CLI tool, not just the
# broker. Setting one large heap on the container therefore gives each
# perf-test client the same large heap, and eight clients each reserving
# 3 GiB inside a 4 GiB container is a self-inflicted memory wall, not a
# Kafka limitation. The broker heap is set on the container; each client
# gets a small heap passed at exec time.
#
# 2 GiB of the container's 4 GiB leaves the rest for the page cache, which
# is where Kafka wants its memory: the broker writes into the cache and
# serves reads from it via sendfile. G1 with a low pause target is
# Confluent's and LinkedIn's recommended production setting.
KAFKA_BROKER_HEAP="${KAFKA_BROKER_HEAP:--Xmx2g -Xms2g}"
KAFKA_BROKER_GC="${KAFKA_BROKER_GC:--XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M -XX:MetaspaceSize=96m -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true}"
KAFKA_CLIENT_HEAP="${KAFKA_CLIENT_HEAP:--Xmx512m -Xms512m}"
KAFKA_CLIENT_GC="${KAFKA_CLIENT_GC:--XX:+UseG1GC -XX:MaxGCPauseMillis=20}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/bench/results}"
RESULTS="$OUT_DIR/matched"
mkdir -p "$RESULTS"
REPORT="$OUT_DIR/matched.md"
CSV="$RESULTS/levels.csv"

stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
info() { printf '    %s\n' "$1"; }
die() { printf '\n\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }
docker_run() { MSYS_NO_PATHCONV=1 docker "$@"; }

cleanup() { docker_run rm -f bench-kafka bench-tcp bench-quic >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
docker_run network create "$NETWORK" >/dev/null 2>&1 || true
: > "$CSV"
printf 'system,phase,clients,records,seconds,msgs_per_sec,client_msgs_per_sec,cpu_avg,cpu_peak,mem_avg,mem_peak\n' >> "$CSV"

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
      if (n == 0) { print "0 0 0 0"; exit }
      printf "%.1f %.1f %.0f %.0f\n", cpu_sum / n, cpu_max, mem_sum / n, mem_max;
    }
  ' "$1"
}
now_ms() { date +%s%3N; }
# Wait on the launched clients only: the stats sampler is a background job
# too, and a bare `wait` would block on it until the run is killed.
wait_all() {
  local pid status=0
  for pid in "${pids[@]}"; do
    wait "$pid" || status=1
  done
  return $status
}
# A missing or unreadable log directory must not end the run: the disk
# figure is a nice-to-have, the throughput measurement is the point.
disk_bytes() {
  local out=""
  out="$(docker_run exec "$1" du -sb "$2" 2>/dev/null || true)"
  printf '%s' "$out" | awk 'NR==1 {print $1}'
}
rate() { awk -v r="$1" -v ms="$2" 'BEGIN { if (ms > 0) printf "%.0f", r * 1000 / ms; else print 0 }'; }

# Each client also reports its own rate, measured from inside the client
# after it has started. Summing those excludes process startup, which is
# milliseconds for the Brahmaputra CLI but seconds for a JVM perf tool —
# large enough to flatter Brahmaputra if wall clock were the only reading.
#
# kafka-producer-perf-test prints a progress line every few seconds and
# then a final summary; only the summary — the line carrying latency
# percentiles — covers the whole run, so each file contributes exactly one
# number.
sum_client_rates() {
  local kind="$1"; shift
  local file total=0 value
  for file in "$@"; do
    [[ -f "$file" ]] || continue
    case "$kind" in
      kafka-produce)
        value="$(sed -n 's/.*records sent, \([0-9.]*\) records\/sec.*99.9th.*/\1/p' "$file" | tail -1)" ;;
      kafka-consume)
        value="$(awk -F', *' 'FNR>1 && NF>=6 { rate=$6 } END { print rate }' "$file")" ;;
      brahmaputra)
        value="$(sed -n 's/.*-> \([0-9.]*\) msgs\/sec.*/\1/p' "$file" | tail -1)" ;;
    esac
    total="$(awk -v t="$total" -v v="${value:-0}" 'BEGIN { printf "%.4f", t + v }')"
  done
  awk -v t="$total" 'BEGIN { printf "%.0f", t }'
}

# Record one measured level.
record_level() {
  local system="$1" phase="$2" clients="$3" records="$4" ms="$5" stats="$6" client_rate="$7"
  local seconds throughput
  seconds="$(awk -v ms="$ms" 'BEGIN { printf "%.2f", ms / 1000 }')"
  throughput="$(rate "$records" "$ms")"
  read -r cpu cpu_max mem mem_max <<<"$(summarize_samples "$stats")"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$system" "$phase" "$clients" "$records" "$seconds" "$throughput" \
    "${client_rate:-0}" "$cpu" "$cpu_max" "$mem" "$mem_max" >> "$CSV"
  info "$system $phase x$clients: $throughput msgs/sec wall (${client_rate:-0} client), CPU ${cpu}% avg, ${mem} MiB"
}

# ---------------------------------------------------------------- kafka

start_kafka() {
  stage "Kafka"
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
    -e KAFKA_SOCKET_SEND_BUFFER_BYTES=1048576 \
    -e KAFKA_SOCKET_RECEIVE_BUFFER_BYTES=1048576 \
    -e KAFKA_QUEUED_MAX_REQUESTS=1000 \
    -e KAFKA_HEAP_OPTS="$KAFKA_BROKER_HEAP" \
    -e KAFKA_JVM_PERFORMANCE_OPTS="$KAFKA_BROKER_GC" \
    "$KAFKA_IMAGE" >/dev/null || die "cannot start Kafka"
  local deadline=$((SECONDS + 180))
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

kafka_topic() {
  docker_run exec bench-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server bench-kafka:9092 --create --if-not-exists \
    --topic "$1" --partitions "$PARTITIONS" --replication-factor 1 \
    --config max.message.bytes="$MAX_BYTES" >/dev/null
}

run_kafka_level() {
  local clients="$1" index start elapsed
  local -a pids=()
  for (( index = 0; index < clients; index++ )); do
    kafka_topic "$TOPIC-k$clients-$index"
  done

  start_sampling bench-kafka "$RESULTS/kafka-produce-$clients.stats"
  start="$(now_ms)"
  pids=()
  for (( index = 0; index < clients; index++ )); do
    docker_run exec \
      -e KAFKA_HEAP_OPTS="$KAFKA_CLIENT_HEAP" \
      -e KAFKA_JVM_PERFORMANCE_OPTS="$KAFKA_CLIENT_GC" \
      bench-kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
      --topic "$TOPIC-k$clients-$index" --num-records "$PER_CLIENT" \
      --record-size "$RECORD_SIZE" --throughput -1 \
      --producer-props bootstrap.servers=bench-kafka:9092 "acks=$ACKS" \
        "batch.size=$BATCH_SIZE" "linger.ms=$LINGER_MS" \
        "compression.type=$COMPRESSION" "max.request.size=$MAX_BYTES" \
        "buffer.memory=268435456" \
      > "$RESULTS/kafka-produce-$clients-$index.txt" 2>&1 &
    pids+=($!)
  done
  wait_all || { stop_sampling; die "Kafka produce failed at $clients clients"; }
  elapsed=$(( $(now_ms) - start ))
  stop_sampling
  record_level kafka produce "$clients" $(( PER_CLIENT * clients )) "$elapsed" \
    "$RESULTS/kafka-produce-$clients.stats" \
    "$(sum_client_rates kafka-produce "$RESULTS"/kafka-produce-"$clients"-*.txt)"

  start_sampling bench-kafka "$RESULTS/kafka-consume-$clients.stats"
  start="$(now_ms)"
  pids=()
  for (( index = 0; index < clients; index++ )); do
    docker_run exec \
      -e KAFKA_HEAP_OPTS="$KAFKA_CLIENT_HEAP" \
      -e KAFKA_JVM_PERFORMANCE_OPTS="$KAFKA_CLIENT_GC" \
      bench-kafka /opt/kafka/bin/kafka-consumer-perf-test.sh \
      --bootstrap-server bench-kafka:9092 --topic "$TOPIC-k$clients-$index" \
      --messages "$PER_CLIENT" --group "matched-$clients-$index-$RANDOM" \
      --timeout 300000 --fetch-size "$MAX_BYTES" \
      > "$RESULTS/kafka-consume-$clients-$index.txt" 2>&1 &
    pids+=($!)
  done
  wait_all || { stop_sampling; die "Kafka consume failed at $clients clients"; }
  elapsed=$(( $(now_ms) - start ))
  stop_sampling
  record_level kafka consume "$clients" $(( PER_CLIENT * clients )) "$elapsed" \
    "$RESULTS/kafka-consume-$clients.stats" \
    "$(sum_client_rates kafka-consume "$RESULTS"/kafka-consume-"$clients"-*.txt)"
}

# ---------------------------------------------------------- brahmaputra

start_brahmaputra() {
  local name="$1" transport="$2"
  stage "Brahmaputra over $transport"
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

run_brahmaputra_level() {
  local name="$1" transport="$2" tag="$3" clients="$4" index start elapsed
  local -a pids=()

  start_sampling "$name" "$RESULTS/$tag-produce-$clients.stats"
  start="$(now_ms)"
  pids=()
  for (( index = 0; index < clients; index++ )); do
    docker_run exec "$name" /usr/local/bin/brahmaputra-cli \
      --transport "$transport" --broker "$name:9092" produce \
      --topic "$TOPIC-$tag$clients-$index" --count "$PER_CLIENT" \
      --value-size "$RECORD_SIZE" --no-key --acks "$ACKS" \
      --batch-size "$BATCH_SIZE" --linger-ms "$LINGER_MS" \
      --in-flight "$IN_FLIGHT" --compression "$COMPRESSION" \
      > "$RESULTS/$tag-produce-$clients-$index.txt" 2>&1 &
    pids+=($!)
  done
  wait_all || { stop_sampling; die "$tag produce failed at $clients clients"; }
  elapsed=$(( $(now_ms) - start ))
  stop_sampling
  record_level "$tag" produce "$clients" $(( PER_CLIENT * clients )) "$elapsed" \
    "$RESULTS/$tag-produce-$clients.stats" \
    "$(sum_client_rates brahmaputra "$RESULTS"/"$tag"-produce-"$clients"-*.txt)"

  start_sampling "$name" "$RESULTS/$tag-consume-$clients.stats"
  start="$(now_ms)"
  pids=()
  for (( index = 0; index < clients; index++ )); do
    docker_run exec "$name" /usr/local/bin/brahmaputra-cli \
      --transport "$transport" --broker "$name:9092" consume \
      --topic "$TOPIC-$tag$clients-$index" --from earliest \
      --max "$PER_CLIENT" --quiet \
      > "$RESULTS/$tag-consume-$clients-$index.txt" 2>&1 &
    pids+=($!)
  done
  wait_all || { stop_sampling; die "$tag consume failed at $clients clients"; }
  elapsed=$(( $(now_ms) - start ))
  stop_sampling
  record_level "$tag" consume "$clients" $(( PER_CLIENT * clients )) "$elapsed" \
    "$RESULTS/$tag-consume-$clients.stats" \
    "$(sum_client_rates brahmaputra "$RESULTS"/"$tag"-consume-"$clients"-*.txt)"
}

# ----------------------------------------------------------------- run

TOPIC="matched-$(date +%s)"

start_kafka
for level in $LEVELS; do run_kafka_level "$level"; done
KAFKA_DISK="$(disk_bytes bench-kafka /tmp/kraft-combined-logs)"
docker_run rm -f bench-kafka >/dev/null

start_brahmaputra bench-tcp tcp
for level in $LEVELS; do run_brahmaputra_level bench-tcp tcp tcp "$level"; done
TCP_DISK="$(disk_bytes bench-tcp /data)"
docker_run rm -f bench-tcp >/dev/null

start_brahmaputra bench-quic quic
for level in $LEVELS; do run_brahmaputra_level bench-quic quic quic "$level"; done
QUIC_DISK="$(disk_bytes bench-quic /data)"
docker_run rm -f bench-quic >/dev/null

# -------------------------------------------------------------- report
# Two readings per phase: the level whose CPU sits closest to Kafka's own
# CPU at that phase (equal-resource comparison), and the best throughput
# any level reached (saturation comparison).

kafka_cpu_at() {
  awk -F, -v phase="$1" '$1=="kafka" && $2==phase { cpu=$8; rate=$6 } END { print cpu+0, rate+0 }' "$CSV"
}
matched_row() {
  awk -F, -v sys="$1" -v phase="$2" -v target="$3" '
    $1==sys && $2==phase {
      diff = $8 - target; if (diff < 0) diff = -diff;
      if (best == "" || diff < best) { best = diff; row = $0 }
    }
    END { print row }
  ' "$CSV"
}
peak_row() {
  awk -F, -v sys="$1" -v phase="$2" '
    $1==sys && $2==phase && $6+0 > best+0 { best = $6; row = $0 }
    END { print row }
  ' "$CSV"
}
field() { printf '%s' "$1" | cut -d, -f"$2"; }
ratio() { awk -v a="$1" -v b="$2" 'BEGIN { if (b+0 > 0) printf "%.2fx", a/b; else print "n/a" }'; }

read -r KAFKA_PRODUCE_CPU KAFKA_PRODUCE_PEAK <<<"$(kafka_cpu_at produce)"
read -r KAFKA_CONSUME_CPU KAFKA_CONSUME_PEAK <<<"$(kafka_cpu_at consume)"
KAFKA_PRODUCE_BEST="$(peak_row kafka produce)"
KAFKA_CONSUME_BEST="$(peak_row kafka consume)"

emit_matched() {
  local phase="$1" target="$2" kafka_row="$3" tcp_row tcp_peak quic_row quic_peak
  tcp_row="$(matched_row tcp "$phase" "$target")"
  quic_row="$(matched_row quic "$phase" "$target")"
  tcp_peak="$(peak_row tcp "$phase")"
  quic_peak="$(peak_row quic "$phase")"
  printf '### %s\n\n' "$phase"
  printf '| Reading | Kafka | Brahmaputra TCP | Brahmaputra QUIC |\n'
  printf '|---|---|---|---|\n'
  printf '| Clients | %s | %s | %s |\n' \
    "$(field "$kafka_row" 3)" "$(field "$tcp_row" 3)" "$(field "$quic_row" 3)"
  printf '| msgs/sec at matched CPU | %s | %s | %s |\n' \
    "$(field "$kafka_row" 6)" "$(field "$tcp_row" 6)" "$(field "$quic_row" 6)"
  printf '| msgs/sec, client-measured | %s | %s | %s |\n' \
    "$(field "$kafka_row" 7)" "$(field "$tcp_row" 7)" "$(field "$quic_row" 7)"
  printf '| CPU %% avg | %s | %s | %s |\n' \
    "$(field "$kafka_row" 8)" "$(field "$tcp_row" 8)" "$(field "$quic_row" 8)"
  printf '| Memory MiB avg | %s | %s | %s |\n' \
    "$(field "$kafka_row" 10)" "$(field "$tcp_row" 10)" "$(field "$quic_row" 10)"
  printf '| Ratio vs Kafka at matched CPU | 1.00x | %s | %s |\n' \
    "$(ratio "$(field "$tcp_row" 6)" "$(field "$kafka_row" 6)")" \
    "$(ratio "$(field "$quic_row" 6)" "$(field "$kafka_row" 6)")"
  printf '| Peak msgs/sec (any level) | %s | %s | %s |\n' \
    "$(field "$kafka_row" 6)" "$(field "$tcp_peak" 6)" "$(field "$quic_peak" 6)"
  printf '| Peak ratio vs Kafka peak | 1.00x | %s | %s |\n\n' \
    "$(ratio "$(field "$tcp_peak" 6)" "$(field "$kafka_row" 6)")" \
    "$(ratio "$(field "$quic_peak" 6)" "$(field "$kafka_row" 6)")"
}

{
  printf '# Resource-matched benchmark\n\n'
  printf 'Each system is driven at %s concurrent clients, %s records of %s B\n' \
    "$(printf '%s' "$LEVELS" | tr ' ' '/')" "$PER_CLIENT" "$RECORD_SIZE"
  printf 'per client, %s partitions per topic, RF=1, acks=%s, batch.size=%s,\n' \
    "$PARTITIONS" "$ACKS" "$BATCH_SIZE"
  printf 'linger.ms=%s, compression=%s. Every container gets %s CPUs and %s,\n' \
    "$LINGER_MS" "$COMPRESSION" "$CPUS" "$MEMORY"
  printf 'and clients run inside the broker container on both sides, so the\n'
  printf 'sampled CPU and memory cover broker plus client for everyone.\n'
  printf 'Kafka image `%s`.\n\n' "$KAFKA_IMAGE"
  printf 'A single client can leave a fast broker idle, so raw single-client\n'
  printf 'throughput understates a system that was never saturated. The\n'
  printf 'matched reading picks, for each system, the concurrency level whose\n'
  printf 'average CPU is closest to Kafka best level, which is what makes\n'
  printf 'the throughput comparison a like-for-like cost comparison.\n\n'

  emit_matched produce "$KAFKA_PRODUCE_CPU" "$KAFKA_PRODUCE_BEST"
  emit_matched consume "$KAFKA_CONSUME_CPU" "$KAFKA_CONSUME_BEST"

  printf '### Disk after the full run\n\n'
  printf '| Kafka | Brahmaputra TCP | Brahmaputra QUIC |\n'
  printf '|---|---|---|\n'
  printf '| %s B | %s B | %s B |\n\n' \
    "${KAFKA_DISK:-n/a}" "${TCP_DISK:-n/a}" "${QUIC_DISK:-n/a}"

  printf '### Every level\n\n'
  printf '| System | Phase | Clients | Records | Seconds | msgs/sec | Client msgs/sec | CPU avg %% | CPU peak %% | Mem avg MiB | Mem peak MiB |\n'
  printf '|---|---|---|---|---|---|---|---|---|---|---|\n'
  tail -n +2 "$CSV" | awk -F, '{ printf "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11 }'
  printf '\nRaw client output and per-second samples: `bench/results/matched/`.\n'
} > "$REPORT"

stage "Resource-matched benchmark complete"
cat "$REPORT"
