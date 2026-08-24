#!/usr/bin/env bash
# Push one lakh (100,000) records into the local three-node cluster with
# acks=1, then prove every one of them landed.
#
#   scripts/run-cluster.sh start     # first, if it is not already up
#   scripts/load-100k.sh
#
# acks=1 means the partition leader answers once the record is in its log,
# without waiting for the followers. Replication still happens — the topic
# is RF=3 — it just is not on the acknowledgement path, so this measures
# leader-side commit latency rather than ISR round-trip latency. That is
# also why the check at the end reads offsets rather than trusting the
# producer's own count: acks=1 is the setting where "the client thinks it
# sent it" and "the log holds it" are worth distinguishing.
#
# Tunable from the environment:
#
#   COUNT=1000000 scripts/load-100k.sh        # a different volume
#   VALUE_SIZE=1024 scripts/load-100k.sh      # bigger records
#   TOPIC=orders PARTITIONS=12 scripts/load-100k.sh
#   RATE=20000 scripts/load-100k.sh           # offer below saturation
#
# A rate matters for the latency line: measured at saturation, latency is
# just queue depth over throughput. Offer load below what the cluster can
# take and the percentiles describe what a caller actually waits for.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT/data/cluster"
ENV_FILE="$RUN_DIR/cluster.env"
CLI_EXE="$ROOT/target/release/brahmaputra-cli"

# Ports of the cluster run-cluster.sh started, so a cluster shifted off the
# default bases is still the one this script talks to.
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi
DATA_PORT_BASE="${DATA_PORT_BASE:-9092}"
CONTROL_PORT_BASE="${CONTROL_PORT_BASE:-19092}"

BROKER="${BROKER:-127.0.0.1:$DATA_PORT_BASE}"
CONTROLLER="${CONTROLLER:-http://127.0.0.1:$CONTROL_PORT_BASE}"

# One lakh.
COUNT="${COUNT:-100000}"
VALUE_SIZE="${VALUE_SIZE:-256}"
TOPIC="${TOPIC:-load-test}"
PARTITIONS="${PARTITIONS:-6}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"
# The whole point of this script. Kept as a variable only so the value is
# stated once and appears in the summary rather than being buried in a flag.
ACKS=1
BATCH_SIZE="${BATCH_SIZE:-65536}"
LINGER_MS="${LINGER_MS:-5}"
COMPRESSION="${COMPRESSION:-lz4}"
IN_FLIGHT="${IN_FLIGHT:-1024}"
RATE="${RATE:-}"

stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
info()  { printf '     %s\n' "$1"; }
pass()  { printf '\033[32mPASS: %s\033[0m\n' "$1"; }
die()   { printf '\n\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

cli() { "$CLI_EXE" --broker "$BROKER" --controller "$CONTROLLER" "$@"; }

[[ -x "$CLI_EXE" ]] || die "missing $CLI_EXE — run: cargo build --release -p brahmaputra-cli"

controller_metadata() {
  curl -sf --max-time 5 "$CONTROLLER/api/v1/controller/metadata"
}

# Sum of `latest` across the topic's partitions: the number of records the
# logs actually hold. Reading it before and after is what turns "the
# producer reported success" into "the cluster kept them".
total_offsets() {
  cli offsets --topic "$TOPIC" 2>/dev/null \
    | awk -F'latest=' '/latest=/ { sum += $2 } END { print sum + 0 }'
}

stage "Checking the cluster"
controller_metadata >/dev/null 2>&1 \
  || die "no controller at $CONTROLLER — start one with: scripts/run-cluster.sh start"

BROKER_COUNT="$(controller_metadata | perl -0777 -ne '
  use JSON::PP;
  my $image = eval { decode_json($_) } or exit 1;
  print scalar keys %{ $image->{brokers} || {} };
')"
info "brokers registered: $BROKER_COUNT"
(( BROKER_COUNT >= REPLICATION_FACTOR )) \
  || die "replication factor $REPLICATION_FACTOR needs $REPLICATION_FACTOR brokers, found $BROKER_COUNT"

stage "Preparing topic '$TOPIC'"
# Created only when absent, so a re-run appends to the same topic instead of
# failing on a name that already exists.
if controller_metadata | perl -0777 -ne '
     use JSON::PP;
     my $image = eval { decode_json($_) } or exit 1;
     exit(exists $image->{topics}{"'"$TOPIC"'"} ? 0 : 1);
   '; then
  info "topic already exists, reusing it"
else
  cli topic create --name "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" >/dev/null
  info "created: $PARTITIONS partitions, replication factor $REPLICATION_FACTOR"
  # The producer's first metadata fetch has to see the new partitions'
  # leaders; without this it races topic creation and retries its way
  # through the first batches.
  sleep 1
fi

BEFORE="$(total_offsets)"
info "records in the topic before this run: $BEFORE"

stage "Producing $COUNT records at acks=$ACKS"
info "$VALUE_SIZE B values, batch.size=$BATCH_SIZE, linger.ms=$LINGER_MS, compression=$COMPRESSION"
[[ -n "$RATE" ]] && info "offered at $RATE records/sec"

RATE_ARGS=()
[[ -n "$RATE" ]] && RATE_ARGS=(--rate "$RATE")

# Kept so the throughput and latency lines can be echoed in the summary.
# `date` here would only have second resolution, and a hundred thousand
# small records take well under a second on a loopback cluster.
PRODUCE_LOG="$(mktemp "${TMPDIR:-/tmp}/brahmaputra-load.XXXXXX")"
trap 'rm -f "$PRODUCE_LOG"' EXIT

cli produce \
  --topic "$TOPIC" \
  --count "$COUNT" \
  --value-size "$VALUE_SIZE" \
  --acks "$ACKS" \
  --batch-size "$BATCH_SIZE" \
  --linger-ms "$LINGER_MS" \
  --compression "$COMPRESSION" \
  --in-flight "$IN_FLIGHT" \
  --latency \
  ${RATE_ARGS[@]+"${RATE_ARGS[@]}"} | tee "$PRODUCE_LOG"

THROUGHPUT_LINE="$(grep -m1 'msgs/sec' "$PRODUCE_LOG" || true)"
LATENCY_LINE="$(grep -m1 '^latency ms:' "$PRODUCE_LOG" || true)"

stage "Verifying what the logs hold"
AFTER="$(total_offsets)"
DELTA=$((AFTER - BEFORE))
info "records in the topic after this run: $AFTER"
cli offsets --topic "$TOPIC" | sed 's/^/     /'

(( DELTA == COUNT )) \
  || die "expected $COUNT new records, the logs gained $DELTA"
pass "$COUNT records acknowledged at acks=$ACKS and present in the partition logs"

# acks=1 does not wait for the followers, so a full ISR afterwards is a
# claim worth checking rather than assuming: it says replication kept up
# with the write rate instead of falling behind and shrinking the ISR.
UNDER_REPLICATED="$(controller_metadata | perl -0777 -ne '
  use JSON::PP;
  my $image = eval { decode_json($_) } or exit 1;
  my $topic = $image->{topics}{"'"$TOPIC"'"} or exit 1;
  my @behind;
  for my $id (sort { $a <=> $b } keys %{ $topic->{partitions} }) {
    my $p = $topic->{partitions}{$id};
    my $isr = scalar @{ $p->{isr} || [] };
    push @behind, "$id(isr=$isr)" if $isr < scalar @{ $p->{replicas} || [] };
  }
  print join(",", @behind);
')"
if [[ -z "$UNDER_REPLICATED" ]]; then
  pass "every partition is fully replicated (ISR = $REPLICATION_FACTOR of $REPLICATION_FACTOR)"
else
  info "under-replicated partitions: $UNDER_REPLICATED"
fi

stage "Summary"
info "topic         $TOPIC ($PARTITIONS partitions, RF=$REPLICATION_FACTOR)"
info "produced      $COUNT records x $VALUE_SIZE B"
[[ -n "$THROUGHPUT_LINE" ]] && info "throughput    $THROUGHPUT_LINE"
[[ -n "$LATENCY_LINE" ]]    && info "$LATENCY_LINE"
info "acks          $ACKS (leader only; followers replicate off the ack path)"
info "dashboard     browse them at http://localhost:${HTTP_PORT_BASE:-8080} -> Topics -> $TOPIC"
