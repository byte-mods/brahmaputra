#!/usr/bin/env bash
# Tail a topic and print every record to the console, staying connected.
#
#   scripts/consume-follow.sh                 # follow load-test, new records only
#   TOPIC=orders scripts/consume-follow.sh    # a different topic
#   FROM=earliest scripts/consume-follow.sh   # replay everything already there first
#   GROUP="" scripts/consume-follow.sh        # read standalone, committing nothing
#
# Runs until interrupted (Ctrl-C). Pair it with the load script to watch
# records arrive:
#
#   scripts/consume-follow.sh &   # in one terminal
#   scripts/load-100k.sh          # in another
#
# It joins a consumer group by default. That costs nothing here and buys
# two things: committed offsets, so a restart resumes where this left off
# instead of re-reading or skipping, and a row in the dashboard's consumer
# group table with live lag. Set GROUP="" to read standalone instead.
#
# The default start position is `latest` — new records only. A tail that
# opened by replaying a hundred thousand old records would scroll the
# arriving ones off the screen, which is the opposite of what a tail is
# for. FROM=earliest replays from the beginning.

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

TOPIC="${TOPIC:-load-test}"
GROUP="${GROUP-console-consumer}"
FROM="${FROM:-latest}"
# How often a group consumer writes its position back. Shorter than the
# 5s default so the dashboard's lag figure tracks a tail that is keeping
# up, rather than sawtoothing between commits.
COMMIT_INTERVAL_MS="${COMMIT_INTERVAL_MS:-1000}"
# Stop after this many records instead of running until interrupted.
MAX="${MAX:-}"

stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
info()  { printf '     %s\n' "$1"; }
die()   { printf '\n\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

[[ -x "$CLI_EXE" ]] || die "missing $CLI_EXE — run: cargo build --release -p brahmaputra-cli"

curl -sf --max-time 5 "$CONTROLLER/api/v1/controller/metadata" >/dev/null 2>&1 \
  || die "no controller at $CONTROLLER — start one with: scripts/run-cluster.sh start"

# Checked up front so an empty screen means "no new records" rather than
# "the topic name was wrong", which look identical once the tail is running.
curl -sf --max-time 5 "$CONTROLLER/api/v1/controller/metadata" | perl -0777 -ne '
  use JSON::PP;
  my $image = eval { decode_json($_) } or exit 1;
  exit(exists $image->{topics}{"'"$TOPIC"'"} ? 0 : 1);
' || die "no topic $TOPIC — create it, or run scripts/load-100k.sh first"

# A consumer group is coordinated by one __consumer_offsets partition,
# picked as crc32c(group) % partition_count. If that partition has no
# leader the join fails with a bare `unknown topic-partition
# "__consumer_offsets"-N`, which reads like a bug in this script rather
# than a broken internal topic. Recomputing the hash here would mean
# reimplementing crc32c in shell, so this reports the condition instead of
# the exact partition — enough to recognise the failure when it happens.
if [[ -n "$GROUP" ]]; then
  LEADERLESS="$(curl -sf --max-time 5 "$CONTROLLER/api/v1/controller/metadata" | perl -0777 -ne '
    use JSON::PP;
    my $image = eval { decode_json($_) } or exit 0;
    my $topic = $image->{topics}{"__consumer_offsets"} or exit 0;
    my $n = grep { ($_->{leader} // -1) < 0 } values %{ $topic->{partitions} };
    print $n;
  ' || true)"
  if [[ -n "$LEADERLESS" && "$LEADERLESS" -gt 0 ]]; then
    printf '\033[33m     warning: %s __consumer_offsets partitions have no leader.\033[0m\n' "$LEADERLESS"
    info "  A group whose coordinator is one of them cannot start, and fails with"
    info '  `unknown topic-partition "__consumer_offsets"-N`. Either pick another'
    info '  --group name, or run standalone: GROUP="" '"$0"
  fi
fi

ARGS=(--broker "$BROKER" --controller "$CONTROLLER" consume --topic "$TOPIC" --follow)
if [[ -n "$GROUP" ]]; then
  # `--from` is a standalone-only flag: a group's start position is what
  # it last committed, and `--auto-offset-reset` decides only where it
  # begins when there is no commit to resume from.
  ARGS+=(--group "$GROUP" --auto-offset-reset "$FROM" --commit-interval-ms "$COMMIT_INTERVAL_MS")
else
  ARGS+=(--from "$FROM")
fi
[[ -n "$MAX" ]] && ARGS+=(--max "$MAX")

stage "Following $TOPIC"
info "broker    $BROKER"
if [[ -n "$GROUP" ]]; then
  info "group     $GROUP (committing every ${COMMIT_INTERVAL_MS}ms; lag shows in the dashboard)"
else
  info "group     none — standalone read, nothing committed"
fi
info "start     $FROM$([[ "$FROM" == latest ]] && echo "  (new records only; FROM=earliest replays)")"
info "stop      Ctrl-C"
printf '\n'

exec "$CLI_EXE" "${ARGS[@]}"
