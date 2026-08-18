#!/usr/bin/env bash
# Translate the image's environment into broker flags.
#
# Kept as a script rather than a long CMD so that a compose file can set one
# variable instead of restating the whole command line, and so unset values
# simply omit their flag rather than passing an empty string the broker
# would have to interpret.

set -Eeuo pipefail

args=(
  --node-id "${BRAHMAPUTRA_NODE_ID}"
  --cluster-id "${BRAHMAPUTRA_CLUSTER_ID}"
  --host "${BRAHMAPUTRA_HOST}"
  --port "${BRAHMAPUTRA_PORT}"
  --control-port "${BRAHMAPUTRA_CONTROL_PORT}"
  --http-port "${BRAHMAPUTRA_HTTP_PORT}"
  --data-dir "${BRAHMAPUTRA_DATA_DIR}"
  --default-partitions "${BRAHMAPUTRA_DEFAULT_PARTITIONS}"
  --transport "${BRAHMAPUTRA_TRANSPORT}"
)

# Peers arrive as "1=host-a:19092,2=host-b:19092"; each becomes its own flag.
#
# With none given, the node is its own single-member quorum. That is not a
# convenience default: the dashboard's users and sessions live in the
# controller's metadata, so a node with no controller has nothing to log in
# against. A one-node cluster is the smallest thing that can serve the UI.
peer_list="${BRAHMAPUTRA_PEERS:-}"
if [[ -z "$peer_list" ]]; then
  peer_list="${BRAHMAPUTRA_NODE_ID}=${BRAHMAPUTRA_HOST}:${BRAHMAPUTRA_CONTROL_PORT}"
  # A lone node must form its own quorum, or it waits for peers forever.
  BRAHMAPUTRA_BOOTSTRAP=1
fi
IFS=',' read -r -a peers <<<"$peer_list"
for peer in "${peers[@]}"; do
  [[ -n "$peer" ]] && args+=(--controller-peer "$peer")
done

if [[ "${BRAHMAPUTRA_BOOTSTRAP:-0}" == "1" ]]; then
  args+=(--bootstrap)
fi

# Optional tuning, each omitted entirely when unset so the broker keeps its
# own default rather than being handed an empty value.
[[ -n "${BRAHMAPUTRA_SEGMENT_BYTES:-}" ]] && args+=(--segment-bytes "${BRAHMAPUTRA_SEGMENT_BYTES}")
[[ -n "${BRAHMAPUTRA_RETENTION_MS:-}" ]] && args+=(--retention-ms "${BRAHMAPUTRA_RETENTION_MS}")
[[ -n "${BRAHMAPUTRA_RETENTION_BYTES:-}" ]] && args+=(--retention-bytes "${BRAHMAPUTRA_RETENTION_BYTES}")
[[ -n "${BRAHMAPUTRA_FLUSH_INTERVAL_MESSAGES:-}" ]] && args+=(--flush-interval-messages "${BRAHMAPUTRA_FLUSH_INTERVAL_MESSAGES}")
[[ -n "${BRAHMAPUTRA_FLUSH_INTERVAL_MS:-}" ]] && args+=(--flush-interval-ms "${BRAHMAPUTRA_FLUSH_INTERVAL_MS}")
[[ -n "${BRAHMAPUTRA_QUOTA_PRODUCE_BYTES_PER_SEC:-}" ]] && args+=(--quota-produce-bytes-per-sec "${BRAHMAPUTRA_QUOTA_PRODUCE_BYTES_PER_SEC}")
[[ -n "${BRAHMAPUTRA_QUOTA_FETCH_BYTES_PER_SEC:-}" ]] && args+=(--quota-fetch-bytes-per-sec "${BRAHMAPUTRA_QUOTA_FETCH_BYTES_PER_SEC}")

# Data-plane authentication. Off by default so a laptop container works out
# of the box; turning it on also requires an encrypted transport, because
# credentials cross the wire in the clear.
if [[ "${BRAHMAPUTRA_REQUIRE_AUTH:-0}" == "1" ]]; then
  args+=(--require-auth)
fi

exec /usr/local/bin/brahmaputra-server "${args[@]}" "$@"
