# Brahmaputra broker + operations dashboard in one image.
#
# The dashboard is compiled into the broker binary — there is no separate UI
# service, no Node build and no CDN at runtime — so hosting the UI is just
# running a node with an HTTP port. That is why this image is the whole
# product rather than a sidecar.

FROM rust:1-bookworm AS build
WORKDIR /src
# Copy manifests first so a source-only change does not redo dependency
# compilation.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY schemas ./schemas
RUN cargo build --release -p brahmaputra-server -p brahmaputra-cli

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/target/release/brahmaputra-server /usr/local/bin/brahmaputra-server
COPY --from=build /src/target/release/brahmaputra-cli /usr/local/bin/brahmaputra-cli

# --- Credentials -----------------------------------------------------------
# The first admin is created on first boot from these. Override them at run
# time (`docker run -e BRAHMAPUTRA_ADMIN_PASSWORD=...`) or in compose; the
# defaults here exist so the image starts at all, and are not a secret.
# Leaving the password unset makes the broker generate one and log it once.
ENV BRAHMAPUTRA_ADMIN_USER=admin
ENV BRAHMAPUTRA_ADMIN_PASSWORD=brahmaputra

# --- Broker configuration --------------------------------------------------
# Everything here is overridable per container. Anything not expressible as
# an environment variable can be appended as CLI flags after the image name.
ENV BRAHMAPUTRA_NODE_ID=1
ENV BRAHMAPUTRA_CLUSTER_ID=brahmaputra
ENV BRAHMAPUTRA_HOST=0.0.0.0
ENV BRAHMAPUTRA_PORT=9092
ENV BRAHMAPUTRA_CONTROL_PORT=19092
ENV BRAHMAPUTRA_HTTP_PORT=8080
ENV BRAHMAPUTRA_DATA_DIR=/var/lib/brahmaputra
ENV BRAHMAPUTRA_DEFAULT_PARTITIONS=3
ENV BRAHMAPUTRA_TRANSPORT=tcp
# Peers for a multi-node cluster, as `id=host:control_port`, comma separated.
# Empty means a single combined node, which is what the default compose
# service runs.
ENV BRAHMAPUTRA_PEERS=""
# Set to 1 on exactly one node the first time a cluster starts, to form the
# Raft quorum.
ENV BRAHMAPUTRA_BOOTSTRAP=0

VOLUME ["/var/lib/brahmaputra"]
EXPOSE 9092 19092 8080

COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# The dashboard answers on the HTTP port before any topic exists, so this is
# a genuine readiness signal rather than a liveness placeholder.
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
  CMD curl -fsS "http://127.0.0.1:${BRAHMAPUTRA_HTTP_PORT}/metrics" >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
