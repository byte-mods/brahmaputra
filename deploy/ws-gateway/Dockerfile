# WebSocket gateway image. Build from the repository root:
#   docker build -f deploy/ws-gateway/Dockerfile -t brahmaputra-ws-gateway .
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY schemas ./schemas
RUN cargo build --release --locked -p brahmaputra-ws-gateway

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 gateway
COPY --from=build /src/target/release/brahmaputra-ws-gateway /usr/local/bin/
COPY --from=build /src/target/release/ws-loadgen /usr/local/bin/
USER gateway
# 8090: WebSocket (public, behind the load balancer). 8091: probes and
# metrics (keep private).
EXPOSE 8090 8091
ENV GW_LISTEN=0.0.0.0:8090 GW_HTTP_LISTEN=0.0.0.0:8091
HEALTHCHECK --interval=10s --timeout=2s CMD curl -fs http://127.0.0.1:8091/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/brahmaputra-ws-gateway"]
