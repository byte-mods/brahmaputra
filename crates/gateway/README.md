# brahmaputra-ws-gateway

A stateless WebSocket service between mobile/web clients and a
Brahmaputra cluster. A client connects, authenticates once, and every
message it sends becomes a record in a Brahmaputra topic. The message's
key chooses the partition, so records sharing a key stay in order.

```
 phones, browsers ──wss──▶ L4 load balancer ──▶ gateway × N ──(few TCP conns each)──▶ brokers
                                                 stateless          batched produce
```

The broker needs no changes: the gateway is an ordinary client built on
`brahmaputra-client`. You scale sockets by adding gateway instances, and
the broker only ever sees a few connections per instance.

## Quick start

```bash
brahmaputra-server --data-dir ./data --default-partitions 16

brahmaputra-ws-gateway --broker 127.0.0.1:9092 \
    --jwt-secret "$(openssl rand -hex 32)" --default-topic events

# A token for testing (your identity provider issues real ones):
TOKEN=$(brahmaputra-ws-gateway mint-token --secret <same secret> --sub user-42)
websocat -H "Authorization: Bearer $TOKEN" ws://127.0.0.1:8090/ws
{"id":1,"value":"hello"}
{"type":"ack","id":1,"topic":"events","partition":11,"offset":0}
```

From a browser, which cannot set headers on a WebSocket:

```js
const ws = new WebSocket(`wss://gw.example.com/ws?topic=orders`,
                         ["brahmaputra.v1", `bearer.${token}`]);
ws.onmessage = (e) => console.log(JSON.parse(e.data));
ws.onopen = () => ws.send(JSON.stringify({ id: 1, key: "cart-7", value: "{...}" }));
```

## Connecting

`GET /ws` upgrade, with the token in one of these places (checked in this
order):

| Where | Form |
|---|---|
| Header | `Authorization: Bearer <jwt>`: native apps, servers |
| Query | `?access_token=<jwt>`: simplest. The token ends up in URLs, so keep TLS on and access logs off |
| Subprotocol | `Sec-WebSocket-Protocol: brahmaputra.v1, bearer.<jwt>`: browsers. The gateway answers `brahmaputra.v1` |

Optional query parameters:

| Parameter | Meaning |
|---|---|
| `topic` | The connection's topic, used by messages that name none and by every binary frame. Must pass the allow-list and the token's `topics` claim, or the upgrade gets **403** |
| `key` | The connection's default key. If absent, the token subject (`sub`) is the key |
| `acks` | `all` (default) acknowledges every message that has an `id`; `errors` sends error frames only |

Refused upgrades get plain HTTP statuses:

| Status | Cause |
|---|---|
| 401 | Missing, forged, expired or not-yet-valid token, wrong `iss` or `aud` |
| 403 | Topic not permitted |
| 400 | Malformed query |
| 404 | Path other than `/ws` |
| 503 | Instance at `--max-connections`, draining, or its broker unreachable. Clients should retry, and the load balancer should send them elsewhere |

### Tokens

HS256 JWTs signed with a secret shared between your identity provider and
the gateway. Claims read:

| Claim | |
|---|---|
| `sub` | required; the user or device. Bound to the connection for its lifetime |
| `exp` | required; expiry (a token without one is a password that cannot be rotated) |
| `nbf`, `iss`, `aud` | checked when present / when `--jwt-issuer` / `--jwt-audience` are set |
| `topics` | optional patterns (`orders`, `orders.*`, `*`) that narrow what this token may write. Never widens `--allow-topic` |

Only `alg: HS256` is accepted. The algorithm in a token is compared
against that, never trusted, which closes the `alg: none` and
algorithm-confusion attacks. To rotate a secret, give the gateway several
at once as `kid:secret` (via `--jwt-secret` or `--jwt-secret-file`); a
token's `kid` selects exactly one key. Secrets shorter than 16 bytes are
refused at startup.

## Messages

**Text frames** are JSON publishes:

```json
{"id": 7, "topic": "orders.eu", "key": "cart-42", "value": "{\"sku\":1}",
 "headers": {"trace": "abc", "empty": null}}
```

| Field | |
|---|---|
| `id` | optional unsigned integer. Echoed in the ack/error; no id, no ack |
| `topic` | optional; defaults to the connection's topic |
| `key` / `key_b64` | optional; defaults to the connection's key, then to `sub` |
| `value` / `value_b64` | one is required. `"value": null` is a **tombstone** (delete on a compacted topic) |
| `headers` | optional string→string-or-null map. Names starting `x-gw-` are reserved |

Unknown fields are rejected, including `partition`: the key decides the
partition, so clients cannot unbalance topics.

**Binary frames** are the whole value, sent to the connection's topic
under its key. They are numbered 1, 2, 3... for acks. This is the cheap
path for high-rate telemetry.

**The gateway answers** with JSON text frames:

```json
{"type":"welcome","user":"user-42","topic":"events","key":"user-42","max_message_bytes":1048576,"max_inflight":64}
{"type":"ack","id":7,"topic":"orders.eu","partition":3,"offset":1841}
{"type":"error","id":8,"code":"TOPIC_NOT_ALLOWED","message":"not permitted to publish to payments","retryable":false}
```

| Error code | Retry? | Meaning |
|---|---|---|
| `BAD_REQUEST` | no | Malformed publish (the connection stays open) |
| `TOPIC_NOT_ALLOWED` | no | Allow-list or token claim forbids the topic |
| `RATE_LIMITED` | yes | Over `--rate-limit` / `--rate-burst` for this connection |
| `OVERLOADED` | yes | The cluster is slower than the edge; back off |
| `BROKER_ERROR` | per `retryable` | The broker or network failed the record |

**Partitioning.** A record's partition is `murmur2(key) % partitions`,
the same function every Brahmaputra client and Kafka's default
partitioner use. The same key maps to the same partition from any gateway
instance, from any language driver, and on every reconnect. The default
key is the user, so one user's stream is one ordered partition.
Acknowledgements for different keys can arrive out of order;
acknowledgements for one key arrive in send order.

**Delivery.** At least once. An `ack` means the broker appended the record
(`--acks 1`, or every in-sync replica with `--acks all`). A client that
reconnects should resend anything it holds no ack for. If the connection
dropped after the append but before the ack, the resend is a duplicate,
so consumers that care should deduplicate on a client-chosen id header.

Every record carries `x-gw-user: <sub>`, which clients cannot set, so
consumers can trust who sent it.

Closing codes: `1000` idle timeout, `1001` server shutting down (resend
unacked), `1002` protocol error, `1009` message over `--max-message-bytes`.

## Why it scales to millions of sockets

Each socket is one tokio task with a 1 KiB read buffer
(`--read-buffer-bytes`) and no broker state. Measured with
`scripts/verify-ws-gateway.sh` (release build, one 4-core box, gateway and
load generator on the same machine):

| | |
|---|---|
| Sockets | **19,000**, all established in 6.4 s, 0 failed, 0 dropped |
| Traffic | 1 msg/s per socket → **19,000 msgs/s**, every one acknowledged, 0 errors |
| Ack latency | p50 **4 ms**, p99 **9 ms**, p99.9 13 ms |
| Gateway memory | **5.2 KB per idle socket** (101 MiB for 19,000; 103 MiB under load) |
| Broker connections | **3** for all 19,000 sockets (2 producers + health) |
| Broker requests | 680,389 records in 40,074 produce requests (~16 per request, ~1,050/s) |

The container this was measured in capped each process at 20,000 file
descriptors, which is where the socket count stopped. At the measured
footprint:

- **1,000,000 sockets ≈ 5 GiB** of gateway memory (4.8 GiB extrapolated). That is one large
  instance, or (better, for blast radius and deploys) 10–20 instances
  behind an L4 load balancer.
- Per-instance capacity is set by memory, file descriptors and kernel
  socket buffers, not by the broker. Tune:

```bash
ulimit -n 1100000                          # or LimitNOFILE= in systemd
sysctl -w fs.nr_open=1100000 fs.file-max=2200000
sysctl -w net.core.somaxconn=65535 net.ipv4.tcp_max_syn_backlog=65535
sysctl -w net.ipv4.tcp_rmem="4096 4096 16777216" net.ipv4.tcp_wmem="4096 4096 16777216"
sysctl -w net.ipv4.tcp_mem="786432 1048576 1572864"   # pages; size to RAM
```

  Small default TCP buffers matter as much as the gateway's own: at a
  million sockets, the kernel's default 16–87 KiB per socket is tens of GB.
- A load *generator* needs many source addresses: one address has ~28k
  ephemeral ports per destination (`--source-ips` in `ws-loadgen`).

## Why it cannot hurt the broker

The gateway is built so that edge traffic cannot become broker load it
didn't budget for:

| Mechanism | Effect |
|---|---|
| Fixed producer pool (`--producers`, default 2) | The broker sees *N* connections per instance, whether it serves 10 sockets or 1,000,000 |
| Batching (`--linger-ms`, `--batch-bytes`, LZ4) | Produce requests track time and partitions, not sockets: ~16 records/request at 19k msgs/s |
| Bounded buffer (`--buffer-bytes`, `--max-block-ms`) | When the cluster is slow, publishes fail fast with `OVERLOADED`. Nothing queues without limit |
| Per-connection in-flight cap (`--max-inflight`) | At the cap the gateway stops reading that socket, so TCP backpressure slows the client |
| Per-connection rate limit (`--rate-limit`, `--rate-burst`) | One noisy client cannot take the fleet's share |
| Readiness (`/readyz`) | Goes 503 when the broker is unreachable or the instance drains, so new clients go elsewhere instead of piling up |
| Broker quota on `--client-id` | `brahmaputra-cli` quota entities cap the whole gateway fleet's produce rate at the broker |
| Retries with backoff inside `--delivery-timeout-ms` | A recovering broker is not hammered |

## Deploying

- **Stateless:** any instance can serve any client; no sticky sessions.
  Put an **L4** (TCP) load balancer in front, or an L7 one with long idle
  timeouts, and terminate TLS there (`wss://`).
- **Probes:** liveness `GET :8091/healthz`, readiness `GET :8091/readyz`.
  Keep `:8091` off the public load balancer.
- **Shutdown:** on SIGTERM the instance goes unready, stops accepting,
  lets each connection finish what it has in flight (within
  `--shutdown-grace-secs`), closes with 1001, and flushes. Set the
  orchestrator's termination grace above it. Clients reconnect to another
  instance and resend unacked messages.
- **Metrics:** `GET :8091/metrics` (Prometheus): open connections,
  handshake rejections by reason, messages received/produced/rejected by
  reason, in-flight, produce latency histogram, idle and slow-reader
  closes, readiness, RSS.

Every flag has an environment variable (`GW_*`): see `--help`.
[`deploy/ws-gateway/`](../../deploy/ws-gateway) has a Dockerfile and
Kubernetes manifests (Deployment, Service, HPA, PodDisruptionBudget).

## Testing

```bash
cargo test -p brahmaputra-ws-gateway          # unit + end-to-end (real broker)
scripts/verify-ws-gateway.sh                  # real processes + load
CONNECTIONS=50000 RATE=2 scripts/verify-ws-gateway.sh
```

The end-to-end tests assert what reached the log, not only what the
gateway replied:
- authentication and authorization refusals;
- every acked record found at its acked partition and offset, with the
  partition equal to `murmur2(key)`, the `x-gw-user` header set and
  per-key order kept;
- binary frames, tombstones, rate limiting, capacity limits, oversize
  messages, idle timeouts;
- 1,000 concurrent sockets costing the broker no extra connections and a
  handful of produce requests;
- graceful shutdown acknowledging every in-flight message before 1001;
- a broker outage turning into fast retryable errors and readiness 503.
