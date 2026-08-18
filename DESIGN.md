# Brahmaputra — Distributed Log Streamer (Kafka-like) in Rust

A design plan for a high-performance, disk-based, partitioned, replicated log
streaming system with producers, consumers, topics, and consumer groups.

---

## 1. Goals & Non-Goals

**Goals**
- Topics split into partitions; partitions are the unit of parallelism and ordering.
- Durable, append-only, segment-based storage on local disk.
- Leader-follower replication with ISR (in-sync replica) semantics.
- Pull-based consumers with consumer groups and offset management.
- Self-contained cluster: no ZooKeeper. Metadata managed by an embedded Raft
  controller quorum (the "KRaft" model).
- High throughput: batching everywhere, zero-copy I/O, page-cache friendly.
- Built-in observability: per-broker HTTP metrics API, an embedded web
  dashboard (Kafka-UI-like) with login and role-based access management
  (admin-managed users and permissions).

**Non-goals (v1)**
- Exactly-once semantics / transactions (design leaves room: producer IDs, epochs).
- Log compaction (retention by time/size only).
- Multi-datacenter replication, quotas, tiered storage.

---

## 2. High-Level Architecture

```
                        ┌─────────────────────────────┐
                        │   Controller Quorum (Raft)  │
                        │  3–5 nodes, one is leader   │
                        │  metadata log: topics,      │
                        │  partitions, ISR, brokers,  │
                        │  configs, ACLs, elections   │
                        └──────────────┬──────────────┘
                                       │ metadata deltas (watch/fetch)
        ┌──────────────────────────────┼──────────────────────────────┐
        ▼                              ▼                              ▼
  ┌───────────┐                  ┌───────────┐                  ┌───────────┐
  │ Broker 1  │                  │ Broker 2  │                  │ Broker 3  │
  │ data plane│                  │ data plane│                  │ data plane│
  │ partition │◄──replication──► │ partition │◄──replication──► │ partition │
  │ logs (L/F)│                  │ logs (L/F)│                  │ logs (L/F)│
  └─────▲─────┘                  └─────▲─────┘                  └─────▲─────┘
        │                              │                              │
   producers/consumers (custom binary protocol over TCP, TLS optional)
```

- A node can run as **broker**, **controller**, or **both** (combined mode for
  small clusters; dedicated controllers for large ones).
- Clients discover the cluster via any broker (`Metadata` API), then connect
  directly to partition leaders.

---

## 3. Metadata & Distributed State (the hard part)

### 3.1 Controller quorum — Raft, KRaft-style

Use [`openraft`](https://docs.rs/openraft) (async, production-proven Raft;
used by databases like Databend) rather than writing Raft from scratch.

- The cluster's **entire metadata is an event-sourced log** replicated by Raft:
  - broker registration / fencing (broker epoch)
  - topic creation/deletion, partition count changes
  - partition assignments (replica placement)
  - partition leader + ISR + leader epoch changes
  - configs, ACLs
- The **active controller** (Raft leader) is the only writer of metadata.
- **Brokers do not talk to Raft directly.** Each broker runs a metadata
  subscriber that fetches the metadata log (deltas with offsets) from the
  active controller and materializes a local, immutable, snapshot-able
  metadata cache. This is exactly Kafka's KRaft `MetadataImage` model.
- Clients get `Metadata` responses served from any broker's local cache —
  reads never hit the controller.

### 3.2 Broker lifecycle

1. Broker starts → loads local partition logs → registers with active
   controller (`BrokerRegistration` with a broker-unique epoch for fencing).
2. Broker sends **heartbeat** every N seconds (lease). Missed heartbeats →
   controller marks broker dead, shrinks ISRs, triggers leader elections.
3. Controlled shutdown: broker asks controller to move its leaders away first.

### 3.3 Partition leader election (controller-driven, ISR-based)

Chosen over per-partition Raft (see tradeoffs below):

- The controller tracks each partition's **ISR** (replicas within
  `replica.lag.max` of the leader's log-end offset).
- On leader failure, the controller picks a new leader **from the ISR**,
  bumps the **leader epoch**, and persists the change in the Raft metadata
  log *before* telling brokers.
- Brokers learn of the change via their metadata subscription and a direct
  `LeaderAndIsr`-style RPC, then start/stop serving.
- Leader epoch prevents split-brain: requests carrying a stale epoch are
  rejected; a zombie old leader fences itself.

**Alternative considered — per-partition Raft** (like Redpanda): simpler
correctness story, but N partitions × Raft groups = heavy memory/heartbeat
overhead, harder rebalancing, and consumer-group coordination still needs a
separate mechanism. Controller+ISR scales better and matches the Kafka mental
model. Decision: **controller + ISR**, keep Raft only for metadata.

### 3.4 Why not etcd / ZooKeeper

Embedding Raft keeps the system self-contained (one binary, no ops burden) and
avoids a network hop + external dependency on the write path of every topic
operation. `openraft` gives us this without building consensus ourselves.

---

## 4. Storage Engine

Per partition, per broker: a directory of **segments**.

```
data/
  orders-0/                          # topic "orders", partition 0
    00000000000000000000.log         # record batches, append-only
    00000000000000000000.index       # sparse offset → file-position index
    00000000000000000000.timeindex   # sparse timestamp → offset index
    00000000000000481234.log
    ...
```

### 4.1 Record batch format (the only unit on disk & wire)

```
base_offset:   i64
batch_length:  i32
leader_epoch:  i32          (for truncation after leader change)
magic:         u8
crc32c:        u32          (covers everything after this field)
attributes:    u16          (compression type, timestamp type)
last_offset_delta: i32
max_timestamp: i64
records:       [Record]     (varint-encoded: len, key, value, headers, ts_delta)
```

- Producers send **batches**; batches are written to disk and replicated
  **unmodified** — the same bytes flow network → page cache → follower →
  consumer. No per-record re-encoding anywhere.
- CRC per batch, verified on read when data is served from disk (not page
  cache) and on follower append.

### 4.2 Writes

- Append to active segment; rely on the OS **page cache** — no userspace
  read cache at all.
- Sparse index (one entry per ~4 KB of log): offset lookup = binary search
  index → scan forward a few KB.
- Durability is **replication-first**: `acks=all` + ISR ≥ 2 gives durability
  without fsync per batch. fsync on a configurable interval
  (`flush.interval.ms`) as backstop.

### 4.3 Reads

- `sendfile`/splice-style zero-copy from page cache to socket
  (`tokio::fs` + `sendfile` on Linux via `tokio-io-uring` path, or
  `nix::sys::sendfile`), so fetch responses never copy bytes into userspace.
- Consumers read from leader (v1). Follower fetching for replication only.

### 4.4 Retention & recovery

- Segment rolls at size (`segment.bytes`) or age (`segment.ms`).
- Retention threads delete whole segments past `retention.ms`/`retention.bytes`.
- On startup: scan segments, rebuild index for the last segment, truncate to
  the last valid batch using CRC + leader-epoch checkpoints.

---

## 5. Replication Protocol

- Follower issues long-poll `Fetch` (like a consumer) to its partition leader.
- Leader tracks each follower's fetch offset → computes **high watermark**
  (HW = min ISR log-end offset) and piggybacks it on fetch responses.
- Only records ≤ HW are visible to consumers (`read_committed`-ish by
  construction for v1).
- Follower out of `replica.lag.time.max.ms` → leader reports ISR shrink to
  controller; catch-up → ISR expand. All ISR changes go through the Raft log.
- **Log truncation on leader change**: each broker persists leader-epoch
  checkpoints; a new follower truncates back to the last offset of the common
  epoch before fetching (the Kafka leader-epoch protocol — this avoids
  data loss/inconsistency that naive offset comparison causes).

### Producer acks

- `acks=0`: fire and forget. `acks=1`: leader append. `acks=all`: appended by
  all ISR members (leader holds a pending-ack map completed as HW advances).
- Idempotent producer (v1.1): `producer_id + epoch + sequence` per partition,
  dedup window of last N batches per PID on the leader. Foundation for
  transactions later.

---

## 6. Wire Protocol

Two protocols, two ports:

**Control plane** — gRPC ([`tonic`](https://docs.rs/tonic) + `prost`):
admin (create/delete topic, alter configs), controller↔broker RPCs,
heartbeats, join/leave group. Protobuf keeps this evolvable with zero effort.

**Data plane** — custom binary over TCP (length-prefixed, versioned
request/response frames), because gRPC/HTTP-2 framing costs too much for
multi-GB/s append/fetch paths:

```
frame := length:i32  api_key:i16  api_version:i16  correlation_id:i32  client_id:string  body
Apis: Produce, Fetch, ListOffsets, Metadata, JoinGroup, SyncGroup, Heartbeat,
      OffsetCommit, OffsetFetch, InitProducerId
```

- **Request/response bodies are defined in `.buff` schemas and encoded with
  [BitPacker](https://github.com/byte-mods/bit-packer)** — schema-driven,
  VarInt/ZigZag wire format, zero runtime dependencies, generated Rust code
  checked into `crates/protocol/src/gen/`. Schemas live in
  `schemas/protocol.buff`; regeneration is a `just`/`make`-style task.
- **Exception — record batches stay hand-rolled** (`bytes`-based codec): the
  batch format needs a fixed header layout for CRC-bounded crash recovery,
  sparse indexing, broker-stamped base offsets, and zero-copy passthrough of
  unmodified batch bytes, none of which a varint/zigzag schema format can
  guarantee. Batches appear inside BitPacker messages as opaque byte fields.

- Versioned per-API like Kafka — independent evolution, rolling upgrades.
- Optional TLS via `rustls`; SASL/PLAIN auth v1.1.

---

## 7. Consumers & Consumer Groups

- **Pull model** with long polling (`fetch.max.wait.ms`, `fetch.min.bytes`) —
  simple flow control, consumer-driven replay, no broker push state.
- **Group coordinator**: partition `hash(group_id) % N` of internal topic
  `__consumer_offsets` → that partition's leader brokers the group.
  Groups survive coordinator failover because membership state is rebuilt from
  the (replicated) offsets topic + rejoin.
- **Rebalancing** (v1: eager, "stop the world"): JoinGroup/SyncGroup, one
  member is group leader, assignor strategies: `range`, `round-robin`,
  `sticky` (later: cooperative-sticky).
- **Offsets**: committed as records in `__consumer_offsets` (compacted later;
  in-memory map per coordinator, rebuilt from log on leadership).
- Dead members detected via session timeout / heartbeat expiry → rebalance.

---

## 8. Performance Techniques (the "high performance" part)

1. **Batching at every layer**: producer (`linger.ms`, `batch.size`,
   per-partition buffers), network (one request = many batches), disk
   (append batches, not records), replication (fetch = batch stream),
   consumer (fetch returns many batches).
2. **Zero-copy**: `bytes::Bytes` end-to-end; `sendfile` for fetch responses;
   batch bytes written/replicated/read unmodified.
3. **Page-cache-centric design**: hot reads never touch disk; sequential
   I/O only; let the OS do readahead/writeback.
4. **Runtime**: `tokio` multi-threaded, one connection task + per-partition
   actor (single-writer principle — no locks on the log append path; a
   partition's append loop owns its segment files).
   *Stretch goal*: evaluate `glommio`/`monoio` (thread-per-core, io_uring)
   for the storage path; keep it behind an abstraction so it's swappable.
5. **Backpressure**: bounded channels everywhere (tokio `mpsc` with
   `try_send` + watermarks), max in-flight requests per connection,
   producer `max.in.flight=5` with idempotence ordering guarantees.
6. **Memory discipline**: pooled buffers (`bytes` + a slab pool), no
   per-message allocation on hot path, `#[repr]`-packed wire structs,
   pre-sized index files.
7. **Observability built-in**: `tracing` + `tracing-subscriber`,
   `metrics` crate facade (Prometheus exporter), per-partition lag,
   request latency histograms, ISR shrink/expand events.

---

## 9. Metrics API, Dashboard & Access Management

A Kafka-UI-style operations surface, built into the product (no external
tooling required).

### 9.1 Metrics collection

- All components instrument with the `metrics` facade crate (counters,
  gauges, histograms): produce/fetch request rates and latency, bytes in/out,
  per-topic/partition message rates, log end offsets, high watermarks,
  consumer-group lag, ISR shrink/expand counts, under-replicated partitions,
  Raft leader/commit index, request queue depth, failed authentications.
- Each broker/controller keeps an **in-process time-series store**: a
  ring buffer per metric at 5 s granularity retaining ~6 h (bounded memory).
  This powers dashboard charts without Prometheus.
- `GET /metrics` additionally exposes everything in Prometheus text format
  (`metrics-exporter-prometheus`) for real deployments.

### 9.2 HTTP metrics/admin API

An `axum` HTTP server embedded in every broker (separate port from the data
plane, e.g. 8080):

```
POST /api/v1/auth/login                  → { token }
POST /api/v1/auth/logout
GET  /api/v1/overview                    → cluster summary (broker count, topics, partitions, URP)
GET  /api/v1/brokers                     → broker list, status, roles, log dirs
GET  /api/v1/topics                      → topics, partition counts, replication factor
GET  /api/v1/topics/{name}               → per-partition detail: leader, ISR, LEO, HW, sizes
GET  /api/v1/groups                      → consumer groups, state, members
GET  /api/v1/groups/{id}/lag             → per-partition lag
GET  /api/v1/metrics/timeseries?metric=…&from=…&to=…  → chart data from ring buffer
GET  /api/v1/metrics/snapshot            → current values of all key metrics
# admin-only:
POST   /api/v1/topics                    → create topic
DELETE /api/v1/topics/{name}
GET    /api/v1/users                     → list users
POST   /api/v1/users                     → create user
PATCH  /api/v1/users/{name}              → change role / reset password
DELETE /api/v1/users/{name}
GET    /metrics                          → Prometheus text (unauthenticated, bind-restricted)
```

### 9.3 Dashboard (web UI)

- Static SPA served by the same axum server, assets embedded in the binary
  via `rust-embed` — **no Node.js toolchain required**: vanilla JS +
  Chart.js (or similar lightweight charting from a vendored file).
- Pages: login; cluster overview; brokers; topics & partitions (leader/ISR/
  LEO/HW, sizes); consumer groups & lag; throughput/latency charts; user
  administration (admin only).
- Polls the JSON API every few seconds with the session token.

### 9.4 Access management (RBAC)

- **Users live in the Raft metadata log** (`UserRecord { username,
  password_hash, roles }`) — replicated and consistent cluster-wide, so any
  broker can authenticate a login locally. Passwords hashed with `argon2`.
- **First boot**: controller creates an `admin` user; initial password comes
  from env var `BRAHMAPUTRA_ADMIN_PASSWORD` (or generated and printed once to
  the log). Admin creates further users via API/UI.
- **Sessions**: login returns a JWT (HS256 via `jsonwebtoken`) signed with a
  cluster secret stored in the Raft metadata (rotatable); expiry ~12 h.
  Middleware validates token + role on every request.
- **Roles**:
  - `admin` — everything: user management, topic create/delete, all reads.
  - `operator` — topic management + all read APIs, no user management.
  - `viewer` — read-only APIs and dashboard.
- Enforcement: axum middleware extracting role claims; route-level guards.
  (Data-plane ACLs for produce/fetch are a later milestone and will reuse the
  same user store.)

---

## 10. Crate / Library Selection

| Concern | Crate | Why |
|---|---|---|
| Async runtime | `tokio` | ecosystem, maturity, `mpsc`/`oneshot`, timers |
| Consensus | `openraft` | async Raft with pluggable storage/log; don't build this |
| Control-plane RPC | `tonic` + `prost` | gRPC for admin/heartbeat/election RPCs |
| Data-plane codec | **BitPacker** `.buff` schemas → generated Rust (build-time, no runtime dep) + hand-rolled `bytes` codec for record batches; `tokio-util` `LengthDelimitedCodec` framing | zero-copy, full control |
| Buffers | `bytes` | refcounted zero-copy slices, the standard |
| Serialization (metadata, raft log) | `serde` + `bincode` (or `prost`) | versioned snapshots |
| Compression | `lz4_flex`, `zstd`, `snap` | per-batch compression; lz4 default |
| Checksums | `crc32c` (or `crc` with CASTAGOLI) | hardware-accelerated |
| mmap (indices) | `memmap2` | sparse index files |
| File I/O | `tokio::fs`, `nix` (sendfile/fallocate/fdatasync) | zero-copy & durability syscalls |
| Concurrency utils | `dashmap`, `parking_lot`, `arc-swap`, `crossbeam` | metadata cache, hot read paths |
| Time | `tokio::time`, `quanta` or std | timers, monotonic clocks |
| Errors | `thiserror` (libs), `anyhow` (binaries) | standard split |
| Observability | `tracing`, `tracing-subscriber`, `metrics`, `metrics-exporter-prometheus` | |
| HTTP API & dashboard | `axum`, `tower-http`, `rust-embed`, `serde_json` | embedded admin/metrics server + SPA |
| Authn/authz | `argon2`, `jsonwebtoken` | password hashing, session tokens |
| CLI / config | `clap`, `serde` + `toml` | |
| Testing | `tokio::test`, `proptest`, `criterion`, `loom` (lock-free bits), `turmoil` (network-fault simulation of the whole cluster!) | turmoil is a big win for distributed correctness |

---

## 11. Workspace Layout

```
brahmaputra/
  Cargo.toml                 # workspace
  crates/
    protocol/                # wire types, codecs, API versioning (no_std-ish, pure)
    storage/                 # segments, indices, record batch, recovery — no tokio in core
    metadata/                # metadata log types, image/cache, controller state machine
    raft/                    # openraft glue: storage impl, network impl, quorum mgmt
    broker/                  # data plane server, partition actors, replica manager,
                             #   group coordinator, fetch/produce handlers
    controller/              # election logic, ISR mgmt, partition assignment, heartbeats
    client/                  # producer + consumer + admin Rust client
    server/                  # binary: role flags (broker/controller/both), config, wiring
    cli/                     # brahmaputra-cli: topics, produce, consume, groups
    metrics/                 # metrics registry, in-process time-series ring store, Prom export
    dashboard/               # axum HTTP API, auth middleware, user mgmt, embedded SPA
    testing/                 # turmoil-based cluster simulation, fault injection
```

Key rule: `storage` and `protocol` are pure/sync and separately testable;
async lives at the edges (`broker`, `controller`).

---

## 12. Milestones

- **M1 — Single-node log**: storage engine + protocol + producer/consumer
  over TCP, one broker, no replication. Benchmark vs. a Kafka single broker.
- **M2 — Metadata & multi-broker**: Raft controller quorum, topic admin,
  partition assignment, broker registration, metadata cache, client routing.
- **M3 — Replication**: follower fetch, HW, ISR, leader election,
  leader-epoch truncation, `acks=all`, idempotent producer.
- **M4 — Consumer groups**: `__consumer_offsets`, coordinator, rebalancing,
  offset commit/fetch.
- **M5 — Hardening**: retention, quotas, turmoil fault-injection suite,
  chaos runs, fsync policies, rolling upgrades, Prometheus dashboards,
  TLS/authn, benchmark suite in CI.
- **M6 — Metrics & dashboard**: broker-embedded metrics registry +
  time-series store, HTTP JSON API, login + RBAC user management on the Raft
  metadata store, embedded SPA dashboard. (Instrumentation hooks land
  incrementally from M2 onward; M6 wires up the full surface.)

---

## 13. Known Hard Problems (read before coding)

1. **Leader-epoch truncation** — get this wrong and you silently lose or
   duplicate data after failover. Port Kafka's KIP-101 semantics carefully.
2. **Fencing zombie leaders** — broker epochs + leader epochs + rejecting
   stale generations everywhere (produce, fetch, metadata writes).
3. **ISR flapping** — lag detection must be time-based with hysteresis, not
   offset-based, or partitions flap in/out of ISR under bursty load.
4. **Rebalance storms** — session timeouts vs. GC pauses; static membership
   (group instance IDs) later.
5. **Slow-disk backpressure** — page cache writeback stalls will surface as
   produce latency spikes; need dirty-page-aware throttling, not just app
   watermarks.
