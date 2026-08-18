# Brahmaputra

A distributed, disk-based log streaming platform in Rust — topics,
partitions, replication, consumer groups — in the Kafka mould, with an
embedded Raft control plane (no ZooKeeper), a TCP/TLS/QUIC data plane, and
a built-in metrics dashboard with login and role-based access.

```
brahmaputra-server --data-dir ./data                    # a broker
brahmaputra-cli produce --topic orders --value hello    # write
brahmaputra-cli consume --topic orders --from earliest  # read
open http://localhost:8080                              # dashboard
```

---

## Contents

- [Status](#status)
- [Why it exists](#why-it-exists)
- [Quick start](#quick-start)
- [Running a cluster](#running-a-cluster)
- [Transports](#transports)
- [Producing and consuming](#producing-and-consuming)
- [Consumer groups](#consumer-groups)
- [Dashboard, metrics and access control](#dashboard-metrics-and-access-control)
- [Durability, retention and quotas](#durability-retention-and-quotas)
- [What happens when things fail](#what-happens-when-things-fail)
- [Configuration reference](#configuration-reference)
- [Verification](#verification)
- [Performance](#performance)
- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Building](#building)
- [Kafka parity and non-goals](#kafka-parity-and-non-goals)
- [License](#license)

---

## Status

| Milestone | Scope | Status |
|---|---|---|
| M1 | Single node: storage engine, wire protocol, producer/consumer client, CLI | complete |
| M2 | Raft controller quorum, metadata, multi-broker | complete |
| M3 | Replication: ISR, high watermark, leader-epoch failover | complete |
| M4 | Consumer groups and offset management | complete |
| M5 | Hardening: retention, fsync policies, quotas, TLS, fault injection, benchmarks | complete |
| M6 | Metrics API, embedded dashboard, login and RBAC | complete |
| M7 | Multi-partition Produce/Fetch, concurrent request handling, benchmark vs Kafka | complete |

Every milestone is verified by live scripts that start real brokers, kill
them, and audit what survived — not only by unit tests. See
[Verification](#verification).

## Why it exists

Kafka's model — partitioned, replicated, append-only logs that consumers
read at their own pace — is the right one. What it costs is a JVM per
broker, a heap to tune, and an operational surface that assumes a team.
Brahmaputra keeps the model and removes those costs:

- **One static binary.** No JVM, no ZooKeeper, no separate controller
  process, no external metrics stack. The dashboard is compiled in.
- **Memory that does not grow with load.** The broker passes refcounted
  byte slices and leans on the page cache. Under a 2 GiB workload it holds
  ~200–300 MiB resident where Kafka's JVM holds 1.4–2.8 GiB
  ([benchmarks](docs/benchmarks.md)).
- **Choice of transport.** Plain TCP, TLS 1.3 over TCP, or QUIC — same
  wire format, one flag.

## Quick start

```bash
cargo build --release

# A single broker with four partitions per auto-created topic.
./target/release/brahmaputra-server --data-dir ./data --default-partitions 4

# In another shell:
./target/release/brahmaputra-cli produce --topic orders --key user-7 --value '{"id":1}'
./target/release/brahmaputra-cli consume --topic orders --from earliest --max 10
./target/release/brahmaputra-cli offsets --topic orders
```

The dashboard is on <http://localhost:8080>. A single broker has no
controller, so it serves metrics and read views but not user management —
for that, run a cluster.

## Running a cluster

A node runs as broker, controller, or both. Three or five combined nodes is
the usual shape: the controllers form a Raft quorum that owns all metadata,
and the brokers serve data.

```bash
# Repeat per node, changing --node-id and the ports.
brahmaputra-server \
  --node-id 1 --cluster-id prod \
  --host 10.0.0.1 --port 9092 --control-port 19092 --http-port 8080 \
  --controller-peer 1=10.0.0.1:19092 \
  --controller-peer 2=10.0.0.2:19092 \
  --controller-peer 3=10.0.0.3:19092 \
  --data-dir /var/lib/brahmaputra

# Once, on any node: form the quorum.
curl -X POST http://10.0.0.1:19092/api/v1/controller/bootstrap
```

Then create a topic through the controller:

```bash
brahmaputra-cli --controller http://10.0.0.1:19092 \
  topic create --name orders --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2
```

Clients connect to **any** broker and are routed to partition leaders
automatically; there is no bootstrap-server list to maintain beyond one
reachable address.

## Transports

Same frames, three carriers. Broker and client must agree.

| `--transport` | Multiplexing | Encryption | Head-of-line blocking |
|---|---|---|---|
| `tcp` (default) | one byte stream, correlation ids | none | yes |
| `tcp-tls` | one byte stream, correlation ids | TLS 1.3 | yes |
| `quic` | one bidirectional stream per request | TLS 1.3 | no |

```bash
brahmaputra-server --transport quic ...
brahmaputra-cli --transport quic --broker host:9092 metadata
```

All three carry producers, consumers, group coordination *and*
inter-broker replication, and all three pass the same correctness suite
(`scripts/verify-transport-parity.sh`). TCP is the default because it is
substantially faster on a LAN — QUIC's advantages appear on lossy or
long-haul links, and its costs are measured in
[docs/benchmarks.md §4](docs/benchmarks.md).

TLS uses a self-signed certificate generated at startup. That gives
confidentiality and integrity, **not** authentication: the data plane has
no client identity yet, so anyone who can reach the port can read and
write. Restrict it by network.

## Producing and consuming

```bash
# One record. A key pins the record to a partition, so records sharing a
# key keep their relative order (murmur2, as Kafka).
brahmaputra-cli produce --topic orders --key user-7 --value '{"id":1}'

# A file, one record per line, waiting for the full ISR to acknowledge.
brahmaputra-cli produce --topic orders --file orders.ndjson --acks all

# Load generation, with the producer knobs exposed.
brahmaputra-cli produce --topic orders --count 1000000 --value-size 512 \
  --batch-size 65536 --linger-ms 10 --compression lz4 --in-flight 4096

# Read.
brahmaputra-cli consume --topic orders --from earliest --max 100
brahmaputra-cli consume --topic orders --follow          # tail
brahmaputra-cli consume --topic orders --partition 3 --offset 4200
```

`--acks` chooses durability: `0` fire-and-forget, `1` leader append,
`all` every in-sync replica. With `--idempotent`, an ambiguous send is
retried safely — the broker recognises the replay and returns the original
offset instead of appending twice.

## Consumer groups

```bash
# Two shells, same group: partitions are split between them.
brahmaputra-cli consume --topic orders --group billing --follow
brahmaputra-cli consume --topic orders --group billing --follow

brahmaputra-cli groups list
brahmaputra-cli groups describe --group billing
brahmaputra-cli groups lag --group billing
```

Offsets are committed to the internal `__consumer_offsets` topic, so a
group survives the loss of its coordinator: coordinator failover is
ordinary partition-leader failover, and the new coordinator rebuilds group
state by replaying the log.

Assignment strategies are `range` (default) and `roundrobin`, computed on
the group leader *member*, so a new strategy needs no broker upgrade.

Delivery is **at-least-once**: commit after processing. A consumer that
dies mid-batch has its partitions reassigned, and the replacement resumes
from the last commit — see
[verify-failures.sh](scripts/verify-failures.sh), which asserts nothing is
skipped or duplicated across a mid-stream kill.

## Dashboard, metrics and access control

Every broker serves an operations surface on `--http-port` (default 8080):

| Endpoint | Role | Purpose |
|---|---|---|
| `GET /` | — | the dashboard |
| `POST /api/v1/auth/login` | — | exchange credentials for a 12-hour token |
| `GET /api/v1/overview` | viewer | cluster summary, under-replicated and offline counts |
| `GET /api/v1/brokers` | viewer | broker list, liveness, roles |
| `GET /api/v1/topics`, `/topics/{name}` | viewer | topics, per-partition leader/ISR/offsets |
| `POST /api/v1/topics`, `DELETE /topics/{name}` | operator | topic administration |
| `GET /api/v1/groups`, `/groups/{id}/lag` | viewer | consumer groups and lag |
| `GET /api/v1/metrics/snapshot`, `/timeseries` | viewer | current values, chart history |
| `GET /api/v1/users`, `POST`, `DELETE` | admin | user administration |
| `GET /metrics` | none | Prometheus text format |

On first boot the cluster creates an `admin` user and a signing secret,
both stored in the Raft metadata so any broker can authenticate a session:

```bash
BRAHMAPUTRA_ADMIN_PASSWORD='choose-something-long' brahmaputra-server ...
```

Without that variable a password is generated and logged **once**.
Passwords are argon2 hashes; no endpoint ever returns one. Roles are
ordered `viewer < operator < admin`, and each route declares the minimum it
requires, so a new route cannot default to public.

`GET /metrics` is deliberately unauthenticated — scrapers do not hold
sessions. Bind the HTTP port to a trusted interface.

Metrics are kept in-process: a ring buffer per series at 5-second
granularity holding six hours. Memory is bounded by construction, and the
dashboard charts work with no Prometheus installed.

## Durability, retention and quotas

**Durability** is replication-first, as Kafka's is. `acks=all` plus
`min.insync.replicas=2` means an acknowledged write exists on at least two
brokers before the client hears about it. fsync is *optional* on top:

```bash
--flush-interval-messages 1000   # fsync every 1000 records
--flush-interval-ms 100          # ...and/or at least every 100 ms
```

Segments are always fsynced when they roll, so crash recovery only ever
has to rebuild the active segment's tail.

The high-watermark checkpoint — the file a restart reads to learn how much
of the log was committed — is written on a 5 second timer, matching Kafka's
`replica.high.watermark.checkpoint.interval.ms`. It is a recovery hint
rather than the durability guarantee, so fsyncing it per append would be
paying the most expensive operation available for the weakest promise in
the system. After an unclean stop the recovered watermark may lag what
consumers last saw, and those records are briefly invisible until it
advances again; Kafka makes the same trade. `__consumer_offsets` is the
exception and checkpoints eagerly, because a committed consumer offset
that disappears on restart is a correctness break, and commits are far too
infrequent for it to cost anything.

**Retention** deletes whole sealed segments; the active segment is never
deleted:

```bash
--retention-ms 604800000      # a week
--retention-bytes 10737418240 # or 10 GiB per partition
--retention-check-interval-ms 60000
```

A consumer whose committed offset falls off the log restarts at the new
log start rather than failing.

**Quotas** bound a noisy client without losing its data:

```bash
--quota-produce-bytes-per-sec 10485760
--quota-fetch-bytes-per-sec 52428800
```

The write is completed and made durable first; only the *acknowledgement*
is delayed. A quota therefore costs latency and never a record.

## What happens when things fail

Each row is asserted by a script, not by argument.

| Failure | Behaviour | Verified by |
|---|---|---|
| **Broker (leader) dies** | Controller elects a new leader from the ISR, bumps the leader epoch; acknowledged records are all present on the new leader | `verify-replication.sh` |
| **Broker returns** | Truncates to the last common epoch offset, catches up, re-enters the ISR (~1–2 s in test runs), log byte-identical to the leader | `verify-replication.sh` |
| **ISR below `min.insync.replicas`** | `acks=all` is refused with `NotEnoughReplicas` rather than accepting a write that cannot be made durable; resumes automatically when the ISR recovers | `verify-replication.sh` |
| **Producer killed mid-send** | No torn record is ever served; a replacement producer appends normally | `verify-failures.sh` |
| **Broker killed `-9` mid-produce** | Recovery truncates any partial tail using CRC + leader-epoch checkpoints; offsets continue with no gap or rewind | `verify-failures.sh` |
| **Consumer killed mid-stream** | Its partitions are reassigned after the session timeout; the replacement resumes from the last commit — nothing skipped, nothing duplicated | `verify-failures.sh` |
| **Consumer leaves a group** | Rebalance; remaining members take its partitions and resume from committed offsets | `verify-m4.sh` |
| **Coordinator broker dies** | Group state is rebuilt from `__consumer_offsets` by the new coordinator; committed offsets intact | `verify-m4.sh` |
| **Repeated random kills under load** | Every acknowledged record survives, exactly once, offsets contiguous, replicas byte-identical | `verify-chaos.sh` |
| **Controller quorum lost** | Metadata writes halt; the data plane keeps serving existing leaderships from cached metadata | by design (DESIGN §3.7) |

## Configuration reference

### Broker (`brahmaputra-server`)

| Flag | Default | Meaning |
|---|---|---|
| `--host`, `--port` | `127.0.0.1:9092` | data-plane bind and advertised address |
| `--data-dir` | `./data` | one directory per partition, plus `meta.toml` |
| `--default-partitions` | 1 | partitions for auto-created topics |
| `--transport` | `tcp` | `tcp`, `tcp-tls` or `quic` |
| `--segment-bytes` | 64 MiB | segment roll size |
| `--retention-ms`, `--retention-bytes` | off | segment deletion policies |
| `--retention-check-interval-ms` | 1000 | how often retention and timed flush run |
| `--flush-interval-messages`, `--flush-interval-ms` | off | fsync policy |
| `--quota-produce-bytes-per-sec`, `--quota-fetch-bytes-per-sec` | off | per-client byte rates |
| `--quota-max-throttle-ms` | 30000 | ceiling on a single throttle |
| `--http-port` | 8080 | dashboard and metrics; 0 disables |
| `--node-id`, `--cluster-id`, `--control-port`, `--controller-peer`, `--bootstrap` | — | cluster mode |
| `--heartbeat-interval-ms`, `--session-timeout-ms` | 1000 / 5000 | broker liveness |
| `--replica-lag-time-max-ms` | 10000 | ISR eviction threshold |
| `--offsets-topic-partitions` | 50 | internal offsets topic |
| `--rack` | — | rack label (recorded, not yet used for placement) |

### Producer (`brahmaputra-cli produce`, `ProducerConfig`)

`--acks`, `--batch-size`, `--linger-ms`, `--compression` (`none`/`lz4`),
`--max-in-flight`, `--in-flight` (records buffered by bulk modes),
`--timeout-ms`, `--idempotent`.

One interaction is worth knowing before tuning: `--in-flight` bounds how
many records may be outstanding at once, so it must comfortably exceed
`batch-size ÷ record size` or a batch can never fill and every flush waits
out `linger-ms` instead. At 256 B records with a 64 KiB batch, an in-flight
window of 64 caps the producer in the low thousands of messages per second
no matter how fast the broker is; 4096 lets it batch properly. It is the
rough analogue of Kafka's `buffer.memory`.

Partitions that share a broker are sent in one `ProduceMulti` request
(`batch_partitions`, on by default). They are split into `max-in-flight`
fixed shards so that a partition still has at most one request outstanding
— preserving per-partition ordering — while the shards overlap on the wire.

### Consumer (`Consumer`, `GroupConsumer`)

`max.poll.records` (500), session and rebalance timeouts, auto-commit
interval, assignor, fetch `max_bytes` / `min_bytes` / `max_wait_ms`.

A full config-by-config comparison against Kafka, including what is
missing, is in [docs/kafka-parity.md](docs/kafka-parity.md).

## Verification

```bash
cargo test --workspace          # 194 unit and integration tests

bash scripts/verify-m1.sh       # single-node storage and protocol
bash scripts/verify-m2.ps1      # controller quorum and metadata
bash scripts/verify-m3.sh       # exhaustive replication
bash scripts/verify-m4.sh       # consumer groups, 5 nodes
bash scripts/verify-m5.sh       # fsync, quotas, version negotiation
bash scripts/verify-m6.sh       # metrics, login, RBAC, dashboard
bash scripts/verify-replication.sh        # focused replication
bash scripts/verify-retention.sh          # retention
bash scripts/verify-failures.sh           # producer/broker/consumer kills
bash scripts/verify-transport-parity.sh   # tcp vs tcp-tls vs quic
bash scripts/verify-chaos.sh              # random kills under load
```

Each script starts real brokers on real ports, fails loudly on the first
missed assertion, and cleans up after itself. `TRANSPORT=quic` runs most of
them over QUIC. [.github/workflows/ci.yml](.github/workflows/ci.yml) runs
the suite on every pull request.

Last full run on the development host:

| Suite | Checks | Result |
|---|---|---|
| `cargo test --workspace` | 194 | pass |
| `verify-m1.sh` — storage, protocol, concurrent producers, SIGKILL recovery | 31 | pass |
| `verify-m4.sh` — consumer groups across 5 nodes | 30 | pass |
| `verify-m5.sh` — fsync policies, quotas, version negotiation | 15 | pass |
| `verify-m6.sh` — metrics, login, RBAC, dashboard | 30 | pass |
| `verify-replication.sh` — ISR, failover, resync | 14 | pass |
| `verify-retention.sh` — time and size retention, group resume | 21 | pass |
| `verify-failures.sh` — producer/broker/consumer kills | 15 | pass |
| `verify-transport-parity.sh` — tcp vs tcp-tls vs quic | 18 | pass |
| `verify-chaos.sh` — 5 nodes, random kills under load | — | **flaky, see below** |

**Known issue.** `verify-chaos.sh` — the roughest suite, which kills and
restarts brokers in a five-node cluster while producing continuously — does
not complete reliably. Three runs reached rounds 1, 3 and 6 of 6 before
stopping. In every case the run ended *during* a kill or restart step, and
no assertion ever failed: every round that executed acknowledged all 150 of
its records. So this is not a known data-loss bug — it is an unresolved
early exit, and it is unresolved whether the fault lies in the harness or
in broker behaviour under repeated restarts. It is listed as unverified
rather than passing until that is settled.

The narrower failure suites cover the same ground with deterministic
timing and do pass: `verify-failures.sh` (15/15) includes `kill -9`
mid-produce with recovery from a partial tail, and `verify-replication.sh`
(14/14) includes leader failover with no acknowledged record lost and a
killed broker rejoining the ISR byte-identical.

## Performance

Head-to-head with Apache Kafka 3.9.0, same host, same container limits
(4 CPUs, 4 GiB), same record size, partition count and durability setting,
each system driven by its own client from inside its own container.

Throughput alone is a weak comparison, because a single client can leave a
fast broker idle — that reads as "similar throughput" when it really means
"the client ran out of work to give". So each system is driven at 1, 2, 4
and 8 concurrent clients, and the headline compares them **at the same CPU
draw**: with `--cpus 4`, 400 % is the ceiling, and all three saturate it.

**256 B records, ~400 % CPU on both sides:**

| Metric | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| **Produce msgs/sec** | 257 848 | **772 947** (3.0×) | 418 498 (1.6×) |
| Produce CPU % | 399.3 | 403.2 | 402.7 |
| Produce memory MiB | 2080 | **256** | 197 |
| **Consume msgs/sec** | 572 656 | **3 238 866** (5.7×) | 924 642 (1.6×) |
| Consume CPU % | 350.5 | 331.1 | 320.9 |
| Consume memory MiB | 2722 | **98** | 95 |

Best result any concurrency level reached, which is the number to quote for
capacity planning:

| | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| Peak produce msgs/sec | 257 848 | **916 380** (3.6×) | 419 287 (1.6×) |
| Peak consume msgs/sec | 572 656 | **3 652 968** (6.4×) | 1 230 769 (2.1×) |

At 1 MiB records the picture is different and Kafka's `sendfile` shows:
Brahmaputra/TCP produces 490 MB/sec to Kafka's 311, and Kafka consumes
575 MB/sec to Brahmaputra's 377. Details in
[docs/benchmarks.md](docs/benchmarks.md).

Memory is the most stable difference and it is structural rather than
tuning: the JVM holds its heap and copies records through it, while the
Rust broker passes refcounted `Bytes` slices and leans on the page cache,
so its resident set stays in the tens or low hundreds of MiB regardless of
load — **8–28× less** across these runs.

### What made it fast

Every one of these was found by measuring, not by guessing, and each is
described where it lives in [docs/benchmarks.md](docs/benchmarks.md).

| Problem | Fix | Effect |
|---|---|---|
| The high-watermark checkpoint fsynced on **every append**, per partition — the most expensive operation available spent on a recovery hint, while the record data itself is left to the OS as Kafka leaves it | Write it on a 5 s timer, as Kafka's `replica.high.watermark.checkpoint.interval.ms` does; `__consumer_offsets` still checkpoints eagerly | 116k → **356k** msgs/sec produce (3.1×) |
| One topic-partition per Produce/Fetch request, so per-request cost was paid per partition — and it dominates at small records | `ProduceMulti`/`FetchMulti` carry every partition a client holds on a broker in one request | 6.4× consume |
| The broker processed one request at a time per connection, so the client's in-flight window bought nothing and a long poll blocked everything behind it | Dispatch requests concurrently behind a bounded in-flight limit | +17 % produce |
| A batched flush held every partition's send lock across the round trip, allowing one request in flight per broker | Split partitions into `max.in.flight` fixed shards, so ordering holds per partition while shards overlap | +20 % produce |
| Every `send()` that found a full batch queued its own flush behind the partition lock, so raising concurrency *lowered* throughput | Skip the size-triggered flush when one is already running; the running flusher drains the buffer | 5 162 → **61 187** msgs/sec (11.9×) |
| The broker never set `TCP_NODELAY` on accepted sockets, so responses waited on Nagle plus the peer's delayed ACK | `set_nodelay(true)` on accept | 6 415 → **14 790** msgs/sec |
| The broker decoded and re-encoded every batch on append | Validate the header only and stamp `base_offset`/`leader_epoch` in place — both sit before the CRC, so it stays valid | +34 % |

### Benchmark method

The exact machine, both systems' full configuration, and how the numbers
are taken are documented in
[docs/benchmarks.md §1](docs/benchmarks.md). In summary:

| | |
|---|---|
| Host | AMD Ryzen AI MAX+ 395, 32 logical CPUs, 64 GB RAM |
| OS | Windows 11 26200, Docker Desktop 29.6.2, WSL2 kernel 6.18.33.2 |
| Per container | `--cpus 4 --memory 4g`, overlayfs on the WSL2 virtual disk |
| Workload | 1 000 000 × 256 B records per client, 6 partitions, RF=1, acks=1 |
| Producer | `batch.size` 64 KiB, `linger.ms` 5, no compression |
| Kafka | `apache/kafka:3.9.0` KRaft, 2 GiB G1 heap, 8 I/O + 4 network threads, 1 MiB socket buffers |

Kafka is tuned rather than left at defaults — a 2 GiB heap inside a 4 GiB
container so the page cache it relies on has room, G1 with a 20 ms pause
target, and a separate small heap for the perf-test clients (they read the
same `KAFKA_HEAP_OPTS`, so one large setting silently gives every client a
large heap too, which is what makes an 8-client run fall over).

Reproduce:

```bash
bash scripts/bench-matched.sh          # resource-matched, all three systems
bash scripts/bench-three-way.sh        # 1 MiB records, all three
bash scripts/bench-vs-kafka.sh         # small records, Kafka vs Brahmaputra
```

**Caveats.** Docker Desktop on Windows virtualises disk and network, so
absolute numbers are below bare metal for every system measured; the
comparison between them is the useful part. Run-to-run variance is high —
Kafka's own produce peak moved between 147k and 258k msgs/sec across runs
on this host — so all three systems are always measured in a single run,
and only within-run comparisons are quoted. One run per configuration, no
confidence intervals.

## Architecture

```
                    ┌─────────────────────────────┐
                    │   Controller quorum (Raft)   │
                    │  topics, partitions, ISR,    │
                    │  brokers, configs, users     │
                    └──────────────┬───────────────┘
                                   │ metadata deltas
        ┌──────────────────────────┼──────────────────────────┐
        ▼                          ▼                          ▼
  ┌───────────┐              ┌───────────┐              ┌───────────┐
  │ Broker 1  │◄─replication─►│ Broker 2  │◄─replication─►│ Broker 3  │
  │ partition │              │ partition │              │ partition │
  │ logs (L/F)│              │ logs (L/F)│              │ logs (L/F)│
  └─────▲─────┘              └─────▲─────┘              └─────▲─────┘
        │                          │                          │
   producers / consumers  (TCP · TCP+TLS · QUIC, BitPacker frames)
```

- **Single-writer partitions.** Each (topic, partition) is owned by one
  actor task that alone touches its log — no locks on the append path.
- **Requests are concurrent, per connection.** Correlation ids already
  allow responses to return out of order, so a broker dispatches every
  request on a socket concurrently behind a bounded in-flight limit. One
  slow request — a long poll, an `acks=all` wait — no longer blocks the
  requests queued behind it, and partition ordering is unaffected because
  each partition is still a single writer.
- **Many partitions per request.** `ProduceMulti` and `FetchMulti` carry
  every partition a client holds on one broker in a single request, and
  the broker serves them concurrently. At small records the per-request
  cost dominates, so paying it once instead of per partition is the
  difference between trailing Kafka and beating it.
- **Batches are never rewritten.** The broker validates a batch's header
  and stamps only `base_offset` and `leader_epoch`, both of which sit
  *before* the CRC field. The bytes a producer sent are the bytes on disk,
  replicated to followers and served to consumers.
- **Metadata is an event-sourced Raft log**; brokers subscribe and
  materialise a local immutable image, so client metadata reads never touch
  the controller.
- **Leader-epoch truncation** (KIP-101 semantics) makes divergence
  detection exact after a failover, rather than guessing from offsets.

Deeper dives, one per subsystem, are in
[docs/blueprint/](docs/blueprint/README.md); the design rationale and the
alternatives considered are in [DESIGN.md](DESIGN.md).

## Repository layout

```
DESIGN.md                   architecture and design decisions
docs/blueprint/             per-subsystem internals
docs/kafka-parity.md        config-by-config audit against Kafka
docs/benchmarks.md          measured performance and the fixes it drove
schemas/protocol.buff       BitPacker wire schemas (data plane)
scripts/                    live verification and benchmark harnesses
crates/
  protocol/                 wire types, record batch codec (pure, sync)
  storage/                  segments, indices, retention, recovery
  metadata/                 cluster image, commands, users and roles
  controller/               openraft glue, controller HTTP
  broker/                   data plane, partition actors, groups, quotas
  client/                   producer, consumer, group consumer, transports
  metrics/                  registry, time-series ring, Prometheus export
  dashboard/                HTTP API, auth, RBAC, embedded UI
  server/                   the binary that wires it together
  cli/                      brahmaputra-cli
tools/bit-packer/           vendored schema compiler (Go)
```

## Building

```bash
cargo build --release
cargo test --workspace
```

Regenerating wire types after editing `schemas/protocol.buff` needs the
BitPacker generator, built once from the vendored source (requires Go):

```bash
cd tools/bit-packer && go build -o ../bitpacker ./cmd/bitpacker
bash scripts/gen-protocol.sh
```

## Kafka parity and non-goals

Present: partitioned segmented logs, leader/ISR replication with
leader-epoch truncation, high-watermark visibility, `acks=0/1/all`,
idempotent producer, consumer groups with generation fencing, retention,
quotas, fsync policies, API version negotiation, TLS, metrics and RBAC.

Deliberately **not** in v1 (DESIGN.md §1): log compaction, transactions and
exactly-once semantics, multi-datacentre replication, tiered storage.

Known gaps, ranked, in [docs/kafka-parity.md](docs/kafka-parity.md) §8.
The ones that matter most:

1. **No data-plane authentication.** TLS encrypts; it does not identify.
2. **No `sendfile`** — a fetch copies bytes through userspace twice.
3. Topic-level configs other than `min.insync.replicas` are stored but not
   applied.
4. **No log compaction**, so `__consumer_offsets` grows without bound on a
   long-lived cluster.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
