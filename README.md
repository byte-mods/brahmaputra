<div align="center">

# 🌊 Brahmaputra

**A distributed log streaming platform in Rust.**
Kafka's model — partitioned, replicated, append-only logs — in one static
binary, with no JVM, no ZooKeeper and no heap to tune.

[![CI](https://github.com/byte-mods/brahmaputra/actions/workflows/ci.yml/badge.svg)](https://github.com/byte-mods/brahmaputra/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)
[![Tests](https://img.shields.io/badge/tests-306%20passing-brightgreen.svg)](#verification)
[![Throughput](https://img.shields.io/badge/vs%20Kafka-3.4%C3%97%20produce%20at%20RF%3D3-brightgreen.svg)](#performance)
[![Transports](https://img.shields.io/badge/transport-TCP%20%C2%B7%20TLS%201.3%20%C2%B7%20QUIC-informational.svg)](#transports)
[![Auth](https://img.shields.io/badge/auth-SASL--style%20%C2%B7%20ACLs-blueviolet.svg)](#authentication-and-access-control)

</div>

```bash
brahmaputra-server --data-dir ./data                    # a broker
brahmaputra-cli produce --topic orders --value hello    # write
brahmaputra-cli consume --topic orders --from earliest  # read
open http://localhost:8080                              # dashboard
```

|  | |
|---|---|
| 🚀 **Faster than Kafka where it counts** | Three brokers at RF=3, `acks=all` — the durable setting — **3.4× produce** and **2.1× consume**, on **4.5× less memory**. Replication costs it 1.47× against Kafka's 3.18×. [Measured, with method →](#performance) |
| 🧩 **One static binary** | Broker, controller, dashboard and metrics compiled in. No JVM, no ZooKeeper, no Prometheus required. |
| 🔁 **Kafka semantics, not just Kafka shape** | Leader/ISR replication, leader-epoch truncation (KIP-101), high-watermark visibility, `acks=0/1/all`, idempotent producer, consumer groups with generation fencing. |
| 🔌 **Three transports, one flag** | Plain TCP, TLS 1.3, or QUIC — same wire format, same correctness suite. |
| 🧪 **Verified by killing things** | Live scripts start real brokers, `kill -9` them mid-write, and audit what survived. Not only unit tests. |
| 📊 **Operations built in** | Browse and live-tail messages, add partitions, change topic config, consumer lag, Prometheus endpoint, login and RBAC — [in one container](#docker). |
| 🔐 **Authentication and ACLs** | Principals bound per connection, deny-by-default authorization on topics, groups and the cluster. [Details →](#authentication-and-access-control) |

---

## Contents

- [Status](#status)
- [Why it exists](#why-it-exists)
- [Quick start](#quick-start)
- [Running a cluster](#running-a-cluster)
- [Transports](#transports)
- [Producing and consuming](#producing-and-consuming)
- [Consumer groups](#consumer-groups)
- [Using the Rust client](#using-the-rust-client)
- [Authentication and access control](#authentication-and-access-control)
- [Dashboard, metrics and access control](#dashboard-metrics-and-access-control)
- [Docker](#docker)
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
| M1 | Single node: storage engine, wire protocol, producer/consumer client, CLI | ✅ complete |
| M2 | Raft controller quorum, metadata, multi-broker | ✅ complete |
| M3 | Replication: ISR, high watermark, leader-epoch failover | ✅ complete |
| M4 | Consumer groups and offset management | ✅ complete |
| M5 | Hardening: retention, fsync policies, quotas, TLS, fault injection, benchmarks | ✅ complete |
| M6 | Metrics API, embedded dashboard, login and RBAC | ✅ complete |
| M7 | Multi-partition Produce/Fetch, concurrent request handling, benchmark vs Kafka | ✅ complete |
| M8 | Data-plane authentication and ACLs, log compaction, message explorer, Docker image | ✅ complete |

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
  byte slices and leans on the page cache. A three-broker cluster under
  RF=3 `acks=all` load holds under 1 GiB across the whole cluster where
  Kafka holds 4.5 GiB ([benchmarks](#performance)).
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
[docs/benchmarks.md §5](docs/benchmarks.md).

TLS uses a self-signed certificate generated at startup. That gives
confidentiality and integrity; identity comes from
[authentication](#authentication-and-access-control), which must be
enabled separately.

## Authentication and access control

Off by default, matching a Kafka `PLAINTEXT` listener. Production should
turn it on:

```bash
brahmaputra-server --require-auth --transport tcp-tls ...
```

With `--require-auth`, a connection starts anonymous and is refused until
it authenticates, and an authenticated principal still needs a matching
ACL. **The default is denial** — enabling authentication cannot silently
widen access.

```rust
let connection = Connection::connect_with(Transport::TcpTls, addr, id, 5).await?;
connection.authenticate(&Credentials {
    username: "billing".into(),
    password: "…".into(),
}).await?;
```

Credentials are checked against the same argon2 user store the dashboard
uses, so there is one set of accounts rather than two to keep in sync. A
password crosses the wire in the clear exactly as SASL/PLAIN does, so the
broker **refuses to accept one on a plaintext listener** — use `tcp-tls`
or `quic`.

ACLs are stored in the Raft metadata and evaluated deny-over-allow:

| Field | Values |
|---|---|
| `principal` | a username, or `*` |
| `resource_type` | `topic`, `group`, `cluster` |
| `resource_name` | an exact name, or `*` |
| `operation` | `read`, `write`, `describe`, `all` |
| `permission` | `allow`, `deny` |

An `admin` is exempt, so a bad rule cannot lock a cluster out of its own
administration. An unknown user fails identically to a wrong password, so
probing cannot enumerate accounts. The inter-broker replication APIs are
covered too — they serve raw log bytes above the high watermark, so leaving
them open would hand out every topic to anyone who can speak the protocol.

Six tests assert the *denial* direction specifically, which is the only
direction that matters for a security control: anonymous refused, wrong
password refused, unknown user indistinguishable from a wrong password, an
authenticated principal still bound by its ACLs, permission scoped to the
named topic, and a deny rule beating a wildcard allow.

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

## Using the Rust client

The CLI is a thin wrapper over the `brahmaputra-client` crate; anything the
CLI does is available as a library.

```rust
use brahmaputra_client::{Consumer, GroupConsumer, Producer, ProducerConfig, EARLIEST};
use bytes::Bytes;
use std::time::Duration;

// Produce. `send` returns the record's offset once it is acknowledged.
let producer = Producer::connect("127.0.0.1:9092".parse()?, ProducerConfig {
    acks: 1,
    batch_size: 64 * 1024,
    linger_ms: 5,
    ..ProducerConfig::default()
}).await?;
let offset = producer
    .send("orders", None, Some(Bytes::from("user-7")), Bytes::from(r#"{"id":1}"#))
    .await?;
producer.flush().await?;

// Read one partition directly, no group. Returns (offset, key, value).
let consumer = Consumer::connect("127.0.0.1:9092".parse()?, "reader").await?;
let records = consumer.fetch("orders", 0, EARLIEST, 500).await?;

// Or join a group and let the coordinator assign partitions.
let mut group =
    GroupConsumer::connect("127.0.0.1:9092".parse()?, "reader-1", "billing").await?;
group.subscribe(&["orders"]);
loop {
    for record in group.poll(Duration::from_millis(500)).await? {
        handle(&record.value);
    }
    group.commit_sync().await?;   // at-least-once: commit after processing
}
```

`Producer` batches internally and is shared across tasks rather than
created per message. `GroupConsumer` is single-task by design, matching
Kafka's consumer: use one per thread and give each its own client id.

This snippet is compiled as
[crates/client/examples/readme_snippet.rs](crates/client/examples/readme_snippet.rs)
(`cargo check -p brahmaputra-client --example readme_snippet`), so it
cannot drift out of date with the API.

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
| `GET /api/v1/topics/{name}/messages` | viewer | browse records, with `search`, `order`, `partition`, `limit` |
| `GET /api/v1/topics/{name}/stream` | viewer | live tail as server-sent events |
| `POST /api/v1/topics/{name}/partitions` | operator | increase the partition count |
| `POST /api/v1/topics/{name}/config` | operator | change topic configuration |
| `GET /api/v1/groups`, `/groups/{id}/lag` | viewer | consumer groups and lag |
| `GET /api/v1/metrics/snapshot`, `/timeseries` | viewer | current values, chart history |
| `GET /api/v1/users`, `POST`, `DELETE` | admin | user administration |
| ACL rules | admin | via the controller, `put_acl` / `delete_acl` |
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

Exported series, all on `GET /metrics` in Prometheus text format:

| Metric | Kind | What it tells you |
|---|---|---|
| `brahmaputra_produce_requests_total` | counter | produce request rate |
| `brahmaputra_produce_records_total` | counter | records accepted |
| `brahmaputra_produce_bytes_total` | counter | bytes accepted |
| `brahmaputra_produce_errors_total` | counter | rejected appends — the first thing to alert on |
| `brahmaputra_fetch_requests_total` | counter | fetch request rate |
| `brahmaputra_fetch_bytes_total` | counter | bytes served to consumers |
| `brahmaputra_requests_total` | counter | all requests, labelled by API |
| `brahmaputra_throttled_requests_total` | counter | requests a quota delayed |
| `brahmaputra_throttle_ms_total` | counter | total delay imposed by quotas |
| `brahmaputra_connections_open` | gauge | live client connections |
| `brahmaputra_partition_log_end_offset` | gauge | per-partition write position |
| `brahmaputra_partition_log_start_offset` | gauge | per-partition retention position |
| `brahmaputra_partition_high_watermark` | gauge | per-partition committed position |
| `brahmaputra_partition_isr_size` | gauge | in-sync replica count |
| `brahmaputra_under_replicated_partitions` | gauge | partitions below their replica count — alert on any non-zero |
| `brahmaputra_leader_partitions` | gauge | partitions this broker leads |
| `brahmaputra_group_members` | gauge | members per consumer group |
| `brahmaputra_group_lag` | gauge | committed offset behind log end, per group |

For an operator the three that matter most are
`brahmaputra_under_replicated_partitions` (durability at risk),
`brahmaputra_group_lag` (consumers falling behind), and
`brahmaputra_produce_errors_total` (writes being refused).


### Browsing messages

The dashboard is not only a status page: it reads the log.

- **Browse** any topic's records — partition, offset, timestamp, key, value
  and size. Reads backwards from the high watermark by default, because an
  operator opening a busy topic wants the newest records and scanning from
  offset zero to reach them would be slow and pointless.
- **Filter** by substring across key and value, and order newest- or
  oldest-first.
- **Live tail** over server-sent events, so records appear as they are
  produced. This is a poll loop rather than a hook in the append path: the
  dashboard is an observer and must never be able to slow a producer down,
  so it reads on its own schedule and falls behind if it has to.
- **Administer** from the same page — increase a topic's partition count,
  change its configuration, delete it — and watch consumer-group lag.

Partitions only ever increase. Removing one would strand the records
already written to it and silently re-route a keyed producer, so a request
to shrink is refused rather than obeyed.

Payloads that are not valid UTF-8 are rendered lossily and *labelled* as
binary, rather than quietly shown as mojibake.

## Docker

The dashboard is compiled into the broker, so there is no separate UI
service, no Node build and no CDN at runtime — hosting the UI is just
running a node with an HTTP port.

```bash
docker compose -f docker/docker-compose.yml up --build
open http://localhost:8080
```

That brings up a three-node cluster with the dashboard on 8080 (and 8081,
8082 — every node serves it). Credentials come from the image environment,
so they are set where the container is defined:

```yaml
environment:
  BRAHMAPUTRA_ADMIN_USER: admin
  BRAHMAPUTRA_ADMIN_PASSWORD: change-me-please
```

A single container works too, and needs no peer list — a node given none
becomes its own one-member quorum, because the dashboard's users and
sessions live in controller metadata and a node with no controller has
nothing to log in against:

```bash
docker build -f docker/Dockerfile -t brahmaputra .
docker run -p 8080:8080 -p 9092:9092 \
  -e BRAHMAPUTRA_ADMIN_PASSWORD='choose-something-long' brahmaputra
```

Everything the broker takes as a flag is available as an environment
variable — `BRAHMAPUTRA_DEFAULT_PARTITIONS`, `BRAHMAPUTRA_RETENTION_MS`,
`BRAHMAPUTRA_SEGMENT_BYTES`, `BRAHMAPUTRA_TRANSPORT`,
`BRAHMAPUTRA_REQUIRE_AUTH`, and the rest — and anything unset simply omits
its flag rather than passing an empty value. Extra flags can be appended
after the image name.

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

**Compaction** applies where deleting by age would be wrong. A consumer
group rewrites the same key — its committed offset — forever, so the
internal `__consumer_offsets` topic is compacted rather than aged out: only
the newest record per key is kept. Without it, a cluster committing every
few seconds fills its disk, and coordinator failover slows without limit
because it replays every superseded commit.

Offsets are preserved exactly. A surviving record is rewritten as a
single-record batch at its original offset, so compaction leaves gaps
rather than renumbering anything, and a previously committed offset still
means the record it always meant. Only sealed segments below the high
watermark are eligible — the active segment is still being appended to, and
uncommitted records are not the broker's to discard — and records with no
key are never removed, having nothing that could supersede them. In a live
run the offsets topic plateaus at tens of kilobytes instead of growing with
the commit count.

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
| `--require-auth` | off | refuse unauthenticated connections and authorize every request against the ACLs |
| `--admin-user`, `--admin-password` | `admin` / generated | first admin, created on first boot; also `BRAHMAPUTRA_ADMIN_USER` / `BRAHMAPUTRA_ADMIN_PASSWORD` |

### Producer (`brahmaputra-cli produce`, `ProducerConfig`)

| Flag / field | Default | Kafka equivalent | Meaning |
|---|---|---|---|
| `--acks` | 1 | `acks` | `0` fire-and-forget, `1` leader append, `all` full ISR |
| `--batch-size` | 16 KiB | `batch.size` | flush a partition buffer once it holds this many bytes |
| `--linger-ms` | 5 | `linger.ms` | flush every non-empty buffer at least this often; `0` sends each record immediately |
| `--compression` | `lz4` | `compression.type` | `none` or `lz4` |
| `--max-in-flight` | 5 | `max.in.flight.requests.per.connection` | unacknowledged requests per connection; also the flush shard count |
| `--in-flight` | — | closest to `buffer.memory` | records the bulk modes keep outstanding |
| `--timeout-ms` | 30000 | `request.timeout.ms` | broker-side wait for `acks` |
| `--idempotent` | off | `enable.idempotence` | producer id + sequence; safe replay of an ambiguous send |
| `--key` | — | — | pins the record to `murmur2(key) % partitions`, as Kafka |
| `--partition` | — | — | explicit partition, bypassing the partitioner |
| `batch_partitions` | on | — | send all of a broker's partitions in one `ProduceMulti` |

Two behaviours are worth knowing before tuning.

**`--in-flight` must exceed `batch-size ÷ record size`.** It bounds how many
records may be outstanding, so if it is smaller than a batch, the buffer
can never reach `batch-size` and every flush waits out `linger-ms` instead.
At 256 B records with a 64 KiB batch, a window of 64 pins the producer at
roughly 4 000 msgs/sec no matter how fast the broker is; 4096 lets it batch
properly.

**Ordering under batching.** Partitions that share a broker travel in one
request, split into `max-in-flight` fixed shards. A partition always lands
in the same shard, so it still has at most one request outstanding —
which is what preserves per-key order — while the shards overlap on the
wire.

### Consumer (`brahmaputra-cli consume`, `Consumer`, `GroupConsumer`)

| Flag / field | Default | Kafka equivalent | Meaning |
|---|---|---|---|
| `--from` | `earliest` | `auto.offset.reset` | `earliest` or `latest` start position |
| `--offset` | — | — | explicit start offset, overriding `--from` |
| `--partition` | all | — | read one partition instead of every partition |
| `--max` | — | — | stop after this many records |
| `--follow` | off | — | keep long-polling for new records |
| `--group` | — | `group.id` | join a consumer group instead of reading standalone |
| `--commit-interval-ms` | 5000 | `auto.commit.interval.ms` | `0` disables auto-commit |
| `--assignor` | `range` | `partition.assignment.strategy` | `range` or `roundrobin` |
| `max_poll_records` | 500 | `max.poll.records` | records returned per `poll`; the rest stay buffered and uncommitted |
| `session_timeout_ms` | 10000 | `session.timeout.ms` | coordinator evicts a silent member after this |
| `rebalance_timeout_ms` | 3000 | `max.poll.interval.ms` | how long the coordinator waits for members to rejoin |
| `max_bytes` | 8 MiB | `fetch.max.bytes` | response cap, split across the partitions in one request |
| `min_bytes` | 1 | `fetch.min.bytes` | return early once this many bytes are ready |
| `max_wait_ms` | 500 | `fetch.max.wait.ms` | long-poll ceiling when caught up |

A full config-by-config comparison against Kafka, including every knob that
is missing or inert, is in [docs/kafka-parity.md](docs/kafka-parity.md).

### Topic configuration

Set at creation with `brahmaputra-cli topic create --config K=V`, stored in
the Raft metadata and returned in metadata responses. **Only
`min.insync.replicas` currently changes broker behaviour**; `retention.ms`,
`retention.bytes`, `segment.bytes`, `cleanup.policy`, `max.message.bytes`
and `compression.type` are accepted and stored but not yet applied per
topic — set them broker-wide with the flags above. There is no
`AlterConfigs` equivalent, so topic configs are fixed at creation.

## Verification

```bash
cargo test --workspace          # 230 unit and integration tests

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
| `cargo test --workspace` | 230 | pass |
| `verify-m1.sh` — storage, protocol, concurrent producers, SIGKILL recovery | 31 | pass |
| `verify-m4.sh` — consumer groups across 5 nodes | 30 | pass |
| `verify-m5.sh` — fsync policies, quotas, version negotiation | 15 | pass |
| `verify-m6.sh` — metrics, login, RBAC, dashboard | 30 | pass |
| `verify-replication.sh` — ISR, failover, resync | 14 | pass |
| `verify-retention.sh` — time and size retention, group resume | 21 | pass |
| `verify-failures.sh` — producer/broker/consumer kills | 15 | pass |
| `verify-transport-parity.sh` — tcp vs tcp-tls vs quic | 18 | pass |
| `verify-chaos.sh` — 5 nodes, random kills under load | 7 | pass |
| `authentication` tests — anonymous, wrong password, ACL denial | 6 | pass |

`verify-chaos.sh` is the roughest of these: it kills and restarts brokers
in a five-node cluster while producing continuously with `acks=all`, then
asserts the only invariant that must hold regardless of the order events
happened in — every acknowledged record still readable, exactly once, from
every surviving replica, with the replicas byte-identical.

It used to exit early on every run, which looked like a broker fault for a
long time. It was the harness: `writer="$(… | head -1)"` under
`set -o pipefail` lets `head` exit as soon as it has its line, the upstream
loop dies of `SIGPIPE`, and `set -e` then aborted the whole run **silently**
— no failed assertion, no message. It now completes reliably (three
consecutive runs, 7/7 checks each).

**On load sensitivity.** `verify-m4.sh` failed once at *"controllers did not
agree on a live Raft leader"* while seven other suites were running on the
same host, and passes 30/30 in isolation. Its election deadline is already
120 seconds, so this is recorded as contention on a busy machine rather
than papered over with a larger timeout. Run the live suites serially.

## Performance

Head-to-head with Apache Kafka 4.3.1 in the configuration a durable
deployment actually runs: **three brokers each, RF=3, `acks=all`,
`min.insync.replicas=2`**. Same host, same container limits (4 CPUs and
4 GiB per broker, so 1200 % is the CPU ceiling for a three-node cluster),
same record size and partition count, each system driven by its own
clients from inside its own containers.

Means of three consecutive runs, 8 000 000 × 256 B records across
4 concurrent clients and 6 partitions.

```
RF=3 · acks=all · min.insync.replicas=2 · 256 B records

produce   Kafka         ████████                          231 454 msgs/sec
          Brahmaputra   ████████████████████████████      787 385  (3.40×)

consume   Kafka         ██████████████                  1 413 122 msgs/sec
          Brahmaputra   ██████████████████████████████  2 977 061  (2.11×)

memory    Kafka         ████████████████████████████        4 464 MiB
          Brahmaputra   ██████                                999 MiB  (4.5× less)
```

| Metric | Kafka | Brahmaputra |
|---|---|---|
| **Produce msgs/sec** (RF=3, `acks=all`) | 231 454 | **787 385** (3.40×) |
| Produce msgs/sec, client-measured | 257 503 | **838 655** (3.26×) |
| Produce cluster CPU % (ceiling 1200) | 940 | **828** |
| Produce cluster memory MiB | 4 464 | **999** |
| **Consume msgs/sec** (RF=3) | 1 413 122 | **2 977 061** (2.11×) |
| Consume msgs/sec, client-measured | 3 159 988 | **3 712 925** (1.17×) |

Consume is close, and honestly so: wall clock favours Brahmaputra because
it charges Kafka roughly two seconds of JVM startup, while each client's
own reported rate puts them within 17 %.

### What replication costs

The number that decides whether a design replicates cheaply. Each ratio is
computed *within* one system, using the same client and the same metric.

| System | RF=1 `acks=1` | RF=3 `acks=all` | Kept | Cost |
|---|---|---|---|---|
| Kafka | 735 668 | 231 454 | 31 % | 3.18× |
| **Brahmaputra** | 1 157 006 | **787 385** | **68 %** | **1.47×** |

Brahmaputra keeps more than twice the share of its unreplicated throughput
that Kafka keeps. Across three runs the two cost ranges — 3.04–3.31× and
1.39–1.59× — do not overlap.

### What made it fast

Two defects found by building this benchmark, both fixed in 0.2.0.

| Problem | Fix | Effect |
|---|---|---|
| A follower learned about new appends only by asking again, and slept 50 ms between empty answers. Under `acks=all` the high watermark cannot advance until followers have fetched, so **every producer waited out that sleep before its record could commit** — the cluster ran at the polling interval, not at the speed of the log | Leaders hold a caught-up follower's fetch until an append arrives or 500 ms passes, waiting on a log-end-offset watch rather than the high watermark (which cannot advance until this follower fetches — waiting on it would be waiting on itself) | RF=3 produce **106 077 → 787 385** msgs/sec; replication cost 13.35× → **1.47×** |
| The client resolved the broker hostname on **every send** and never cached it — a measured 6 418 `lookup_host` calls to produce 2 000 records. Only name-advertised clusters paid it, which is every Kubernetes or Compose deployment | Cache resolved addresses on the router with a 30 s TTL, dropped when the connection to that address is invalidated so a broker returning at a new IP is re-resolved at once | 400 000 records to a name-advertised cluster: **6.22 s → 1.45 s** |

Memory is the most stable difference and it is structural rather than
tuning: the JVM holds its heap and copies records through it, while the
Rust broker passes refcounted `Bytes` slices and leans on the page cache.

The earlier read- and write-path work that got the single-node numbers
here — removing four copies from the fetch path, `sendfile`, multi-partition
requests, concurrent request dispatch — is documented with its own
measurements in [docs/benchmarks.md](docs/benchmarks.md).

### Benchmark method

| | |
|---|---|
| Host | AMD Ryzen AI MAX+ 395, 32 logical CPUs, 64 GB RAM |
| OS | Windows 11 26200, Docker Desktop 29.6.2, WSL2 |
| Per container | `--cpus 4 --memory 4g`, overlayfs on the WSL2 virtual disk |
| Cluster | 3 brokers per system; Kafka `apache/kafka:4.3.1` in KRaft mode |
| Workload | 2 000 000 × 256 B records per client, 4 clients, 6 partitions |
| Producer | `batch.size` 64 KiB, `linger.ms` 5, no compression |

Load generators run inside the broker containers, spread round-robin
across all three, so sampled CPU and memory cover broker *plus* client for
both systems. Both clusters advertise static IPs, and **both consumers
join a consumer group** — `kafka-consumer-perf-test` always does, and
comparing it against an uncoordinated reader overstates the other side by
about 3×.

Every RF=3 level asserts it actually replicated: Kafka must report ISR=3 on
all six partitions, and Brahmaputra must hold partition logs on all three
nodes with log end offsets summing exactly to the records produced.

Reproduce:

```bash
PER_CLIENT=2000000 LEVELS=4 bash scripts/bench-replicated-vs-kafka.sh
bash scripts/bench-replicated.sh    # native, no Docker: RF=3 against RF=1
```

**Caveats.** Three brokers per system share one host's disk and NIC, so
absolute numbers sit below real hardware — for both systems equally, which
is what preserves the ratios. Kafka gains up to 40 % on longer runs from
JIT warmup and was still climbing when measurement stopped, so its side of
every ratio is a floor. Throughput only; no latency percentiles. Full
report, including two findings this benchmark surfaced that are not fixed,
in [docs/replicated-benchmark-2026-08-22.md](docs/replicated-benchmark-2026-08-22.md).

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

### On disk

```
data/
  meta.toml                              node id, cluster id, listeners
  orders-0/                              one directory per topic-partition
    00000000000000000000.log             record batches, named by base offset
    00000000000000000000.index           sparse offset -> file position
    00000000000000000000.timeindex       sparse timestamp -> offset
    00000000000000004096.log             the next segment, and so on
    hwm                                  high-watermark checkpoint
    leader-epoch-checkpoint              epoch -> first offset, for truncation
```

Segment names are the base offset zero-padded to 20 digits, so a directory
listing is in offset order. Both indices are *sparse* — one entry per
`index.interval.bytes` of log — so a lookup binary-searches the index and
then scans forward a bounded amount, which is what keeps them small enough
to stay in the page cache. All of it is the same layout Kafka uses, and the
`.log` files hold exactly the bytes the producer sent.

Recovery on startup reads the active segment's tail, validating each
batch's CRC, and truncates at the first incomplete or corrupt record — so a
`kill -9` costs at most the un-fsynced tail, never the whole segment.

Deeper dives, one per subsystem, are in
[docs/blueprint/](docs/blueprint/README.md); the design rationale and the
alternatives considered are in [DESIGN.md](DESIGN.md).

## Repository layout

```
DESIGN.md                   architecture and design decisions
LICENSE, NOTICE             Apache 2.0
docs/blueprint/             per-subsystem internals
docs/kafka-parity.md        config-by-config audit against Kafka
docs/benchmarks.md          method, machine, results, and the fixes they drove
schemas/protocol.buff       BitPacker wire schemas (data plane)
scripts/                    live verification and benchmark harnesses
bench/                      Dockerfile and results for the Kafka comparison
.github/workflows/ci.yml    tests plus the live suites on every PR
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

Needs a stable Rust toolchain (edition 2024, so 1.85 or newer; CI builds on
`rust:1-bookworm`). Nothing else — no JVM, no ZooKeeper, no system
libraries beyond libc.

```bash
cargo build --release          # brahmaputra-server and brahmaputra-cli
cargo test --workspace         # 230 unit and integration tests
```

The live verification scripts additionally need `bash`; they run on Git
Bash on Windows as well as on Unix. The benchmark harnesses need Docker,
because they run Kafka and Brahmaputra under identical container limits.

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
log compaction, quotas, fsync policies, API version negotiation, TLS,
data-plane authentication with ACLs, metrics and RBAC.

Deliberately **not** in v1 (DESIGN.md §1): transactions and exactly-once
semantics, multi-datacentre replication, tiered storage.

Known gaps, ranked, in [docs/kafka-parity.md](docs/kafka-parity.md) §8.
The ones that matter most:

1. **No Kafka wire-protocol compatibility.** Existing Kafka clients,
   Connect, Streams and the surrounding ecosystem do not work against it;
   this speaks its own protocol.
2. **No transactions or exactly-once semantics.** A read-process-write
   pipeline cannot be built on it. Delivery is at-least-once, which plenty
   of production Kafka also runs on, but it is a real ceiling on which
   workloads qualify.
3. **`sendfile` is Linux and plaintext-TCP only.** TLS and QUIC must see
   the bytes to encrypt them, so those paths keep one copy out of the page
   cache — Kafka has the same limitation whenever SSL is enabled. On
   non-Linux platforms the fallback reads the range and writes it: correct
   everywhere, just not free. That fallback is what every test on the
   Windows development host exercises.
4. **No JBOD.** One data directory per broker, so a single disk failure
   takes the whole broker rather than the partitions on that disk.
5. **No soak history.** The failure suites kill brokers under load and
   assert what survived, but they run for minutes. Nothing here has been
   run for a week.

On production readiness: with `--require-auth` and TLS this is no longer
open to anyone who can reach the port, and the offsets topic no longer
grows without bound. That is a real change in what can responsibly be run.
It is still young software with no production track record, and the last
two gaps above are the ones to close before trusting it with data you
cannot lose.

## License

Copyright 2026 the Brahmaputra authors.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy
of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.

Full text in [LICENSE](LICENSE); attribution notices in [NOTICE](NOTICE).
Apache Kafka is a trademark of the Apache Software Foundation; this project
is not affiliated with or endorsed by the ASF, and references to Kafka
describe compatibility of model and behaviour only.
