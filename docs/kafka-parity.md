# Kafka Parity Audit

Where Brahmaputra stands against Apache Kafka (3.x/4.x) as of the end of
M4 — every row checked against the code, not against DESIGN.md's
intentions. Kafka defaults are quoted for context; the benchmark harness
pins the exact Kafka version it measures.

Legend: **✅ present** · **🟡 partial** · **❌ missing**

---

## 1. Verdict

Brahmaputra implements the *core* Kafka model faithfully: partitioned
append-only segmented logs, leader/ISR replication with leader-epoch
truncation, high-watermark visibility, `acks=0/1/all`, an idempotent
producer, a Raft (KRaft-style) control plane, and consumer groups with
generation fencing and offsets in `__consumer_offsets`.

M5 added fsync policies, per-client quotas, TLS on the TCP path, and
`ApiVersions` negotiation, so the list below is what remains.

M6 added multi-partition Produce and Fetch, which closes the request-shape
gap that dominated small-record throughput, plus concurrent request
handling on the broker.

It is *not* yet Kafka-equivalent in two areas that matter for the
head-to-head benchmark and for production semantics:

1. **No `sendfile` on the fetch path.** Every fetch makes *two* userspace
   copies of each byte served: `pread` into a `BytesMut`
   (`crates/storage/src/log.rs`), then a second copy while assembling the
   response frame (`encode_fetch_response`), before the kernel copies it
   again into the socket. Kafka's `transferTo` makes none. This is the
   single biggest expected benchmark gap on consume — and the second copy
   is removable today with vectored writes, independently of `sendfile`.
2. **Topic-level configs are stored but mostly ignored.** Only
   `min.insync.replicas` is read. `retention.ms`, `retention.bytes`,
   `segment.bytes` are broker-wide CLI flags, not per topic.

Everything else is either present, deliberately out of scope for v1
(transactions, compaction, quotas, tiered storage — DESIGN.md §1), or a
small, well-understood gap listed below.

---

## 2. Wire protocol / APIs

Data plane is a custom length-prefixed binary protocol with BitPacker
bodies (`schemas/protocol.buff`); record batches are a hand-rolled codec.
Control plane is HTTP+JSON on the controller (DESIGN.md §6 anticipated
gRPC; the code uses axum, no tonic/prost dependency exists).

| Kafka API | Brahmaputra | Notes |
|---|---|---|
| Produce | ✅ key 0 | single topic-partition per request |
| ProduceMulti | ✅ key 15 | many topic-partitions in one request; the default client path |
| Fetch | ✅ key 1 | single topic-partition; long poll, `min_bytes`/`max_wait_ms` |
| FetchMulti | ✅ key 16 | many topic-partitions in one request, read concurrently; the default client path |
| ListOffsets | ✅ key 2 | earliest / latest / by-timestamp (linear scan, timeindex unused) |
| Metadata | ✅ key 3 | brokers, leaders, ISR, leader epoch |
| OffsetForLeaderEpoch | ✅ key 5 | KIP-101 truncation |
| InitProducerId | ✅ key 6 | idempotent producer |
| JoinGroup / SyncGroup / Heartbeat | ✅ keys 7–9 | eager rebalance, generation fencing |
| OffsetCommit / OffsetFetch | ✅ keys 10–11 | |
| ListGroups / DescribeGroups | ✅ keys 12–13 | added in M4 (see Blueprint 05 §6) |
| LeaderAndIsr / UpdateMetadata | 🟡 | equivalent effect via Raft metadata subscription + `ChangePartition` |
| ApiVersions | ✅ key 14 | answered at any requested version, so a client can negotiate before it commits to one |
| DeleteRecords, AlterConfigs, DescribeConfigs, DescribeCluster, DescribeLogDirs | ❌ | topic create/delete only, via controller HTTP |
| AddPartitionsToTxn / EndTxn / TxnOffsetCommit | ❌ | transactions are a v1 non-goal |
| SASL handshake/authenticate | ❌ | no auth on the data plane |
| LeaveGroup | ❌ | members leave by session timeout only — a clean shutdown costs one session-timeout rebalance delay |

## 3. Broker configuration

Flags on `brahmaputra-server` (`crates/server/src/main.rs`) plus
`BrokerConfig`/`LogConfig` defaults.

| Kafka config | Kafka default | Brahmaputra | Status |
|---|---|---|---|
| `broker.id` | — | `--node-id` (cluster) / 0 | ✅ |
| `listeners` / `advertised.listeners` | — | `--host` + `--port` (single listener) | 🟡 no multi-listener, no listener security protocol map |
| `log.dirs` | `/tmp/kafka-logs` | `--data-dir` (one dir) | 🟡 no multi-log-dir / JBOD |
| `num.partitions` | 1 | `--default-partitions` (1) | ✅ |
| `log.segment.bytes` | 1 GiB | `--segment-bytes` (64 MiB) | ✅ broker-wide only |
| `log.roll.ms` / `segment.ms` | 7 days | — | ❌ segments roll on size only |
| `log.retention.ms` | 7 days | `--retention-ms` (off) | 🟡 broker-wide; no per-topic override |
| `log.retention.bytes` | -1 | `--retention-bytes` (off) | 🟡 broker-wide; no per-topic override |
| `log.retention.check.interval.ms` | 5 min | `--retention-check-interval-ms` (1 s) | ✅ |
| `log.index.interval.bytes` | 4096 | `LogConfig.index_interval_bytes` | 🟡 not exposed as a flag |
| `log.cleanup.policy` | delete | delete | ❌ `compact` not implemented |
| `log.flush.interval.messages` | ~never (OS) | `--flush-interval-messages` | ✅ |
| `log.flush.interval.ms` | ~never (OS) | `--flush-interval-ms` | ✅ |
| `min.insync.replicas` | 1 | topic config `min.insync.replicas` | ✅ honored on `acks=all` |
| `default.replication.factor` | 1 | explicit `--replication-factor` on create | 🟡 no default |
| `unclean.leader.election.enable` | false | always false (ISR-only election) | ✅ matches the safe default |
| `replica.lag.time.max.ms` | 30 s | `--replica-lag-time-max-ms` | ✅ |
| `offsets.topic.num.partitions` | 50 | `--offsets-topic-partitions` (50) | ✅ |
| `offsets.topic.replication.factor` | 3 | derived from cluster size at creation | 🟡 not configurable |
| `offsets.retention.minutes` | 7 days | — | ❌ no offset expiry sweeper (record format supports tombstones) |
| `group.initial.rebalance.delay.ms` | 3 s | — | ❌ every join starts collecting immediately |
| `group.min/max.session.timeout.ms` | 6 s / 30 min | — | ❌ client-supplied timeout is accepted unbounded |
| `num.network.threads` / `num.io.threads` | 3 / 8 | tokio multi-threaded runtime + per-partition actor | ✅ different model, same effect |
| `socket.request.max.bytes` | 100 MiB | `max_frame_bytes` (32 MiB) | 🟡 not exposed as a flag |
| `queued.max.requests` | 500 | `channel_capacity` (1024/partition) | ✅ bounded, backpressured |
| `broker.rack` | — | `--rack` registered in metadata | 🟡 not used for replica placement |
| `auto.create.topics.enable` | true | standalone: on; cluster: off | ✅ |
| `delete.topic.enable` | true | ✅ via controller | ✅ |
| `quota.producer.default` | — | `--quota-produce-bytes-per-sec` | ✅ per client id, throttles by delaying |
| `quota.consumer.default` | — | `--quota-fetch-bytes-per-sec` | ✅ per client id, separate budget |
| `replication.quota.*` | — | — | ❌ replication traffic is unthrottled |
| `ssl.*` | — | `--transport tcp-tls` or `quic` (TLS 1.3) | 🟡 encryption yes; self-signed, no client certs |
| `sasl.*` | — | — | ❌ no authentication; needs the M6 user store |

## 4. Topic configuration

Topic configs are accepted by `brahmaputra-cli topic create --config
K=V`, replicated in the Raft metadata log, and returned in metadata —
but **only `min.insync.replicas` changes broker behavior**
(`crates/broker/src/handlers.rs:634`). `retention.ms`,
`retention.bytes`, `segment.bytes`, `cleanup.policy`,
`max.message.bytes`, `compression.type` are inert today.

There is also no `AlterConfigs` equivalent: configs are fixed at topic
creation.

## 5. Producer configuration

`ProducerConfig` (`crates/client/src/producer.rs`).

| Kafka config | Kafka default | Brahmaputra | Status |
|---|---|---|---|
| `client.id` | "" | `client_id` | ✅ |
| `acks` | all | `acks` (1) | ✅ 0/1/all supported; default differs |
| `batch.size` | 16384 | `batch_size` (16384) | ✅ |
| `linger.ms` | 0 | `linger_ms` (5) | ✅ default differs |
| `compression.type` | none | `compression` (**lz4**) | 🟡 only none/lz4; no gzip/snappy/zstd |
| `max.in.flight.requests.per.connection` | 5 | `max_in_flight` (5) | ✅ |
| transport | TCP only | `--transport tcp|quic` | ✅ beyond parity (§11) |
| `enable.idempotence` | true | `idempotence` (false) | ✅ supported; default differs |
| `request.timeout.ms` | 30 s | `timeout_ms` (30 s) | ✅ (broker-side ack wait) |
| partitioner | murmur2(key) % n, else sticky | murmur2(key) % n, else round-robin | ✅ **fixed in this audit** (was round-robin even for keyed records) |
| `buffer.memory` / `max.block.ms` | 32 MiB / 60 s | — | ❌ unbounded client-side buffering |
| `retries` / `retry.backoff.ms` / `delivery.timeout.ms` | ∞ / 100 ms / 2 min | — | ❌ no automatic retry except the idempotent replay of one ambiguous send |
| `max.request.size` | 1 MiB | — | ❌ (broker caps the frame at 32 MiB) |
| `transactional.id` | — | — | ❌ v1 non-goal |
| record headers | supported | — | ❌ not in the record format |
| record timestamps | CreateTime/LogAppendTime | batch `max_timestamp` only, per-record delta always 0 | 🟡 no per-record create time, no timestamp type config |

## 6. Consumer configuration

`Consumer` + `GroupConsumer` (`crates/client/src/consumer.rs`,
`group_consumer.rs`).

| Kafka config | Kafka default | Brahmaputra | Status |
|---|---|---|---|
| `group.id` | — | `GroupConsumer::connect(.., group)` | ✅ |
| `partition.assignment.strategy` | range,cooperative-sticky | `Assignor::Range` / `RoundRobin` | 🟡 no sticky / cooperative |
| `enable.auto.commit` | true | `with_auto_commit(Some/None)` | ✅ |
| `auto.commit.interval.ms` | 5000 | 5000 | ✅ |
| `max.poll.records` | 500 | 500 | ✅ **added in this audit** |
| `session.timeout.ms` | 45 s | 10 s | ✅ configurable |
| `heartbeat.interval.ms` | 3 s | derived: session/3 | 🟡 not independently configurable |
| `max.poll.interval.ms` | 5 min | — | ❌ no liveness separation between processing and heartbeating |
| `fetch.min.bytes` | 1 | 1 | ✅ |
| `fetch.max.wait.ms` | 500 | 500 | ✅ |
| `max.partition.fetch.bytes` | 1 MiB | `max_bytes` (8 MiB, per fetch) | 🟡 one knob, per-partition |
| `fetch.max.bytes` | 50 MiB | — | ❌ no request-level cap (one partition per request anyway) |
| `auto.offset.reset` | latest | always earliest when no commit | 🟡 not configurable (`NoOffsetForPartition` error type exists, unused) |
| `isolation.level` | read_uncommitted | HW-bounded by construction | ✅ equivalent for a non-transactional broker |
| `group.instance.id` (static membership) | — | — | ❌ |
| `client.rack` / follower fetching | — | — | ❌ consumers always read the leader |
| `check.crcs` | true | always verified on read from disk | ✅ |
| `exclude.internal.topics` | true | `__consumer_offsets` is visible in metadata | 🟡 cosmetic |

## 7. Architecture & behavior

| Kafka property | Brahmaputra | Status |
|---|---|---|
| Append-only segmented log per partition | ✅ `.log` + `.index` + `.timeindex` | ✅ |
| Sparse offset index, binary search + scan | ✅ in-memory mirror of an on-disk index | ✅ (Kafka mmaps; here it's read into memory) |
| CRC per batch, verified on read | ✅ | ✅ |
| Batches stored and replicated byte-identically | ✅ verified in M3 tests | ✅ |
| Page-cache-centric, sequential I/O, no user read cache | ✅ | ✅ |
| **`sendfile` for fetch responses** | ❌ `pread` into `BytesMut`, then write | ❌ **primary perf gap** |
| Zero-copy *within* the process (`Bytes` refcounts, no re-encode) | ✅ | ✅ |
| Leader/follower replication, ISR, HW | ✅ | ✅ |
| Leader-epoch truncation (KIP-101) | ✅ verified in M3 live runs | ✅ |
| Controller-driven election from ISR, epoch fencing | ✅ Raft metadata log | ✅ |
| Broker registration/heartbeat/fencing | ✅ | ✅ |
| Consumer groups: coordinator on `__consumer_offsets`, generations | ✅ | ✅ |
| Rebalance protocol | 🟡 eager only (stop-the-world) | 🟡 |
| Offsets survive coordinator failover by log replay | ✅ verified in M4 live runs | ✅ |
| Log compaction | ❌ | v1 non-goal |
| Transactions / EOS | ❌ | v1 non-goal |
| Quotas / throttling | ❌ | v1 non-goal |
| TLS / SASL / ACLs | ❌ | M5–M6 |
| Tiered storage | ❌ | v1 non-goal |
| Metrics endpoint / dashboard | ❌ | M6 |

## 8. Gaps ranked by impact

**Performance (will show in the benchmark)**

1. No `sendfile`/`splice` on fetch — two extra copies per byte served.
2. No `zstd`/`snappy`/`gzip`; `lz4` only.
3. Fetch responses do not use a fetch session / incremental fetch
   (KIP-227), so every fetch re-sends full request metadata.

**Semantics**

5. Topic configs other than `min.insync.replicas` are inert — a user who
   sets `retention.ms` on a topic gets silence, not an error.
6. No `max.poll.interval.ms`, no static membership, no cooperative
   rebalancing ⇒ rebalance storms are mitigated only by timeouts.
7. No `LeaveGroup`: a clean consumer shutdown still costs a
   session-timeout before its partitions move.
8. No `auto.offset.reset` policy (always earliest).
9. No offset expiry (`offsets.retention.ms`).
10. No per-record timestamps (all records in a batch share
    `max_timestamp`), so timestamp-based seeks are batch-granular.

**Operational**

11. No authentication of any kind: TLS gives confidentiality, but any
    client that can reach the port can read and write anything. The user
    store this needs is M6 work (DESIGN.md §9.4).
12. `index.interval.bytes` and `max_frame_bytes` exist in code but have no
    CLI flag.
13. No multi-log-dir/JBOD, no rack-aware replica placement.
14. Replication traffic is not quota-limited, so a catching-up follower
    can still crowd out client traffic.

## 9. Fixed while auditing

- **Keyed records now hash to a partition** (`murmur2(key) % n`, Kafka's
  algorithm, cross-checked against an independent transcription of
  `Utils.murmur2`). Previously every keyless *and keyed* record went
  round-robin, so per-key ordering — a core Kafka guarantee — did not
  hold.
- **`max.poll.records` (default 500)** with a proper split between fetch
  position and consumed position. Previously a bounded reader
  (`consume --group g --max N`) committed every record it had *fetched*,
  silently skipping records it never delivered.
- **`ListOffsets(latest)` now returns the high watermark**, not the
  leader's log end offset, so "latest" is an offset a consumer can
  actually reach and consumer lag is not overstated during replication
  lag.
- **Producer batching collapse under concurrency.** Every `send()` that
  found its partition buffer at `batch.size` queued its own flush behind
  the partition's send lock. Each queued flusher then drained whatever
  little was left and paid a full round trip for it, so raising client
  concurrency *lowered* throughput: 512 concurrent records gave 44k
  msgs/sec, 4096 gave 5k. A size-triggered flush is now skipped when a
  flush is already running (that flusher drains until the buffer is
  empty, so nothing is lost, and the linger ticker is the backstop).
  Same configuration after the fix: **5,162 → 61,187 msgs/sec, 11.9×**.
  Kafka avoids this shape entirely by never doing I/O on the calling
  thread — a dedicated sender thread owns batching.
- **Multi-partition Produce and Fetch** (`ProduceMulti`, api_key 15;
  `FetchMulti`, api_key 16). A client that holds six partitions on one
  broker previously sent six requests and the group consumer fetched its
  partitions one after another; both now travel in a single request, so
  the per-request cost is paid once instead of per partition. That cost
  dominates at small records, which is exactly where the gap against
  Kafka was. Raw record batches still trail the encoded struct as opaque
  bytes, so the zero-copy property is unchanged.
- **Concurrency, at three points that each serialised the whole
  pipeline.** The broker handled one request at a time per connection, so
  a client's in-flight window bought nothing and one slow request (a long
  poll, an `acks=all` wait) blocked every request behind it; requests are
  now dispatched concurrently behind a bounded in-flight semaphore, which
  is safe because correlation ids already permit out-of-order responses
  and each partition remains a single-writer actor. Within one batched
  request the broker also appended to — and read from — its partitions in
  sequence, turning six concurrent round trips into one serial one; those
  are now concurrent. On the client, a batched flush held every
  partition's send lock across the round trip, allowing one request in
  flight per broker; partitions are now split into `max.in.flight` fixed
  shards, so a partition still has at most one request outstanding while
  the shards overlap.
- **Config knobs that existed only in code are now on the CLI**:
  `--retention-bytes` and `--retention-check-interval-ms` on the server;
  `--batch-size`, `--linger-ms`, `--compression` on `produce`. Without
  them, size retention and producer batching could not be exercised or
  benchmarked at all.

## 10. Verification scripts

| Script | Covers |
|---|---|
| `scripts/verify-m1.sh` … `verify-m4.sh` | per-milestone live checks |
| `scripts/verify-retention.sh` | time + size retention, restart survival, a group whose committed offset falls off the log, retention off by default (21 checks) |
| `scripts/verify-replication.sh` | RF=3 byte-identical replicas, ISR failover without loss, restarted broker resync, `min.insync.replicas` enforcement and recovery (14 checks) |
| `scripts/verify-failures.sh` | producer killed mid-send, broker kill -9 mid-produce with torn-tail recovery, consumer killed mid-stream with group resume, and a data-loss sweep across all three (15 checks, runs under either transport) |
| `scripts/verify-transport-parity.sh` | the same correctness checks over TCP and QUIC, side by side (6 checks x 2) |
| `scripts/bench-vs-kafka.sh` | head-to-head throughput against Apache Kafka in Docker |

Note on `verify-m3.sh`: it is timing-sensitive on a loaded workstation
(64 concurrent producer processes, 5 s session timeouts) and fails
intermittently at different assertions there. `verify-replication.sh`
covers the same invariants deterministically and is the better routine
check; `verify-m3.sh` remains the exhaustive one.

## 11. Transport option (TCP / QUIC)

The data plane runs over either transport, selected with `--transport` on
the broker and the client. Kafka offers no equivalent — it is TCP-only —
so this is a capability beyond parity rather than a gap in it.

| | Brahmaputra TCP | Brahmaputra QUIC | Kafka |
|---|---|---|---|
| Multiplexing | one byte stream + correlation ids | one bidirectional stream per request | one byte stream + correlation ids |
| Head-of-line blocking between requests | yes | no | yes |
| Encryption | none yet (M5) | TLS 1.3, mandatory | optional TLS |
| Covers replication traffic | yes | yes | yes |
| Throughput at 1 MiB records | 309 MB/s | 113 MB/s | 187 MB/s |

Behavioural parity between the two transports is enforced by
`scripts/verify-transport-parity.sh` (accuracy, durability across
restart, per-key ordering, idempotent retry, zero-copy bytes on disk,
consumer groups) and by running the full replication suite under
`TRANSPORT=quic`. See [benchmarks.md](benchmarks.md) §4 for why QUIC
costs more CPU.
