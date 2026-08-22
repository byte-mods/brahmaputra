# Kafka Parity Audit

Where Brahmaputra stands against Apache Kafka (3.x/4.x), checked against
the code rather than against DESIGN.md's intentions. Kafka defaults are
quoted for context; the benchmark harness pins the exact Kafka version it
measures.

Legend: **✅ present** · **🟡 partial** · **❌ missing**

---

## 1. Verdict

Brahmaputra implements the *core* Kafka model faithfully: partitioned
append-only segmented logs, leader/ISR replication with leader-epoch
truncation, high-watermark visibility, `acks=0/1/all`, an idempotent
producer, a Raft (KRaft-style) control plane, consumer groups with
generation fencing and offsets in `__consumer_offsets`, log compaction,
retention, quotas, TLS, authentication and ACLs.

The control plane can now also *reshape* a cluster: partitions move
between brokers, placement is rack-aware, and topic-configuration changes
reach running partitions. That is what decides whether a cluster can be
operated for years rather than merely started once, and it was the
largest non-protocol gap.

The one gap that dominates every other consideration is **§2: this speaks
its own wire protocol, not Kafka's**. No Kafka client, Connect, Streams,
Schema Registry, MirrorMaker or ecosystem tool works against it. That is
not a feature checklist item; it decides whether "replace Kafka" is even
the right verb.

Beyond that, the remaining gaps are ranked in §9.

---

## 2. Wire protocol and clients

The data plane is a custom length-prefixed binary protocol with BitPacker
bodies (`schemas/protocol.buff`); record batches are a hand-rolled codec.
The control plane is HTTP+JSON on the controller.

**Nothing in Kafka's ecosystem speaks this.** In exchange, native clients
are cheap to write, and four now exist ([clients/](../clients)):

| Language | State |
|---|---|
| Go | verified against a live broker, 34/34 |
| Node.js | verified against a live broker, 34/34 |
| Python | written, never executed (no interpreter on the build host) |
| Java | written, never compiled (no JDK on the build host) |

They cover producer, consumer and group APIs including sticky assignment,
static membership and `auto.offset.reset`. They do **not** cover TLS, QUIC
or the idempotent producer.

| Kafka API | Brahmaputra | Notes |
|---|---|---|
| Produce | ✅ key 0 | single topic-partition |
| ProduceMulti | ✅ key 15 | many partitions per request; the default client path |
| Fetch | ✅ key 1 | long poll, `min_bytes`/`max_wait_ms` |
| FetchMulti | ✅ key 16 | many partitions, read concurrently; the default client path |
| ListOffsets | ✅ key 2 | earliest / latest / by-timestamp (linear scan; timeindex unused) |
| Metadata | ✅ key 3 | brokers, leaders, ISR, leader epoch |
| OffsetForLeaderEpoch | ✅ key 5 | KIP-101 truncation |
| InitProducerId | ✅ key 6 | idempotent producer |
| JoinGroup / SyncGroup / Heartbeat | ✅ keys 7–9 | eager rebalance, generation fencing, static membership |
| OffsetCommit / OffsetFetch | ✅ keys 10–11 | |
| ListGroups / DescribeGroups | ✅ keys 12–13 | |
| ApiVersions | ✅ key 14 | answered at any requested version |
| Authenticate | ✅ key 17 | SASL/PLAIN-equivalent; refused on a plaintext listener |
| **LeaveGroup** | ✅ key 18 | **added since the last audit** |
| LeaderAndIsr / UpdateMetadata | 🟡 | equivalent effect via Raft metadata subscription |
| DeleteRecords, AlterConfigs, DescribeConfigs, DescribeCluster, DescribeLogDirs | ❌ | topic create/delete only, via controller HTTP |
| AddPartitionsToTxn / EndTxn / TxnOffsetCommit | ❌ | transactions are a v1 non-goal |

The wire version is **2**. The broker requires an exact match, so a
version-1 client gets `UNSUPPORTED_VERSION` rather than misparsing.

## 3. Broker configuration

| Kafka config | Kafka default | Brahmaputra | Status |
|---|---|---|---|
| `broker.id` | — | `--node-id` | ✅ |
| `listeners` / `advertised.listeners` | — | `--host` + `--port` | 🟡 single listener, no security protocol map |
| `log.dirs` | `/tmp/kafka-logs` | `--data-dir` | 🟡 no multi-dir / JBOD |
| `num.partitions` | 1 | `--default-partitions` | ✅ |
| `log.segment.bytes` | 1 GiB | `--segment-bytes`, **per topic** | ✅ |
| `log.roll.ms` / `segment.ms` | 7 days | **per topic `segment.ms`** | ✅ *new* |
| `log.retention.ms` | 7 days | `--retention-ms`, **per topic** | ✅ *now applied* |
| `log.retention.bytes` | -1 | `--retention-bytes`, **per topic** | ✅ *now applied* |
| `log.retention.check.interval.ms` | 5 min | `--retention-check-interval-ms` | ✅ |
| `log.cleanup.policy` | delete | **per topic `cleanup.policy`** | ✅ *now applied* |
| `log.flush.interval.messages` / `.ms` | ~never | `--flush-interval-*`, **per topic** | ✅ |
| `message.max.bytes` | 1 MiB | **per topic `max.message.bytes`** | ✅ *new* |
| `min.insync.replicas` | 1 | per topic | ✅ honoured on `acks=all` |
| `unclean.leader.election.enable` | false | always false | ✅ matches the safe default |
| `replica.lag.time.max.ms` | 30 s | `--replica-lag-time-max-ms` | ✅ |
| `offsets.topic.num.partitions` | 50 | `--offsets-topic-partitions` | ✅ |
| `offsets.retention.minutes` | 7 days | **`--offsets-retention-ms`** | ✅ *new* |
| `offsets.topic.replication.factor` | 3 | derived from cluster size | 🟡 not configurable |
| `group.initial.rebalance.delay.ms` | 3 s | fixed 1 s | ✅ not tunable |
| `group.min/max.session.timeout.ms` | 6 s / 30 min | clamped to 1 s / 30 min | ✅ not tunable |
| `num.network.threads` / `num.io.threads` | 3 / 8 | tokio runtime + per-partition actor | ✅ different model, same effect |
| `socket.request.max.bytes` | 100 MiB | `max_frame_bytes` (32 MiB) | 🟡 no flag |
| `log.index.interval.bytes` | 4096 | `LogConfig` | 🟡 no flag |
| `broker.rack` | — | `--rack`, used for replica placement | ✅ |
| `auto.create.topics.enable` | true | standalone on, cluster off | ✅ |
| `quota.producer.default` / `.consumer.` | — | `--quota-*-bytes-per-sec` | ✅ throttles by delaying the ack |
| `replication.quota.*` | — | `--quota-replication-bytes-per-sec` | ✅ |
| `ssl.*` | — | `--transport tcp-tls` / `quic` | 🟡 self-signed, no client certs |
| `sasl.*` | — | `--require-auth` + ACLs | 🟡 password auth, no SASL mechanisms |

## 4. Topic configuration

**Now applied**, which is the change since the last audit: `retention.ms`,
`retention.bytes`, `segment.bytes`, `segment.ms`, `cleanup.policy`,
`flush.messages`, `flush.ms`, `max.message.bytes` and
`min.insync.replicas` all change broker behaviour per topic. An
unparseable value falls back to the broker-wide default rather than to
zero, so a typo cannot delete a log.

Still missing: `compression.type` per topic (the producer chooses), and
there is no `AlterConfigs` — configs are fixed at topic creation.

## 5. Producer configuration

| Kafka config | Kafka default | Brahmaputra | Status |
|---|---|---|---|
| `acks` | all | `acks` (1) | ✅ default differs |
| `batch.size` | 16384 | same | ✅ |
| `linger.ms` | 0 | 5 | ✅ default differs |
| `compression.type` | none | `lz4` | ✅ **none/lz4/zstd/snappy/gzip** |
| `max.in.flight.requests.per.connection` | 5 | same | ✅ |
| `enable.idempotence` | true | off | ✅ supported; default differs |
| `request.timeout.ms` | 30 s | same | ✅ |
| `retries` | ∞ | **5** | ✅ *new*, gated on codes that prove no append |
| `retry.backoff.ms` | 100 ms | **100 ms** | ✅ *new* |
| `delivery.timeout.ms` | 2 min | **120 s** | ✅ *new* |
| `buffer.memory` | 32 MiB | **32 MiB** | ✅ *new*, blocks rather than growing |
| `max.block.ms` | 60 s | **60 s** | ✅ *new* |
| partitioner | murmur2(key) % n | same | ✅ |
| **record headers** | supported | **supported** | ✅ *new* |
| **record timestamps** | CreateTime | **per-record create time** | ✅ *new* |
| `transactional.id` | — | — | ❌ v1 non-goal |
| `max.request.size` | 1 MiB | — | 🟡 broker enforces `max.message.bytes` |

## 6. Consumer configuration

| Kafka config | Kafka default | Brahmaputra | Status |
|---|---|---|---|
| `group.id` | — | ✅ | ✅ |
| `partition.assignment.strategy` | range,cooperative-sticky | range / roundrobin / sticky / **cooperative-sticky** | ✅ |
| `enable.auto.commit` / `auto.commit.interval.ms` | true / 5 s | ✅ | ✅ |
| `max.poll.records` | 500 | 500 | ✅ |
| `session.timeout.ms` | 45 s | 10 s, clamped 1 s–30 min | ✅ configurable |
| `heartbeat.interval.ms` | 3 s | derived | 🟡 not independently configurable |
| **`max.poll.interval.ms`** | 5 min | **300 s** | ✅ *new* |
| **`group.instance.id`** | — | **supported** | ✅ *new*, static membership |
| `fetch.min.bytes` / `fetch.max.wait.ms` | 1 / 500 | same | ✅ |
| `max.partition.fetch.bytes` | 1 MiB | `max_bytes` (8 MiB) | 🟡 one knob |
| **`auto.offset.reset`** | latest | **earliest/latest/none** | ✅ *new*, default differs |
| `isolation.level` | read_uncommitted | HW-bounded | ✅ equivalent, non-transactional broker |
| `client.rack` / follower fetching | — | — | ❌ consumers always read the leader |
| `check.crcs` | true | always | ✅ |

## 7. Architecture and behaviour

| Kafka property | Brahmaputra | Status |
|---|---|---|
| Append-only segmented log, sparse offset + time index | ✅ | ✅ |
| CRC per batch, verified on read | ✅ | ✅ |
| Batches stored and replicated byte-identically | ✅ | ✅ |
| Page-cache-centric, sequential I/O | ✅ | ✅ |
| `sendfile` on the fetch path | 🟡 **plaintext TCP on Linux only** | 🟡 |
| Leader/follower replication, ISR, HW | ✅ | ✅ |
| Leader-epoch truncation (KIP-101) | ✅ verified live | ✅ |
| Controller-driven election from ISR, epoch fencing | ✅ Raft | ✅ |
| **Partition reassignment** | ✅ union-then-narrow; drained brokers free their disk | ✅ |
| **Rack-aware replica placement** | ✅ interleaved, leadership rotated | ✅ |
| **Live topic-config changes** | ✅ pushed to running partitions | ✅ |
| Consumer groups, coordinator failover by log replay | ✅ verified live | ✅ |
| Rebalance protocol | ✅ eager and cooperative (KIP-429) | ✅ |
| **Offset expiry** | ✅ *new* | ✅ |
| Log compaction | ✅ | ✅ |
| Quotas / throttling | ✅ client produce/fetch **and replication** | ✅ |
| TLS / auth / ACLs | ✅ | ✅ |
| Transactions / EOS | ❌ | v1 non-goal |
| Tiered storage | ❌ | v1 non-goal |
| Multi-log-dir / JBOD | ❌ | one disk per broker |
| Metrics endpoint / dashboard | ✅ | ✅ |

**On `sendfile`:** the zero-copy fetch path is `#[cfg(target_os = "linux")]`
and plaintext-TCP only. macOS, BSD and Windows read the bytes and write
them; so do TLS and QUIC everywhere, since they must see the bytes to
encrypt them. Kafka draws the same line for SSL but not for OS.

## 8. Performance, including the replicated path

Published numbers are single-node, RF=1, `acks=1` — 3.0× Kafka on produce
and 5.7× on consume at 256 B, on 8–28× less memory
([docs/benchmarks.md](benchmarks.md)).

**The replicated path is now measured too**
(`scripts/bench-replicated.sh`), and it is the number that matters for a
durable deployment. 100k × 256 B across 6 partitions, three brokers on one
host:

| Configuration | msgs/sec |
|---|---|
| RF=1, `acks=1` (what the headline measures) | 109,298 |
| RF=3, `acks=all`, `min.insync.replicas=2` | **25,618** |

**Replication keeps roughly a quarter of single-node throughput — a 4.2×
cost.** The run verifies it actually replicated: all three nodes hold all
six partition logs and the log end offsets sum to exactly the records
produced.

Two caveats. Three brokers on one machine share a disk and a NIC, so these
absolute numbers are below any real deployment. And **Kafka has not been
measured in the same configuration**, so "3.0× Kafka" cannot be carried
over to RF=3 — Kafka pays a replication cost too, and nobody here has
measured how much. Comparing a replicated Brahmaputra against a
non-replicated Kafka would be dishonest, so no such comparison is made.

## 9. Gaps ranked by impact

**Decisive**

1. **No Kafka wire-protocol compatibility.** No Kafka client, Connect,
   Streams, ksqlDB, Schema Registry, MirrorMaker, Debezium, Flink/Spark
   connector or `kafka-*.sh` tool works. Everything that talks to this
   must be rewritten against one of the native drivers. Nothing else on
   this list compensates for it.

**Semantics**

2. **No transactions or exactly-once semantics.** Read-process-write
   pipelines that need atomicity cannot be built on this.
3. No `buffer.memory` / `max.block.ms`: a client that outruns its broker
   buffers without bound rather than blocking.
4. No `delivery.timeout.ms`: retries are bounded per attempt, not
   end to end.

**Operational**

5. **No production track record.** `scripts/soak.sh` now exists and
   sustains `acks=all` load through repeated broker kills while watching
   memory, segment counts and offset monotonicity — but the longest run to
   date is measured in minutes. A soak that would actually move this is
   measured in weeks.
6. **No multi-log-dir / JBOD.** One data directory per broker, so a single
   disk failure takes the whole broker rather than the partitions on it.
7. `sendfile` is Linux-plus-plaintext only; elsewhere the fallback reads
   the range and writes it — correct, just not free.
8. TLS is self-signed with no client certificates; authentication is
   password-only with no SASL mechanism negotiation.

**Efficiency at scale**

9. No follower fetching (`client.rack`), so every consumer read crosses
   AZs.
10. No tiered storage: retention is bounded by local disk.
11. No incremental fetch sessions (KIP-227), so per-fetch metadata cost
    grows with partition count.
12. `heartbeat.interval.ms` is derived from the session timeout rather
    than set independently.

## 9a. Closed since the last audit

Recorded because this document has drifted before, and a gap list that
only ever grows is not being read against the code.

- **Partition reassignment.** `ReassignPartition` / `CompleteReassignment`
  move a partition between brokers, keeping the union of old and new
  replicas until the targets are in the ISR so durability never dips. A
  drained broker deletes the partition's local data, so rebalancing
  reclaims disk instead of merely relabelling it.
- **Rack-aware placement.** `--rack` is read: replicas interleave across
  racks and leadership rotates between them, so RF=3 spans three failure
  domains instead of possibly landing in one.
- **Live topic configuration.** Changes reach running partitions on the
  maintenance tick rather than waiting for a restart.
- **Replication quotas.** `--quota-replication-bytes-per-sec` bounds what
  a catching-up follower can take, so one broker restart is no longer a
  cluster-wide latency event.
- **Cooperative rebalancing** (KIP-429), alongside eager.
- **Bounded session timeouts** and `group.initial.rebalance.delay.ms`.
## 10. Verification

```bash
cargo test --workspace                    # 304
bash scripts/verify-m1.sh                 # 31  storage, protocol, SIGKILL recovery
bash scripts/verify-m4.sh                 # 30  consumer groups across 5 nodes
bash scripts/verify-m5.sh                 # 15  fsync, quotas, version negotiation
bash scripts/verify-m6.sh                 # 29  metrics, login, RBAC, dashboard
bash scripts/verify-replication.sh        # 14  ISR, failover, resync
bash scripts/verify-retention.sh          # 21  time and size retention
bash scripts/verify-failures.sh           # 15  producer/broker/consumer kills
bash scripts/verify-transport-parity.sh   #     tcp vs tls vs quic, identical
bash scripts/verify-chaos.sh              # 7   random kills under load
bash scripts/verify-reassignment.sh       # 10  rack placement, partition moves, disk freed
SOAK_MINUTES=20 bash scripts/soak.sh      # 5   sustained load through repeated kills
bash scripts/bench-replicated.sh          #     RF=3 acks=all vs RF=1 acks=1
```

All of the above pass on the development host as of this audit.

Client drivers, against a live broker:

```bash
cd clients/go     && go run ./cmd/manualtest      # 34/34
cd clients/nodejs && node test_manual.js          # 34/34
cd clients/python && python3 test_manual.py       # never run
cd clients/java   && javac ... && java ManualTest # never run
```

## 11. Transports (beyond parity)

The data plane runs over plain TCP, TLS 1.3 over TCP, or QUIC, selected
with one flag on broker and client. Kafka is TCP-only, so this is a
capability beyond parity rather than a gap in it. All three carry
producers, consumers, group coordination *and* inter-broker replication,
and `verify-transport-parity.sh` asserts they behave identically on every
correctness check.

The native drivers speak plaintext TCP only; TLS and QUIC are reachable
today only from the Rust client and the CLI.
