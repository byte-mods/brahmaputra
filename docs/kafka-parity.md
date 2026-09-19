# Kafka Parity Audit

## 0.8.0 review update

Consumer groups now expose `read_committed` in the Rust client and honor
the CLI isolation flag. Previously only the direct consumer could select
isolation, so a grouped transactional pipeline could consume aborted data.
Committed fetches also continue past filtered batches larger than their
byte budget. These changes close a functional gap in the existing
transaction implementation; they do not add Kafka wire compatibility.

Fetch sessions now bind to an authenticated principal and recheck read
permission for their restored partitions on every request, including
incremental requests carrying no descriptors. Broker-lease refusals are
retryable by producers and tolerated during group polling. Non-idempotent
retries remain at-least-once.

Use `scripts/verify-release.sh` and `scripts/bench-release.sh` for the
release verification and finite comparison matrices. Historical benchmark
figures below retain their original methodology; the new harnesses use
group consumers on both systems and separate Kafka broker/client heaps.

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
between brokers, placement is rack-aware, topic-configuration changes reach
running partitions, and leadership returns to the preferred replica after a
broker comes back. That is what decides whether a cluster can be operated
for years rather than merely started once, and it was the largest
non-protocol gap.

The operator's surface has caught up with it. A broker can now be asked
what cluster it belongs to, what a topic is configured to do, and which
partition is using which disk; records can be deleted without deleting
their topic; byte-rate limits bind to a tenant rather than to the whole
cluster; a broker can be given its disks directly, with a failed one taking
only its own partitions instead of the whole node; and a client can be
authenticated by a certificate the operator's CA signed, with ACLs enforced
against that certificate's subject.

Transactions are also no longer a non-goal. A coordinator, control batches,
a last stable offset and `read_committed` isolation are implemented and
verified, which removes the ceiling on which workloads qualify —
read-process-write pipelines can now be built on this.

Two things this document previously recorded as present have since been
found *half* present, and are now finished. **Log compaction could not
delete a key**: the record format had no null value, so a compacted topic's
key space could only grow, and the group coordinator's own deletions were
markers compaction could not act on. And **`transaction.timeout.ms` was
stored and never read**: a producer that died mid-transaction pinned the
last stable offset on every partition it had touched, permanently, unless
something happened to claim its `transactional.id` again. Both were worse
than the gaps this document ranked, because both looked like features.

With those closed, the remaining efficiency work is done too: consumers can
read from an in-sync replica in their own rack (KIP-392), a fetch sends
only what changed (KIP-227), and authentication no longer requires
encryption to be meaningful (SCRAM-SHA-256).

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
are cheap to write; besides the in-repo Rust one, four exist
([clients/](../clients)):

| Language | State |
|---|---|
| Rust | the in-repo client; covered by the workspace suite |
| Go | updated for wire v4, verified against a live v4 broker, 38/38 |
| Node.js | updated for wire v4, verified against a live v4 broker, 38/38 |
| Python | updated for wire v4, still never executed (no interpreter on the build host) |
| Java | updated for wire v4, still never compiled (no JDK on the build host) |

They cover producer, consumer and group APIs including sticky assignment,
static membership and `auto.offset.reset`. They do **not** cover TLS, QUIC
or the idempotent producer.

| Kafka API | Brahmaputra | Notes |
|---|---|---|
| Produce | ✅ key 0 | single topic-partition |
| ProduceMulti | ✅ key 15 | many partitions per request; the default client path |
| Fetch | ✅ key 1 | long poll, `min_bytes`/`max_wait_ms` |
| FetchMulti | ✅ key 16 | many partitions, read concurrently; the default client path |
| ListOffsets | ✅ key 2 | earliest / latest / by-timestamp (time-index bounded) |
| Metadata | ✅ key 3 | brokers, leaders, ISR, leader epoch |
| OffsetForLeaderEpoch | ✅ key 5 | KIP-101 truncation |
| InitProducerId | ✅ key 6 | idempotent producer |
| JoinGroup / SyncGroup / Heartbeat | ✅ keys 7–9 | eager rebalance, generation fencing, static membership |
| OffsetCommit / OffsetFetch | ✅ keys 10–11 | |
| ListGroups / DescribeGroups | ✅ keys 12–13 | |
| ApiVersions | ✅ key 14 | answered at any requested version |
| Authenticate | ✅ key 17 | SCRAM-SHA-256 (two round trips, password never sent) and PLAIN (refused on a plaintext listener) |
| **LeaveGroup** | ✅ key 18 | **added since the last audit** |
| LeaderAndIsr / UpdateMetadata | 🟡 | equivalent effect via Raft metadata subscription |
| **DescribeCluster** | ✅ key 19 | brokers, racks, current controller |
| **DescribeConfigs** | ✅ key 20 | topic and broker resources; marks inherited defaults |
| **DescribeLogDirs** | ✅ key 21 | per-partition disk usage, fanned out across brokers |
| **DeleteRecords** | ✅ key 22 | leader-only, clamped to the high watermark |
| **AlterConfigs** | ✅ key 28 | forwards to the controller; incremental or replace, unknown names refused |
| **AddPartitionsToTxn** | ✅ key 23 | announced before the first write to a partition |
| **AddOffsetsToTxn** | ✅ key 24 | brings a group's offsets into the transaction |
| **EndTxn** | ✅ key 25 | prepare, mark every partition, complete |
| **TxnOffsetCommit** | ✅ key 26 | to the group coordinator, as transactional records |
| **WriteTxnMarkers** | ✅ key 27 | cluster-internal, as in Kafka |
| **DescribeProducers** | ✅ key 29 | who has written to a partition, and what transaction is open |
| **ListTransactions** | ✅ key 30 | every transactional id the cluster coordinates, fanned out |
| **DescribeTransactions** | ✅ key 31 | one transaction in full, routed to its coordinator |
| **AlterReplicaLogDirs** | ✅ key 32 | move a partition between a broker's disks; the partition is offline while it copies |
| FindCoordinator | 🟡 | not needed: a client hashes the id to a coordinator partition and routes to its leader, exactly as it does for a group |

The wire version is **4**. The broker requires an exact match, so an older
client gets `UNSUPPORTED_VERSION` rather than misparsing.

Version 4 added **tombstones**: a record's value may be null, signalled by
a new attributes bit on the batch. That bit is the reason the version had
to move — a version-3 client would read a tombstone's length prefix as a
value length and misparse every record after it, which is precisely the
failure the exact-match rule exists to make impossible. Alongside it:
`client.rack` and an incremental fetch session on `Fetch`/`FetchMulti`, a
rack per broker in `Metadata`, a SASL mechanism and payload on
`Authenticate`, and five new APIs.

All four native drivers were updated; the Go and Node.js suites were re-run
against a version-4 broker (**38/38** each, including new tombstone
coverage). Python and Java are updated and, as before, unexecuted — there
is no interpreter and no JDK on the build host.

`ApiVersions` advertises all **33** dispatched APIs.

## 3. Broker configuration

| Kafka config | Kafka default | Brahmaputra | Status |
|---|---|---|---|
| `broker.id` | — | `--node-id` | ✅ |
| `listeners` / `advertised.listeners` | — | `--host`/`--port`, `--advertised-host`/`--advertised-port`, `--internal-port`/`--internal-tls` | 🟡 two listeners (client, inter-broker) with an advertised address; no arbitrary named listener map |
| `log.dirs` | `/tmp/kafka-logs` | `--data-dir`, repeatable | ✅ JBOD: one partition per disk, failure isolated per disk |
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
| `offsets.topic.replication.factor` | 3 | `--offsets-topic-replication-factor`, or derived | ✅ *new* |
| `group.initial.rebalance.delay.ms` | 3 s | fixed 1 s | ✅ not tunable |
| `group.min/max.session.timeout.ms` | 6 s / 30 min | clamped to 1 s / 30 min | ✅ not tunable |
| `num.network.threads` / `num.io.threads` | 3 / 8 | tokio runtime + per-partition actor | ✅ different model, same effect |
| `socket.request.max.bytes` | 100 MiB | `--max-frame-bytes` (32 MiB) | ✅ *new* |
| `log.index.interval.bytes` | 4096 | `--index-interval-bytes` | ✅ *new* |
| `broker.rack` | — | `--rack`, used for replica placement | ✅ |
| `auto.create.topics.enable` | true | standalone on, cluster off | ✅ |
| `quota.producer.default` / `.consumer.` | — | `--quota-*-bytes-per-sec`, plus per-user / per-client-id entities | ✅ throttles by delaying the ack |
| `replication.quota.*` | — | `--quota-replication-bytes-per-sec` | ✅ |
| `ssl.*` | — | `--transport tcp-tls` / `quic`, `--tls-cert` / `--tls-key` / `--tls-client-ca` | ✅ operator certificates and mutual TLS |
| `sasl.enabled.mechanisms` | GSSAPI | SCRAM-SHA-256 and PLAIN, chosen per connection | 🟡 no SCRAM-SHA-512, GSSAPI, OAUTHBEARER or delegation tokens |
| `log.cleaner.delete.retention.ms` | 24 h | `--delete-retention-ms`, per topic | ✅ *new* |
| `log.cleaner.min.cleanable.ratio` | 0.5 | `--min-cleanable-dirty-ratio`, per topic | ✅ *new* |
| `log.cleaner.min.compaction.lag.ms` | 0 | `--min-compaction-lag-ms`, per topic | ✅ *new* |
| `log.cleaner.max.compaction.lag.ms` | ∞ | `--max-compaction-lag-ms`, per topic | ✅ *new* |
| `message.max.bytes` (broker default) | 1 MiB | `--max-message-bytes` | ✅ *new* |
| `transaction.max.timeout.ms` | 15 min | `--transaction-max-timeout-ms` | ✅ *new*, and enforced |
| `transactional.id.expiration.ms` | 7 days | `--transactional-id-expiration-ms` | ✅ *new* |

## 4. Topic configuration

**Applied per topic**: `retention.ms`, `retention.bytes`, `segment.bytes`,
`segment.ms`, `cleanup.policy`, `delete.retention.ms`,
`min.cleanable.dirty.ratio`, `min.compaction.lag.ms`,
`max.compaction.lag.ms`, `flush.messages`, `flush.ms`,
`max.message.bytes`, `message.timestamp.type`, `compression.type` and
`min.insync.replicas` all change broker behaviour. An unparseable value
falls back to the broker-wide default rather than to zero, so a typo cannot
delete a log.

Configs are not fixed at creation. `AlterConfigs` (key 20's counterpart,
key 28) changes them from the data plane — the broker forwards to the
controller, so durability and ordering are unchanged and a client no longer
needs a second protocol and a second address to write what
`DescribeConfigs` already let it read. A name the broker does not read is
**refused** rather than stored, because a config that is accepted, echoed
back and silently ignored is indistinguishable from a broker that does not
honour it.

`compression.type` is applied by **refusal**: a topic that names a codec
rejects a batch in any other one. Kafka would recompress it; doing that
means decompressing and recompressing every batch on the way in, which is
exactly the cost byte-identical storage and the zero-copy fetch path exist
to avoid. The operator's guarantee is the same, and the producer is told
what to send rather than having its data rewritten.

`message.timestamp.type=LogAppendTime` overwrites the batch timestamp in
place and recomputes the CRC — no decompression. Kafka additionally
flattens every record in the batch to that instant; here the per-record
deltas survive, so records inside one batch keep their relative spacing.

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
| `retries` | ∞ | **5** | ✅ transient broker errors; non-idempotent retries can duplicate |
| `retry.backoff.ms` | 100 ms | **100 ms** | ✅ *new* |
| `delivery.timeout.ms` | 2 min | **120 s** | ✅ *new* |
| `buffer.memory` | 32 MiB | **32 MiB** | ✅ *new*, blocks rather than growing |
| `max.block.ms` | 60 s | **60 s** | ✅ *new* |
| partitioner | murmur2(key) % n | same | ✅ |
| **record headers** | supported | **supported** | ✅ *new* |
| **record timestamps** | CreateTime | **per-record create time** | ✅ *new* |
| `transactional.id` | — | `TransactionalProducer::init` | ✅ fences the previous instance and resolves what it abandoned |
| `max.request.size` | 1 MiB | — | 🟡 broker enforces `max.message.bytes` |

## 6. Consumer configuration

| Kafka config | Kafka default | Brahmaputra | Status |
|---|---|---|---|
| `group.id` | — | ✅ | ✅ |
| `partition.assignment.strategy` | range,cooperative-sticky | range / roundrobin / sticky / **cooperative-sticky** | ✅ |
| `enable.auto.commit` / `auto.commit.interval.ms` | true / 5 s | ✅ | ✅ |
| `max.poll.records` | 500 | 500 | ✅ |
| `session.timeout.ms` | 45 s | 10 s, clamped 1 s–30 min | ✅ configurable |
| `heartbeat.interval.ms` | 3 s | `with_heartbeat_interval`, else timeout ÷ 3 | ✅ *new* |
| **`max.poll.interval.ms`** | 5 min | **300 s** | ✅ *new* |
| **`group.instance.id`** | — | **supported** | ✅ *new*, static membership |
| `fetch.min.bytes` / `fetch.max.wait.ms` | 1 / 500 | same | ✅ |
| `max.partition.fetch.bytes` | 1 MiB | `max_bytes` (8 MiB) | 🟡 one knob |
| **`auto.offset.reset`** | latest | **earliest/latest/none** | ✅ *new*, default differs |
| `isolation.level` | read_uncommitted | `read_uncommitted` / `read_committed` | ✅ LSO-bounded, aborted records and markers withheld |
| `client.rack` / follower fetching | — | `--rack`, `Consumer::with_rack` | ✅ *new*, KIP-392: the leader names an in-sync replica in the consumer's rack |
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
| **Preferred-leader election and rebalancing** | ✅ failover follows replica order; leadership returns on `--auto-leader-rebalance-interval-ms` | ✅ |
| **Live topic-config changes** | ✅ pushed to running partitions | ✅ |
| **Administrative introspection** | ✅ DescribeCluster / DescribeConfigs / DescribeLogDirs / DescribeProducers / ListTransactions / DescribeTransactions | ✅ |
| **Data-plane configuration changes** | ✅ AlterConfigs, forwarded to the controller | ✅ |
| **DeleteRecords** | ✅ leader-only, checkpointed log start offset | ✅ |
| Consumer groups, coordinator failover by log replay | ✅ verified live | ✅ |
| Rebalance protocol | ✅ eager and cooperative (KIP-429) | ✅ |
| **Offset expiry** | ✅ *new* | ✅ |
| Log compaction | ✅ tombstones delete keys; `delete.retention.ms`, dirty ratio and lag knobs; batches, codecs and producer metadata preserved; crash-safe swap | ✅ |
| **Transaction timeout enforcement** | ✅ the coordinator aborts and fences past `transaction.timeout.ms`, re-sends half-written markers, expires idle ids | ✅ |
| **Follower fetching (KIP-392)** | ✅ the leader names an in-sync replica in the consumer's rack | ✅ |
| **Incremental fetch sessions (KIP-227)** | ✅ bounded, LRU-evicted, epoch-checked | ✅ |
| **SASL mechanisms** | 🟡 SCRAM-SHA-256 and PLAIN | 🟡 |
| **Moving a partition between disks** | 🟡 AlterReplicaLogDirs, but the partition is offline while it copies | 🟡 |
| Quotas / throttling | ✅ client produce/fetch **and replication**, bound to user and/or client id | ✅ |
| TLS / auth / ACLs | ✅ | ✅ |
| **Transactions and exactly-once** | ✅ coordinator, control batches, LSO, `read_committed` | ✅ |
| Tiered storage | ❌ | v1 non-goal |
| **Multi-log-dir / JBOD** | ✅ least-loaded placement; a failed disk takes only its own partitions | ✅ |
| Metrics endpoint / dashboard | ✅ | ✅ |

**On `sendfile`:** the zero-copy fetch path is `#[cfg(target_os = "linux")]`
and plaintext-TCP only. macOS, BSD and Windows read the bytes and write
them; so do TLS and QUIC everywhere, since they must see the bytes to
encrypt them. Kafka draws the same line for SSL but not for OS.

## 8. Performance, including the replicated path

Published single-node numbers are RF=1, `acks=1` — 3.0× Kafka on produce
at 256 B, on 8–28× less memory ([docs/benchmarks.md](benchmarks.md)). The
5.7× consume figure in that document **does not survive matched
methodology**: it compares an uncoordinated reader against a Kafka
consumer group, and group-to-group the two systems land within about 17 %.

**Kafka is now measured in the same replicated configuration**
(`scripts/bench-replicated-vs-kafka.sh`), which is the comparison that
decides anything for a durable deployment. Three brokers per system on one
host, 8M × 256 B across 6 partitions, 4 clients, means of three runs
([replicated-benchmark-2026-08-22.md](replicated-benchmark-2026-08-22.md)):

| Produce configuration | Kafka | Brahmaputra |
|---|---|---|
| RF=1, `acks=1` | 735,668 | **1,157,006** |
| RF=3, `acks=all`, `min.insync.replicas=2` | 231,454 | **787,385** |
| Replication cost | 3.18× | **1.47×** |
| Cluster memory at RF=3 | 4,464 MiB | **999 MiB** |

**Brahmaputra is 3.40× faster than Kafka at RF=3 and replicates at less
than half Kafka's cost.** Every RF=3 level asserts it actually replicated:
Kafka at ISR=3 on all six partitions, Brahmaputra with logs on all three
nodes and log end offsets summing exactly to the records produced.

This is a reversal of the previous audit, which recorded 25,618 msgs/sec
and a 4.2× replication cost. Both were real: the follower fetch path slept
50 ms between empty fetches, and under `acks=all` every producer waited out
that sleep before its record could commit. Replacing it with a leader-side
long poll took the native RF=3 figure from 28,568 to 299,013 msgs/sec.

One caveat stands: three brokers on one machine share a disk and a NIC, so
the absolute numbers are below any real deployment — for both systems
equally, which is what preserves the ratios.

## 9. Gaps ranked by impact

**Decisive**

1. **No Kafka wire-protocol compatibility.** No Kafka client, Connect,
   Streams, ksqlDB, Schema Registry, MirrorMaker, Debezium, Flink/Spark
   connector or `kafka-*.sh` tool works. Everything that talks to this
   must be rewritten against one of the native drivers. Nothing else on
   this list compensates for it.

**Operational**

2. **No production track record.** `scripts/soak.sh` sustains `acks=all`
   load through repeated broker kills while watching memory, segment counts
   and offset monotonicity — but the longest run to date is measured in
   minutes. A soak that would actually move this is measured in weeks.
   Nothing in this document can close this one.
3. `sendfile` is Linux-plus-plaintext only; elsewhere the fallback reads
   the range and writes it — correct, just not free. This is a property of
   the platform rather than a thing left undone: macOS, BSD and Windows
   have no equivalent that covers this case, and TLS and QUIC must see the
   bytes to encrypt them. Kafka draws the same line for SSL.
4. **No general listener map.** There are now two listeners — a client one
   and an optional inter-broker one, each with its own transport — and an
   advertised address distinct from the bound one. What is still missing is
   Kafka's arbitrary `listeners` / `advertised.listeners` map with named
   endpoints and a security-protocol per name. The common shape it exists
   for, plaintext between brokers and TLS to clients, is covered; three or
   more listeners with different protocols is not.
5. **A log-directory move takes the partition offline while it copies.**
   `AlterReplicaLogDirs` (key 32) moves a partition between a broker's
   disks, which is the gap that mattered — a disk added to a running broker
   used to take only new partitions forever. But Kafka builds the second
   copy alongside, lets it catch up, and swaps; this closes the partition,
   copies, and reopens. The pause is bounded by the partition's size and is
   the reason to move followers, or to hand leadership away first.
6. **SASL is SCRAM-SHA-256 and PLAIN only.** No SCRAM-SHA-512, GSSAPI,
   OAUTHBEARER, and no delegation tokens. SCRAM covers the case that
   actually blocked deployments — authentication that is meaningful on a
   listener that is not encrypted — and certificate authentication remains
   the strongest option available.

**Efficiency at scale**

7. No tiered storage: retention is bounded by local disk. A v1 non-goal,
   and the one item on this list that is a deliberate scope decision rather
   than something not yet built.
8. A `read_committed` fetch cannot use the zero-copy path: choosing which
   batches to withhold means reading their headers, and the point of
   handing the kernel a file range is that nobody reads them. Filtering is
   per batch rather than per record, so this costs a copy and never a
   decompression.
9. **`compression.type` is enforced by refusal, not conversion.** A topic
   that names a codec rejects a batch in any other one; Kafka would
   recompress it. Converting means decompressing and recompressing every
   batch on the way in, which is exactly the cost that byte-identical
   storage and the zero-copy fetch path exist to avoid — so the guarantee
   an operator gets ("every batch on this topic is zstd") is the same, and
   the producer is told what to send rather than having its data silently
   rewritten.
10. **`message.timestamp.type=LogAppendTime` keeps per-record deltas.**
    Kafka flattens every record in a batch to the same instant; here the
    batch timestamp becomes the broker's clock and the per-record deltas
    survive, so records inside one batch keep their relative spacing. What
    the setting is *for* — retention and timestamp seeks no longer
    depending on a client's clock — holds either way, and the batch is
    never decompressed to do it.

**Newer Kafka**

11. **KIP-848**, the broker-side consumer group protocol and the default in
    Kafka 4.0. The client-side JoinGroup/SyncGroup rebalance implemented
    here is the older protocol, which Kafka still supports. Moving the
    assignment into the coordinator is a re-architecture of the group
    machinery rather than a feature to add to it.
12. **KIP-932 share groups** (queue semantics) and **KIP-890 transaction
    fencing v2**. Both are Kafka 4.x work; neither is a gap against the
    Kafka most deployments are running.

## 9a. Closed since the last audit

Recorded because this document has drifted before, and a gap list that
only ever grows is not being read against the code.

### Closed in 0.7.0

None of these were on the gap list. They were found by reading the code
against §7's claims, and each is a claim that was not true under a leader
change or on a platform without `sendfile`:

- **Transactions at RF>1 did not survive a failover.** A control batch was
  written with leader epoch 0; a follower that had seen epoch 1 rejected it
  as non-monotonic and retried the same offset forever. §7's "leader-epoch
  truncation" row was true for data batches only.
- **A returning coordinator served state from before it left.** Group and
  transaction shards were cached for the process lifetime; regaining a
  `__consumer_offsets` partition rewound every consumer past the commits
  the other coordinator had taken. Shards are now tied to the partition
  actor's disruption count and rebuilt when the log moved without them.
- **`read_committed` did not extend to the coordinator or the cleaner.**
  Transactional offset commits were never applied live and were replayed
  uncommitted; compaction let aborted records supersede committed values.
  Both now resolve by marker, and the cleaner stops at the last stable
  offset.
- **The buffered fetch path raced on a shared file cursor** everywhere
  `sendfile` is not used. Reads are positional now.
- **Client:** a batched producer never refreshed a moved leader; auto-commit
  was at-most-once for the batch in flight.

### Closed in 0.6.0

- **A controller node that stays down no longer takes the surviving
  brokers with it** — gap 3 on the previous list, and the one that turned a
  single failure into a total one. A broker that could not renew its lease
  treated that as proof it had been superseded and exited; it is not proof,
  because a broker that cannot reach a controller at all learns nothing
  about whether a newer incarnation of itself exists.

  An expired lease now **suspends** the data plane instead: the broker
  stops serving, keeps its process, listeners and actors alive, and
  re-registers with `expected_epoch` set to the epoch it held. The
  controller accepts that only if no newer incarnation registered
  meanwhile, so a still-current process resumes and a zombie is rejected.
  `fence()` remains irreversible for the case that *is* proof — an epoch
  change observed while the lease is live. This is the distinction the
  previous audit named as missing.

  Surviving the outage took four more changes at other layers, each of
  which could take the cluster down on its own:

  - a controller fence of the epoch a broker holds is recoverable, not
    fatal — the controller fences whatever it has not heard from, and an
    election is silence, not replacement;
  - a **new controller leader fences nobody for one session timeout**,
    because renewing a lease is a quorum write and every timestamp it
    inherits predates its own election. Kafka's controller gives the same
    grace after a failover;
  - `RegisterBroker` and `Heartbeat` are stamped when proposed rather than
    when built, so a registration that waited for a quorum does not commit
    already looking overdue;
  - a partition whose whole ISR was fenced is recovered by any replica
    that was in that ISR (`last_isr`), which is Kafka's clean-election
    rule. 0.5.0 could recover only a sole replica, so a replicated
    partition could still be permanently offline after this outage.

  Verified by `scripts/verify-bugfixes.ps1`: three combined nodes, the Raft
  leader hard-killed and left down, survivors asserted alive, electing,
  renewing and accepting `acks=all` writes; then a second node killed, the
  last one asserted to suspend rather than exit *and* to refuse to serve
  without a lease, and to resume once a peer returns.

- **A deleted topic's data can no longer be served under a recreated
  topic's name.** This was not on the gap list — it was found by reading
  the code, and it is the third instance of the same mistake the two items
  below are: assuming an absence of evidence is evidence. A partition
  directory had no durable link to the topic incarnation that created it,
  so recreating `orders` reopened the deleted `orders-0` log with its
  offsets, records and watermark intact. `CreateTopic` now stamps a
  monotonic `topic_epoch` into the metadata, brokers persist it in
  `.topic-epoch` beside each replica, and a mismatch discards the stale
  directory before the log can be opened.

- **A malformed frame can no longer read out of bounds.** The generated
  BitPacker decoder read varints, booleans and lengths through
  `get_unchecked` because "we trust the data source"; the data source is
  the network. Every read is now bounds-checked behind a decode-error flag
  each generated `decode` inspects, and the generator in `tools/bit-packer`
  emits the checked form. Quota accounting had the same shape one layer up
  — buckets keyed by the peer-supplied `client.id` and kept forever — and
  is now keyed by a seeded hash and capped.

### Closed in 0.4.0

The first two were not on the previous gap list at all. They were found by
reading the code against this document's own claims, and both are worse
than anything that *was* listed, because a missing feature is visible and a
half-present one is not.

- **Log compaction can delete a key.** `Record.value` is `Option<Bytes>`; a
  null value is a tombstone, and a new attributes bit (0x0040) says a batch
  contains one so that a batch without one encodes to exactly the bytes it
  always did.

  Before this, compaction kept the newest record for every key that had
  ever existed. Half of what `cleanup.policy=compact` means in Kafka was
  missing, and `__consumer_offsets` expiry wrote an application-level
  marker under a *different key* than the records it was deleting — which
  replay understood and compaction could not act on, so the comment
  claiming it "lets compaction reclaim them" was false.

  The pass itself was rewritten around the same change:

  | Was | Is |
  |---|---|
  | every survivor re-emitted as its own single-record batch | contiguous survivors share one batch |
  | compression dropped, producer metadata erased, control batches turned into data | codec, producer identity and transactional/control flags preserved |
  | one segment for the whole cleaned range | rolls at `segment.bytes` |
  | every survivor held in memory | streamed, with the key map covering only the dirty range |
  | ran whenever anything was removable | `min.cleanable.dirty.ratio`, `min/max.compaction.lag.ms` |
  | tombstones did not exist | `delete.retention.ms`, after which the tombstone goes too |
  | a crash mid-swap could lose the survivors | staging directory, commit marker, swap; recovered on open |
  | advanced the log start offset | leaves it alone, as Kafka does |

  Verified live by `scripts/verify-compaction.sh` (17 checks), including
  that an empty value is not a deletion and that a `kill -9` mid-run
  changes nothing about what a consumer reads.

- **`transaction.timeout.ms` is enforced.** The coordinator sweeps its
  shards, fences the producer and aborts any transaction that has outlived
  its timeout.

  The field was persisted in `TransactionMetadata` and read by nothing. A
  producer that died mid-transaction — scaled down, redeployed under a
  different id, crashed for good — left records in doubt on every partition
  it had written to, and the last stable offset there never advanced past
  them. Every `read_committed` consumer stopped, permanently. The only
  resolution path was the same `transactional.id` being claimed again,
  which for a producer that is not coming back is never.

  The epoch bump is the part that makes it safe: without it a producer that
  was merely slow would carry on writing into a transaction already marked
  aborted, producing exactly the stall the timeout was meant to prevent.
  The same sweep re-sends the markers of a transaction left in `Prepare*`
  and retires `transactional.id`s idle past
  `transactional.id.expiration.ms`, writing a tombstone so their state
  stops occupying disk. Verified live by `scripts/verify-transactions.sh`
  (24 checks, up from 17).

- **Follower fetching (KIP-392).** A consumer that sets `client.rack` is
  told by the leader which in-sync replica in its own rack to read from.
  Only ISR members are ever named — a follower outside it is behind by an
  unbounded amount, and pointing a consumer there converts a replication
  problem into a consumer that has silently stopped — and a consumer
  already in the leader's rack is not redirected, which would trade a
  fresher read for nothing. Verified live by `scripts/verify-reassignment.sh`.

- **Incremental fetch sessions (KIP-227).** A consumer holding a thousand
  partitions of which three are moving sends three descriptors instead of a
  thousand. The client remains the authority on where it is reading: the
  session is a cache of what it last said, so a broker that has evicted or
  forgotten one answers `FETCH_SESSION_NOT_FOUND` and the client sends a
  full fetch again. The failure mode is a wasted round trip, never a
  consumer reading from the wrong offset. Sessions are bounded and
  LRU-evicted, because a session is memory a *client* causes a broker to
  allocate.

- **SASL/SCRAM-SHA-256.** The password never crosses the wire, which is
  what makes authentication meaningful on a plaintext listener — where
  PLAIN remains, correctly, refused. Both credentials are derived when a
  password is set; a SCRAM credential cannot be back-derived from an Argon2
  hash, so a user created before this must have their password set again.
  Every connection a client's router opens authenticates, not just the
  first: a pooled connection created after a reconnect would otherwise be
  anonymous.

- **An inter-broker listener and an advertised address.**
  `--internal-port` / `--internal-tls` give replication and transaction
  markers their own socket and transport, so a cluster can present TLS to
  clients and speak plaintext to itself — without encrypting every record
  two or three more times to reach the followers.
  `--advertised-host` / `--advertised-port` publish an address that differs
  from the bound one, which is what a broker behind NAT, in a bridged
  container network, or on a pod IP needs.

- **`AlterReplicaLogDirs`.** A partition can be moved between a broker's
  disks. JBOD placed partitions once, at creation, so a disk added to a
  running broker took only new partitions forever. The partition is closed
  while its bytes are copied — the honest cost of moving data that is being
  appended to — which is why this is an explicit operator action.

- **`AlterConfigs`, `DescribeProducers`, `ListTransactions`,
  `DescribeTransactions`** (keys 28–31), with matching `Admin` methods and
  CLI commands. The first removes the need for a second protocol to change
  what the data plane could already read; the other three answer "why has
  my committed reader stopped?" without reading the log by hand.

- **Configuration that was unreachable.** `socket.request.max.bytes`,
  `log.index.interval.bytes`, `message.max.bytes`,
  `offsets.topic.replication.factor` and `heartbeat.interval.ms` were
  hard-coded constants this document listed as such. All are flags now, as
  are the compaction settings and `message.timestamp.type`.

### Closed in earlier audits

- **Multi-log-dir / JBOD.** `--data-dir` is repeatable, one directory per
  disk. A new partition is placed on the online directory holding the
  fewest, and the mapping is rebuilt on startup by scanning the directories
  themselves rather than from a side file that could disagree with them.

  The capacity argument is the smaller half. The point is blast radius: a
  directory that fails takes **only its own partitions** offline — their
  actors are closed, requests for them are refused with `LOG_DIR_OFFLINE`,
  and the broker keeps serving everything on the other disks. With one
  directory a disk failure had no partial mode at all.

  Failover needed no new machinery: a partition on a dead disk stops
  fetching, and the controller already elects around a replica that stops
  fetching. Failure *isolation* is what was added.

  Failure is detected two ways — an IO error on any log operation, and a
  write-and-fsync probe every 5 s, so a disk that dies under an idle topic
  is noticed in seconds rather than whenever something next touches it. An
  offline directory stays offline until the broker restarts, as in Kafka: a
  disk that appears to recover has usually been remounted, potentially
  having lost the tail of every file on it.

  The case that needed care is a disk **already broken at startup**, which
  cannot be scanned. Placement is therefore also recorded in the first
  directory, used for exactly one thing: naming the partitions on a
  directory that is offline, so they are reported unavailable rather than
  silently re-created empty somewhere else — which would be
  indistinguishable from a partition that had lost every record.

  Verified live by `scripts/verify-jbod.sh` (18 checks), including a disk
  failing under load while the others keep taking writes.

- **Transactions and exactly-once semantics** — the item this document has
  ranked second-most-decisive since it was written. A transaction
  coordinator sharded over a compacted `__transaction_state` topic, control
  batches marking each partition, a per-partition transaction index giving
  the last stable offset and the aborted set, and `isolation.level` on the
  fetch path.

  The properties that matter, and where each is enforced:

  | Property | Mechanism |
  |---|---|
  | Atomic across partitions | one marker per partition, written by the coordinator after a durable `Prepare*` record |
  | An open transaction blocks committed readers | the LSO is the first offset of the oldest open transaction, and it bounds a `read_committed` fetch instead of the high watermark |
  | Aborted records are never delivered | remembered in the partition's transaction index and skipped per batch, with no decompression |
  | A crashed producer is resolved | the next claim of its `transactional.id` fences it and finishes what it left, using the durable prepare record rather than a guess |
  | A fenced producer cannot finish | every request carries an epoch; one behind the coordinator's is refused |
  | Read-process-write is atomic | offsets are committed to `__consumer_offsets` as transactional records, so they become visible with the output or not at all |
  | A follower agrees with its leader | transaction state is derived from the replicated bytes on every append path, so a failover does not change what a committed reader sees |

  Verified live by `scripts/verify-transactions.sh` (17 checks), including
  the case that distinguishes this from filtering after the fact: an
  abandoned transaction holds a committed reader at the LSO while
  `read_uncommitted` reads straight past it, and the decision survives a
  broker restart.

- **Preferred-leader election.** Failover now walks `replicas` in order
  instead of taking the lowest-numbered ISR member, so leadership lands
  where placement intended; and the controller moves leadership back to the
  preferred replica once it rejoins the ISR
  (`--auto-leader-rebalance-interval-ms`, default 300 s, matching Kafka's
  `leader.imbalance.check.interval.seconds`). Without both halves, every
  rolling restart left leadership permanently on whichever brokers stayed
  up.
- **`DescribeCluster`, `DescribeConfigs`, `DescribeLogDirs`,
  `DeleteRecords`** (keys 19–22), with `Admin` in the Rust client and
  `describe-cluster` / `describe-configs` / `describe-log-dirs` /
  `delete-records` in the CLI. `DeleteRecords` is the only way to reclaim
  space on a topic retention will not touch, and the log start offset it
  sets is checkpointed so a restart cannot resurrect deleted records.
- **Quota entities.** Byte-rate limits bind to a user, a client id, or
  both, live in replicated metadata, and resolve most-specific-first per
  direction (`brahmaputra-cli quota set|list|delete`). Previously one
  broker-wide rate applied to every tenant.
- **`ListOffsets` by timestamp uses the time index.** It walked the log
  from the start, reading and CRC-checking every batch — so asking "where
  was I an hour ago?" cost a read of everything older than an hour.
  Segments are now skipped by their newest record and the scan starts
  within one index interval of the answer.
- **Operator-supplied TLS certificates and mutual TLS.** `--tls-cert` /
  `--tls-key` present a real chain instead of a certificate generated at
  startup; `--tls-client-ca` requires a client certificate and binds its
  **subject** common name to the connection as the principal, so ACLs are
  enforceable with no password on the wire. Both TCP-TLS and QUIC share one
  identity. The client side is `--tls-ca` / `--tls-cert` / `--tls-key`.
- **`ApiVersions` advertises every dispatched API** (23, was 16).

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
cargo test --workspace                    # 405
bash scripts/verify-m1.sh                 # 31  storage, protocol, SIGKILL recovery
bash scripts/verify-m4.sh                 # 30  consumer groups across 5 nodes
bash scripts/verify-m5.sh                 # 15  fsync, quotas, version negotiation
bash scripts/verify-m6.sh                 # 29  metrics, login, RBAC, dashboard
bash scripts/verify-replication.sh        # 14  ISR, failover, resync
bash scripts/verify-retention.sh          # 21  time and size retention
bash scripts/verify-failures.sh           # 15  producer/broker/consumer kills
bash scripts/verify-transport-parity.sh   #     tcp vs tls vs quic, identical
bash scripts/verify-chaos.sh              # 7   random kills under load
bash scripts/verify-reassignment.sh       # 12  rack placement, moves, follower fetching
bash scripts/verify-admin-and-security.sh # 29  admin APIs, quotas, mTLS, SCRAM, AlterConfigs
bash scripts/verify-transactions.sh       # 24  commit, abort, in doubt, expiry
bash scripts/verify-compaction.sh         # 17  tombstones, superseding, delete horizons
bash scripts/verify-jbod.sh               # 26  multi-disk placement, failure, moves between disks
pwsh scripts/verify-bugfixes.ps1          # 5   controller outage survival, topic recreation
SOAK_MINUTES=20 bash scripts/soak.sh      # 5   sustained load through repeated kills
bash scripts/bench-replicated.sh          #     RF=3 acks=all vs RF=1 acks=1
```

All of the above pass on the development host as of this audit.

Client drivers, against a live broker:

```bash
cd clients/go     && go run ./cmd/manualtest      # 38/38
cd clients/nodejs && node test_manual.js          # 38/38
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
