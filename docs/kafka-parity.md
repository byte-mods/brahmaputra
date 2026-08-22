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
| Go | updated for wire v3, verified against a live v3 broker, 34/34 |
| Node.js | updated for wire v3, verified against a live v3 broker, 34/34 |
| Python | updated for wire v3, still never executed (no interpreter on the build host) |
| Java | updated for wire v3, still never compiled (no JDK on the build host) |

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
| Authenticate | ✅ key 17 | SASL/PLAIN-equivalent; refused on a plaintext listener |
| **LeaveGroup** | ✅ key 18 | **added since the last audit** |
| LeaderAndIsr / UpdateMetadata | 🟡 | equivalent effect via Raft metadata subscription |
| **DescribeCluster** | ✅ key 19 | brokers, racks, current controller |
| **DescribeConfigs** | ✅ key 20 | topic and broker resources; marks inherited defaults |
| **DescribeLogDirs** | ✅ key 21 | per-partition disk usage, fanned out across brokers |
| **DeleteRecords** | ✅ key 22 | leader-only, clamped to the high watermark |
| AlterConfigs | 🟡 | `SetTopicConfig` through the controller, not a data-plane API |
| **AddPartitionsToTxn** | ✅ key 23 | announced before the first write to a partition |
| **AddOffsetsToTxn** | ✅ key 24 | brings a group's offsets into the transaction |
| **EndTxn** | ✅ key 25 | prepare, mark every partition, complete |
| **TxnOffsetCommit** | ✅ key 26 | to the group coordinator, as transactional records |
| **WriteTxnMarkers** | ✅ key 27 | cluster-internal, as in Kafka |
| FindCoordinator | 🟡 | not needed: a client hashes the id to a coordinator partition and routes to its leader, exactly as it does for a group |

The wire version is **3**. The broker requires an exact match, so an older
client gets `UNSUPPORTED_VERSION` rather than misparsing. Version 3 added
`isolation_level` to `Fetch`/`FetchMulti` and a request-level `error_code`
to `MetadataResponse`; all four native drivers were updated with it, and
the Go and Node.js suites were re-run against a version-3 broker (34/34
each).

`ApiVersions` now advertises all **28** dispatched APIs. It previously
listed 16, omitting `ProduceMulti`, `FetchMulti` and `Authenticate` — the
multi-partition forms being precisely the ones a client is supposed to
prefer, and undiscoverable to any client that trusted the answer.

## 3. Broker configuration

| Kafka config | Kafka default | Brahmaputra | Status |
|---|---|---|---|
| `broker.id` | — | `--node-id` | ✅ |
| `listeners` / `advertised.listeners` | — | `--host` + `--port` | 🟡 single listener, no security protocol map |
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
| `offsets.topic.replication.factor` | 3 | derived from cluster size | 🟡 not configurable |
| `group.initial.rebalance.delay.ms` | 3 s | fixed 1 s | ✅ not tunable |
| `group.min/max.session.timeout.ms` | 6 s / 30 min | clamped to 1 s / 30 min | ✅ not tunable |
| `num.network.threads` / `num.io.threads` | 3 / 8 | tokio runtime + per-partition actor | ✅ different model, same effect |
| `socket.request.max.bytes` | 100 MiB | `max_frame_bytes` (32 MiB) | 🟡 no flag |
| `log.index.interval.bytes` | 4096 | `LogConfig` | 🟡 no flag |
| `broker.rack` | — | `--rack`, used for replica placement | ✅ |
| `auto.create.topics.enable` | true | standalone on, cluster off | ✅ |
| `quota.producer.default` / `.consumer.` | — | `--quota-*-bytes-per-sec`, plus per-user / per-client-id entities | ✅ throttles by delaying the ack |
| `replication.quota.*` | — | `--quota-replication-bytes-per-sec` | ✅ |
| `ssl.*` | — | `--transport tcp-tls` / `quic`, `--tls-cert` / `--tls-key` / `--tls-client-ca` | ✅ operator certificates and mutual TLS |
| `sasl.*` | — | `--require-auth` + ACLs, or a client certificate's subject | 🟡 no SASL mechanism negotiation |

## 4. Topic configuration

**Applied per topic**: `retention.ms`, `retention.bytes`, `segment.bytes`,
`segment.ms`, `cleanup.policy`, `flush.messages`, `flush.ms`,
`max.message.bytes` and `min.insync.replicas` all change broker behaviour.
An unparseable value falls back to the broker-wide default rather than to
zero, so a typo cannot delete a log.

Configs are **not** fixed at creation, contrary to what this section said
for two revisions: `MetadataCommand::SetTopicConfig` changes them on a
running cluster and §7 has recorded the live-reload path working the whole
time. What is missing is a data-plane `AlterConfigs` — changes go through
the controller. Reading them back is now `DescribeConfigs` (key 20), which
distinguishes a value that was set from one merely inherited.

Still missing: `compression.type` per topic. The producer chooses the
codec and the broker stores the batch byte-identically, which is what keeps
replication and the zero-copy fetch path free; enforcing a topic-level
codec would mean the broker decompressing and recompressing every batch.

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
| `heartbeat.interval.ms` | 3 s | derived | 🟡 not independently configurable |
| **`max.poll.interval.ms`** | 5 min | **300 s** | ✅ *new* |
| **`group.instance.id`** | — | **supported** | ✅ *new*, static membership |
| `fetch.min.bytes` / `fetch.max.wait.ms` | 1 / 500 | same | ✅ |
| `max.partition.fetch.bytes` | 1 MiB | `max_bytes` (8 MiB) | 🟡 one knob |
| **`auto.offset.reset`** | latest | **earliest/latest/none** | ✅ *new*, default differs |
| `isolation.level` | read_uncommitted | `read_uncommitted` / `read_committed` | ✅ LSO-bounded, aborted records and markers withheld |
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
| **Preferred-leader election and rebalancing** | ✅ failover follows replica order; leadership returns on `--auto-leader-rebalance-interval-ms` | ✅ |
| **Live topic-config changes** | ✅ pushed to running partitions | ✅ |
| **Administrative introspection** | ✅ DescribeCluster / DescribeConfigs / DescribeLogDirs | ✅ |
| **DeleteRecords** | ✅ leader-only, checkpointed log start offset | ✅ |
| Consumer groups, coordinator failover by log replay | ✅ verified live | ✅ |
| Rebalance protocol | ✅ eager and cooperative (KIP-429) | ✅ |
| **Offset expiry** | ✅ *new* | ✅ |
| Log compaction | ✅ | ✅ |
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
   the range and writes it — correct, just not free.
4. No SASL mechanism negotiation. Password authentication is a single
   custom `Authenticate` exchange: no SCRAM, GSSAPI or OAUTHBEARER, and no
   delegation tokens. Certificate authentication is the stronger option
   and is available today.
5. Single listener: no `advertised.listeners`, no security-protocol map, so
   a broker cannot offer plaintext internally and TLS externally.
6. No cross-directory rebalancing. A disk added to a running broker takes
   only *new* partitions, and there is no `AlterReplicaLogDirs` to move an
   existing one between disks without deleting and refetching it.

**Efficiency at scale**

7. No follower fetching (`client.rack`), so every consumer read crosses
   AZs.
8. No tiered storage: retention is bounded by local disk.
9. No incremental fetch sessions (KIP-227), so per-fetch metadata cost
   grows with partition count.
10. `heartbeat.interval.ms` is derived from the session timeout rather
    than set independently.
11. A `read_committed` fetch cannot use the zero-copy path: choosing which
    batches to withhold means reading their headers, and the point of
    handing the kernel a file range is that nobody reads them. Filtering is
    per batch rather than per record, so this costs a copy and never a
    decompression.

## 9a. Closed since the last audit

Recorded because this document has drifted before, and a gap list that
only ever grows is not being read against the code.

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
cargo test --workspace                    # 355
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
bash scripts/verify-admin-and-security.sh # 23  admin APIs, quotas, mTLS principals
bash scripts/verify-transactions.sh       # 17  commit, abort, in doubt, recovery
bash scripts/verify-jbod.sh               # 18  multi-disk placement and disk failure
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
