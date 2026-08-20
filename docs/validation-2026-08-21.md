# Brahmaputra validation report — 2026-08-21

## Verdict

Brahmaputra passed the exercised core Kafka-semantics, durability,
replication, producer, consumer-group, transport, retention, quota,
authentication, observability, and broker-failover scenarios. No acknowledged
record was lost, duplicated, reordered within a partition, exposed above the
high watermark, or committed with a torn batch in these runs. Replicas were
byte-identical through the committed high watermark after recovery.

This is evidence for the scenarios and loads below, not a proof that data loss
is impossible. The project is Kafka-like, not Kafka-compatible or feature
equivalent. Transactions/EOS, the Kafka wire protocol/ecosystem, tiered and
multi-datacenter replication, and several Kafka administration and group
features remain absent or partial.

The embedded UI was also exercised directly in Safari against a live
three-broker cluster. The pass covered login/logout, overview and charts,
broker/topic/group tables, all-partition and partition-specific message
browsing, filtering, refresh, live tail, partition increase, topic config,
broker-down and recovery states. The destructive delete confirmation was not
submitted in Safari; its authenticated API/RBAC behavior is covered by the M6
run.

## Scope and environment

- Host: Apple arm64, Darwin 25.5.0.
- Rust: `rustc 1.89.0`, `cargo 1.89.0`.
- Container limits for each benchmarked system: 4 vCPU, 4 GiB.
- Benchmark host VM allocation: 6 vCPU, 12 GiB.
- Docker client/server: 29.7.2 / 29.5.2.
- Kafka comparison image: `apache/kafka:4.3.1`.
- Kafka 4.3.1 was the latest Apache Kafka release published when this run was
  performed. Sources: [Apache Kafka downloads](https://kafka.apache.org/community/downloads/)
  and [4.3.1 release announcement](https://kafka.apache.org/blog/2026/06/25/apache-kafka-4.3.1-release-announcement/).
- Brahmaputra encoding: existing BitPacker schemas and generated protocol;
  no BitPacker/schema/protocol file was modified.

## Executed verification

| Area | Result | Principal evidence |
|---|---:|---|
| Full Rust workspace | 231/231 pass | all targets; storage, protocol, broker, client, controller, dashboard, metadata, metrics, server and integration tests |
| M1 core data path | 31/31 pass | concurrent producers, exact values, partitioning, offsets, broker kill, acknowledged-prefix recovery |
| Replication focused run | 14/14 pass; repeated 3 times (42/42) | RF=3 byte preservation, HWM gating, ISR behavior |
| M3 replication/failover | 97/97 pass | idempotence, fencing, two hard-kill storms, sole-ISR leader, divergent-tail repair, extended catch-up, quorum survival |
| M4 consumer groups | 30/30 pass | disjoint assignments, kill/rebalance, uncommitted-tail replay, coordinator failover, committed resume |
| M5 APIs/durability/quotas | 15/15 pass | all 15 advertised APIs, fsync count/time policies and restart, quota throttling without loss |
| M6 dashboard/security | 29/29 pass | login, RBAC denials, forged/expired/cross-node tokens, metrics, Prometheus, cluster views, under-replication visibility |
| Manual Safari UI | pass | authenticated visual flow, message explorer/filter/live tail, topic administration, broker failure/recovery, logout |
| Retention | 21/21 pass | time/size retention, committed-prefix protection, restart behavior |
| Process failures | 15/15 pass | producer killed mid-send, broker killed mid-produce, consumer committed-offset resume, clean restart |
| Broker chaos | 7/7 pass | six kill/restart rounds, 900 acknowledged records, exact offsets, identical replicas, continued writes |
| Transport parity | pass | TCP, TLS-over-TCP and QUIC: data accuracy, durability, ordering, retry and consumer groups |
| Config rejection matrix | 16/16 pass | invalid cluster, broker, transport, producer and consumer combinations rejected before unsafe operation |
| Tight lease live boundary | pass | 3 brokers, 100 ms heartbeat/500 ms session, 30 session windows, RF=3 `acks=all` write and follower-seeded readback |
| Formatting/diff hygiene | pass | `cargo fmt --check`, `git diff --check` |

Clippy was attempted but the pinned `1.89.0-aarch64-apple-darwin`
toolchain does not have the `cargo-clippy` component installed. No Clippy
result is claimed.

### Full workspace composition

The final workspace pass included 21 broker unit tests, 35 broker integration
tests outside replication, 7 replication-manager tests, 19 CLI tests, 14
client tests, 1 client leader-routing integration test, 3 controller unit
tests, 3 three-controller HTTP/restart tests, 8 dashboard tests, 23 metadata
tests, 5 metrics tests, 42 protocol tests, 6 server tests, and 44 storage
tests. Total: 231.

The protocol set explicitly exercised BitPacker-framed request/response round
trips, strict lengths, unknown APIs, truncation at every cut point, CRC
corruption, LZ4, multi-partition messages, replica messages, variable integers,
and 10,000-record batches.

## Data-loss and replication evidence

The strongest run sustained `acks=all` traffic while one replica accumulated
32 MiB of incompressible backlog. Its lag fell from 1,149 offsets to zero.
After catch-up, every assigned replica had persisted HWM 1,158 and the same
committed-prefix digest:

```text
30ef7f6ef96f6e35e10ad79ad08cf1d6cf3f5d8a7d798cd663764e56c27aa889
33,687,763 bytes; 71 batches
```

Two of three replicas were then killed while the controller quorum remained
available. ISR fell to one; `acks=all` was rejected below
`min.insync.replicas`, and the rejected request advanced neither HWM nor LEO.
After one replica returned, production resumed and reused the rejected offset;
the record was readable. After all replicas returned, every copy was
byte-identical through HWM 1,159:

```text
eaf42a94780539d08d0468e40e0f597e6329343299948c679bf9461b669212ec
33,687,834 bytes; 72 batches
```

Additional fault evidence:

- Two forced leader-kill storms each produced three acknowledged writes and 61
  deliberately ambiguous in-flight calls at the kill point.
- An idempotent exact replay returned the original offset; conflicting
  sequence reuse and old producer epochs were fenced.
- A promoted sole-ISR leader reconciled HWM before accepting new traffic.
- A CRC-valid divergent follower tail was truncated/replaced using leader
  epochs.
- The process-failure run recovered 28,653 exact offsets with no torn tail.
- Six broker kill/restart chaos rounds retained all 900 acknowledged records
  without a duplicate, hole, or partition-order violation.
- The manual UI failover run hard-killed broker 2, committed a new RF=3
  `acks=all` record at offset 2 with the two survivors, and then restarted the
  broker. All three dashboard APIs returned the same three partition records,
  and each replica's 239-byte segment had SHA-256
  `168962d0c1a83fda5cc169eccfc9abdde0f4b3051e3045be3616aa15070000f4`.

## Configuration coverage

“Every configuration” is treated as every implemented knob and interaction,
using defaults, representative non-defaults, boundaries, and invalid
combinations; enumerating every integer value is neither finite nor useful.

### Broker/controller

Exercised standalone and clustered modes, fixed peer maps, bootstrap, node and
broker IDs, data/control/HTTP ports, partitions, segment sizing, time/size
retention, retention cadence, message/time fsync policies, produce/fetch
quotas, maximum throttle, authentication, rack metadata, heartbeat/session
timing, replica-lag fencing, offsets-topic partitioning, TCP, TLS-over-TCP, and
QUIC.

Invalid values covered cluster flags without `--node-id`, empty cluster ID,
zero control port, zero heartbeat, session timeout not greater than heartbeat,
zero replica lag, zero offsets partitions, empty/missing/duplicate peer maps,
missing local peer, unrepresentable node IDs, and invalid transport.

The boundary audit found and fixed a real interaction: heartbeat metadata
checkpoints used a fixed one-second coalescing window even for an accepted
sub-second session timeout. The window now derives from the configured
heartbeat/session margin, remains one second at normal settings, and has a
direct boundary unit test. A live three-broker cluster using 100 ms heartbeats
and a 500 ms session also remained healthy for 30 session windows, then
completed an RF=3 `acks=all` write and readback through a follower seed.

### Producer

Exercised `acks=0`, `acks=1`, `acks=all/-1`, timeout, partition selection,
Kafka-compatible Murmur2 keyed partitioning, null/unkeyed round-robin traffic,
batch size, linger, none/LZ4 compression, in-flight limits, concurrent clients,
small and 1 MiB records, idempotent identity allocation, exact replay,
sequence ordering, epoch fencing, leader change, and explicit producer fields.

Invalid modes included unsupported/negative acks, negative timeout,
idempotence with `acks=0`, incomplete explicit producer identity, duplicate or
malformed topic configs, and non-positive topic partition/replication counts.

### Consumer/groups

Exercised standalone earliest/latest/explicit offset reads, per-partition and
all-partition reads, bounded and follow modes, exact committed-offset resume,
range and round-robin assignment, multiple topics, auto-commit on/off,
membership generation fencing, uncommitted-tail redelivery, member death,
coordinator death, and broker failover.

Invalid combinations included a group consumer with manual offset/partition
positioning, assignor options without a group, and unsupported assignors.

## Kafka feature comparison

| Capability | Status | Validation/qualification |
|---|---|---|
| Partitioned segmented append log, offsets, CRC | Present | live and storage/protocol suites pass |
| Leader/follower replication, ISR, HWM | Present | RF=3 failover and byte-digest checks pass |
| ISR-only election and leader-epoch truncation | Present | kill storms and divergent-tail repair pass |
| `acks=0/1/all`, `min.insync.replicas` | Present | live rejection/resume semantics pass |
| Idempotent producer | Present | replay, sequencing, epoch fencing and failover pass |
| Consumer groups and committed offsets | Present | rebalance/coordinator failover/resume pass |
| Retention, compaction, fsync policies, quotas | Present | storage/live policy tests pass; most topic-level overrides remain inert |
| TLS, QUIC, authentication, ACL/RBAC | Present/partial | transport and negative-security cases pass; security model is not Kafka SASL/mTLS |
| Metrics and embedded UI | Present | endpoints and behavior pass; authenticated visual click pass remains blocked |
| Kafka wire protocol and existing Kafka clients | Missing | custom BitPacker data plane; Kafka clients do not connect |
| Kafka Connect, Streams and ecosystem tooling | Missing | consequence of custom wire protocol |
| Transactions / exactly-once transactions | Missing | v1 non-goal |
| Multi-datacenter and tiered storage | Missing | v1 non-goal |
| Cooperative/sticky rebalance, static membership, LeaveGroup | Missing/partial | range/round-robin eager rebalance only |
| Full Kafka administration/config surface | Missing/partial | several APIs and per-topic config effects absent |
| Multi-listener/JBOD/rack-aware placement | Missing/partial | one listener/log dir; rack recorded but not used for placement |

The correct compatibility statement is: **core Kafka storage and delivery
semantics are substantially implemented and passed these tests; Brahmaputra is
not a drop-in Kafka replacement and does not match the complete Kafka feature
surface.**

## Kafka 4.3.1 benchmark

### Method

- Same 4 vCPU/4 GiB container limit for Kafka, Brahmaputra TCP and
  Brahmaputra QUIC.
- Six partitions, RF=1, `acks=1`, no compression, clients inside their own
  broker container, so samples include broker plus client.
- Kafka broker: 2 GiB fixed heap, G1 GC, 20 ms pause target, 35% initiating
  occupancy, 16 MiB G1 regions and metaspace tuning. Kafka performance-test
  client heaps were kept separate at 512 MiB.
- Small records: 256 B, 1,000,000 records per client, 1/2/4 clients,
  65,536-byte batches, 5 ms linger.
- Large records: 1 MiB, 2,000 records, 2 MiB batches, 5 ms linger.

These are tuned same-host system-plus-client measurements, not an exhaustive
search of every Kafka/JVM configuration and not an RF=3 production benchmark.

### 256-byte records, best saturated four-client produce level

| System | Produce msg/s | Avg CPU | Avg memory | Produce vs Kafka |
|---|---:|---:|---:|---:|
| Kafka 4.3.1 | 619,099 | 400.9% | 1,828 MiB | 1.00x |
| Brahmaputra TCP | 1,554,606 | 402.8% | 121 MiB | 2.51x |
| Brahmaputra QUIC | 1,145,803 | 387.7% | 241 MiB | 1.85x |

Kafka consumed 999,500 msg/s at the four-client level. Brahmaputra's reported
TCP/QUIC peaks were 7,827,789 / 3,952,569 msg/s, but those phases completed in
about 0.5 / 1.0 seconds and the one-second resource sampler captured no valid
Brahmaputra CPU/memory sample. Those consume figures are throughput
observations, **not trustworthy CPU-matched ratios**.

### 1 MiB records

Because each record is exactly 1 MiB, the numerical payload rates below are
MiB/s even though the harness report labels them MB/s.

| System | Produce MiB/s | Consume MiB/s | Produce CPU avg | Produce memory avg |
|---|---:|---:|---:|---:|
| Kafka 4.3.1 | 394.87 | 1,273.07 | 213.5% | 1,032 MiB |
| Brahmaputra TCP | 984 | 1,573 | 188.6% | 433 MiB |
| Brahmaputra QUIC | 504 | 433 | 326.1% | 270 MiB |

Kafka disk usage is `n/a` because the harness's Kafka path lookup did not find
the image-owned log directory. No disk number is inferred or fabricated.
Brahmaputra TCP/QUIC used 2,097,230,156 / 2,097,227,186 bytes.

Raw benchmark outputs are in `bench/results/matched/` and
`bench/results/three-way/`; generated summaries are
`bench/results/matched.md` and `bench/results/three-way.md`.

## Defects corrected during validation

- Made Linux `sendfile` conditional and retained the portable buffered fetch
  fallback on macOS, allowing native validation on this host.
- Ensured the default periodic HWM checkpoint runs for user topics even when
  retention/flush/compaction options are otherwise disabled.
- Fixed stale controller leader hints and future broker-epoch heartbeat routing
  after restart.
- Removed controller write-mutex scope across forwarding/quorum waits.
- Prevented short cancellation/retry loops from flooding Raft with ambiguous
  heartbeat writes.
- Batched quorum-confirmed broker heartbeats into durable metadata checkpoints,
  preventing serialized heartbeat writes from collapsing healthy broker
  leases under catch-up load.
- Separated controller maintenance from broker lease renewal.
- Made heartbeat checkpoint coalescing safe for sub-second accepted session
  configurations.
- Made M3/M4 executable discovery and retention byte counting portable across
  Windows/Linux/macOS; made hard-kill storms deterministic on fast loopback.
- Updated all benchmark defaults and tuned Kafka JVM settings for Kafka 4.3.1.
- Made the dashboard message explorer read committed local replicas rather
  than silently omitting follower-led partitions from “all partitions.”
- Fixed the message partition picker to consume the numeric `partitions` field
  returned by the topics API.
- Initialized live-tail cursors from every local replica's high watermark, so
  starting a tail does not replay an existing follower-partition record.
