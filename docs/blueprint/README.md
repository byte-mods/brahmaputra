# Brahmaputra Blueprints

How the internals are built and why. Each document is design-first and gets
reconciled against the actual code at the end of its milestone (see the
Verification checklist at the bottom of each doc).

| # | Document | Milestone | Covers |
|---|----------|-----------|--------|
| 01 | [Storage & record batches](01-storage-and-protocol.md) | M1 | batch format, segments, sparse index, crash recovery, retention |
| 02 | [Broker data plane & client flows](02-broker-data-plane.md) | M1 | wire framing, partition actors, backpressure, produce/fetch |
| 03 | [Control plane: Raft metadata](03-control-plane-raft.md) | M2 | metadata log, broker lifecycle, elections, epoch fencing |
| 04 | [Replication & zero-loss failover](04-replication.md) | M3 | HW, ISR, acks, leader-epoch truncation, rejoin catch-up |
| 05 | [Consumer groups](05-consumer-groups.md) | M4 | `__consumer_offsets`, coordinator, rebalancing, commits |
| 06 | [Metrics, dashboard & access](06-observability-dashboard.md) | M6 | instrumentation, time-series store, HTTP API, RBAC |

Top-level architecture and rationale: [`../../DESIGN.md`](../../DESIGN.md).

## M5 hardening

Verified by the scripts in `scripts/`, each of which fails loudly on any
missed assertion:

| Script | Covers | Checks |
|---|---|---|
| `verify-retention.sh` | time and size retention, restart survival, a group whose committed offset falls off the log | 21 |
| `verify-replication.sh` | RF=3 byte-identical replicas, ISR failover, restarted-broker resync, `min.insync.replicas` | 14 |
| `verify-failures.sh` | producer / broker / consumer killed mid-operation, plus a data-loss sweep | 15 |
| `verify-m5.sh` | fsync policies, client quotas, API version negotiation | 15 |
| `verify-transport-parity.sh` | the same correctness checks over tcp, tcp-tls and quic | 18 |
| `verify-chaos.sh` | continuous `acks=all` produce while brokers are killed and restarted at random | 6 |

`bench-vs-kafka.sh`, `bench-three-way.sh` and `bench-tune-brahmaputra.sh`
produce the numbers in [`../benchmarks.md`](../benchmarks.md).
