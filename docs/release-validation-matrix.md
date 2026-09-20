# Release validation matrix

The matrix is finite: passing it does not prove every combination of broker
flags, operating systems, storage devices and workloads. Keep correctness
results separate from Kafka throughput comparisons. An empty or missing result
is not a pass. The [0.8.1 release review](release-0.8.1-review.md) records actual
execution results, including failures and their retests.

| Area | Coverage | Executable evidence |
| --- | --- | --- |
| Replication and controller recovery | RF=3, min ISR, idempotence, fencing, quorum loss, follower outage, recovery, committed-prefix identity | `verify-m3.sh`, `verify-replication.sh`, controller integration tests |
| Message survival | Producer kill, broker kill during append, consumer kill, acknowledged-record audit, restart | `verify-failures.sh`, `verify-chaos.sh`, `soak.sh` |
| Consumer groups | New member, member expiry, eager/cooperative reassignment, committed offsets, earliest/latest reset, poll timeout | `group_consumer_e2e.rs`, `group_coordinator.rs`, `verify-m4.sh` |
| Transactions | Commit, abort, pending transactions, read-committed group consumers, small fetch budgets | `verify-transactions.sh`, broker and client tests |
| Transport | TCP and QUIC ordering, payload identity, recovery, groups, idempotent retries | `verify-transport-parity.sh` |
| Security and configuration | TLS/mTLS, SCRAM, ACLs, quotas, broker describe, topic alter, invalid settings | `verify-admin-and-security.sh`, authentication tests |
| Storage lifecycle | Retention, compaction, tombstones, directory failures, reassignment | `verify-retention.sh`, `verify-compaction.sh`, `verify-jbod.sh`, `verify-reassignment.sh` |
| Dashboard | Authentication, RBAC, metrics, cluster health, embedded assets, analytics controls | `verify-m6.sh` |
| Dashboard JavaScript | Counter reset, repeated timestamps, time windows, sparse charts, polling cleanup, stale status, pause | `node --test scripts/test-dashboard.cjs` |
| Kafka performance | RF=1/3, 1/2/4 clients, TCP/QUIC 1/2/4/8 clients, large records, all codecs, acks=0/1/all, idempotence, offered-rate limit | `bench-release.sh` |

Run `cargo test --workspace --locked -- --test-threads=1`, formatting and Clippy first. Build debug
and release server/CLI binaries before `bash scripts/verify-release.sh`. Run
`bash scripts/bench-release.sh` after correctness checks, without concurrent
builds or other benchmark workloads. Each harness preserves reports and raw
client counts. Shared-host interference must be disclosed. Benchmark throughput
is not evidence of message survival during failures; use the explicit audits.

Broker startup flags are described in the README configuration table and
`brahmaputra-server --help`. Broker settings are read-only through the admin API;
supported topic settings can be changed dynamically. This matrix exercises
representative configurations and boundary cases, not the Cartesian product
of all flag values. Kafka security, transactions, disk failure and failover
performance are not measured by the current comparative matrix.
