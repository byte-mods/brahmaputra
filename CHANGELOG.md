# Changelog

## Unreleased

### Added

- `brahmaputra-cli produce --latency` reports acknowledgement-latency
  percentiles (`avg`/`p50`/`p95`/`p99`/`p99.9`/`max`) by nearest rank, so a
  reported percentile is a wait some record actually experienced. Behind a
  flag: the throughput line above it is parsed positionally by the verify
  scripts.
- `brahmaputra-cli produce --rate` offers records at a fixed rate rather
  than as fast as the broker accepts them, the analogue of
  `kafka-producer-perf-test --throughput`. Latency measured at saturation
  is queue depth divided by throughput — Kafka's own saturated run reports
  a 1,185 ms p50 for that reason — so a bounded offered rate is the only
  way to measure what an `acks=all` caller actually waits for.
- `RATE` and `BRAHMA_IMAGE` in `scripts/bench-replicated-vs-kafka.sh`, the
  latter so a before/after can be taken against an image built from an
  older commit without touching the working tree.

### Measured

The 0.2.0 long-poll fix cut RF=3 `acks=all` **median commit latency from
67.4 ms to 5.4 ms (12.5×) and p95 from 96.8 ms to 11.6 ms (8.4×)** at
20,000 records/sec offered. The pre-fix median is the 50 ms poll interval
plus overhead. RF=1 latency is unchanged — 4.34 ms to 4.33 ms p50 — which
is the control that attributes the gain to the replication path. The fix
costs CPU at low offered rates (160 % to 394 % for the cluster), because
followers are woken per append rather than coalescing 50 ms of them.

A hundreds-of-milliseconds RF=3 p99 remains, in Kafka and in the pre-fix
build as well as this one, clean at RF=1 in both systems, and unstable
across runs. No p99 claim is made for either system; see
`docs/replicated-benchmark-2026-08-22.md` §4a.

## 0.2.0 — 2026-08-22

The first release measured against Kafka in the configuration a durable
deployment actually runs: three brokers, RF=3, `acks=all`,
`min.insync.replicas=2`. Building that benchmark found two defects, and
fixing them changed the result from a 3.19× loss to a 3.40× win.

### Fixed

- **Follower fetches no longer poll.** A follower learned about new appends
  only by asking the leader again, and its loop slept 50 ms between empty
  answers. Under `acks=all` the high watermark cannot advance until
  followers have fetched, so every producer waited out that sleep before
  its record could commit, and the cluster ran at the polling interval
  rather than at the speed of the log. Leaders now hold a caught-up
  follower's fetch until an append arrives or 500 ms passes.

  The wait is on a new log-end-offset watch rather than the high watermark:
  under `acks=all` the watermark cannot advance until *this* follower
  fetches, so waiting on it would be waiting on itself. Leadership is
  re-validated after the wait, so a broker demoted while the request was
  parked cannot serve batches under an epoch it no longer owns. No
  wire-format change — `ReplicaFetchRequest` carries no client-chosen wait,
  so the hold is the leader's own policy.

  RF=3 `acks=all` produce: **106 077 → 787 385 msgs/sec**. Replication cost
  **13.35× → 1.47×**, against Kafka's 3.18×. Native, without Docker:
  28 568 → 299 013 msgs/sec.

- **Broker addresses are resolved once, not per send.** The client called
  `lookup_host` on every send and never cached the result — a measured
  6 418 lookups to produce 2 000 records. Each was individually fast, but
  every one went to tokio's blocking pool. `BrokerEndpoint::resolve`
  short-circuits on an IP literal, so only name-advertised clusters paid
  it, which is every Kubernetes or Compose deployment. Resolved addresses
  are now cached with a 30 s TTL and dropped when the connection to that
  address is invalidated, so a broker returning at a new IP is re-resolved
  immediately.

  400 000 records to a name-advertised cluster: **6.22 s → 1.45 s**,
  matching the IP-advertised path.

### Added

- `scripts/bench-replicated-vs-kafka.sh` — Kafka and Brahmaputra measured
  head to head at RF=3 `acks=all` and at RF=1 `acks=1` on the same
  three-node clusters, with replication asserted at every level: Kafka must
  report ISR=3 on all partitions, and Brahmaputra must hold partition logs
  on all three nodes with log end offsets summing exactly to the records
  produced.
- `docs/replicated-benchmark-2026-08-22.md` — the full report, including
  two findings that are **not** fixed: a saturated broker can miss its
  controller heartbeat and exit the process, and `acks=all` latency
  percentiles remain unmeasured.

### Changed

- README and `docs/kafka-parity.md` now carry the replicated numbers.
  The previously published **5.7× consume figure has been withdrawn**: it
  compared an uncoordinated reader against a Kafka consumer group, and
  measured group-to-group the two systems land within about 17 %. The
  produce figures survive matched methodology; that one did not.
- Kafka is measured on longer runs, because it gains up to 40 % from JIT
  warmup between 500k and 2M records per client. Short-run comparisons
  understate it.

### Known issues

- A broker that misses its controller heartbeat exits the process. Under
  enough load — the heartbeat is a durable metadata write competing for the
  same disk as replication — a saturated broker can trigger this, and
  because load arrives everywhere at once, brokers can go together.
  Surviving a lost lease means supporting several incarnations per process,
  which `Broker::fence` and `activate_broker_epoch` deliberately prevent
  today. Not fixed in this release; see the report §6a.
- No Kafka wire-protocol compatibility, no transactions or exactly-once
  semantics, no tiered storage, no JBOD. See `docs/kafka-parity.md`.

## 0.1.0

Initial release: partitioned append-only logs, Raft control plane,
leader/ISR replication with leader-epoch truncation, consumer groups,
compaction, retention, quotas, TLS, authentication and ACLs, an embedded
dashboard, and TCP, TLS 1.3 and QUIC transports.
