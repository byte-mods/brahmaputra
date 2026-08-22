# Changelog

## 0.3.0 — 2026-08-23

The release that closes the two gaps the parity audit had ranked most
decisive after the wire protocol itself: **transactions** and **JBOD**.
Between them they change which workloads qualify and how much of a broker
one disk can take down with it.

Alongside those, the operator's surface caught up with the control plane:
a broker can now be asked what cluster it is in, what a topic is configured
to do and which disk each partition sits on; records can be deleted without
deleting their topic; byte-rate limits bind to a tenant; leadership returns
to its preferred replica after a restart; and a client can be authenticated
by a certificate rather than a password.

**Breaking:** the wire version is now 3. Broker and clients must be
upgraded together — a version-2 client gets a clean `UNSUPPORTED_VERSION`
rather than misparsing. All four native drivers ship updated.

### Added

- **JBOD: several log directories per broker, one per disk.**

  ```bash
  brahmaputra-server --data-dir /mnt/disk1 --data-dir /mnt/disk2 --data-dir /mnt/disk3
  ```

  Each partition lives on exactly one disk; a new one is placed on whichever
  holds the fewest, so a disk added later fills rather than sitting idle.
  The mapping is rebuilt on startup by scanning the directories themselves,
  so it cannot disagree with the data.

  Capacity is the smaller half of why this matters. **The point is blast
  radius.** With one data directory a disk failure had no partial mode: the
  broker died and every partition it led failed over at once. A failed
  directory now takes only its own partitions — their actors are closed,
  requests for them are refused with a distinct `LOG_DIR_OFFLINE` (never
  "unknown topic", which a client would read as a deleted topic), and
  everything on the other disks keeps serving reads *and* writes.

  Failover needed nothing new. A partition on a dead disk stops fetching,
  and the controller already elects around a replica that stops fetching.
  What was missing was the isolation, not the recovery.

  - Failure is detected from an IO error on any log operation *and* from a
    write-and-fsync probe every 5 s, so a disk that dies under an idle topic
    is caught in seconds instead of whenever something next touches it.
  - An offline directory stays offline until the broker restarts, as in
    Kafka: a disk that appears to recover has usually been remounted,
    possibly having lost the tail of every file on it.
  - `DescribeLogDirs` reports one entry per directory — which is what that
    response was always shaped for — with the failed one marked and its
    reason attached, and the partitions that were on it still listed.
  - `brahmaputra_offline_log_dirs` is the metric to alert on: non-zero while
    the process is perfectly healthy is a state liveness cannot see.
  - A disk **broken at startup** cannot be scanned, so placement is also
    recorded in the first directory and used for exactly one thing: naming
    the partitions on an offline disk, so they are reported unavailable
    rather than silently re-created empty elsewhere — which would be
    indistinguishable from having lost every record.

  Verified live by `scripts/verify-jbod.sh` (18 checks), including a disk
  failing under load while the others keep taking writes, and a broker
  starting with a disk already dead.

- **Transactions and exactly-once semantics.** A producer can write across
  many partitions and decide once whether all of it counts.

  ```bash
  brahmaputra-cli transaction --id orders-etl \
    --send "orders:0=a" --send "audit:0=b"          # both, or neither
  brahmaputra-cli consume --topic orders --isolation-level read_committed
  ```

  Records are appended as they are produced, not buffered until commit —
  buffering a transaction's whole output in the client would put durability
  back in the process least able to provide it. What makes them atomic is
  the marker written afterwards, and the rule that a `read_committed`
  consumer will not look past the first record of a transaction that has
  not been marked.

  The pieces:

  - A **transaction coordinator** sharded over a compacted
    `__transaction_state` topic, found by hashing the `transactional.id` to
    a partition exactly as a group coordinator is — so a client routes to
    it with metadata it already has, and no discovery API can disagree.
  - **Control batches** (attributes bit 5) marking each partition committed
    or aborted, and a **transactional bit** (bit 4) on data batches. Both
    additive: a batch that uses neither encodes exactly the bytes it did
    before, so every existing log decodes unchanged.
  - A per-partition **transaction index** giving the last stable offset and
    the aborted set, journalled to disk so a restart does not rebuild it by
    scanning, and pruned when retention or `DeleteRecords` moves the log
    start.
  - `isolation.level` on the fetch path. A committed read is bounded at the
    LSO and filtered **per batch** — a batch belongs entirely to one
    producer and one transaction, so dropping it needs no decompression.
  - `AddPartitionsToTxn` (23), `AddOffsetsToTxn` (24), `EndTxn` (25),
    `TxnOffsetCommit` (26) and the cluster-internal `WriteTxnMarkers` (27).
  - `TransactionalProducer` in the Rust client.

  The two properties that make it a transaction rather than filtering after
  the fact:

  - **An unfinished transaction blocks committed readers where it starts.**
    Its records exist and the log end has moved past them; a
    `read_committed` consumer still refuses to advance.
  - **A producer that never comes back is resolved by its replacement.**
    `EndTxn` writes its decision to the state log *before* sending any
    marker, so a coordinator that dies mid-commit finishes the markers on
    recovery instead of guessing. The next claim of a `transactional.id`
    fences the previous holder and settles whatever it abandoned.

  Verified live by `scripts/verify-transactions.sh` (17 checks), including
  survival across a broker restart.

- **Preferred-leader election and rebalancing.** Failover walks `replicas`
  in placement order instead of taking the lowest-numbered ISR member, and
  the controller moves leadership back to the preferred replica once it
  rejoins the ISR (`--auto-leader-rebalance-interval-ms`, default 300 s).
  Without both halves, every rolling restart left leadership permanently on
  whichever brokers happened to stay up — the ones already carrying the
  most work.
- **Four administrative APIs**, with `Admin` in the Rust client and
  matching CLI subcommands:
  - `DescribeCluster` (key 19) — brokers, racks, current controller.
  - `DescribeConfigs` (key 20) — what a topic or broker is configured to
    do, marking values that are inherited rather than set. That
    distinction is what decides whether changing a broker flag will move
    them.
  - `DescribeLogDirs` (key 21) — per-partition disk usage, fanned out
    across brokers, so "which topic filled the disk?" stops being a
    question answered by hand on each machine in turn.
  - `DeleteRecords` (key 22) — discard records below an offset and reclaim
    their segments. The only way to reclaim space on a topic retention will
    not touch, and the only answer to "delete this data now" that stops
    short of deleting the topic. Clamped to the high watermark, so it
    cannot discard what the ISR has not committed, and the resulting log
    start offset is checkpointed to disk: a restart must not resurrect
    records somebody deleted.
- **Quota entities.** Byte-rate limits bind to a user, a client id, or
  both, live in replicated metadata so every broker enforces the same
  number, and resolve most-specific-first per direction
  (`brahmaputra-cli quota set|list|delete`). One broker-wide rate meant the
  tenant filling the disk and the tenant reading a topic an hour got the
  same ceiling — set for the worst case, and therefore too loose for
  everyone.
- **Operator TLS certificates and mutual TLS.** `--tls-cert`/`--tls-key`
  present a chain from your own CA instead of one generated at startup;
  `--tls-client-ca` requires a client certificate and binds its subject
  common name to the connection as the principal, so ACLs are enforceable
  with no password crossing the wire. TCP-TLS and QUIC share one identity.
  Client side: `--tls-ca`, `--tls-cert`, `--tls-key`, `--tls-server-name`.
- `brahmaputra-cli offsets --timestamp` resolves the first offset at or
  after a point in time — where a consumer would start a replay.
- `scripts/verify-admin-and-security.sh`: 23 live checks covering all of
  the above against real brokers and real sockets.

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

### Changed

- **The wire version is now 3.** `Fetch` and `FetchMulti` carry an
  `isolation_level`, and `MetadataResponse` carries a request-level
  `error_code` — without which an authorization denial reached the client
  as "unknown topic", sending an operator to look for the wrong thing. All
  four native drivers were updated; the Go and Node.js suites were re-run
  against a version-3 broker and pass 34/34 each.
- `InitProducerId` optionally carries a `transactional.id`, appended after
  the existing fixed fields so a request without one is byte-identical to
  what it always was.
- `ListOffsets` by timestamp consults the time index instead of walking the
  log. It read and CRC-checked every batch from the log start, so asking
  "where was I an hour ago?" cost a read of everything older than an hour.
  Whole segments are now skipped by their newest record and the scan begins
  within one index interval of the answer; the offset returned is identical
  to the one a full walk produced.
- `ApiVersions` advertises all 23 dispatched APIs. It listed 16, omitting
  `ProduceMulti`, `FetchMulti` and `Authenticate` — the multi-partition
  forms being exactly the ones a client is meant to prefer, and
  undiscoverable to any client that trusted the answer.
- Quota accounting is keyed by (user, client id) rather than by client id
  alone, so two tenants shipping the same default `client.id` no longer
  draw down each other's budget.

### Fixed

- The principal derived from a client certificate was read from the
  certificate's **issuer**, not its subject, because an X.509 certificate
  carries the issuer name first and both use the same common-name OID.
  Every client a CA ever signed would have presented as the CA itself, and
  ACLs could not have told two of them apart. Caught by a live test with
  two client certificates from one authority; the self-signed certificate
  the unit test used has issuer == subject and could never have caught it.


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
