# Changelog

## 0.7.0 — 2026-09-02

A review release. The code was read against the guarantees it claims, and
what follows is what that turned up: four ways a transaction or a consumer
group could lose its state under a leader change, a file-position race on
every platform without `sendfile`, two client contract breaks, and a set of
hot-path costs that scaled with the wrong thing.

**No wire change and no on-disk format change.** The protocol stays at
version 4 and a 0.6.0 client talks to a 0.7.0 broker. The high-watermark
checkpoint file is folded back to its 8-byte prefix once it grows past
64 KiB; a file written by an older broker is read unchanged, and an older
broker reads a folded one unchanged too.

### Fixed

- **Transaction markers no longer break replication after a leader
  change.** A commit or abort control batch was encoded with leader epoch
  0 while every other batch carried the partition's real epoch. A follower
  that had recorded epoch 1 rejected the marker as non-monotonic, retried
  the same offset forever, and fell out of the ISR — so every transactional
  topic at RF>1 lost durability after its first failover. The marker now
  carries the epoch of the assignment it was written under.

- **Coordinator state is rebuilt when the log moved underneath it.** The
  group and transaction coordinators cached a shard per partition for the
  life of the process. A broker that lost and later regained a
  `__consumer_offsets` partition served the offsets it remembered from
  before — rewinding every consumer of those groups past everything
  committed through the other coordinator in between — and stamped its
  writes with the old leader epoch. Each partition actor now counts its
  *disruptions* (batches replicated from a leader, truncations, resets); a
  shard remembers the count it loaded at and is discarded and replayed
  when it has moved. The stamped epoch is refreshed on every lookup.

- **Transactional offset commits are decided by their marker, on both the
  live path and replay.** `TxnOffsetCommit` appended the records and never
  touched the shard, so `OffsetFetch` kept answering the previous position
  until a restart; replay then read *uncommitted*, so an aborted commit
  became the group's position after a failover. Commits are now parked per
  producer and applied when that producer's commit marker lands on the
  partition, or dropped on its abort marker — the marker writer tells the
  coordinator directly, and replay resolves them the same way as it meets
  the markers.

- **Compaction no longer lets an aborted transaction win.** Pass one took
  the newest record per key regardless of whether its transaction had
  aborted, and the cleanable range was bounded by the high watermark rather
  than the last stable offset. On `__consumer_offsets` an aborted
  transactional commit could supersede — and delete — the committed one.
  Aborted batches are now dropped by both passes, and nothing at or past
  the last stable offset is cleaned. Pinned by tests.

- **The fetch fallback no longer races on a shared file position.**
  Everywhere `sendfile` is not used — Windows, macOS, and the buffered
  paths — the response was produced by *seeking* a segment handle shared
  with the partition actor and then reading it. Two readers interleaved,
  and each got the other's bytes: a batch from the wrong offset, or an
  end-of-file in the middle of a segment. Reproduced with eight threads in
  under a millisecond. Every segment read is now positional, and the
  fallback read runs off the runtime instead of stalling a worker thread.

- **A batched producer retries a moved leader instead of failing the
  caller.** The linger-driven multi-partition flush failed its waiters on
  `NOT_LEADER_OR_FOLLOWER` or a transport error and never refreshed the
  route, so a low-rate producer stayed broken after any leadership move
  until it was restarted. Retriable failures now refresh the route, pay
  the retry backoff, and put the records back at the head of their buffer;
  only records past `delivery.timeout.ms` are failed.

- **Auto-commit no longer acknowledges the batch still being processed.**
  The timer committed the positions advanced when records were *handed
  out*; a crash mid-batch left the unprocessed remainder never redelivered
  — a gap, which this project asserts never happens. The timer now commits
  the positions as they stood at the application's last `poll`, which is
  when the previous batch is known to be done. Explicit `commit_sync`
  still commits everything handed out.

- **A deleted-and-recreated `__consumer_offsets` no longer refuses every
  group request forever.** 0.6.0's topic incarnations refused an actor
  whose recorded incarnation no longer matched the metadata and relied on
  the drain pass to replace it — but the drain pass skipped the offsets
  topic entirely, because coordinator state is not reassignable. The two
  rules together made a recreated offsets topic permanently unusable on
  every broker that had opened it, which is exactly what the consumer-group
  suite does before its first group. Ownership still never drains the
  offsets topic; a replaced incarnation now does. An actor opened before
  the metadata named its topic — the creating node opens an internal topic
  straight away — is settled against the directory marker instead of
  being refused with nothing on record to compare against.

- **The dashboard's message browser cannot spin forever.** An undecodable
  batch broke the inner loop but not the outer one, and the offset never
  advanced past it, so one bad batch turned a browse or a live tail into
  a request that never returned and never stopped reading.

- **A sealed segment's maximum timestamp is right after a restart.** Only
  the tail past the last index entry was scanned, so a segment whose
  newest timestamp sat earlier looked older than it was — and retention
  could delete it early. The time index is consulted first.

### Performance

- **A partition lookup no longer takes the broker-wide lifecycle mutex.**
  Every produce and fetch serialized on one `std::Mutex` that was also
  held across `Log::open`, so one cold partition's recovery scan stalled
  every request on the broker. A running actor is now found with a
  read-only lookup and the same incarnation check.
- **A fetch reads what it asked for.** The storage read loop treated its
  1 MiB chunk size as a floor, so a 64 KiB fetch zero-filled and read a
  megabyte; with a hundred partitions per poll that was a sixteen-fold
  read and memory amplification. The chunk is now a cap.
- **`FetchMulti` waits on watermarks instead of polling.** The long poll
  re-read every partition every 5 ms (200,000 reads per second per idle
  thousand-partition consumer), used the leader-only lookup so a
  rack-redirected consumer never woke, and bypassed the frame budget so a
  burst during the poll could exceed `max_frame_bytes` and kill the
  connection. It now waits on each partition's high-watermark watch and
  re-reads through the budgeted path.
- **The high-watermark checkpoint no longer grows forever.** Every
  advance appended 36 bytes and nothing ever removed one, so an active
  partition added hundreds of kilobytes a day and read all of it on open.

### Known, not yet fixed

Recorded so the list does not only shrink: non-atomic checkpoint writes in
`write_log_start` and the leader-epoch rewrite (truncate-then-write,
remove-then-rename); no timeout on client requests or connects; transaction
coordinator state updated read-modify-write without per-id serialization;
`reconcile_metadata` scanning every follower entry per produce; the
producer-state table re-reading the whole retained log on every retention
tick; the controller persisting the full image JSON per write; the
aborted-transaction index scanned linearly per `read_committed` fetch and
never pruned on compacted topics.

## 0.6.0 — 2026-09-01

The release that stops a controller outage from taking the cluster with it,
stops a deleted topic's records from reappearing under its name, and stops
the wire decoder from reading out of bounds on a malformed frame.

All three were the same kind of mistake in three places: treating an
absence of evidence as evidence. A broker that could not reach a controller
concluded it had been replaced. A partition directory with the right name
was assumed to belong to the topic that now has that name. A decoder that
had bytes assumed it had enough of them.

**No wire change and no on-disk format change.** The protocol stays at
version 4 and a 0.5.0 client talks to a 0.6.0 broker. Each partition
directory gains one 8-byte `.topic-epoch` marker on first open; a directory
without one is adopted in place, so existing logs, indexes and checkpoints
are read unchanged.

### Fixed

- **A controller outage no longer terminates the brokers that survived
  it.** Losing the Raft leader for longer than `--session-timeout-ms` used
  to take down every remaining node: each surviving broker failed to renew
  its lease, treated that as proof it had been superseded, and exited. A
  cluster that should have survived one failure lost all three. This was
  gap 3 in [docs/kafka-parity.md](docs/kafka-parity.md) and reproduced
  identically on 0.3.0.

  Failure to renew is not proof of anything. A broker that cannot reach a
  controller at all learns nothing about whether a newer incarnation of
  itself exists, and the two cases need different answers. Closing this
  took five changes, because the outage exposed the same assumption at
  five layers:

  - An expired lease now **suspends** the data plane: the broker stops
    serving, keeps its listeners, actors and process alive, and retries.
    `validate_local_broker_lease` fails while suspended, so a broker
    without a lease still refuses to serve — the guarantee that made
    self-termination look correct is kept without the exit.
  - `RegisterBroker` takes an optional `expected_epoch`. A suspended
    process re-registers with the epoch it held, and the controller
    refuses the registration if any newer incarnation registered in the
    meantime. A process that is still current resumes; a zombie is
    rejected, and now fails loudly instead of retrying forever. `fence()`
    stays irreversible for the case that *is* proof: an epoch change
    observed while the lease is live.
  - **A controller fence of the current epoch is recoverable, not fatal.**
    Both the heartbeat rejection and the metadata image now distinguish
    "the controller fenced the epoch I hold" — for lateness, with nothing
    newer registered — from "something newer holds my id". The first
    suspends and re-registers; only the second stops the process.
  - **A new controller leader fences nobody for one session timeout.**
    Renewing a lease is a quorum write, so an election is a window in
    which no broker *can* renew, and every timestamp the new leader
    inherits is already stale. Fencing on them fenced the survivors at the
    moment the cluster recovered. Each broker now gets a full session
    timeout to check in first, which is the grace Kafka's controller gives
    after a failover.
  - **Time-bearing commands are stamped when they are proposed, not when
    they are built.** The state machine cannot read a clock, so the caller
    supplies `now_ms` — and a registration that waits seconds for a quorum
    to exist committed with a timestamp already older than the session
    timeout, so the next maintenance pass fenced the broker that had just
    come back. `RegisterBroker` and `Heartbeat` are re-stamped on each
    attempt.
  - **A partition whose whole ISR was fenced can be recovered by any
    replica that was in it.** `PartitionMetadata::last_isr` records the
    in-sync set as it stood when the partition lost the last member of it,
    and a returning replica from that set is elected on registration.
    In-sync means it holds every committed record, so this is a clean
    election on the same terms Kafka uses — 0.5.0 could only recover the
    single-replica case. A replica that had already fallen out of the ISR
    is still not eligible: believing that one can discard acknowledged
    writes, which is what unclean election means, and it stays an explicit
    decision.

- **A deleted topic's data can no longer be served under a recreated
  topic's name.** Topic names are reusable and a partition directory had no
  durable link to the incarnation that created it, so recreating `orders`
  reopened the deleted `orders-0` log — with its offsets, its records and
  its watermark — and served an operator's deleted data back to consumers.

  - `CreateTopic` stamps a monotonic `topic_epoch` (the metadata offset
    that created it) into `TopicMetadata`. It is `#[serde(default)]`, so an
    existing Raft log still means what it meant.
  - Brokers persist that epoch in `.topic-epoch` beside each replica and
    sync it before the log can be opened. A mismatch discards the stale
    directory; an unmarked directory predates the marker and is adopted in
    place; an epoch-zero image is never allowed to replace a marked
    directory.
  - `open_partition` refuses a partition whose open incarnation no longer
    matches the metadata, so a recreated topic waits for the stale data to
    be removed rather than briefly serving it.
  - Partition deletion now shuts the actor down cleanly — checkpointing the
    high watermark and closing every file — before removing the directory,
    and a removal that fails is retried instead of leaking the directory
    forever. That also stops a transient Windows sharing violation from
    surfacing as a whole-disk failure.

- **Malformed frames are rejected instead of read out of bounds.** The
  generated BitPacker decoder read varints, booleans and lengths through
  `get_unchecked`, on a comment that said the data source was trusted. The
  data source is the network, which made every unauthenticated frame a
  potential out-of-bounds read.

  - Every read is bounds-checked and raises a decode-error flag that each
    generated `decode` checks before it returns. Overlong varints, negative
    and impossible collection lengths, and invalid UTF-8 are errors rather
    than things to follow.
  - The generator in `tools/bit-packer` emits the checked form:
    `scripts/gen-protocol.sh` reproduces the checked-in
    `crates/protocol/src/gen/*` byte for byte.
  - Tests assert that every truncated prefix of a response, every
    single-byte payload, and every hostile length is an error and not a
    panic.

- **A producer no longer gives up on a topic the broker has not heard of
  yet.** Topic creation is a controller write that reaches brokers
  asynchronously, so a send that closely follows `CreateTopic` could fail
  outright with `UNKNOWN_TOPIC_OR_PARTITION`; leadership moves produced the
  same answer briefly. That code is now retriable — like every other
  metadata-staleness code, and like Kafka classifies it — and a retry
  refreshes the route first. It is also honoured when the staleness is the
  client's own: a topic missing from the client's metadata is retried
  rather than raised immediately. A topic that genuinely does not exist
  still fails, after the retry budget, with the broker's own code.

- **A peer can no longer grow broker memory by rotating `client.id`.**
  Quota buckets were keyed by the client-supplied string and kept for the
  life of the process. They are now keyed by a per-broker seeded hash — no
  attacker-controlled string is retained, and collisions cannot be
  manufactured — and the table is capped at 4,096 identities. Eviction can
  grant a churned identity a fresh one-second burst; it can never lose or
  corrupt a request.

### Verification

`scripts/verify-bugfixes.ps1` starts three combined nodes, hard-kills the
Raft leader and leaves it down, and asserts that the survivors stay alive,
elect a leader, renew their leases and accept `acks=all` writes. It then
kills a second node and asserts the last one suspends rather than exits and
refuses to serve without a lease, and that restoring a peer lets it
re-register and resume. It also deletes and immediately recreates a
replicated topic and asserts every replica discarded its old incarnation
marker and that only the fresh record is readable.

## 0.4.0 — 2026-08-23

The release that finishes log compaction and makes `transaction.timeout.ms`
mean something. Both were features the system claimed to have: a compacted
topic could not delete a key, and a transaction timeout was a number the
client sent and nothing enforced.

Around those, the gaps the parity audit had ranked below the wire protocol
are closed: consumers can read from a replica in their own availability
zone, a fetch no longer resends a thousand partition descriptors to say
nothing changed, authentication works on a plaintext listener, brokers can
talk to each other on their own listener, and a partition can be moved
between a broker's disks.

**Breaking:** the wire version is now 4. Broker and clients must be
upgraded together — a version-3 client gets a clean `UNSUPPORTED_VERSION`
rather than misparsing a tombstone's length prefix and losing every record
after it. All four native drivers ship updated.

**Nothing on disk changes shape.** Existing logs, indexes, checkpoints and
transaction journals are read unchanged: a batch without a tombstone
encodes to exactly the bytes it did before, which is why the record format
gained an attributes bit rather than a magic bump.

### Fixed

- **Log compaction can now delete a key.** A record's value may be null —
  a tombstone — and that is what removes a key from a compacted topic.

  Before this, `Record.value` was `Bytes` with no null form, so compaction
  kept the newest record for every key that had ever existed and a
  compacted topic's key space could only grow. Half of what
  `cleanup.policy=compact` means in Kafka was missing, and the group
  coordinator worked around it with an application-level marker that
  compaction could not act on.

  - A tombstone is delivered to consumers as a null value, distinct from an
    empty one, because on a compacted topic the deletion *is* the event a
    consumer needs to see.
  - It is itself removed once `delete.retention.ms` (default 24 h) has
    passed, which is the window a consumer has to observe the deletion.
  - `brahmaputra-cli produce --key k --tombstone` writes one;
    `value=null` is how the consumer prints it.
  - `__consumer_offsets` group expiry now writes real tombstones, keyed to
    the records they delete. It previously wrote one marker under a
    different key, which replay understood and compaction ignored — so the
    offsets topic grew forever with groups that no longer existed.

- **`transaction.timeout.ms` is enforced.** The coordinator sweeps for
  transactions that have outlived it, fences the producer and aborts them.

  Before this the timeout was stored and never read. A producer that died
  mid-transaction — scaled down, redeployed under a different
  `transactional.id`, crashed for good — left records in doubt on every
  partition it had written to, and the last stable offset on those
  partitions never advanced past them. Every `read_committed` consumer
  stopped there, permanently, and the only thing that would ever resolve it
  was the same `transactional.id` being claimed again.

  - The producer is fenced before the abort, so one that is merely slow
    cannot carry on writing into a transaction that has already been
    marked aborted.
  - A transaction left in `Prepare*` — decision durable, markers not all
    written — has its markers re-sent rather than waiting for the next
    `InitProducerId`.
  - The sweeper loads the coordinator shards it leads, so a failover
    resolves what the previous coordinator left open without anyone asking.
  - `--transaction-max-timeout-ms` (15 min) clamps what a client may ask
    for; `--transactional-id-expiration-ms` (7 days) retires idle ids,
    writing a tombstone so their state stops occupying disk.

- **Compaction no longer destroys batching, compression or producer
  metadata.** Every surviving record used to be rewritten as its own
  single-record batch with compression dropped, producer identity erased
  and control batches turned into ordinary records — so compacting a
  transactional or idempotent topic corrupted exactly the state that made
  it one.

  Contiguous survivors are now re-emitted as one batch carrying the codec,
  producer metadata and transactional/control flags they were written with.
  Offsets are still preserved exactly; compaction leaves gaps rather than
  renumbering anything.

- **Compaction is crash-safe.** A pass writes its output to a staging
  directory, records a commit marker, then swaps. Interrupted before the
  marker, the output is discarded and the original log is untouched;
  interrupted after it, the swap is completed on the next open. Previously
  a crash between deleting the old segments and moving the new ones in
  would have lost the records that survived the pass.

- **Compaction no longer moves the log start offset.** Removing the record
  at offset 0 does not make offset 0 out of range: a consumer reading a
  compacted topic from the beginning gets the oldest record that still
  exists, as in Kafka. Only retention and `DeleteRecords` move the start.

### Added

- **Compaction has the settings that decide when it runs.**
  `min.cleanable.dirty.ratio` (0.5), `min.compaction.lag.ms`,
  `max.compaction.lag.ms` and `delete.retention.ms`, per topic and as
  broker-wide defaults. Without the dirty ratio a pass ran on every
  maintenance tick and rewrote the whole cleanable log to remove a handful
  of records, which is how compaction becomes the dominant write load on a
  partition that is barely changing.

- **Follower fetching (KIP-392).** A consumer that sets `client.rack` is
  told by the leader which in-sync replica in its own rack to read from,
  and reads from that instead.

  ```bash
  brahmaputra-cli consume --topic orders --rack us-east-1a
  ```

  Only in-sync replicas are ever named, and a consumer already in the
  leader's rack is not redirected — trading a fresher read for nothing.
  Any error forgets the redirect and goes back to the leader.

- **Incremental fetch sessions (KIP-227).** A consumer holding a thousand
  partitions of which three are moving now resends three descriptors
  instead of a thousand. The client stays the authority on where it is
  reading: a broker that has forgotten a session answers
  `FETCH_SESSION_NOT_FOUND` and the client sends a full fetch again, so the
  failure mode is one wasted round trip rather than a consumer reading from
  the wrong offset.

- **SASL/SCRAM-SHA-256.** The password never crosses the wire, which is
  what makes authentication meaningful on a plaintext listener — where
  PLAIN is, correctly, still refused.

  ```bash
  brahmaputra-cli --sasl-username alice --sasl-password ... consume --topic orders
  ```

  Every connection a client opens authenticates, not just the first: a
  pooled connection created after a reconnect would otherwise be anonymous.
  Credentials are derived when a password is set, so a user created before
  this cannot use SCRAM until their password is set again — a SCRAM
  credential cannot be back-derived from a hash, which is the point of a
  hash.

- **An inter-broker listener.** `--internal-port` and `--internal-tls` bind
  a second data-plane listener that replication and transaction markers
  use, so a cluster can present TLS to its clients and speak plaintext to
  itself on a private network — without encrypting every record two or
  three more times to reach the followers.

- **`--advertised-host` and `--advertised-port`.** What a broker publishes
  in metadata, when it differs from what it bound. A broker behind NAT, in
  a bridged container network, or on a Kubernetes pod IP used to publish
  the address it bound and send every client somewhere unreachable.

- **`AlterConfigs` (key 28).** Change a topic's configuration over the data
  plane. The broker forwards to the controller, so durability and ordering
  are unchanged; what changes is that one connection is enough to both read
  and write a topic's configuration. An unknown config name is refused
  rather than stored and ignored.

  ```bash
  brahmaputra-cli alter-configs --topic orders --config retention.ms=604800000
  ```

- **`DescribeProducers` (key 29), `ListTransactions` (key 30),
  `DescribeTransactions` (key 31).** The answer to "why has my
  `read_committed` consumer stopped?" — the gap between the last stable
  offset and the high watermark is the stall, and the producer holding it
  is named.

  ```bash
  brahmaputra-cli describe-producers --topic orders --partition 0
  brahmaputra-cli list-transactions
  ```

- **`AlterReplicaLogDirs` (key 32).** Move a partition between a broker's
  disks. JBOD placed partitions once, at creation, so a disk added to a
  running broker took only new partitions and a filling disk could only be
  relieved by deleting a partition and letting it re-replicate. The
  partition is closed while its bytes are copied, so this is an explicit
  operator action rather than something the broker does on its own.

- **`heartbeat.interval.ms`** is settable independently of the session
  timeout. Deriving one from the other forced a fleet that wanted a
  generous failure-detection window to also accept being blind for a third
  of it.

- **`offsets.topic.replication.factor`.** The internal topics are created
  once, when the first nodes register, so a three-node cluster whose other
  two nodes had not started yet would pin committed offsets to a single
  broker forever.

- **`message.timestamp.type=LogAppendTime`**, so retention and timestamp
  seeks stop depending on a client's clock. The batch timestamp is
  overwritten in place and the CRC recomputed — the records are never
  decompressed.

- **`compression.type` per topic**, enforced by refusing a batch in another
  codec rather than by recompressing it. The guarantee is the same and the
  producer is told what to send instead of having its data silently
  rewritten.

- **`--max-frame-bytes`, `--index-interval-bytes`, `--max-message-bytes`,
  `--cleanup-policy`** and the compaction defaults, as broker flags. Three
  of these were hard-coded constants the parity audit had listed as
  unreachable.

### Changed

- `Record.value`, `FetchedRecord.value` and `ConsumedRecord.value` are now
  `Option<Bytes>`. `None` is a tombstone.
- `LogConfig` gained the compaction and timestamp settings and is no longer
  `Eq` (a dirty *ratio* is a ratio, as it is in Kafka).
- `Log::compact` returns a `CompactionOutcome` describing what the pass did
  rather than a bare count.
- `ApiVersions` advertises 33 APIs, up from 28.

### Also fixed, found while verifying the above

- **A node that lost the internal-topic creation race could die at
  startup.** Several nodes attempt to create `__consumer_offsets` and
  `__transaction_state` at once and all but one is expected to lose. The
  loser confirmed the topic exists by reading its *own* Raft copy, which
  trails the leader that just refused the create — so a single look could
  miss a topic that certainly existed, and the node exited. It now waits
  for its copy to catch up before treating the failure as real.

- The transaction-expiry sweep observes partitions that are already open
  rather than opening them. A background timer that creates partition
  actors as a side effect is a timer that does I/O for topics nothing has
  used.

### Known, unchanged, and not from this release

A controller node that *stays* down takes the surviving brokers with it:
with `--session-timeout-ms 3000` they cannot renew their lease before it
expires and self-terminate. Killing and restarting that node — a rolling
restart, or a crash with a supervisor — is handled cleanly, which is why
the soak survives repeated kills of it. Reproduced identically on 0.3.0, so
this release neither causes nor fixes it; recorded in
[docs/kafka-parity.md](docs/kafka-parity.md) §9 with the reproduction.

### Verification

```bash
cargo test --workspace                    # 389
bash scripts/verify-compaction.sh         # 17  tombstones, superseding, horizons
bash scripts/verify-transactions.sh       # 24  commit, abort, in doubt, expiry
bash scripts/verify-admin-and-security.sh # 29  admin APIs, quotas, mTLS, SCRAM
bash scripts/verify-jbod.sh               # 26  placement, disk failure, moves
SOAK_MINUTES=3 bash scripts/soak.sh       # 5   sustained acks=all through kills
cd clients/go     && go run ./cmd/manualtest      # 38/38
cd clients/nodejs && node test_manual.js          # 38/38
```

Produce throughput was measured against 0.3.0 on the same host: RF=1
394k → 423k msgs/sec, RF=3 `acks=all` 344k → 363k. Group consume 295k →
302k. No path regressed.


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
