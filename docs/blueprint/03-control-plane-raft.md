# Blueprint 03 — Control Plane: Raft Metadata, Broker Lifecycle & Elections

> Status: implemented and reconciled at the end of M2 against the actual
> `metadata`, `controller`, `broker`, `server`, `client`, and `cli` crates.

This document explains how cluster-wide state — which brokers exist, which
topics exist, who leads every partition — is kept consistent without any
external system (no ZooKeeper, no etcd).

---

## 1. One Raft group for all metadata

A **controller quorum** of 3–5 nodes runs a single Raft group (via
`openraft`). The Raft log *is* the metadata: every entry is a metadata
record, and applying the log in order yields the full cluster state.

```
MetadataRecord (one per Raft entry)
├─ RegisterBroker    { broker_id, host, port, broker_epoch, roles }
├─ UnregisterBroker  { broker_id }
├─ BrokerHeartbeat   { broker_id, broker_epoch }          (lease refresh)
├─ CreateTopic       { name, num_partitions, replication_factor, configs }
├─ DeleteTopic       { name }
├─ PartitionAssignment { topic, partition, replicas[] }   (placement)
├─ PartitionChange   { topic, partition, leader, isr[], leader_epoch }
├─ UpdateConfigs     { entity, configs }
└─ RegisterUser      { username, password_hash, roles }   (from M6)
```

Why a log and not a key-value store: the log gives every change a **strictly
increasing offset**, which is exactly what brokers need to incrementally
sync their local caches ("give me everything after offset X") and what the
controller needs for fencing stale writers.

## 2. Roles

- **Active controller** = the Raft leader. The only node allowed to write
  metadata. Handles broker registration/heartbeats, topic admin, ISR
  changes, leader elections.
- **Follower controllers** = hot standbys applying the same Raft log; one
  takes over in seconds if the active controller dies (Raft election).
- **Brokers** = never talk to Raft. Each runs a **metadata subscriber**
  task that long-polls the active controller for metadata-log deltas and
  materializes a local immutable **metadata cache** (snapshot + deltas).

Combined mode (one node = broker + controller) is supported for dev and
small clusters; production runs dedicated controllers.

## 3. Broker lifecycle

```
start → load local logs → REGISTER with active controller
      → controller appends RegisterBroker{broker_id, epoch} to Raft log
      → broker starts metadata subscriber + heartbeat loop (every ~2s)
```

- **Broker epoch**: a persistent, monotonically increasing number per broker
  identity, bumped on every (re)registration. Any RPC arriving at the
  controller with an older epoch is rejected — this fences a zombie broker
  that was partitioned away and comes back believing it still leads
  partitions.
- **Lease**: if heartbeats stop for `broker.session.timeout.ms`, the
  controller appends tombstones: every partition led by the dead broker gets
  a `PartitionChange` electing a new leader from the ISR; the dead broker is
  dropped from all ISRs.
- **Controlled shutdown**: the broker asks the controller to move its
  leaderships away first → zero-downtime restarts and upgrades.

## 4. Topic creation & partition placement

`CreateTopic` is validated (name, RF ≤ live brokers) and appended to the
Raft log; on apply, the controller state machine computes placement:

- Replicas assigned round-robin over live brokers with rack-aware spreading
  (later), first replica = preferred leader.
- Initial `PartitionChange` records set leader = first live replica,
  ISR = all replicas, `leader_epoch = 0` (or incremented on reassignment).

Brokers learn of new assignments via their metadata subscription plus a
direct `LeaderAndIsr`-style control RPC (gRPC, from M2) telling them to
start leading/following a partition locally.

## 5. Leader election & fencing (correctness core)

1. Trigger: broker death, ISR shrink to empty, admin reassignment, or
   preferred-leader rebalance.
2. The controller picks the new leader **from the current ISR only**
   (guarantees the new leader has every acknowledged write). `unclean
   leader election` (non-ISR candidate) is off by default — availability
   vs. data loss tradeoff, config per topic.
3. `leader_epoch` increments monotonically per partition.
4. The `PartitionChange` is committed to the Raft log **before** any broker
   is notified → election decisions survive controller failover.
5. Brokers reject any produce/fetch/control RPC carrying a stale
   `leader_epoch`; the deposed leader fences itself on seeing a newer
   epoch in its metadata stream.

The epoch pair **(broker_epoch, leader_epoch)** is the fencing backbone:
every cross-node request carries its sender's view, and stale views are
always rejected.

## 6. What Raft buys us, concretely

- **No split-brain metadata**: two controllers can never both commit
  conflicting `PartitionChange`s — a minority-side controller can't commit.
- **Durable elections**: if the active controller dies right after electing
  a new partition leader, the next controller replays the same log and
  reaches the same state.
- **Incremental replication**: broker metadata caches sync by offset; after
  a network blip a broker asks for "everything after offset 42110" and
  catches up without a full snapshot.

## 7. Failure matrix

| Failure | Detection | Recovery |
|---|---|---|
| Broker crash | heartbeat lease expiry | re-elect its leaders from ISR; on restart it re-registers (new epoch) and follows |
| Active controller crash | Raft election timeout | follower controller becomes leader; brokers re-subscribe to it |
| Network partition (broker side) | lease expiry on controller | partitioned broker fenced by epochs; rejoins as follower, truncates to epoch checkpoint (Blueprint 04) |
| Controller quorum loss (2 of 3 dead) | — | metadata writes halt; **data plane keeps serving** from caches; restore quorum to re-enable admin/elections |

The last row is a deliberate design property: losing metadata consensus
degrades the cluster to read/write of *existing* partition leaderships, not
a full outage.

---

## Verification (M2)

- [x] 3-node combined cluster starts; `CreateTopic` via CLI on any node;
      `Metadata` from every node agrees (same leaders/ISR/epochs).
- [x] Kill the active controller: a follower takes over within seconds;
      topic creation still works; prior metadata intact.
- [x] Kill a broker: its partitions' leadership moves to ISR members; on
      restart it re-registers with a bumped epoch and rejoins ISR.
- [x] Stale-epoch injection (test hook): RPC with old leader_epoch is
      rejected.
