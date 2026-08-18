# Blueprint 05 — Consumer Groups, Rebalancing & Offset Management

> Status: written from DESIGN.md §7 ahead of the M4 implementation;
> reconciled against the actual group-coordinator code at the end of M4
> (see §6, §7 and the Verification section).

This document explains how a set of consumer processes divides up
partitions, survives member crashes, and remembers where it got to — all
without any external state store.

---

## 1. The internal offsets topic

All group state is stored **in the log itself**: `__consumer_offsets`, an
internal topic (default 50 partitions, RF = cluster default, later
compacted).

- The **group coordinator** for `group_id` = the leader broker of partition
  `hash(group_id) % 50` of `__consumer_offsets`.
- Coordinator failover is therefore just partition leadership failover
  (Blueprint 04) — group state survives because the offsets topic is
  replicated like any other.
- On becoming coordinator, a broker **rebuilds in-memory state by reading
  the partition's log**: offset commits, group metadata records.

Record types in `__consumer_offsets`:

```
OffsetCommit   { group, topic, partition, offset, commit_timestamp, expiry }
GroupMetadata  { group, generation, protocol, leader_member_id, members[] }
Tombstones     { group, ... }   (offset expiry, group deletion)
```

## 2. Group state machine

Each group is a small state machine on its coordinator:

```
            ┌──────────┐  first member joins   ┌────────────┐
            │  Empty   │ ────────────────────▶ │ Preparing  │
            └──────────┘                       │ Rebalance  │
                  ▲                            └─────┬──────┘
                  │ all members leave                │ JoinGroup from all
                  │                                  ▼
            ┌──────────┐  sync complete      ┌────────────┐
            │  Stable  │ ◀────────────────── │ Awaiting   │
            └────┬─────┘                     │ Sync       │
                 │                           └────────────┘
                 │ member join/leave/timeout → back to PreparingRebalance
                 ▼
              Dead (offsets expired)
```

- **JoinGroup**: members send their subscription; the coordinator collects
  them for `rebalance.timeout.ms`, picks a **group leader** member, bumps
  the **generation**, and returns membership.
- **SyncGroup**: the group leader computes the assignment (others send
  empty); the coordinator fans the assignment out to every member.
- **Heartbeat**: per-member keepalive; miss `session.timeout.ms` → member
  evicted → rebalance.
- Every transition appends `GroupMetadata` to `__consumer_offsets` — a
  coordinator crash mid-rebalance resumes from the log.

## 3. Assignment strategies

Pluggable assignors run on the group leader member (client side), so new
strategies need no broker upgrade:

- **range** (default): contiguous partition ranges per topic — good
  locality, can be uneven.
- **round-robin**: maximal evenness.
- **sticky** (later): minimizes partition movement across rebalances;
  **cooperative-sticky** (later still): incremental rebalancing without
  stop-the-world revocation.

## 4. Offset commits

- Consumers commit offsets (auto-commit every N ms, or explicit
  sync/async) → `OffsetCommit` records on the coordinator.
- On rebalance, each member reads the committed offset for its newly
  assigned partitions and resumes from there.
- **At-least-once by default**: commit after processing. (Exactly-once
  requires the transactional producer/consumer — out of v1 scope, but the
  idempotent producer from M3 is its foundation.)
- Offset expiry: commits older than `offsets.retention.ms` for groups with
  no active members are tombstoned.

## 5. The failure modes that actually bite

1. **Rebalance storms**: a slow consumer (long processing, GC pause)
   misses heartbeats → evicted → rebalance → it rejoins → rebalance...
   Mitigations: `max.poll.interval.ms` separating liveness from processing
   progress, static group membership (`group.instance.id`, later), sane
   timeout defaults.
2. **Zombie members**: a partitioned member keeps fetching with a fenced
   generation → coordinator rejects with `UnknownMemberId`/`RebalanceIn
   Progress`, forcing rejoin. Generation fencing is the group-level
   analogue of leader epochs.
3. **Commit-before-process vs process-before-commit**: the former is
   at-most-once (data skip on crash), the latter at-least-once
   (duplicates). The blueprint mandates documenting this in the client
   README; the library defaults to at-least-once.

## 6. Lag — the metric everyone watches

`lag = log_end_offset − committed_offset` per (group, topic, partition).
Lag is the primary health signal for streaming pipelines and a first-class
dashboard widget from M6.

As built, two data-plane APIs expose group state and the CLI derives lag
from them:

- **ListGroups** (api_key 12) — each broker answers for the
  `__consumer_offsets` partitions it *leads*, so the cluster-wide list is
  the union over brokers. Shards are loaded from the log on demand, so a
  broker that has just taken over a partition lists the groups in that
  log rather than an empty set. `brahmaputra-cli groups list` names any
  broker that failed to answer instead of silently returning a partial
  union.
- **DescribeGroup** (api_key 13) — the group's coordinator returns state,
  generation, members with their assignments, and every committed offset
  it holds.

`brahmaputra-cli groups lag --group g` joins those committed offsets to
each partition's log end offset (ListOffsets against the partition
leader, since the coordinator does not hold other topics' log ends) and
prints per-partition and total lag. The M6 metrics API
(`GET /api/v1/groups/{id}/lag`, DESIGN.md §9.2) will serve the same join
from the broker side.

## 7. Consumed position vs fetch position

The consumer keeps two positions per partition: the **fetch position**
(next offset to request) and the **consumed position** (next offset to
deliver — the one that gets committed). Records fetched beyond
`max.poll.records` (default 500, as Kafka) sit in a client-side buffer:
they have advanced the fetch position but *not* the consumed position, so
a commit can never cover a record the caller never received. A bounded
reader (`consume --group g --max N`) shrinks `max.poll.records` to its
remaining budget for the same reason. A rebalance drops the buffer and
resets fetch positions to the consumed positions re-seeded from
`__consumer_offsets`.

---

## Verification (M4)

All four scenarios passed in the live five-node run of
`scripts/verify-m4.ps1` / `scripts/verify-m4.sh` (exit 0, 30 assertions;
the script hard-fails on any missed assertion):

- [x] 3 consumers in one group over a 6-partition topic → each owns
      exactly 2 partitions; all produced records consumed exactly once
      (modulo duplicates only on forced rebalance).
- [x] Kill one consumer mid-stream → rebalance within session timeout;
      its partitions resume from last committed offset; no offset rewind
      beyond commits.
- [x] Kill the coordinator broker → new coordinator rebuilds from
      `__consumer_offsets`; committed offsets intact; group rejoins.
- [x] `consume --group g` twice with a restart in between → second run
      resumes at the first run's committed offset.

Reconciled against the code at the end of M4:

- Assignors: `range` and `round-robin` ship; they run on the group leader
  member (client side) as designed. `sticky`/`cooperative-sticky` remain
  future work.
- Offset expiry (`offsets.retention.ms`) and group deletion tombstones
  are defined in the record format (§1) and applied on replay, but no
  expiry sweeper writes them yet — deferred to M5 with retention.
- The offsets topic is not log-compacted yet (compaction is a v1
  non-goal); replay of the full partition rebuilds coordinator state.
