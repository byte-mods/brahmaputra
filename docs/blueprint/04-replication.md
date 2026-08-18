# Blueprint 04 — Replication, ISR & Zero-Loss Failover

> Status: reconciled against the replica-manager code at the end of M3.
> One implementation divergence from the original sketch: instead of a
> `pending_acks: BTreeMap` on the partition actor (§2), the produce handler
> subscribes to the actor's high-watermark watch channel before appending
> and blocks until the HW covers the batch or the request times out.
> Functionally equivalent; ISR changes are written by each combined node
> directly into its local Raft-replicated controller rather than via a
> separate broker→controller RPC.

This document explains how a write becomes durable on multiple machines,
what "in sync" means precisely, and how the system survives broker crashes
— including mid-write crashes — without losing acknowledged data.

---

## 1. The replication machinery per partition

Each partition has one **leader** replica and RF−1 **follower** replicas on
different brokers (placement from the controller, Blueprint 03 §4).

- A follower is just a consumer with privileges: it runs a **fetcher task**
  that long-poll `Fetch`es its leader, appending returned batches verbatim
  to its local log.
- The leader tracks, per follower, the offset of the next batch the
  follower will fetch (`follower_fetch_offset`) — learned from every fetch
  request.

```
leader log:  ... 45 46 47 48 49 50 51 52      LEO = 53
                       ▲           ▲
                       HW = 47     follower A fetched up to 52
                                   follower B fetched up to 47
```

**High watermark (HW)** = min over ISR members' fetch offsets = the largest
offset replicated to *every* in-sync replica. Consumers may only read
offsets < HW. The leader piggybacks the current HW on every fetch response;
followers persist it (the `hwm` checkpoint file, Blueprint 01 §2).

## 2. Acks semantics (what "acknowledged" means)

| `acks` | Acknowledged when | Loss on leader crash |
|---|---|---|
| 0 | sent to socket | anything in flight |
| 1 | appended to leader's log | everything past HW |
| all (−1) | appended by **every ISR member** (HW covers the batch) | **nothing** — this is the zero-loss mode |

Implementation: the partition actor holds `pending_acks: BTreeMap<offset,
oneshot>` for `acks=all` batches; each advance of HW completes the covered
senders. Produce latency for `acks=all` = one follower round trip, amortized
by batching.

## 3. ISR membership (time-based, with hysteresis)

A replica is **in sync** if it has fetched from the leader within
`replica.lag.time.max.ms` (default 10 s) — *not* "within N offsets", which
flaps under bursty load.

- Follower stalls → leader asks the controller to **shrink** the ISR
  (`PartitionChange` in the Raft log; Blueprint 03 §5).
- Stalled follower catches up to within the lag window → **expand**.
- ISR changes go through Raft *before* taking effect: a controller crash
  mid-shrink cannot produce divergent ISR views.
- `min.insync.replicas` (default 2 with RF=3): `acks=all` produce requests
  are rejected with `NotEnoughReplicas` when |ISR| < min — choosing
  durability over availability, loudly.

## 4. Failover without loss (the crash case the user cares about)

Scenario: leader of partition P crashes between acknowledging batch 47
(HW=47) and replicating batch 50.

1. Heartbeat lease expires → controller elects a new leader **from the ISR**
   (say follower A, which had fetched through 52) with `leader_epoch+1`,
   committed to the Raft log first.
2. A had everything ≤ 47 (that is what HW means) plus possibly more
   (48–52, un-acked). All acknowledged data survives. Un-acked batches may
   survive too (as on A) or not — producers of un-acked batches never got
   a success response, so no contract is violated.
3. The old leader, when it restarts, is now a follower. Its log may contain
   un-acked batches beyond the new HW that the new leader never had.
   Keeping them would create **divergent logs** (same offset, different
   data). Hence:

### 4.1 Leader-epoch truncation (KIP-101 semantics)

- Every broker persists a **leader-epoch checkpoint** file per partition:
  `(epoch, start_offset)` entries appended whenever it becomes leader.
- A restarting follower asks the new leader: *"what is the end offset of
  epoch E?"* for its newest epoch E (`OffsetsForLeaderEpoch` RPC).
- The reply tells it where its log diverges; it **truncates back** to that
  point (physically, in the segment files — the storage engine supports
  `truncate_to(offset)`) and then resumes fetching.
- This replaces naive "truncate to HW" logic, which loses acknowledged
  data in corner cases (e.g. HW itself regresses across elections). Epochs
  make divergence detection exact.

## 5. Rejoining broker catch-up (the "comes up later" case)

A broker down for hours rejoins with a stale log:

1. Re-registers with a new **broker epoch** (old identity fenced).
2. Controller assigns it follower duties; it truncates via §4.1 and
   fetches from the leader at full speed (fetcher is not rate-limited by
   consumer quotas).
3. While behind, it is **out of the ISR** — it does not block `acks=all`,
   and it cannot be elected.
4. When its fetch offset stays within the lag window, the controller
   expands the ISR. Only then is the partition fully redundant again.
5. Retention interaction: if the rejoining broker is so stale that its log
   start is beyond what remains, it simply fetches from the leader's
   `log_start_offset` — it converges to whatever the leader still holds.

## 6. Consistency invariants (tested, not hoped)

- **I1**: HW never decreases on any replica.
- **I2**: consumers never observe an offset ≥ HW.
- **I3**: after any sequence of kills/restarts, all replicas' logs are
  identical up to the cluster-wide HW.
- **I4**: any batch for which a producer received success with `acks=all`
  and |ISR| ≥ `min.insync.replicas` is present on every subsequent leader.

---

## Verification (M3)

All four scenarios passed in the live five-node run of
`scripts/verify-m3.ps1` (exit 0; the script hard-fails on any missed
assertion):

- [x] RF=3 topic, `acks=all`: kill −9 the leader mid-produce-storm → new
      leader elected from ISR; consumer from earliest sees every
      acknowledged offset, contiguous, no gaps, no duplicates.
- [x] Same, but kill during a follower stall (ISR=2): no loss; stalled
      broker rejoins, truncates, catches up, re-enters ISR.
- [x] Broker down 10 min while producing continues; restart → catch-up
      observed via lag metrics; final logs byte-identical on all 3 brokers
      up to HW (checksum comparison of segment files).
- [x] `min.insync.replicas=2` with 2 of 3 brokers down → produce gets
      `NotEnoughReplicas`; after one broker returns, produce resumes with
      no loss of previously acknowledged data.
