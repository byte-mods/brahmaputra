# Kafka vs Brahmaputra at RF=3, acks=all — 2026-08-22

The first head-to-head measurement of both systems in the configuration a
durable deployment actually runs: three brokers each, replication factor
3, `acks=all`, `min.insync.replicas=2`.

It found that Brahmaputra lost that comparison by 3.2×, traced the loss to
a 50 ms sleep on the follower fetch path, and re-measured after removing
it. Both sets of numbers are kept below, because the before/after pair is
more informative than either half.

Reproduce with:

```bash
bash scripts/bench-replicated-vs-kafka.sh                                # standard
PER_CLIENT=2000000 LEVELS=4 bash scripts/bench-replicated-vs-kafka.sh    # steady state
bash scripts/bench-replicated.sh                                        # native, no Docker
```

---

## 1. Verdict

**Before the fix**, replication cost Brahmaputra **13.3×** of its
unreplicated throughput while costing Kafka 2.6×, and Kafka produced
**3.19× faster** at RF=3. The published 3.0× produce advantage — measured
single-node at RF=1 — inverted completely under replication.

**After the fix**, replication costs Brahmaputra **1.47×** against Kafka's
3.18×, and Brahmaputra produces **3.40× faster** at RF=3, on **4.5× less
memory**. The advantage now survives replication, which it did not before.
Those are means of three consecutive runs; the pessimistic bound —
Brahmaputra's worst run against Kafka's best — is still 3.09×.

The tell was in the resource numbers, not the throughput ones. At RF=3
Brahmaputra averaged **96 % CPU against a 1200 % ceiling** — under one core
across three brokers — while Kafka burned 1,095 % to win. It was not
losing because it was working hard and falling short; it was asleep.

## 2. Root cause and fix

A follower's only way to learn about a new append was to ask the leader
again, and its loop slept 50 ms between empty answers
(`ReplicaManagerConfig::idle_fetch_interval`). Under `acks=all` the high
watermark cannot advance until followers have fetched — so **every
producer waited out that sleep before its record could commit**, and the
cluster ran at the polling interval rather than at the speed of the log.

Confirmed before writing any fix by setting the interval to 1 ms: native
RF=3 throughput went from 28,568 to 113,508 msgs/sec. That proved the
diagnosis, but a 1 ms poll is a busy-wait, not a fix.

**The fix: leaders long-poll a caught-up follower rather than answering it
empty** (`replica_fetch` in `crates/broker/src/handlers.rs`). Two details
carry the correctness:

- **The wait is on log-end-offset, not the high watermark.** The consumer
  fetch path long-polls on the watermark, but a follower cannot: under
  `acks=all` the watermark does not advance *until this follower fetches*,
  so waiting on it would be waiting on itself. A separate append watch was
  added to the partition actor, published once at the end of the actor
  loop rather than from each append arm — a notification that some future
  append command forgot to send would be indistinguishable from a stalled
  replica.
- **Leadership is re-validated after the wait.** A request can sit parked
  for up to 500 ms, and leadership can move in that time. Without the
  re-check a demoted leader would serve batches under an epoch it no
  longer owns, and the follower would accept them as committed history.
  Validation demands an exact leader-epoch match, so passing it a second
  time also confirms the epoch reported in the response is still correct.

No wire-format change: `ReplicaFetchRequest` carries no client-chosen
wait, so the hold is the leader's own policy. `idle_fetch_interval` stays
at 50 ms and is now reached only when a leader has genuinely had nothing
to send for a full 500 ms.

## 3. Method

| | |
|---|---|
| Host | AMD Ryzen AI MAX+ 395, 32 logical processors, 60.6 GiB visible |
| OS | Windows 11 26200, Docker Desktop 29.6.2, WSL2 backend |
| Kafka | `apache/kafka:4.3.1`, KRaft, 3 nodes, each broker+controller |
| Brahmaputra | 3 nodes, Raft control plane |
| Per container | `--cpus 4`, `--memory 4g` — **1200 % is the CPU ceiling** for a three-node cluster |
| Workload | 256 B records, 6 partitions, `batch.size` 64 KiB, `linger.ms` 5, no compression |
| Steady-state run | 2,000,000 records per client × 4 clients = 8,000,000 per phase |

Both clusters are three nodes on one machine, so both share a disk and a
NIC and both read below real hardware. The penalty is symmetric, which is
what makes the ratios meaningful even though the absolutes are not
deployment figures. Load generators run inside the broker containers,
spread round-robin across all three, so sampled CPU and memory cover
broker *plus* client for both systems.

### Three corrections the method needed

**Both consumers join a consumer group.** `kafka-consumer-perf-test`
always does, and pays for it — 696 ms of rebalance inside a 2.4 s run. An
earlier revision let the Brahmaputra CLI read partitions directly with no
coordination, which overstated its consume rate about 3×
(3,236,246 → 1,117,318 msgs/sec once the group was added). **The published
5.7× consume figure was measured the uncoordinated way.**

**Run length changes the answer.** Kafka gains up to 40 % between 500k and
2M records per client from JIT warmup; Brahmaputra moves under 4 %. Any
Kafka comparison in this repository taken on short runs is biased in
Brahmaputra's favour. The steady-state run is the fair one.

**Both clusters advertise static IPs, not container names**, because of
the defect in §6.

## 4. Steady state: 8,000,000 records, 4 clients

All post-fix figures below are the mean of **three consecutive runs** with
min–max in brackets. Kafka and Brahmaputra are measured inside the same
run, so every ratio is a within-run comparison and immune to the
session-to-session drift described in §4.

### Produce, RF=3 `acks=all` `min.insync.replicas=2`

| Reading | Kafka | Brahmaputra (before) | Brahmaputra (after) |
|---|---|---|---|
| msgs/sec, wall clock | 231,454 [225k–235k] | 106,077 | **787,385 [727k–869k]** |
| msgs/sec, client-measured | 257,503 | 116,093 | **838,655** |
| Cluster CPU avg (ceiling 1200 %) | 940 % | 96 % | 828 % |
| Cluster memory avg | 4,464 MiB | 1,275 MiB | **999 MiB** |

**Brahmaputra is now 3.40× faster than Kafka on the replicated produce
path** (3.26× client-measured), on 4.5× less memory. Before the fix it was
3.19× slower. Taking Brahmaputra's worst run against Kafka's best still
leaves 3.09×, so the margin does not depend on picking a favourable run.

### Everything else

| Phase | Kafka | Brahmaputra | |
|---|---|---|---|
| Produce RF=1 `acks=1` | 735,668 | **1,157,006** | Brahmaputra 1.57× |
| Consume RF=3, wall clock | 1,413,122 | **2,977,061** | Brahmaputra 2.11× |
| Consume RF=3, client-measured | 3,159,988 | **3,712,925** | Brahmaputra 1.17× |
| Consume RF=1, wall clock | 1,593,103 | **2,993,970** | Brahmaputra 1.88× |
| Consume RF=1, client-measured | **4,161,295** | 3,759,693 | Kafka 1.11× |

Consume is close. Wall clock favours Brahmaputra because it charges Kafka
roughly two seconds of JVM startup; on each client's own reported rate the
two are within 17 % at RF=3 and Kafka is ahead at RF=1. **The 5.7× consume
figure does not survive replication and group coordination** — that
correction stands regardless of the fix, which touched only the produce
path.

### Run-to-run spread across the three post-fix runs

| Measurement | Mean | Spread |
|---|---|---|
| Kafka RF=3 produce | 231,454 | 4.3 % |
| Kafka RF=3 consume | 1,413,122 | 12.7 % |
| Brahmaputra RF=3 produce | 787,385 | 18.1 % |
| Brahmaputra RF=3 consume | 2,977,061 | 3.2 % |

Worth recording because it **corrects an earlier claim in this document's
own history**. Before the fix, Brahmaputra's RF=3 produce varied by 0.3 %
between runs, and that was written up as an operational virtue. It was
nothing of the sort: a workload pinned to a 50 ms polling interval is
perfectly reproducible *because* it is sleeping. Now that it actually uses
the machine, it varies like any other saturating workload — more than
Kafka does on the same measurement. The determinism was a symptom.

## 5. What replication costs each system

Each ratio is computed within one system, using the same client and
metric, so it is immune to any cross-system metric disagreement.

| System | RF=1 `acks=1` | RF=3 `acks=all` | Kept | Cost |
|---|---|---|---|---|
| Kafka | 735,668 | 231,454 | 31 % | 3.18× [3.04–3.31] |
| Brahmaputra — before | 1,415,679 | 106,077 | 7.5 % | 13.35× |
| Brahmaputra — after | 1,157,006 | 787,385 | 68 % | **1.47× [1.39–1.59]** |

The two cost ranges do not overlap across three runs.

Brahmaputra now replicates *more cheaply than Kafka does*. Before the fix
its replication cost also grew with load — 4.2× native, 8.4× at 500k
records per client, 13.3× at 2M — which was the signature of a saturating
serial resource rather than a fixed per-record overhead.

Native, without Docker (`scripts/bench-replicated.sh`, 200k × 256 B):

| | Before | After |
|---|---|---|
| RF=3 `acks=all` | 28,568 msgs/sec | **299,013 msgs/sec** |
| Replication cost | 4.17× | **1.17×** |
| Throughput kept | 24 % | **86 %** |

## 6. Second defect, also fixed: hostname-advertised brokers

Found while building the benchmark, and initially mis-described in this
document as a "~10 s one-time stall" whose mechanism was unidentified. It
is neither one-time nor mysterious.

**The client resolved the broker hostname on every send and never cached
it.** Instrumenting `lookup_host` counted **6,418 resolutions to produce
2,000 records**. Each is individually fast — none exceeded the 20 ms probe
threshold — but tokio dispatches every one to the blocking pool, and at
that volume the cost dominates. It read as a fixed cost because the call
count tracks *flushes*, not records: at small record counts `linger.ms`
forces many tiny flushes, at large counts batches fill by size, so the
total barely moves between 10k and 400k records.

`BrokerEndpoint::resolve` short-circuits on an IP literal without a
syscall, which is why only name-advertised clusters paid it — and why
using IP listeners removed it from the benchmark entirely.

**The fix** (`crates/client/src/router.rs`) caches resolved addresses on
the router with a 30 s TTL, and drops a cached entry when the connection
to that address is invalidated, so a broker that comes back at a new IP is
re-resolved immediately rather than after the TTL.

Same cluster, brokers advertising hostnames, before and after:

| Records | Before | After |
|---|---|---|
| 20,000 | 5.53 s | **0.99 s** |
| 100,000 | 5.74 s | **1.08 s** |
| 400,000 | 6.22 s | **1.45 s** |

That matches the IP-advertised path measurement of 0.97 s at 20,000
records, so the penalty is gone rather than reduced. This mattered well
beyond benchmarking: Kubernetes, Docker Compose and every DNS-based
service discovery mechanism advertise names.

## 6a. Separate finding: a saturated broker kills itself

Found while taking the repeat runs, and **the fix is what made it
reachable**. One run in four died with every producer reporting
`connection closed`; all three brokers had exited with status 1, the first
of them saying:

```
Error: broker 3 epoch 1 heartbeat exceeded its session timeout
```

A broker that misses its controller heartbeat calls `bail!` and exits the
process (`crates/server/src/main.rs:737`). Under enough load a broker can
starve its own heartbeat task and terminate — and because load arrives at
every broker at once, they go together and take the cluster with them.
Kafka fences a broker from the controller side; the process survives and
re-registers. Here there is nothing left to re-register.

This was latent before. At 96 % CPU the heartbeat never came close to its
deadline; at 828 % it does. **The fix moved this cluster from "too slow to
reach its own failure mode" to "fast enough to reach it."**

The benchmark harness contributed the trigger and has been corrected: it
was passing `--heartbeat-interval-ms 500 --session-timeout-ms 3000`
against defaults of 1000/5000, inherited from `bench-replicated.sh`, which
descends from a failover test where a short lease is the point. Five
consecutive runs at the default lease under identical load did not
reproduce the crash, against one failure in four with the tight one.

Correcting the harness removes it from *these* measurements. It does not
remove the underlying behaviour.

**Why it is not fixed here.** The obvious repair — don't exit, re-register
instead — would break a load-bearing invariant. `Broker::fence` is
documented as irreversibly fencing *this broker incarnation*, and
`activate_broker_epoch` compare-exchanges the local epoch from zero exactly
once, "instead of participating in epoch ping-pong". One process is one
incarnation is one epoch, and that is what stops a zombie broker serving
under an epoch the cluster has moved past. Surviving a lost lease means
supporting several incarnations per process, which is a design change with
real correctness stakes, not a patch.

**And the trigger is not what it looks like.** The broker that died was the
one that had just become the Raft controller leader, six seconds earlier:

```
11:03:18.313  controller leader maintenance applied  ControllerChanged { broker_id: 3 }
11:03:24.045  shutdown signal received
```

The heartbeat is a durable metadata write — redb at `Durability::Immediate`,
an fsync — competing for the same disk that the replication fix had just
made roughly eight times busier. This is control-plane durability losing a
race against data-plane throughput on one spindle, not a task starved of
CPU. A dedicated runtime for the heartbeat, the reflexive fix, would not
have helped.

The real options are architectural: isolate control-plane I/O from the data
plane, stop requiring a durable write per heartbeat, or model broker
incarnations so a lost lease is survivable. All three are design decisions
this report does not make.

## 7. Verification of the fix

| Suite | Result |
|---|---|
| `cargo test --workspace` | 306 passed |
| `scripts/verify-replication.sh` | 14/14 — leader kill, failover, rejoin, byte-identical replicas, `min.insync` refusal |
| `scripts/verify-chaos.sh` | 7/7 — six kill rounds, 900 records, no loss, no duplicates, no holes |
| `scripts/verify-failures.sh` | 15/15 |
| `scripts/verify-transport-parity.sh` | TCP, TLS and QUIC identical on all six checks |
| `scripts/verify-m3.sh` | 44 pass, then one **pre-existing** failure |

Every RF=3 benchmark level also asserts its own replication: Kafka must
report ISR=3 on all six partitions, and Brahmaputra must hold partition
logs on all three nodes with log-end offsets summing exactly to the
records produced. All passed.

**On the m3 failure:** it stops at "produce storm to have acknowledged and
in-flight calls". Stashing the change, rebuilding and re-running produces
the identical failure at the identical point with the same 44 passes, so
it is not a regression. The test suspends a follower with `kill -STOP` to
hold producer calls in flight, and that does not suspend native Windows
processes under Git Bash. The suite passes on macOS per
[validation-2026-08-21.md](validation-2026-08-21.md); it cannot pass on
this host, before or after this change.

## 8. Limitations

- Three brokers per system share one host's disk and NIC. Absolutes sit
  below real hardware for both.
- Kafka's *absolute* numbers drifted across this session as the host
  warmed and filled: RF=3 produce measured 58,830 cold, 338,581 at its
  best, and 225k–235k in the final three runs hours later. Only within-run
  ratios are safe to quote across time, which is what §4 and §5 use.
- One record size (256 B), one partition count (6). Larger records shift
  the balance toward whichever system has the better zero-copy path.
- No latency percentiles. `acks=all` p99 is arguably the more important
  number for a durable deployment and is still unmeasured — and it is the
  number this fix should most improve.
- Unrelated containers on the host drew ~9 % of one core, constant across
  both systems.

## 9. What this means for replacing Kafka

The wire-protocol gap in [kafka-parity.md](kafka-parity.md) §2 remains the
decisive blocker and nothing here changes it.

What has changed is the argument that sat alongside it. Before this work,
"worth rewriting every client because it is 3× faster" was false in the
configuration that matters: at RF=3 it was 3.19× *slower*. It is now 3.40×
faster at RF=3, on a fifth of the memory, with a replication cost less than
half Kafka's own.

The documentation has been brought in line: the README badge now carries
its conditions and drops the consume multiplier, and
[kafka-parity.md](kafka-parity.md) §8 has been rewritten around the
measured Kafka-versus-Brahmaputra replicated comparison rather than the
superseded 25,618 msgs/sec figure.

**What remains open**, in the order it deserves attention:

1. **The heartbeat self-exit (§6a).** The most consequential item on this
   list, and the one the fix made most reachable. It needs a design
   decision, not a patch.
2. **`acks=all` latency percentiles.** The fix removed a 50 ms sleep from
   the commit path, so p99 should have moved further than throughput did —
   and nothing here measures it.
3. **Longer runs.** Kafka was still gaining from JIT warmup at 2M records
   per client when measurement stopped.
