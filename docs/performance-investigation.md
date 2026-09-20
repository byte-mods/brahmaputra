# Performance investigation after 0.8.0

Goal: exceed Kafka throughput while using less CPU and memory in every published
scenario, retain correctness and durability, publish verified results, and push
the resulting changes to main. This goal is not yet achieved.

Latest status (2026-09-21): the final twelve-group comparison passed and is
published in [the 0.8.1 reports](benchmarks/0.8.1/README.md). Windows and Linux
each passed 426 Rust tests. Performance losses and lease-recovery failures
remain, so the universal performance objective is still open. Earlier progress
entries below describe their status at the time.

Baseline: commit `f95d72b`, with the unchanged 0.8.0 comparison reports under
`docs/benchmarks/0.8.0/`. Comparisons must retain equal resource limits, record
counts and payloads, compression, acknowledgments, replication, and idempotence.
Report wall-clock and client timing separately. A successful run is not a
performance win. Repeat measurements before attributing small differences.

## Baseline evidence and code map

- `crates/protocol/src/batch.rs`, `RecordBatch::encode`: allocates a temporary
  buffer per record, copies it into the payload, and copies the whole payload
  twice even when compression is disabled.
- `crates/broker/src/handlers.rs`, produce handling, and
  `crates/broker/src/actor.rs`, `ProducerStateTable::decide` / `AppendIdempotent`:
  decode an idempotent batch and re-encode it for deduplication and storage.
- `crates/client/src/producer.rs`: partition send locks preserve order;
  idempotent sends bypass multi-partition batching. Batch encoding currently
  happens inside the shared buffer lock on the multi-partition path.
- `crates/cli/src/main.rs`, `drive_ordered`: completed sends cannot refill the
  window ahead of the oldest outstanding send. Investigate this separately
  from broker performance and retain input-order guarantees where required.
- `crates/controller/src/lib.rs`, heartbeat preflight/checkpoint and serialized
  Raft writes; `crates/server/src/main.rs`, broker lifecycle: the final RF=3
  baseline logged a five-second lease timeout and broker re-registration during
  the run. Correlation is evidence for investigation, not a proven root cause.
- `crates/client/src/quic.rs` and `crates/broker/src/quic.rs`: encrypted QUIC
  request streams are compared against Kafka plaintext TCP in the existing
  transport matrix; keep that distinction explicit.

## Required verification

1. Reproduce slow cases with the unchanged release and capture useful timing,
   CPU, and memory measurements without concurrent correctness workloads.
2. Optimize measured hot paths with regression tests for wire bytes, ordering,
   deduplication, cancellation, fencing, recovery, and message survival.
3. Repeat all 12 benchmark groups, examining every concurrency level, both
   produce and consume, and CPU/memory costs. Keep losing rows visible.
4. Run the release correctness matrix, update reports and documentation, and
   push only verified changes. Do not claim the full objective until every
   required comparison is demonstrated.

## First controlled changes and results

The cold route-cache regression reproduces 64 metadata requests from 64
concurrent sends against the unchanged router. Rechecking the cache after
acquiring the refresh lock reduces that to one; explicit leader-change
refreshes still pass. Partition-address misses receive the same fix.

Record encoding now writes directly into a pre-sized payload buffer. It avoids
per-record temporary allocations and redundant whole-payload copies. A
differential test compares the old framing algorithm's bytes across all five
codecs, varint boundaries, Unicode headers, null/empty values, extreme timestamp
deltas, and both producer-identity layouts. All 63 protocol tests pass.

First exploratory Docker comparisons, with original broker group-delay settings
unchanged, measured the following client-reported production rates:

| Case | Unchanged 0.8.0 | Candidate | Kafka in candidate run |
| --- | ---: | ---: | ---: |
| 50,000 records, idempotent | 48,411/s | 677,649/s | 95,238/s |
| 500,000 records, Zstd | 334,680/s | 944,945/s | 487,805/s |

These single passes establish an improvement to investigate further, not final
release claims. Raw artifacts are local under
`bench/results/performance-{baseline,candidate}-{short,zstd}/`. The unchanged
image is retained as `brahmaputra-bench:baseline-0.8.0`. The longer 500,000-record
idempotent baseline already outperformed Kafka, showing how much fixed startup
cost distorted the shorter result.

Consumption still lost in the short case. The harness configured Kafka's
initial group rebalance delay to zero, while Brahmaputra hard-coded one second.
`BrokerConfig::group_initial_rebalance_delay` and the server flag
`--group-initial-rebalance-delay-ms` now expose that setting, retain the one-second
shipping default, and allow the benchmark to set both systems to the same value
using `GROUP_INITIAL_REBALANCE_DELAY_MS` (default zero). Broker configuration
inspection includes the value. All 18 coordinator integration tests pass,
including default fleet formation and immediate assignment when configured zero.

Multi-partition compression has also moved outside the producer's shared buffer
lock, retaining per-partition ordering guards. The full Windows workspace suite
now passes 422 tests; formatting, Clippy with warnings denied, and all nine
dashboard tests also pass. The candidate comparison stopped at the replicated
four-client scenario under
`bench/results/performance-candidate-matrix/`, with its source diff saved there
as `source.patch`. One broker expired its lease and exited when conditional
re-registration was rejected with `expected 2, got 1`; two producers reported
fenced broker epochs. These results are incomplete and cannot support a release.
The one- and two-producer RF=3 phases completed at 446,828 and 673,401 wall-clock
records/second respectively, but those partial results do not override the
four-client failure.

Conditional lease-recovery requests now retain a random operation identity
across retries. Metadata retains the last receipt per broker, so a lost response
does not turn a retry into a stale-epoch rejection. A receipt neither renews a
lease nor revives a fenced broker; the server confirms a fresh heartbeat before
activating. Replay after a replacement still fails the epoch guard. Initial
unconditional process registrations retain their existing semantics. The
metadata and server suites pass (54 and 10 tests), including receipt replay
across serialization, expiry without resurrection, recovery after a lost
response, and replacement fencing.

The controller's synchronous redb transactions previously ran directly on Tokio
workers. Mutating storage callbacks now use the blocking pool, retaining an
owned state write guard through durable commit and in-memory publication even
if the caller is cancelled. Immediate durability is unchanged. A single-thread
executor regression deliberately blocks the database writer and verifies that
timers still run and the cancelled write survives reopening the store. All six
controller unit tests (including OpenRaft's storage suite) and three controller
integration tests pass, as do the server tests and workspace Clippy. The live
first four-client retest is under `bench/results/performance-recovery/` and
failed: brokers 2 and 3 recovered epoch 2, then exited because a concurrent
data-plane validation still held local epoch 1. Publishing a recovery identity
before submission now lets that validation recognize its own receipt while
continuing to reject traffic. Lease transitions are serialized on the rejected
validation path; normal valid traffic retains the existing fast path. The
broker regression covers metadata arriving before activation and a different
operation's receipt still fencing the suspended broker. The second live retest
under `bench/results/performance-recovery-v2/` passed all four-client RF=3 and
RF=1 produce, consume and replica-count checks, with two million records per
phase. Two brokers expired their leases and recovered without exiting. RF=3
wall-clock production reached 472,478 records/second; consumption reached
820,681. These are recovery evidence, not a paired Kafka comparison.

The Windows workspace passed 425 tests after the lifecycle changes. A later
client regression exposed a second cold-cache stampede: concurrent callers
opened 65 connections where two were sufficient. Per-endpoint setup gates
now coalesce DNS and authenticated connection setup while allowing unrelated
brokers to connect independently. All 43 client tests pass with that change.

The first Linux workspace run failed the five-second coordinator recovery
test. Three isolated repetitions with the latest client changes passed without
extending its timeout. The next full Linux run passed all 425 tests with that
client change (`bench/results/performance-linux-v2/workspace.log`). The earlier
failure is retained under `bench/results/performance-linux/`. All fifteen
Linux live suites subsequently passed, with an admin/security fixture retest:
it now waits for data-plane visibility after controller topic creation before
writing the quota baseline. That retest passed 29 checks; the other fourteen
passed in the original run. The 600-second M3 outage passed all 97 checks and
the six-kill chaos audit retained all 900 acknowledged records. Final Windows
validation passed all 425 tests as version 0.8.1, along with formatting,
Clippy and all twelve JavaScript checks. Refreshed paired comparisons remain
required. The first full comparison attempt (`performance-candidate-matrix-v2`)
was invalidated by a host timing discontinuity: both Kafka RF=3 producers
completed 500,000 records, the shared resource window was about 13 seconds,
but the Windows phase exceeded its 180-second deadline. A requested 45-second
status wait also returned after 256 seconds. The rerun uses a new directory
(`performance-candidate-matrix-v3`) and temporarily inhibits automatic system
sleep for the benchmark process; it restores the power request on exit.

That rerun exposed an adjustable-clock rate error: native RF=1 production
finished in 2 seconds inside the client, with a 5.15-second sampling window,
but `date` subtraction recorded 310.27 seconds. The harness now uses Node's
OS monotonic clock for durations and deadlines and rejects phases outside
their resource windows. The fourth resource regression rejects the recorded
anomaly. The final matrix is rerunning under `performance-candidate-matrix-v4`.

Offloading disk I/O addresses a concrete scheduling hazard, but the first retest
still saw five-second lease expirations. Their cause is not yet resolved. The
replicated harness now limits an individual workload phase to 180 seconds
(overridable with `PHASE_TIMEOUT_SECONDS`) so a broken quorum cannot leave a
benchmark waiting indefinitely.

## Follow-up measurements

- Compare all individual rows using the local `bench/results/compare-matrix.cjs`
  audit, including both client and wall-clock rates. The historical matrix has
  292 throughput/resource comparisons; 20 resource rows were unmeasured.
- The once-per-second Docker sampler is too sparse for the now much shorter
  native phases. Collect cumulative cgroup CPU time and denser working-set
  memory samples before claiming lower resource cost in every scenario. The
  new probe, shared wrapper and summary parser are integrated into all four
  comparison harnesses. Four parser tests pass, covering clock discontinuities,
  subsecond CPU accounting, time-weighted memory, aligned multi-node sampling,
  counter resets and incomplete input. A live temporary-container smoke test
  recorded 30 samples and 0.198 core-seconds across a 1.53-second observation
  window (`bench/results/resource-probe-test/`); this validates collection, not
  application efficiency. Sampling includes broker and client
  startup/exit dispatch plus the probe's own cost. Multi-node metrics use
  the common sampling window; peaks are observed/interpolated at the sample
  interval, not instantaneous hardware peaks.
- If QUIC remains behind, inspect packet size and UDP socket pressure. The
  installed Quinn endpoint defaults advertise at most 1,472-byte UDP payloads;
  MTU discovery is capped at 1,452 bytes. Existing tuning changes flow-control
  windows but neither limit. Adaptive discovery on jumbo/loopback links and
  appropriately bounded socket buffers are candidates, requiring fallback
  tests for ordinary-MTU paths. The continuation now enables discovery up to
  a 9,000-byte UDP payload ceiling at both endpoints. Its Windows regression
  passed on a jumbo path and a relay that drops every packet above 1,400 bytes,
  preserving all echoed payloads. Final cross-platform tests and a fresh full
  comparison are running; this is not yet a measured performance claim.

The first continuation Linux workspace run exposed a faulty final-MTU assertion
in that new test. All nine jumbo probes succeeded, but loss during the relay's
bulk transfer triggered a legitimate fallback to 1,200 bytes. The test now
checks acknowledged discovery up to 9,000 bytes before the bulk transfer and
retains every payload-identity and narrow-path drop assertion. The isolated
Linux retest passed; the original failure log is preserved. Final verification
and the complete comparison are restarting with the corrected fixture.

Final continuation checks passed 426 Rust tests on each of Windows and Linux,
formatting, Clippy, all 13 JavaScript checks and all 18 live transport checks.
The fifth full comparison then failed RF=3 production at four clients. Brokers
1, 2 and 3 expired their five-second leases and recovered at epoch 2. Two clients
exhausted their retries with `FENCED_BROKER_EPOCH`; two produced all 500,000
records. Resource probes continued with gaps no larger than 60 ms, so the
earlier clock anomaly does not explain this failure. The raw failure remains
under `performance-candidate-matrix-v5`. The sixth comparison repeats the same
source and settings without extending retry or lease timeouts. Recovery can
outlast the producer's default five 100-ms retries; heartbeat stalls under
replicated load remain an unresolved limitation even if a later run passes.

The unchanged sixth attempt failed at two RF=3 producers during recurring lease
recovery. A host diagnostic then showed roughly 20% full I/O pressure over five
minutes and 30% over ten seconds, with negligible CPU/memory pressure. Fourteen
unrelated Docker applications were running. With the user's approval, the
seventh attempt temporarily stops those containers and restores their saved
IDs in a `finally` block. Source and benchmark settings remain unchanged;
passing in isolation would not establish robustness under that contention.

The seventh attempt completed all twelve groups with verified record counts,
344 paired comparisons and 94 resource observations. All fourteen application
containers were restored and verified running. Brahmaputra leads 69 of 84
throughput comparisons and 50 of 52 CPU core-second comparisons; all 104
working-set memory comparisons are lower. Kafka wins the remaining throughput
and CPU-work comparisons. The passing replicated run also contains lease expiry
and successful epoch-2 recovery. The heartbeat-stall cause remains unresolved.
See the [release review](release-0.8.1-review.md#final-measurements) for losses
and measurement limits. Source hashes and image IDs accompany the reports.

After testing, the user's requested disk cleanup removed the generated Windows
`target/` directory and the unused Docker volumes `brahma-bench-target` and
`brahma-bench-cargo`. Source files, published results, local diagnostic logs,
screenshots and application data were retained. Future builds will regenerate
these caches.
