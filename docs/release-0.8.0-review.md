# 0.8.0 release review

## Scope

This review covers the Rust broker, controller, client and CLI, live failure
tests, and a finite Kafka performance matrix. It does not establish that every
possible workload is correct or faster, and does not certify the other language
drivers. Protocol version 4 and the storage format are unchanged.

## Fixes and the missing feature

Transactional consumer groups were missing a usable isolation setting. The Rust
`GroupConsumer` now accepts `with_isolation_level(ReadCommitted)`, and CLI group
consumption honors `--isolation-level read_committed`. Tests cover aborts larger
than the fetch budget, pending transactions, commits, and resumed consumers.

The review also found and fixed:

- Cancellation could drop the controller write mutex while an already submitted
  Raft write was still pending. The next command could reuse its metadata offset
  or overwrite its state using an older image. An owned task now retains the
  mutex until Raft completes. A three-node regression blocks quorum responses,
  cancels the caller, queues another registration and verifies both survive.
- Fresh group consumers could fail immediately when the offsets partition had
  no routable leader. Coordinator routing refusals now receive the same recovery
  handling as protocol refusals, with a ten-second retry window. A live broker
  regression restores the coordinator route and verifies delivery and commit.

- Four-client replicated load exposed a fatal ISR-maintenance path: temporary
  broker lease suspension propagated out of the maintenance task and stopped
  the process. Reconciliation now retains its partition mutation guard while
  the lease recovers and the metadata resolves any ambiguous write. A regression
  verifies that the guard stays held, recovery resumes, and an actual replacement
  still irreversibly fences the old broker.

- A local controller could reject conditional re-registration using an older
  broker epoch, confirm leadership afterward, and return that obsolete error.
  Rejection classification now revalidates the command against the post-confirmation
  image. Future-epoch conditional registrations discover the remote leader, and
  brokers remain suspended while their older local metadata catches up.

- A seed broker renewing its lease returned empty routing metadata, making
  fresh clients report nonexistent topics even with healthy remote leaders.
  Authorized cluster metadata remains readable while the local data lease is
  suspended; data operations remain fenced. Metadata error responses now carry
  their error code, which the client checks before updating cached routes.

- Automatic expiry could fence a running broker after a newer heartbeat had
  renewed its lease. The extended outage run reproduced the resulting loss of
  partition leadership. Maintenance now emits conditional expiry and rechecks
  the observed heartbeat against the image being committed, including coalesced
  observations. Heartbeat timestamps advance monotonically. Metadata regression
  tests cover delayed expiry and out-of-order heartbeat timestamps.

- A committed reader could remain stuck forever behind aborted/control batches
  larger than its fetch budget. It now scans to visible data or the stable bound.
- Incremental fetch sessions could restore topics without checking current ACLs
  and were not bound to a principal. Ownership is now checked before mutation;
  every expanded partition is authorized on each request.
- Multi-fetch could hide request/partition failures as empty successful reads.
  Errors now propagate, and only missing sessions trigger session recovery.
- Changing a fetch byte budget at an unchanged offset did not update the cached
  descriptor. Both offset and budget now participate in session deltas.
- Temporary broker lease suspension failed producers and group polling instead
  of allowing bounded recovery. Broker fencing is distinct from producer fencing.
  Non-idempotent retries can duplicate after an ambiguous append; use idempotence
  when duplicate suppression is required.

The largest ecosystem gap remains Kafka wire-protocol compatibility. Existing
Kafka clients and Kafka Connect are not interchangeable with native clients.
This release does not implement that compatibility layer.

## Verification

The final correctness run passed 419 Rust workspace tests on both Windows and
Linux, formatting, Clippy with warnings denied, and nine dashboard JavaScript
tests. Fourteen Windows live shell suites passed, as did the 31-check M2
metadata/failover suite and the separate controller-outage verification.
A five-minute smoke soak retained all 1,260,000 acknowledged records across six
broker kills, with contiguous offsets, 630 successful batches and no failed
batches. This short soak does not establish long-term production reliability.

The M2 fixture now compares TCP metadata against a fresh HTTP image on each
attempt and rechecks it after the sweep. Normal ISR reconciliation can advance
leader epochs while the fixture runs; the old fixed snapshot could never match.
Topic identity, peer agreement, deadlines and failover assertions are retained.

The extended replication test exposed fixture problems and a recovery failure.
Native Windows brokers require an actual Windows process suspension rather than
POSIX STOP/CONT. The fixture now verifies process identity before pausing, samples
requests during launch, and uses idempotence for its exact-once assertions.
Its recovery deadline and ten-minute follower outage remain unchanged.

Controller diagnostics then showed repeated 50-ms AppendEntries timeouts to live
peers, election churn, and stalled quorum progress. The candidate controller
defaults use a 200-ms Raft heartbeat and 1–2-second election window. This
stabilized the quorum through the full ten-minute outage, but catch-up then
stalled after applying one 300-entry batch without confirming it to the leader.
Replication batches are now capped at 32 metadata entries. The durable restart
regression covers a 340-entry backlog with a 100-partition metadata image and
uses shipping controller defaults. With the lease and metadata fixes above,
the extended live suite passed all 97 checks: production continued throughout
the 600-second outage, persisted follower lag decreased from 1,613 to zero,
and the final committed prefix was byte-identical on all three replicas through
HWM 1,622. Refused below-minimum-ISR writes did not append; production resumed
at the unused offset after a follower returned.

Timing-sensitive Rust tests are run serially within each test executable on
this shared host; concurrency inside each scenario is retained. A parallel run
expired a 300-ms test consumer before its expected join generation completed.
The desktop dashboard was visually inspected in headless Edge using local
sample data. This exposed and corrected tiny chart labels and rounding that
hid fractional error rates. Dashboard JavaScript and live API checks are
tracked separately from this sample-data visual inspection.

## Benchmark method

`bash scripts/bench-release.sh` runs scenarios sequentially and records exit
codes, reports and client output under `bench/results/release-benchmarks/`.
Comparisons use Kafka 4.3.1 and the locally built 0.8.0 broker on one Docker host,
with 4 CPUs and 4 GiB per broker. Native and Kafka clients execute inside their
respective broker containers, so resource samples include clients. Unrelated
host services remain running; these are shared-host measurements.

The eight-client single-broker concurrency case uses Kafka client heaps of
64 MiB initially / 128 MiB maximum and a 32-MiB producer buffer matching the
native client's default. This keeps eight load generators and the 2-GiB Kafka
broker heap within a useful 4-GiB container budget. Other cases use separate
512-MiB Kafka client heaps. These are explicit harness settings, not broker
defaults or a claim that heap tuning is equivalent between runtimes.

The host CPU is an AMD Ryzen AI Max+ 395 (16 cores / 32 logical processors).
Docker Desktop exposes 28 CPUs and 50,510,594,048 bytes of memory through WSL2,
kernel `6.18.33.2-microsoft-standard-WSL2`.

The matrix includes RF=1 and RF=3 at 1/2/4 clients, TCP/QUIC concurrency at
1/2/4/8 clients, 1-MiB records, none/LZ4/gzip/Snappy/Zstd codecs, acks=0/1/all,
an idempotent case, and a 10,000-record/s offered rate per client. Each case
measures both production and consumption. Producer idempotence is explicit and
matched. Both consumers use groups. Codec cases use identical repeated `x`
payloads: they represent highly compressible data, not random payloads.

Container limits are equal; measured CPU usage is not held equal. Nearest-CPU
rows are descriptive samples, not controlled equal-CPU experiments. Missing
resource samples are `NA`. Client-reported rates and wall-clock rates have
different startup costs. Multi-client latency percentiles are averages of each
client's percentile, not percentiles of a pooled sample. Short single passes do
not provide confidence intervals.
TLS, ACLs, transactions, disk failures and failover receive correctness tests;
the matrix does not measure comparative Kafka performance for those features.

All 12 scenario groups passed, including expected producer and consumer record
counts. The [reports and measurement CSVs](benchmarks/0.8.0/README.md) preserve
every concurrency level. The first run stopped after replication because the
wrapper attempted an unsupported `server --version` command. After removing
that redundant check, the remaining scenarios completed against the same
freshly built image; replication results and counts were retained and checked.

Performance remains workload-dependent. RF=3 production at four clients reached
158,529 records/sec versus Kafka's 182,050, with native maximum request latency
around 9.3 seconds. Native RF=3 throughput was higher at two clients than four,
so saturation stalls remain an optimization target. QUIC production also trails
Kafka in this RF=1 concurrency run. These results do not support a universal
speedup claim. Earlier failed exploratory runs are not release measurements,
and historical README ratios do not describe this release.
