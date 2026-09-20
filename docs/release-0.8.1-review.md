# 0.8.1 release review

## Scope and encoding

This release addresses measured client startup and encoding overhead, recovery
failures under replicated load, and benchmark resource accounting. It retains
BitPacker data-plane messages, wire version 4 and existing record/disk formats.
Controller metadata remains JSON. Native Kafka clients still require a Kafka
protocol implementation and cannot use Brahmaputra's native protocol directly.

## Changes and evidence

- A 64-request cold-client regression originally issued 64 metadata requests
  and opened 65 connections across two clients. Cache rechecks and per-endpoint
  setup gates reduce these to one metadata request per client and two leader
  connections in total. The leader-change routing regression still passes.
- Record encoding writes into one sized payload buffer. The differential test
  preserves legacy framing across varint boundaries, tombstones, nullable
  headers, Unicode keys, timestamp extremes, five codecs and both producer
  layouts. Compression no longer holds the shared producer buffer lock.
- A conditional registration keeps a random operation identity and receipt
  across retries. Receipt replay neither changes the heartbeat nor revives a
  fenced broker. Serving resumes only after a fresh heartbeat succeeds.
  Replacement processes remain fenced by broker epoch.
- Metadata can arrive before the registration response. A suspended broker
  recognizes its own receipt, keeps requests rejected, and waits for activation.
  An unrelated receipt still shuts down the old incarnation. Tests also cover
  a receipt expiring before activation and a subsequent recovery attempt.
- Controller redb mutations use the blocking pool. Their owned serialization
  guard spans durable commit and memory publication even after cancellation.
  A blocked-writer regression verifies that async timers remain responsive and
  an aborted caller's write survives reopening the database.
- QUIC endpoints advertise a 9,000-byte UDP payload ceiling and discover the
  usable path size with acknowledged probes. The regression transfers and
  echoes eight 512-KiB payloads on both a jumbo path and a relay that silently
  drops packets above 1,400 bytes. The Windows run discovered 9,000 and 1,378
  bytes respectively and delivered every payload unchanged.

Upgrade every controller to retain receipts throughout recovery. Mixed-version
controller recovery has not been validated. Initial unconditional registrations
retain their prior semantics; this receipt mechanism covers conditional recovery.

## Verification status

The final 0.8.1 workspace passed all 426 Rust tests on both Windows and Linux,
including the corrected jumbo-path and narrow-path QUIC regression. The client
setup change also passed all 43 client tests separately before that regression
was added.

Formatting, Clippy with warnings denied, nine dashboard JavaScript tests and
four benchmark resource-accounting tests passed before the final comparison run.

An earlier Linux run timed out in coordinator recovery. Three isolated repeats
and the subsequent full workspace run passed with the original five-second
timeout. That earlier failure remains recorded in the investigation artifacts.

The first final Linux run also caught an incorrect QUIC test assertion. All
nine jumbo probes were acknowledged and 9,000-byte packets crossed the relay,
but later packet loss triggered Quinn's fallback to 1,200 bytes. The fixture
had asserted that the final MTU must remain large. It now verifies acknowledged
discovery before bulk traffic, then checks every echoed byte and the narrow
path's size limit. The isolated Linux retest and both full workspace reruns
passed. The failed workspace log
is retained as `performance-final-validation/linux-workspace-before-relay-fix.log`.

All fifteen Linux live suites passed, covering 386 checks, including the
separate admin/security retest described below. The [suite results](release-0.8.1-validation.csv)
record the final exit code and attempt count. The 97-check replication suite kept one
follower down for 600 seconds, built a 32-MiB incompressible backlog, verified
catch-up and byte-identical committed prefixes, and tested minimum-ISR refusal
without appending. Consumer groups passed 30 checks. The six-kill chaos audit
retained all 900 acknowledged records without duplicates or offset gaps.

The admin/security suite initially stopped before its quota baseline: topic
creation had committed at the controller before the broker exposed the new
topic. Its readiness check now waits, with a deadline, for read-only offsets
to expose the empty partition. It does not retry writes or alter the expected
record counts. The retest passed all 29 checks using a Go-enabled container
for certificate generation. Quota enforcement preserved all 8,000 records;
mutual TLS, SCRAM, ACL denial and runtime topic configuration checks passed.
After the final QUIC change, the Linux transport suite was rerun and passed all
18 TCP/TLS/QUIC checks for payload identity, restart durability, ordering,
idempotent replay, stored bytes and consumer groups. The other live-suite
results precede that transport-only change.
The refreshed Kafka comparison passed all twelve scenario groups. The finite coverage is mapped
in the [validation matrix](release-validation-matrix.md).

## Benchmark method

The release matrix covers replication factors 1 and 3, producer/consumer
concurrency, TCP and encrypted QUIC, 1-MiB records, all five codecs,
acknowledgment modes, idempotence and a fixed offered rate. Brokers and their
clients share equally limited Docker containers on one development host.
Each broker receives four CPUs and 4 GiB. Docker exposes 28 CPUs and
50,510,606,336 bytes of memory, running engine 29.7.2 with WSL2 kernel
`6.18.33.2-microsoft-standard-WSL2`. The final isolated attempt temporarily stopped
14 unrelated Docker application containers, with user approval, and restored
them afterward. Other host services remained outside the benchmark's control.
Kafka uses plaintext TCP; its transport comparison with encrypted QUIC includes
encryption and transport costs. Codec payloads are highly compressible.

Kafka broker heaps are 2 GiB. Client heaps are 512 MiB except the eight-client
concurrency harness, which uses 64 MiB initially and 128 MiB maximum. That
harness sets a 32-MiB producer buffer on both systems. Replicated and large-record
Kafka producers instead use a 256-MiB buffer; the native producer retains its
32-MiB default, with CLI outstanding-record bounds of 4,096 and 64 respectively.
These are system-plus-client comparisons with equal container limits, not an
isolation of broker memory or identical client queue implementations.

Initial consumer-group delay is now explicitly matched at zero on both systems;
the shipping Brahmaputra default remains one second. Consequently, reduced
short-run consumption time includes a configuration correction.

Resource probes capture cumulative CPU counters and working-set memory every
50 ms. Multi-node summaries use the overlapping observation window and
simultaneous memory totals. CPU core-seconds measure total work; average CPU
percentage measures utilization during each system's own run. The window
includes client startup/exit dispatch and probe overhead. Sample peaks are
observed/interpolated, not instantaneous hardware peaks. Working-set memory
subtracts inactive file cache and is not total resident/container memory.

Small throughput differences require repeated runs. An offered-rate cap limits
throughput by construction. A finite comparison cannot establish superiority
for every possible configuration or deployment. Retain losing rows in the
published reports and keep performance separate from message-survival audits.

The first new matrix attempt is excluded: both Kafka producers completed their
record counts, but a roughly 13-second cgroup observation crossed the Windows
harness's 180-second deadline. A separate 45-second status wait returned after
256 seconds. This host timing discontinuity invalidates that comparison. The
fresh run preserves the failed artifacts and temporarily inhibits automatic
system sleep, restoring the request when the benchmark process exits.

A later phase exposed the clock problem directly: the client completed in
2 seconds and cgroup sampling covered 5.15 seconds, but `date` subtraction
reported 310.27 seconds. Benchmark elapsed times and deadlines now use the OS
monotonic clock through Node's `hrtime`. Reported phase durations must fit their
resource observation windows; the regression rejects that recorded anomaly.
That fourth attempt stopped after ten completed groups when the host session
was interrupted. A QUIC change was also left outside its tested source version.
The complete matrix passed with that change under
`bench/results/performance-candidate-matrix-v7/`; earlier artifacts are retained.

The fifth attempt failed RF=3 production at four clients. All three brokers
expired their five-second leases and re-registered at epoch 2; two producers
returned `FENCED_BROKER_EPOCH`, while two completed 500,000 records. The client
defaults allow five retries with a 100-ms backoff, which can run out before
lease recovery finishes. The cgroup probes' largest gap in that native phase
was 60 ms, so this is not attributed to the earlier clock discontinuity. The
cause of the heartbeat delay remains unresolved. The sixth attempt keeps the
same source, limits, counts and retry settings; a successful retest does not
erase this recovery-under-load limitation.

The unchanged sixth attempt also failed, at two RF=3 clients, with broker
fencing and missing-topic errors during recurring lease recovery. A subsequent
host check reported about 20% full I/O pressure over five minutes and 30% over
ten seconds, with negligible CPU and memory pressure. Fourteen unrelated
application containers were running. This supports investigating storage
contention, but does not establish its cause or excuse the failed workloads.
Neither attempt is certified. The seventh attempt pauses the 14 unrelated
containers with user approval, preserving source, broker limits, counts,
timeouts and retry settings. All twelve groups passed; all fourteen application
containers were restored and verified running afterward. The passing replicated
run still recorded lease expiry and successful epoch-2 recovery. Isolation did
not establish a fix for heartbeat stalls or robustness under storage contention.

## Final measurements

The [published matrix](benchmarks/0.8.1/README.md) contains 344 paired comparisons
and 94 resource observations, with no missing measurements. Source SHA-256
hashes and Docker image IDs identify the measured working tree and environment.
The hashes cover file bytes, including the working tree's line endings.

Brahmaputra leads 69 of 84 throughput comparisons and uses fewer CPU core-seconds
in 50 of 52 comparisons. Its sampled average and peak working-set memory are
lower in all 104 comparisons; CPU utilization is lower in 95 of 104 comparisons.
These are correlated measures of finite, single-pass workloads, not independent
trials or a claim of universal superiority.

Losses include RF=1 single-client production measured inside the client,
selected TCP/QUIC concurrency levels, and 1-MiB consumption on both transports.
Large-record QUIC uses 30.39 versus Kafka's 18.06 CPU core-seconds for production
and 22.38 versus 9.06 for consumption. All losing rows remain in the reports.
The original goal of winning every workload with lower CPU and memory is not
achieved; the recovery-under-load failures also remain unresolved limitations.
