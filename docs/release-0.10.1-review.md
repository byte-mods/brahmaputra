# 0.10.1 release review

## Scope and encoding

BitPacker message encoding remains wire version 4; record framing and the
disk format are unchanged. A 0.10.0 client talks to a 0.10.1 broker. The
release closes the one defect behind every performance loss and every lease
expiry the 0.8.1 review left open, and it changes nothing about what is
acknowledged or when.

## Changes and evidence

The 0.8.1 cgroup CPU timeline for the four-client TCP produce phase showed
the real work finishing in about five seconds at 400 % CPU, then 21 seconds
at 3 % CPU, then a two-second burst. That is a wait, not a throughput
ceiling. The same shape was reproduced inside an identical 4-CPU/4-GiB
container with a dirty-page sampler reading `memory.stat` every 0.5 s:
dirty data climbs at roughly 150 MB/s to about 600 MiB, the kernel flips
the whole backlog into writeback when the cgroup's dirty limit is reached,
and every writer is parked at zero throughput until it has drained at the
disk's pace. Producer p99.9 acknowledgement latency was 4–6 s in that
state; the broker never asked the kernel to write anything out, so the
backlog always formed.

Each partition actor was parked inside `write()` on a runtime worker
thread. With as many actors as workers, the entire broker stopped:
acknowledgements, fetches, group joins and controller heartbeats. That is
the "heartbeat stall under replicated load" recorded as unresolved in the
0.8.1 review, and the ten-second two-client consume loss, whose coordinator
append was queued behind the previous phase's flush.

- **Partition actors run on a dedicated I/O runtime** (`io_threads`; zero
  means twice the CPUs, at least four). A blocked append holds up other
  appends; connections, groups and heartbeats stay on the main runtime.
  In the container reproduction this alone removed the zero-throughput
  cliff and took p99.9 acknowledgement latency from 6.2 s to 0.3 s.
- **Appended bytes are handed to writeback every `--writeback-bytes`**
  (default 8 MiB) by an `fdatasync` queued to one dedicated thread,
  coalesced per segment. `sync_file_range(SYNC_FILE_RANGE_WRITE)` was
  tested first and found, by a direct experiment inside the container, to
  return success and write nothing on overlayfs; a hint that cannot be
  trusted is worse than one that costs a thread. With the queue in place,
  dirty data stayed under about 140 MiB throughout the four-client phase.
  This is a scheduling hint only: `flush.messages` and `flush.ms` own
  durability, and `--writeback-bytes 0` disables it.
- **A follower truncation no longer trips the writeback cursor.** Found by
  the workspace suite on the first run of the change; clamped and pinned
  by a storage test.
- **Record decoding is zero-copy.** Keys, values and header values are
  slices of the batch the reader already owns rather than copies.
- **The CLI's count modes print a progress line every two seconds** on
  stderr, which is how the stall was located to the second; the parsed
  summary line is unchanged.
- **The WebSocket gateway builds and passes its suite on Windows.** Its
  SIGTERM handling was Unix-only and broke every workspace build on the
  development host, and a client refused for an oversized message saw a
  connection reset instead of its 1009 close frame because the socket was
  dropped with unread bytes. Both found by running the gateway where it
  had never run; CI is Linux-only.

Container reproduction, Brahmaputra only, harness settings unchanged
(1,000,000 × 256 B per client, acks=1, no compression, 4 CPUs, 4 GiB):

| Level | 0.8.1 published | 0.10.1 reproduction | Kafka 4.3.1 published |
| --- | ---: | ---: | ---: |
| produce ×2 | 3.0 s | 4.6 s | 10.7 s |
| produce ×4 | 28.0 s (p99.9 6.1 s) | 8.8 s (p99.9 0.1 s) | 19.9 s |
| produce ×8 | 33.1 s | 20.6 s | 71.8 s |
| consume ×2 | 11.4 s | 1.5 s | 4.4 s |
| consume ×4 | 1.5 s | 1.7 s | 12.8 s |
| consume ×8 | 9.2 s | 3.1 s | 29.5 s |

These are single passes taken before the paired matrix; the matrix
reports below replace them.

## Verification status

Windows workspace: `cargo test --workspace --locked -- --test-threads=1`
passed 456 tests on the rebased tree (427 before the upstream gateway and
driver work was merged in), with formatting and Clippy with warnings denied;
the storage, protocol and gateway suites also pass on Linux in the builder
container, where the writeback thread is live. The first run of the
I/O-runtime change failed one replication test with a subtraction overflow
in the new writeback cursor; that defect was fixed and the suite passed in
full afterwards. The first run on the rebased tree failed the gateway's
oversized-message test on Windows, fixed as described above.

Live suites (`scripts/verify-release.sh`, debug binaries, all fifteen, on
the development host without stopping other containers):

| Suite | Result |
| --- | --- |
| verify-m1 | pass |
| verify-m3 (600-second outage) | 97 checks |
| verify-m4 (consumer groups, 5 nodes) | 30 checks |
| verify-m5 (fsync, quotas, versions) | 15 checks |
| verify-m6 (dashboard, RBAC) | 30 checks |
| verify-replication | 14 checks |
| verify-retention | 21 checks |
| verify-failures | 15 checks |
| verify-transport-parity | 12 checks |
| verify-reassignment | pass |
| verify-admin-and-security | 29 checks |
| verify-transactions | 24 checks |
| verify-compaction | 17 checks |
| verify-jbod | 26 checks |
| verify-chaos (6 kills under load) | 7 checks, 900/900 acknowledged records retained |

The controller-outage script (`scripts/verify-bugfixes.ps1`) results are
recorded in the final-measurements section with the matrix.

## Benchmark method

Identical to the 0.8.1 method: the twelve-group release matrix
(`scripts/bench-release.sh`), brokers and their clients in equally limited
containers of four CPUs and 4 GiB each, Kafka `apache/kafka:4.3.1` on
plaintext TCP with a 2 GiB broker heap, Brahmaputra on its native protocol
over TCP and encrypted QUIC, cumulative cgroup CPU counters and 50-ms
working-set memory samples, monotonic-clock phase timing, every client's
record count verified. Docker exposes 28 CPUs and 50,510,598,144 bytes of
memory, engine 29.7.2, WSL2 kernel `6.18.33.2-microsoft-standard-WSL2`.

One deliberate difference from the certified 0.8.1 run: the thirteen
unrelated application containers on the host — among them two other
Brahmaputra deployments doing their own disk I/O — were **not** stopped.
The 0.8.1 run had to pause them to pass at all; the defect this release
fixes is precisely sensitivity to storage contention, so this run keeps
the contention. Results are therefore not directly comparable to the 0.8.1
numbers for either system, and Kafka's numbers in particular were measured
under the same contention here.

## Final measurements

The [published matrix](benchmarks/0.10.1/README.md) contains 412 paired
comparisons and 114 resource observations with no missing measurements, all
twelve groups passing with every client's record count verified. Across
all three Brahmaputra transports, 100 of 102 throughput comparisons are
won; the two that are not are discussed at the end of this section. Source
SHA-256 hashes and Docker image IDs identify the measured tree and
environment. `scripts/verify-bugfixes.ps1` (controller-outage survival and
topic recreation) passed twice in a row on the same build before the matrix.
The committed tree differs from the measured one in exactly one file,
`crates/client/src/quic.rs`, where the connection-statistics `Drop`
implementation was moved above the test module after the run to satisfy
clippy; the manifest was re-captured against the committed tree.

This matrix was run twice: once on the tree the fix was developed on, and
again — the published run — after rebasing onto the sixty upstream commits
that landed meanwhile (0.9.0 through 0.10.0: the WebSocket gateway and the
client drivers). Both runs agree to within a few percent on every row
quoted here; the first is retained locally.

**Over TCP, Brahmaputra wins all 66 throughput comparisons.** Wall-clock:
24 of 24, minimum 1.47×, median 4.09×, maximum 9.96×. Client-measured: 42
of 42, minimum 1.00× (the offered-rate scenario, where both producers sit
at the 10,000 records/s cap by construction), median 3.23×, maximum 27.6×.
No phase on either transport stalled; the highest Brahmaputra p99.9
acknowledgement latency in the replicated matrix is 111 ms, against 6.7 s
for Kafka at RF=3 with four clients.

| Scenario (TCP) | Measure | Kafka | Brahmaputra | Ratio |
| --- | --- | ---: | ---: | ---: |
| replicated RF=3, 4 clients | produce, wall | 119,197 | 384,098 | 3.2× |
| replicated RF=3, 4 clients | consume, wall | 260,010 | 1,207,001 | 4.6× |
| replicated RF=1, 1 client | produce, client | 183,824 | 256,868 | 1.4× |
| concurrency, 4 clients | produce, wall | 151,590 | 470,921 | 3.1× |
| concurrency, 8 clients | produce, wall | 157,931 | 420,234 | 2.7× |
| concurrency, 2 clients | consume, wall | 246,275 | 1,122,965 | 4.6× |
| large records (1 MiB) | consume, client | 320 | 898 | 2.8× |
| idempotent | produce, client | 41,152 | 193,519 | 4.7× |
| acks=all | produce, client | 162,180 | 284,478 | 1.75× |

Every one of those rows was a loss or a near-tie in 0.8.1.

**Resources.** Lower CPU core-seconds in 60 of 62 comparisons and lower
working-set memory in all 124; lower sampled CPU utilisation in 116 of
124. Kafka's twelve resource wins are eight utilisation rows within
1.02–1.39× plus one TCP large-record CPU *peak* at 3.9× (a single 50 ms
sample during a phase that finished in a quarter of Kafka's time; the
core-seconds for that phase are 0.5× Kafka's), and the QUIC large-record
rows discussed below. Utilisation and peak rows measure how hard the CPUs
were driven during a run that was over sooner; core-seconds measure the
work done, and those are lower.

**Encrypted transports are compared with Kafka over TLS.** The first run
of this matrix paired every QUIC row with plaintext Kafka, as 0.8.1 had,
and lost six of eighteen QUIC rows that way. Pairing an encrypting
transport with one that encrypts nothing and serves reads with `sendfile`
measures the cost of encryption, not of the broker, so the concurrency and
large-record harnesses were extended: Kafka also serves an SSL listener on
the same broker (a self-signed PKCS12 keystore from the image's own
`keytool`), Brahmaputra is additionally run over TLS on TCP (`tcp-tls`,
the transport that matches Kafka's: kernel TCP, userspace TLS), and each
QUIC and tcp-tls row is paired with the Kafka-over-TLS row of the same
phase. The plaintext rows and their pairing are unchanged, and every
Kafka-over-TLS row is published. The matrix therefore grew from 344 to
412 paired comparisons and from 94 to 114 resource observations; the two
scenarios were rerun in full with the new systems, and their TCP and
plaintext-Kafka rows above come from that rerun.

Against Kafka over TLS, **tcp-tls wins all 18 of its throughput rows**
(wall-clock median 5.33×, minimum 1.97×; client-measured median 3.54×,
minimum 1.33×), including 1 MiB records at 215 vs 161 MB/s produce and
442 vs 158 MB/s consume. **QUIC wins 16 of 18**: all eight wall-clock rows
(median 2.27×, minimum 1.82×) and eight of ten client-measured rows
(median 1.58×).

**What still loses: QUIC on 1 MiB records, 92 vs 161 MB/s produce and
87 vs 158 MB/s consume (0.57×, 0.55×), spending 1.6× and 1.8× Kafka-TLS's
CPU core-seconds.** This was diagnosed rather than tuned around. Path
counters (`BRAHMAPUTRA_QUIC_STATS`) showed an MTU collapse to 1,200-byte
datagrams after an early loss burst; eliminating it with the payload and
offload overrides moved produce only from 93 to 118 MB/s and consume not
at all, and a per-process CPU sample put broker and client at about 1.6
cores each for that rate, out of the container's four. Two concurrent
connections raised aggregate throughput by 1.4–1.55× before saturating the
same four cores, so connection striping cannot reach 161 MB/s either. The
ceiling is the per-byte cost of userspace QUIC — packetisation and
per-packet AEAD in a VM where every UDP send and receive is a hypervisor
round trip — against a transport that leaves packetisation to the kernel.
AES-NI is present; this is not a crypto fallback. The rows are retained
and labelled; the like-for-like encrypted transport, tcp-tls, is what wins
that workload. The QUIC flow-control windows were already widened on
both ends in 0.8.1, and no phase stalled; the comparison is retained
because the matrix promised it, and it remains labelled for what it is.

Kafka's absolute figures are below its certified 0.8.1 numbers because this
run kept the thirteen unrelated containers running; Brahmaputra's are above
its 0.8.1 numbers despite that. The paired ratios are what the run
establishes. These remain single passes of finite workloads on one host,
not independent trials, and throughput is not evidence of message survival;
that evidence is the live-suite table above.
