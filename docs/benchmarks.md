# Benchmarks

Apache Kafka vs Brahmaputra over TCP vs Brahmaputra over QUIC, the method
behind the numbers, and the optimisation work they drove.

Reproduce with:

```bash
bash scripts/bench-matched.sh          # resource-matched, all three systems
bash scripts/bench-three-way.sh        # 1 MiB records, all three
bash scripts/bench-vs-kafka.sh         # small records, Kafka vs Brahmaputra
bash scripts/bench-tune-brahmaputra.sh # producer config sweep
```

---

## 1. Method

### Host

| | |
|---|---|
| CPU | AMD Ryzen AI MAX+ 395 w/ Radeon 8060S, 32 logical processors |
| Memory | 64 GB (60.6 GiB visible to the Linux VM) |
| OS | Windows 11 Home Single Language 10.0.26200 |
| Container runtime | Docker Desktop 29.6.2, WSL2 backend |
| VM kernel | 6.18.33.2-microsoft-standard-WSL2 |
| Storage | overlayfs on the WSL2 virtual disk |

Docker Desktop on Windows virtualises both disk and network, so absolute
numbers are below bare metal **for every system measured**; the comparison
between them is the useful part. Each container is capped at 4 of the
host's 32 logical CPUs, so no run can starve the machine.

### What every container gets

Identical for Kafka, Brahmaputra/TCP and Brahmaputra/QUIC:

| | |
|---|---|
| CPUs | 4 (`--cpus 4`) — so **400 % is the ceiling** in every table below |
| Memory | 4 GiB (`--memory 4g`) |
| Partitions per topic | 6 |
| Replication factor | 1 (single node) |
| Record size | 256 B for throughput, 1 MiB for the transport comparison |
| `acks` | 1 |
| `batch.size` | 64 KiB (2 MiB for 1 MiB records) |
| `linger.ms` | 5 |
| Compression | none |
| Max message / frame bytes | 16 MiB on both sides |
| Records | 1 000 000 per client, at 1, 2, 4 and 8 concurrent clients |

The load generator runs **inside the broker container** on both sides, so
sampled CPU and memory cover broker *plus* client for everyone. Kafka
speaks its own protocol and Brahmaputra speaks its own, so a shared load
generator is impossible; these are system+client numbers, which is what a
user actually experiences.

### Kafka configuration

`apache/kafka:3.9.0` in KRaft mode (broker and controller in one process),
tuned rather than left at defaults:

```
KAFKA_NUM_PARTITIONS=6
KAFKA_LOG_SEGMENT_BYTES=1073741824       # 1 GiB: no segment roll mid-run
KAFKA_NUM_NETWORK_THREADS=4
KAFKA_NUM_IO_THREADS=8
KAFKA_MESSAGE_MAX_BYTES=16777216
KAFKA_REPLICA_FETCH_MAX_BYTES=16777216
KAFKA_SOCKET_REQUEST_MAX_BYTES=104857600
KAFKA_SOCKET_SEND_BUFFER_BYTES=1048576   # up from the 100 KiB default
KAFKA_SOCKET_RECEIVE_BUFFER_BYTES=1048576
KAFKA_QUEUED_MAX_REQUESTS=1000
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 # no artificial rebalance wait
```

JVM, broker process:

```
-Xmx2g -Xms2g
-XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35
-XX:G1HeapRegionSize=16M -XX:MetaspaceSize=96m
-XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80
-XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true
```

Two details here matter, and both were worth 2x or more:

1. **2 GiB heap, not 3.** Kafka wants memory in the *page cache* — it
   writes into the cache and serves reads from it with `sendfile`. A 3 GiB
   heap inside a 4 GiB container starves exactly the thing Kafka depends
   on.
2. **Clients get their own small heap.** `kafka-run-class.sh` reads
   `KAFKA_HEAP_OPTS`, so it applies to every Kafka CLI tool, not just the
   broker. Setting one large heap on the container silently gives each
   perf-test client that same heap, and eight clients each reserving 3 GiB
   inside a 4 GiB container is a self-inflicted memory wall — it dropped
   Kafka's 8-client produce to 62k msgs/sec. Each client now gets
   `-Xmx512m -Xms512m` passed at exec time, and the same run yields
   147k–187k.

Producer: `--throughput -1` (unthrottled), `acks=1`, `batch.size=65536`,
`linger.ms=5`, `compression.type=none`, `max.request.size=16777216`,
`buffer.memory=268435456`. Consumer: `--fetch-size 16777216`, a fresh
group per run.

### Brahmaputra configuration

The same release binary for both transports; only `--transport` differs:

```
brahmaputra-server --host <name> --port 9092 --data-dir /data \
  --default-partitions 6 --segment-bytes 1073741824 \
  --transport tcp|quic
```

Producer: `--acks 1 --batch-size 65536 --linger-ms 5 --in-flight 4096
--compression none --no-key --value-size 256`. Consumer:
`--from earliest --quiet`.

`--in-flight 4096` is the counterpart of Kafka's `buffer.memory=256MB`. It
bounds outstanding *records*, and it must exceed `batch.size / record size`
or a batch can never fill and every flush waits out `linger.ms`: at 256 B
records a window of 64 pins the producer at ~4 000 msgs/sec regardless of
broker speed. That is a real tuning constraint, documented in the README
configuration reference.

### How the numbers are taken

- **Throughput** is reported two ways. *Wall clock* runs from before the
  first client starts to after the last exits, so it includes process
  startup. *Client-measured* sums each client's own reported rate, which
  excludes startup. The distinction matters — JVM startup is seconds, the
  Rust CLI startup is milliseconds, so wall clock alone would flatter
  Brahmaputra. Both are recorded; the tables quote wall clock.
- **CPU and memory** come from `docker stats`, sampled once per second for
  each phase, reported as average and peak. A phase shorter than the first
  sample yields no samples and reads as 0 — that happens on the fastest
  single-client consume runs and is a gap in the harness, not a
  measurement of zero.
- **Resource matching.** A single client can leave a fast broker idle,
  which reads as "similar throughput" when it means "the client ran out of
  work to give". Each system is therefore driven at 1, 2, 4 and 8 clients,
  and the headline compares the level whose average CPU is closest to
  Kafka's, alongside each system's best level.

## 2. Results: 256 B records

Every number below comes from **one run of `scripts/bench-matched.sh`**, so
all three systems saw the same machine state.

### At matched CPU (~400 %, the container ceiling)

| Metric | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| Produce msgs/sec | 257 848 | **772 947** (3.00x) | 418 498 (1.62x) |
| Produce CPU % avg | 399.3 | 403.2 | 402.7 |
| Produce memory MiB avg | 2080 | **256** | 197 |
| Consume msgs/sec | 572 656 | **3 238 866** (5.66x) | 924 642 (1.61x) |
| Consume CPU % avg | 350.5 | 331.1 | 320.9 |
| Consume memory MiB avg | 2722 | **98** | 95 |

### Best level each system reached

| | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| Peak produce msgs/sec | 257 848 | **916 380** (3.55x) | 419 287 (1.63x) |
| Peak consume msgs/sec | 572 656 | **3 652 968** (6.38x) | 1 230 769 (2.15x) |

### Scaling, level by level (wall clock, msgs/sec)

| Clients | Kafka produce | TCP produce | QUIC produce | Kafka consume | TCP consume | QUIC consume |
|---|---|---|---|---|---|---|
| 1 | 226 963 | 394 322 | 231 911 | 193 761 | 981 354 | 505 817 |
| 2 | 223 839 | 676 361 | 388 425 | 420 433 | 1 846 722 | 924 642 |
| 4 | 257 848 | **916 380** | 418 498 | 572 656 | 3 238 866 | **1 230 769** |
| 8 | 187 534 | 772 947 | **419 287** | 391 504 | **3 652 968** | 1 181 509 |

All three fall off at 8 clients on 4 CPUs, which is the expected shape once
context switching costs more than the extra concurrency buys.

Memory is the most stable difference across every run and it is structural
rather than tuning: the JVM holds its heap and copies records through it,
while the Rust broker passes refcounted `Bytes` slices and leans on the
page cache, so its resident set stays in the tens or low hundreds of MiB
regardless of load — 8–28x less here.

## 3. Results: 1 MiB records

2000 records of 1 MiB (2 GiB of payload), 6 partitions, acks=1,
`batch.size=2 MiB`, `linger.ms=5`, no compression.

| Metric | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| Produce MB/sec | 170.9 | **418** (2.45x) | 112 |
| Consume MB/sec | 221.5 | **942** (4.25x) | 106 |
| Produce CPU % (avg) | 222.5 | **207.0** | 311.0 |
| Produce memory MiB (avg) | 1446 | 362 | **356** |
| Consume CPU % (avg) | 149.8 | **116.0** | 270.3 |
| Consume memory MiB (avg) | 2952 | 1065 | **792** |
| Disk bytes per 1 MiB record | — | 1 048 614 | 1 048 612 |
| Produce MB/sec per CPU % | 0.8 | **2.0** | 0.4 |

At megabyte records Brahmaputra over TCP is 2.5x Kafka on produce and
**4.3x on consume**, on less CPU and a third of the memory.

Consume used to be the exception — 377 MB/sec against Kafka's 575. It took
two rounds of read-path work (§4) to turn that around: first removing the
three copies between the page cache and the socket, then removing the copy
*out of* the page cache with `sendfile`. The CPU figure is the tell —
consume went from 603 to 942 MB/sec while its CPU **fell** from 155 % to
116 %, which is what disappears when bytes stop moving through userspace.

QUIC is weak here for the reason set out in §5: at megabyte records it is
CPU-bound on per-packet crypto and userspace congestion control, and a
single connection's packet processing does not parallelise.

Storage overhead is 39 bytes per 1 MiB record (batch header plus record
framing), about 0.004 %.

## 4. Optimisations these benchmarks drove

Each was found by measuring, fixed, and re-measured. The full test suite
and the live verification scripts pass after all of them.

**1. The high-watermark checkpoint fsynced on every append.**
Every batch, on every partition, did an `open` + `set_len` + `seek` +
`write_all` + `sync_all`. An append-heavy run sat at ~99 % of one core with
three cores idle, and got *slower* as partitions were added. It was also
incoherent: the record data itself is left to the operating system, as
Kafka leaves it, so the most expensive operation available was being spent
on the weakest guarantee in the system. Now written on a 5 s timer, as
Kafka's `replica.high.watermark.checkpoint.interval.ms` does, plus on flush
and on clean shutdown. `__consumer_offsets` still checkpoints eagerly,
because a committed consumer offset that vanishes on restart is a
correctness break and commits are far too infrequent to cost anything.
**Produce 115 667 → 356 005 msgs/sec (3.08x); consume 803 471 → 1 633 617
(2.03x).**

**2. One topic-partition per Produce/Fetch request.**
The per-request cost was paid per partition, and at 256 B that cost
dominates. `ProduceMulti` (key 15) and `FetchMulti` (key 16) now carry
every partition a client holds on one broker in a single request, and the
broker appends to and reads from them concurrently. **6.4x consume.**

**3. The broker processed one request at a time per connection.**
The client in-flight window bought nothing, and one long poll or `acks=all`
wait blocked every request behind it. Requests are now dispatched
concurrently behind a bounded in-flight semaphore, with a single writer
task per connection. Safe because correlation ids already allow
out-of-order responses and each partition is still a single writer.
**+17 % produce.**

**4. A batched flush held every partition send lock across the round trip.**
That left exactly one request in flight per broker connection. A broker's
partitions are now split into `max.in.flight` fixed shards; a partition
always travels in the same shard, so it still has at most one request
outstanding — preserving per-partition order — while shards overlap.
**+20 % produce.**

**5. Producer batching collapsed under concurrency.**
Every `send()` that found a full batch queued its own flush behind the
partition send lock; each queued flusher then shipped a near-empty batch
and paid a full round trip, so raising client concurrency *lowered*
throughput. A size-triggered flush is now skipped when one is already
running, and the running flusher drains until the buffer is empty, with the
linger ticker as backstop. **5 162 → 61 187 msgs/sec (11.9x).**

**6. No `TCP_NODELAY` on accepted sockets.**
Only the client set it, so responses waited on Nagle plus the peer delayed
ACK. Invisible on loopback, brutal across a bridge.
**6 415 → 14 790 msgs/sec in Docker.**

**7. The broker decoded and re-encoded every batch on append.**
Decompressing and parsing every record contradicted the design rule that
batches are written unmodified. Now the header alone is validated and
`base_offset`/`leader_epoch` are stamped in place; both sit *before*
`crc32c`, so the checksum stays valid. **68 031 → 90 948 msgs/sec (+34 %).**

**8. A batched fetch could build a response larger than the client's frame.**
Not a performance bug at all — the 1 MiB benchmark was *hanging*. A
partition's read stops only after the batch that crosses its allowance, so
every partition overshoots by up to a whole batch; with multi-megabyte
batches across six partitions those overshoots added up past
`max_frame_bytes`, the client's length-delimited decoder rejected the
frame, and the connection died. It surfaced intermittently, as
"connection closed" or as a stalled consumer, depending on how much data
happened to be available. The budget is now enforced across the whole
response and trimmed from the tail, which is safe because a fetch may
always return less than was asked for — the client simply asks again. The
first partition keeps at least one batch regardless, or a consumer whose
batches exceed the budget could never advance. Covered by
`large_records_across_partitions_stay_inside_the_frame_limit`, which fails
with `ConnectionClosed` without the fix.

**9. Every fetched byte was copied four times before the kernel saw it.**
`pread` into a buffer, concatenate the batches into one body, copy that
body into a frame, then copy the frame into the codec's write buffer.
Responses are now a chain of `Bytes` written with a single `writev`:
the frame prefix is built separately and the record batches go to the
socket as they came off disk. Three of the four copies are gone, and the
encoders are proven byte-identical to the concatenating ones they
replaced. **Consume 377 → 603 MB/sec at 1 MiB records.**

**10. The last copy: `sendfile`.**
After §9 the only remaining copy was the one *out of* the page cache, which
Kafka does not make at all. A fetch on the plaintext path now describes
its result as file ranges rather than buffers — `Log::read_regions` reads
27 bytes per batch, through `last_offset_delta`, which is enough to apply
the offset filter, the high-watermark cutoff and the byte budget without
touching the payload — and the socket sends those ranges with `sendfile`.
Adjacent batches in a segment merge into one range, so a multi-batch fetch
is usually a single call. Segments hold their file behind an `Arc`, so a
range stays valid even if retention unlinks the segment while a response
is still being written. **Consume 603 → 942 MB/sec at 1 MiB, with CPU
falling from 155 % to 116 %** — more bytes on less CPU is what a removed
copy looks like.

Only plaintext TCP qualifies: TLS and QUIC have to see the bytes to
encrypt them, and Kafka draws the same line — enabling SSL disables its
`sendfile` path too. QUIC instead hands its buffers to quinn with
`write_chunks`, which queues them rather than copying each into the send
buffer, so every copy QUIC *can* avoid is avoided.

**11. Reads made two syscalls and a zero-filled allocation per batch.**
One `pread` now covers a run of batches and each is handed out as a slice
of that shared buffer.

**12. A batched fetch held per-partition errors behind the long poll.**
"No bytes served" was treated the same as "no data yet", so a consumer
whose committed offset had fallen off the log could burn its entire poll
deadline waiting instead of being told to reset — and a group resuming
after retention read nothing. Errors now return immediately.

## 5. TCP vs QUIC

QUIC carries the identical frame format; what changes is multiplexing.
Each request gets its own **bidirectional stream**, so correlation ids are
unnecessary and a lost packet delays only its own request instead of
everything queued behind it. `acks=0` uses unidirectional streams. TLS 1.3
is mandatory in QUIC, so the broker generates a self-signed certificate at
startup.

**Behaviour is identical** (`scripts/verify-transport-parity.sh`, 18/18):

| Check | TCP | QUIC |
|---|---|---|
| accuracy — 500 records byte-identical, none lost or duplicated | pass | pass |
| durability — offsets and data survive a broker restart | pass | pass |
| ordering — 200 same-key records on one partition, in send order | pass | pass |
| retry — idempotent replay returns the original offset, appends nothing | pass | pass |
| zero copy — the producer bytes appear verbatim in the segment file | pass | pass |
| consumer groups — bounded run, resume from commit, final lag 0 | pass | pass |

The full replication suite also passes over QUIC
(`TRANSPORT=quic bash scripts/verify-replication.sh`, 14/14): byte-identical
replicas under `acks=all`, ISR failover with no loss, a killed broker
resyncing in ~2 s, and `min.insync.replicas` enforcement.

**Performance is not identical.** At 256 B records QUIC reaches 1.6–2.2x
Kafka where TCP reaches 3.0–6.4x, so QUIC costs roughly half of TCP
throughput. At 1 MiB records the gap is wider. QUIC is often described as
"faster than TCP", so it is worth being precise about why it is not here.

*First hypothesis, and it was wrong.* quinn defaults target a lossy ~100 ms
internet path: `stream_receive_window` 1.25 MB (a single 1 MiB record
nearly fills it), `send_window` 10 MB, `initial_rtt` 333 ms. Those look
fatal for megabyte records on a LAN. Retuning all of them (32 MB per
stream, 256 MB per connection, 5 ms initial RTT, 1350 B initial MTU) bought
**7 %** — 113 → 121 MB/s. Flow control was not the limit.

*What it actually is: CPU.* Throughput against the broker CPU budget, 800
records of 1 MiB:

| CPUs | QUIC | TCP |
|---|---|---|
| 2 | 62 MB/s | 259 MB/s |
| 4 | 118 MB/s | 293 MB/s |
| 8 | 109 MB/s | 278 MB/s |

QUIC **doubles** from 2 to 4 cores — it is CPU-bound. TCP is already near
its ceiling at 2 cores, delivering 4x QUIC throughput per core. Above 4
cores QUIC stops scaling, because a single connection packet processing is
effectively serialised in one task.

The per-packet work TCP does not pay for:

1. **Mandatory per-packet AEAD.** Every datagram is encrypted and
   authenticated, plus header protection. At 121 MB/s with a ~1350 B MTU
   that is ~94 000 packets/sec of crypto. The TCP path here is plaintext,
   so this is *not* like-for-like: it is QUIC-with-TLS against
   TCP-without.
2. **Userspace loss recovery and congestion control.** The kernel does
   this for TCP with state the stack is optimised around; quinn runs it
   per packet in userspace.
3. **No segmentation offload.** TCP gets TSO/GSO. QUIC per-packet framing
   forfeits it, and under Docker Desktop virtual NIC UDP GSO is
   unavailable, so each datagram costs its own syscall.

None of this is a QUIC defect; it is the cost of moving the transport into
userspace, and it is why HTTP/3 deployments care about kernel UDP offloads.
**QUIC advantages do not show on this benchmark**: they appear as loss
rises (a dropped packet stalls one stream, not all of them), as RTT rises,
and when a client network changes under it. A LAN with no loss and a
sub-millisecond RTT is precisely where TCP wins.

**Choose QUIC for** lossy or long-haul links, environments that require
encryption in transit today, and connection migration across network
changes. On a datacentre LAN, TCP is the better default — which is why it
*is* the default.

## 6. Caveats

- One run per configuration, no repetition or confidence intervals.
- **Run-to-run variance is high.** Kafka own produce peak moved between
  147k and 258k msgs/sec across runs on this host, depending on what else
  the machine was doing. Every quoted comparison therefore comes from a
  single run in which all three systems were measured back to back; never
  compare a number here against one from a different run.
- Single node, RF=1. Replication changes the produce path materially
  (`acks=all` waits for the ISR) and is measured by
  `scripts/verify-replication.sh` rather than here.
- Kafka disk usage reads as n/a in some runs: its image runs as uid 1000
  and the harness could not always resolve the log directory.
- The `docker stats` sampler polls once per second, so a phase that
  finishes faster than the first poll records no samples.
