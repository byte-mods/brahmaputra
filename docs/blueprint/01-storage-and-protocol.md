# Blueprint 01 — Record Batches & Segmented Log Storage

> Status: written from DESIGN.md §4/§6 ahead of the M1 implementation; every
> claim here is reconciled against the actual `protocol`/`storage` crate code
> at the end of M1 (see the Verification section).

This document explains how data is represented on disk and on the wire, and
how the storage engine appends, reads, recovers, and retains it.

---

## 1. The record batch is the only unit

Everything — producer requests, disk segments, replication traffic, fetch
responses — moves as **record batches**. The broker never re-encodes records:
the exact bytes a producer sends are appended to the leader's log, streamed
to followers, and later served to consumers.

### 1.1 Batch layout

```
┌────────────────────────────────────────────────────────────┐
│ base_offset        i64   offset of the first record        │
│ batch_length       i32   bytes after this field            │
│ leader_epoch       i32   epoch of leader that wrote it     │
│ magic              u8    format version (evolvability)     │
│ crc32c             u32   covers everything after this field│
│ attributes         u16   bit 0-2: compression (0=none,1=lz4)│
│ last_offset_delta  i32   records_in_batch - 1              │
│ max_timestamp      i64   max record timestamp in batch     │
│ records            ...   varint-framed record list         │
└────────────────────────────────────────────────────────────┘
```

Each record inside the batch:

```
length        varint
timestamp_delta i64   (relative to batch max_timestamp)
key_length    varint (-1 = null key)
key           bytes
value_length  varint
value         bytes
```

Design consequences:

- **Offsets are assigned by the broker**, not the producer. The producer
  sends a batch with `base_offset = 0` placeholder; the leader stamps the
  real base offset at append time. (`last_offset_delta` lets any reader
  compute the last offset without decoding records.)
- **CRC per batch** is the corruption boundary: recovery scans batch-by-batch
  and truncates at the first bad/torn batch — a torn tail write loses at
  most one batch.
- **Compression is per batch**: records are compressed together; the batch
  header stays uncompressed so offsets/CRC can be checked without inflating.

## 2. Partition directory layout

```
<log_dir>/
  <topic>-<partition>/
    00000000000000000000.log        # data segment (append-only)
    00000000000000000000.index      # sparse offset index
    00000000000000000000.timeindex  # sparse time index
    00000000000000481234.log
    ...
    hwm                             # high-watermark checkpoint
```

- A **segment** is one `.log` file plus its indices. The file name is the
  base offset, zero-padded to 20 digits → lexicographic order = offset order.
- Only the **last segment is active** (writable); older segments are
  immutable, which is what makes retention and replication simple.

## 3. Write path

1. `Log::append(batch)` stamps `base_offset = log_end_offset`, appends the
   batch bytes to the active segment with a single positioned write.
2. If the bytes written since the last index entry exceed
   `index_interval_bytes` (4 KiB), an entry `(relative_offset: u32,
   file_position: u32)` is appended to `.index`.
3. When the active segment exceeds `segment_bytes`, it is closed and a new
   segment starts at the next offset (**segment roll**).
4. Durability is **replication-first**: no fsync per append. The page cache
   absorbs writes; `acks=all` + ISR ≥ 2 means acknowledged data lives on
   multiple machines before the client is told. A periodic fsync
   (`flush.interval.ms`) is the backstop against multi-node power loss.

The sparse index is the key space optimization: ~2 bytes of index per KB of
log, binary-searchable, and a miss costs a scan of at most
`index_interval_bytes`.

## 4. Read path

1. Look up the segment: binary search segment base offsets for the greatest
   base ≤ target offset.
2. Binary search that segment's `.index` for the greatest indexed offset ≤
   target → file position.
3. Seek and scan forward batch-by-batch (cheap: ≤ 4 KiB) until the batch
   containing the target offset.
4. Return **raw batch bytes** from there up to `max_bytes` — no decoding on
   the broker's hot path; the consumer decodes.

## 5. Crash recovery (on `Log::open`)

1. List segment files, sort by base offset.
2. For every segment except the last: trust it (indices may be stale only in
   the active segment).
3. For the active segment: scan batches from the start (using the index to
   skip to the last known-good checkpoint), validate each header + CRC, and
   **truncate at the first invalid or torn batch**. Rebuild the index for
   the scanned tail.
4. `log_end_offset` resumes from the last valid batch; the `hwm` checkpoint
   file restores the high watermark (clamped to ≤ log end).

A killed broker therefore always comes back with a prefix-valid log: every
offset < recovered log end is intact and CRC-verified.

## 6. Retention

`apply_retention()` runs periodically:

- Delete whole segments whose **max timestamp** is older than
  `retention.ms`, or while total size exceeds `retention.bytes` (oldest
  first). The active segment is never deleted.
- Deleting segments advances `log_start_offset`; consumers holding an offset
  below it get `OffsetOutOfRange` and reset per their policy
  (earliest/latest).

## 7. Concurrency model

The storage crate is deliberately **sync and single-threaded**. Each
partition log is owned by exactly one broker-side partition actor (a tokio
task); all appends/reads for that partition are messages to that actor.
Single-writer principle: no locks, no atomics, no cross-partition coupling
in the storage engine itself.

---

## Verification (M1)

- [x] Unit tests: batch round-trip, CRC corruption, LZ4, torn-write
      recovery, segment roll, retention — all green (`cargo test`).
- [x] Live check: CLI produce → bytes visible in `<topic>-0/*.log` → CLI
      fetch returns identical records.
- [x] Kill -9 the broker mid-write, restart, fetch again: all previously
      acknowledged offsets still readable, log end continuous.
