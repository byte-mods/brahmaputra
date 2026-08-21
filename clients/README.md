# Client drivers

Native clients for Brahmaputra. Each speaks the wire protocol directly
rather than wrapping the Rust client, so there is no FFI, no sidecar and
no shared native library to ship.

| Language | Directory | Built | Tested against a live broker |
|---|---|---|---|
| Go | [go/](go) | ✅ `go build ./...`, `go vet` clean | ✅ **34/34** |
| Node.js | [nodejs/](nodejs) | ✅ loads on Node 22 | ✅ **34/34** |
| Python | [python/](python) | ❌ no interpreter on the build host | ❌ not run — see below |
| Java | [java/](java) | ❌ no JDK on the build host | ❌ not run — see below |

**On the two untested drivers.** Python and Java are written to the same
design as the two that were verified, and the design is the part that was
in doubt — the wire format is unusual enough that a transcription slip
shows up immediately, and Go and Node caught none. But "written to a
verified design" is not "verified", and the difference matters: neither
has been compiled or executed, so treat them as unreviewed until you have
run their suites. The commands are below; each prints the same 34 checks
the other two do.

## What every driver implements

The configuration names mirror Kafka's, because the point of a client
library is that someone who knows Kafka does not have to learn a new
vocabulary to use this one.

**Producer** — `acks` (0/1/all), `batch.size`, `linger.ms`,
`compression.type`, `request.timeout.ms`, `retries`, `retry.backoff.ms`,
`delivery.timeout.ms`, `buffer.memory`, `max.block.ms`, keyed partitioning
via Kafka's `murmur2`, explicit-partition sends, record headers, and
per-record timestamps.

**Consumer** — `fetch.max.bytes`, `fetch.min.bytes`, `fetch.max.wait.ms`,
`max.poll.records`, offset lookup by `earliest`/`latest`/timestamp, and
the partition high watermark.

**Consumer groups** — join/sync/heartbeat with generation fencing,
`range`, `roundrobin` and `sticky` assignment, `auto.offset.reset`
(`earliest`/`latest`/`none`), `auto.commit.interval.ms`,
`session.timeout.ms`, `max.poll.interval.ms`, `group.instance.id` for
static membership, and an explicit `LeaveGroup` on close.

## The three encodings

Most of the work in each driver is keeping three encodings apart, because
they share one connection and none of them agrees with the others.

| | Encoding |
|---|---|
| **Frame header** | Fixed big-endian: `int32` length prefix, `int16` api key, `int16` api version, `int32` correlation id, `int16`-prefixed client id. |
| **Request/response body** | BitPacker. Every integer is a **zigzag varint**; every string and array is a varint count then its contents; the whole body is prefixed with the schema version string `"1.0.0"`. |
| **Record batch** | Neither. Fixed big-endian header fields, then **plain** (non-zigzag) varints inside each record. |

The record batch is different on purpose: the broker stamps `base_offset`
and `leader_epoch` into it in place — both sit before the CRC, so it stays
valid — and validates the CRC without decoding the records. The bytes a
producer sends are the bytes on disk, which is why a driver that encodes a
batch wrongly corrupts the log rather than merely failing a request.

Two details that are easy to get wrong and fail silently:

- **CRC32C, not CRC32.** Record batches use the Castagnoli polynomial.
  Every standard library's `crc32` is the wrong one.
- **`murmur2` must be Kafka's.** A Python producer and a Rust producer
  writing the same key have to land on the same partition, so each driver
  transcribes the algorithm rather than importing "a murmur2". All of them
  agree that `murmur2("") == 275646681`, which the test suites assert.

## Compression is deliberately not uniform

Each driver carries the codecs its standard library already has and makes
the rest opt-in, so an application that does not want an lz4 or zstd
dependency does not acquire one by using this client.

| Driver | Built in | Opt-in |
|---|---|---|
| Go | `none`, `gzip` | `lz4`, `zstd`, `snappy` via `RegisterCodec` |
| Node.js | `none`, `gzip`, `zstd` (Node ≥ 22.15) | `lz4`, `snappy` via `registerCodec` |
| Python | `none`, `gzip` | `lz4`, `zstd`, `snappy` via `pip install` |
| Java | `none`, `gzip` | `lz4`, `zstd`, `snappy` via `registerCodec` |

If you register lz4 yourself, note that the broker uses
`lz4_flex::compress_prepend_size`: a little-endian `u32` of the
uncompressed length followed by a **raw LZ4 block**. That is not the LZ4
frame format, so a frame-format library will produce batches the broker
cannot read.

## Running the test suites

Start a broker first:

```bash
brahmaputra-server --data-dir ./data --default-partitions 4
```

Then, from this directory:

```bash
cd go && go run ./cmd/manualtest 127.0.0.1:9092
```

```bash
cd nodejs && node test_manual.js 127.0.0.1 9092
```

```bash
cd python && python3 test_manual.py 127.0.0.1 9092
```

```bash
cd java && javac -d out $(find src -name '*.java') ManualTest.java && java -cp out ManualTest 127.0.0.1 9092
```

Each suite asserts properties of the system rather than that a function
ran: records come back byte-identical with contiguous offsets, a key pins
every record to one partition and preserves order within it, headers and
null header values survive, timestamps are real wall-clock values, a group
splits partitions and a rejoining member resumes from its commit instead
of replaying, `auto.offset.reset=none` refuses to guess, and a full client
buffer blocks and then reports rather than growing without limit.

## What none of them does

- **No Kafka wire compatibility.** These speak Brahmaputra's protocol.
  Existing Kafka clients do not work against this broker, and these
  drivers do not work against Kafka.
- **No transactions.** Exactly-once semantics are not implemented
  broker-side, so no client exposes them.
- **No TLS or QUIC yet.** The drivers speak plaintext TCP. The broker's
  `--transport tcp-tls` and `--transport quic` listeners need TLS support
  in each driver, which is not written. Authentication is implemented
  (`Authenticate`, api key 17) but the broker refuses credentials on a
  plaintext listener, so it is unusable from these drivers until TLS
  lands.
- **No idempotent producer.** `InitProducerId` and magic-v2 batches are
  understood on the read path but no driver allocates a producer id, so
  an ambiguous send cannot be safely replayed.
