# Brahmaputra client for Nim

```bash
nimble install https://github.com/byte-mods/brahmaputra?subdir=clients/nim
# or, from a checkout:
cd clients/nim && nimble install
```

Nim 1.6+, standard library only — no nimble dependencies. It speaks the
wire protocol directly over `std/net` sockets. The one native library it
uses is the system zlib (`libz.so.1`, bound with `{.importc, dynlib.}`)
for the built-in gzip codec; build with `-d:brahmaputraZlib=false` to
leave it out.

Compile applications with threads and ORC:

```bash
nim c -d:release --threads:on --mm:orc app.nim
```

Verified end to end against a live broker: **54/54 checks**
(`./test.sh HOST PORT`, which compiles `tests/manual_test.nim` with
`nim c -d:release --threads:on --mm:orc` and runs it; the build fails on
any compiler warning from this package).

## Threads: what runs where

The driver uses real threads (`--threads:on --mm:orc`), like the Go and
Java drivers, rather than doing everything inside your calls:

- **Producer.** One *linger thread* per producer flushes every `lingerMs`.
  Every flush — linger, batch-full, `flush()`, `close()` — takes one
  producer-wide flush lock for the whole round trip (and any retries), so a
  partition has at most one batch in flight and batches leave in the order
  they filled. A linger flush that fails is held and raised by the next
  `flush()` or `close()`; `close()` still stops the thread and closes the
  connections when that happens. The producer is safe to share between
  threads.
- **Group consumer.** One *heartbeat thread* per member heartbeats and
  enforces `maxPollIntervalMs`. It uses **its own connection** to the
  coordinator, so a long fetch or JoinGroup on your thread never delays a
  heartbeat, and it shares only the membership fields (member id,
  generation, joined, poll stamps) with your thread, under a lock. Use a
  group consumer from one thread, as with Kafka.
- **Router.** Every router operation (metadata, leader lookup, request)
  runs under the router's lock, and a request is sent while holding it,
  so connections never cross threads. Shared objects hold no GC'd
  references that another thread copies; the threads work through raw
  pointers to lock-protected state, and the ref types are `{.acyclic.}`.

## Produce

```nim
import brahmaputra

var config = defaultProducerConfig()
config.acks = 1
config.lingerMs = 5
config.compressionType = "gzip"

let producer = newProducer("127.0.0.1:9092", config)
defer: producer.close()

# Keyed: murmur2(key) % partitions, so records sharing a key keep order.
producer.send("orders", """{"id":1}""", key = some("user-7"),
              headers = [header("trace-id", "abc-123"), nullHeader("reason")])

# An explicit partition, an explicit timestamp, an empty value, a tombstone.
producer.sendTo("orders", 0, "", key = some("k"))                  # empty value
producer.sendTo("orders", 0, none(string), key = some("k"))        # tombstone
producer.sendTo("orders", 1, "late", timestamp = 1_700_000_000_000)

# Or wait for one record's offset. A full round trip — correct, and slow.
let offset = producer.sendSync("orders", some("""{"id":2}"""))

producer.flush()
```

Bytes are Nim `string`s. Anything that may be null — key, value, header
value — is an `Option[string]`, so `none(string)` (null) and `some("")`
(empty) stay distinct end to end.

## Consume one partition

```nim
let consumer = newConsumer("127.0.0.1:9092")
defer: consumer.close()

for record in consumer.fetch("orders", 0, 0, 500):
  echo record.offset, " ", record.key, " ", record.value, " ", record.timestamp

let r = consumer.fetchVerbose("orders", 0, 0)       # records + high watermark
echo r.highWatermark
let first = consumer.listOffsets("orders", 0, Earliest)
let next  = consumer.listOffsets("orders", 0, Latest)
let atT   = consumer.listOffsets("orders", 0, 1_700_000_000_000)  # by timestamp
```

## Consume as a group

```nim
var config = defaultGroupConfig()
config.assignor = AssignorSticky
config.autoOffsetReset = AutoOffsetResetEarliest
config.autoCommitIntervalMs = 0          # commit explicitly
config.groupInstanceId = "worker-3"      # static membership

let consumer = newGroupConsumer("127.0.0.1:9092", "billing", config)
consumer.subscribe(["orders"])
defer: consumer.close()   # commits, then leaves so partitions move at once

while true:
  for record in consumer.poll(500):
    handle(record.value)
  # At-least-once: commit after processing, never before.
  consumer.commit()
```

`poll` raises `NoOffsetForPartitionError` when `autoOffsetReset` is
`"none"` and a partition has no committed offset.

## Configuration

| `ProducerConfig` | Kafka name | Default |
|---|---|---|
| `clientId` | `client.id` | `brahmaputra-nim` |
| `acks` | `acks` (0, 1, -1 = all) | 1 |
| `batchSize` | `batch.size` | 16384 |
| `lingerMs` | `linger.ms` | 5 (Kafka: 0) |
| `compressionType` | `compression.type` | `none` |
| `requestTimeoutMs` | `request.timeout.ms` (broker-side ack wait) | 30000 |
| `retries` | `retries` | 5 |
| `retryBackoffMs` | `retry.backoff.ms` | 100 |
| `deliveryTimeoutMs` | `delivery.timeout.ms` | 120000 |
| `bufferMemory` | `buffer.memory` | 33554432 |
| `maxBlockMs` | `max.block.ms` | 60000 |
| `dialTimeoutMs` | TCP connect timeout | 30000 |
| `socketTimeoutMs` | client-side bound on one round trip | 120000 |

| `ConsumerConfig` | Kafka name | Default |
|---|---|---|
| `fetchMaxBytes` | `fetch.max.bytes` | 8388608 |
| `fetchMinBytes` | `fetch.min.bytes` | 1 |
| `fetchMaxWaitMs` | `fetch.max.wait.ms` | 500 |
| `maxPollRecords` | `max.poll.records` | 500 |
| `isolationLevel` | `isolation.level` | `ReadUncommitted` |
| `rack` | `client.rack` | empty |
| `dialTimeoutMs`, `socketTimeoutMs` | as above | 30000, 120000 |

| `GroupConfig` | Kafka name | Default |
|---|---|---|
| `sessionTimeoutMs` | `session.timeout.ms` | 10000 (Kafka: 45000) |
| `rebalanceTimeoutMs` | `rebalance.timeout.ms` | 3000 |
| `maxPollIntervalMs` | `max.poll.interval.ms` | 300000 |
| `autoCommitIntervalMs` | `auto.commit.interval.ms` (0 = off) | 5000 |
| `autoOffsetReset` | `auto.offset.reset` | `earliest` |
| `assignor` | `partition.assignment.strategy` | `range` |
| `groupInstanceId` | `group.instance.id` | empty (dynamic) |
| `maxPollRecords`, `fetchMaxBytes` | as above | 500, 8388608 |

`maxPollIntervalMs` is stamped when `poll` is entered and when it returns,
and never enforced while a poll is running: a poll that blocks on a slow
rebalance or waits for data is the consumer working normally.

## Failures

Errors are exceptions, all derived from `BrahmaputraError`:
`ServerError` (with `.code`), `ConnectionError` and its subtype
`RequestTimeoutError`, `DecodeError`, `BufferFullError`,
`NoOffsetForPartitionError` and `CodecError`.

Every request has a round-trip timeout (`socketTimeoutMs`, or
`conn.setRequestTimeout(ms)` on a bare `Conn`). A timeout, an I/O error or
a correlation-id mismatch closes the connection and marks it `broken`; the
router redials it on next use, the seed connection included. A produce
that fails on a dropped connection is **not** retried automatically (it
may have been appended), so the caller decides.

## Compression

`none` and `gzip` (system zlib) are built in. The rest are opt-in, so this
package pulls in no dependencies of its own:

```nim
proc zstdCompress(data: string): string {.nimcall, gcsafe.} = ...
proc zstdDecompress(data: string): string {.nimcall, gcsafe.} = ...
registerCodec(compressionZstd, zstdCompress, zstdDecompress)
```

Register codecs before creating producers or consumers. If you register
lz4, note that the broker expects a little-endian `uint32` of the
uncompressed length followed by a raw LZ4 **block** — not the LZ4 frame
format, which a frame-format library would silently produce instead.

## Running the end-to-end suite

```bash
brahmaputra-server --data-dir ./data --default-partitions 4
clients/nim/test.sh 127.0.0.1 9092        # works from any directory
```

It prints `54 passed, 0 failed` and exits non-zero on any failure.

## Not implemented

- SASL `Authenticate` (SCRAM-SHA-256): the broker refuses credentials on a
  plaintext listener, and Nim's standard library has no SHA-256.
- TLS/QUIC, idempotent and transactional producers — as for every driver
  other than Rust.
