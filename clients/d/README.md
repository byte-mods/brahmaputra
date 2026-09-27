# Brahmaputra client for D

A native D driver for Brahmaputra's wire protocol, built on Phobos and
druntime alone (`std.socket`, `std.zlib`, `core.thread`, `core.sync`). It
has no dependencies and no FFI.

Verified end to end against a live broker: **54/54 checks**
(`./test.sh HOST PORT`, which builds with `ldc2 -w` and runs
`test/manual_test.d`, a port of the Go suite).

## Install / build

With dub, add the package (it has no dependencies):

```json
"dependencies": { "brahmaputra": { "path": "../clients/d" } }
```

Without dub, compile the sources in with your program; LDC 1.36 (DMD
front end 2.106) or newer:

```bash
ldc2 -w -O -Iclients/d/source clients/d/source/brahmaputra/*.d app.d -of=app
```

The modules are `brahmaputra.protocol` (encodings, CRC32C, murmur2,
codecs), `.connection` (sockets and the leader router), `.producer`,
`.consumer`, `.group` and `.assignor`; `import brahmaputra;` imports them all.

## Null versus empty

Keys, values and header values are `const(ubyte)[]`. A slice that
`is null` is **null**; any other slice, including a zero-length one, is a
value. So a tombstone is `null` and an empty value is `emptyBytes()`. This
matters in D because the literal `[]`, and `.dup` of an empty array, are
both null. `toBytes("text")` converts a string and never returns null.
Decoded records keep the distinction, so test it with `is null`. `==`
treats null and empty as equal.

## Produce

```d
import brahmaputra;

ProducerConfig config;
config.acks = 1;                 // 0, 1, or -1 for "all"
config.lingerMs = 5;
config.compressionType = "gzip";

auto producer = new Producer("127.0.0.1:9092", config);
scope (exit) producer.close();   // flushes; throws if a flush failed

// Keyed: murmur2(key) % partitions, so records sharing a key keep order.
producer.send("orders", toBytes(`{"id":1}`), toBytes("user-7"),
    [RecordHeader("trace-id", toBytes("abc-123")), RecordHeader("none", null)]);

// An explicit partition, and a tombstone (null value) for key k3.
producer.sendTo("orders", 0, null, toBytes("k3"));

// Or wait for one record's offset: a full round trip, correct and slow.
long offset = producer.sendSync("orders", toBytes(`{"id":2}`));

producer.flush();   // also reports a failed background (linger) flush
```

A `Producer` is thread-safe; share one. Each partition has at most one
batch in flight, so a linger flush and a batch-full flush never reorder
it. When `buffer.memory` is full, `send` blocks for up to `max.block.ms`
and then throws `BufferFullException`.

## Consume one partition

```d
auto consumer = new Consumer("127.0.0.1:9092");
scope (exit) consumer.close();

foreach (record; consumer.fetch("orders", 0, 0, 500))
    writeln(record.offset, " ", cast(const(char)[]) record.value, " ", record.timestamp);

auto result = consumer.fetchVerbose("orders", 0, 0, 500);   // + result.highWatermark
long end   = consumer.listOffsets("orders", 0, LATEST);     // EARLIEST, LATEST or a unix-ms time
```

## Consume as a group

```d
GroupConfig config;
config.assignor = ASSIGNOR_STICKY;              // range (default), roundrobin, sticky
config.autoOffsetReset = AUTO_OFFSET_RESET_EARLIEST;
config.autoCommitIntervalMs = 0;                // commit explicitly
config.groupInstanceId = "worker-3";            // static membership

auto consumer = new GroupConsumer("127.0.0.1:9092", "billing", config);
consumer.subscribe(["orders"]);
scope (exit) consumer.close();   // commits, then leaves so partitions move at once

while (running)
{
    foreach (record; consumer.poll(500.msecs))
        handle(record.value);
    consumer.commit();           // at-least-once: after processing, never before
}
```

A `GroupConsumer` belongs to one thread, as Kafka's consumer does. A
background thread heartbeats and enforces `max.poll.interval.ms`. A member
that goes longer than that between polls leaves the group and rejoins on
its next `poll`. Time spent inside `poll`, such as a slow join or a long
wait for data, does not count against the interval.

## Configuration reference

`ProducerConfig`:

| Field | Kafka name | Default |
|---|---|---|
| `acks` | `acks` | `1` |
| `batchSize` | `batch.size` | 16 KiB |
| `lingerMs` | `linger.ms` | `5` (Kafka: 0) |
| `compressionType` | `compression.type` | `"none"` |
| `requestTimeoutMs` | `request.timeout.ms` (broker-side ack wait) | 30 000 |
| `retries` | `retries` | `5` |
| `retryBackoffMs` | `retry.backoff.ms` | `100` |
| `deliveryTimeoutMs` | `delivery.timeout.ms` | 120 000 |
| `bufferMemory` | `buffer.memory` | 32 MiB |
| `maxBlockMs` | `max.block.ms` | 60 000 |
| `connectTimeout` | — | 30 s |
| `socketTimeout` | socket round-trip bound | 120 s |
| `clientId` | `client.id` | `"brahmaputra-d"` |

`ConsumerConfig`: `fetchMaxBytes` (8 MiB), `fetchMinBytes` (1),
`fetchMaxWaitMs` (500), `maxPollRecords` (500), `isolationLevel`
(`READ_UNCOMMITTED`), `rack` (`client.rack`), `connectTimeout`,
`requestTimeout` (120 s), `clientId`.

`GroupConfig`: `sessionTimeoutMs` (10 000), `rebalanceTimeoutMs` (3 000),
`maxPollIntervalMs` (300 000), `autoCommitIntervalMs` (5 000; 0 disables),
`autoOffsetReset` (`earliest`/`latest`/`none`), `assignor`
(`range`/`roundrobin`/`sticky`), `groupInstanceId`, `maxPollRecords`,
`fetchMaxBytes`, `connectTimeout`, `requestTimeout`, `clientId`.

## Errors and connections

Every exception derives from `BrahmaputraException`:

- `ServerException`: the broker returned an error code. It carries `.code`
  and `.context`.
- `ConnectionException`: an I/O error, a timeout or a correlation
  mismatch.
- `ProtocolException`: undecodable bytes.
- `BufferFullException`: `buffer.memory` stayed full past `max.block.ms`.
- `DeliveryException`: a background flush failed. The cause is in
  `.next`.
- `NoOffsetForPartitionException`: raised under
  `auto.offset.reset=none`.

Every request has a round-trip deadline. After a timeout, an I/O error or
a correlation mismatch, the connection is closed and `broken` becomes
true. The router redials it on next use, and that includes the seed
connection.

## Compression

`none` and `gzip` (through `std.zlib`) are built in. The others are
opt-in, so this package pulls in no dependencies of its own:

```d
registerCodec(Compression.zstd,
    (const(ubyte)[] payload) => myZstdCompress(payload),
    (const(ubyte)[] payload) => myZstdDecompress(payload));
```

If you register lz4, note that the broker expects a little-endian `uint`
of the uncompressed length followed by a raw LZ4 **block**. A library that
writes the LZ4 frame format will produce batches the broker cannot read.

## Running the end-to-end suite

```bash
brahmaputra-server --data-dir ./data --default-partitions 4
clients/d/test.sh 127.0.0.1 9092
```

`test.sh` works from any directory. It builds with `ldc2 -w -O`, using
`$DC` if set, or falls back to `dub build --config=manual-test` when ldc2
is absent. It exits non-zero if any check fails. The unit tests run with
`ldc2 -w -unittest -main -Isource source/brahmaputra/*.d`.
