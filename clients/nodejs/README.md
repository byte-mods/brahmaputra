# Brahmaputra client for Node.js

Requires Node 18+ and nothing else: `npm install /path/to/clients/nodejs`
(or `"brahmaputra": "file:../nodejs"`); there is no build step. TypeScript
typings ship in `src/index.d.ts`.

Verified end to end against a live broker: **83/83
checks** (`./test.sh HOST PORT`, i.e. `node test_manual.js HOST PORT`). Beyond
the Go suite's checks it shows every setting below changing behaviour:
linger, batch size, timestamps, retries against a fault-injecting proxy,
fetch limits, heartbeats, static membership, leave on close, generation
fencing and a registered codec.

## Produce

```js
const { Producer, RecordHeader } = require('brahmaputra');

const producer = await Producer.connect('127.0.0.1', 9092, {
  acks: 1,
  lingerMs: 5,
  compressionType: 'gzip',
});

// Keyed: murmur2(key) % partitions, so records sharing a key keep order.
await producer.send('orders', Buffer.from('{"id":1}'), {
  key: Buffer.from('user-7'),
  headers: [new RecordHeader('trace-id', Buffer.from('abc-123'))],
});

// Explicit partition and timestamp (unix ms, number or bigint); a null
// value is a tombstone, distinct from an empty Buffer.
await producer.send('orders', 'replayed', { partition: 3, timestamp: 1_700_000_000_000 });
await producer.send('orders', null, { key: 'user-7' });

// Or wait for one record's offset (a BigInt). Records already buffered for
// that partition go first.
const offset = await producer.sendSync('orders', '{"id":2}', { key: 'user-9' });

await producer.flush();   // rejects with any background (linger) flush failure
await producer.close();   // flushes first
```

## Consume one partition

```js
const { Consumer, LATEST } = require('brahmaputra');

const consumer = await Consumer.connect('127.0.0.1', 9092);
const records = await consumer.fetch('orders', 0, 0n);
for (const record of records) {
  console.log(record.offset, record.key?.toString(), record.value.toString());
}
const end = await consumer.listOffsets('orders', 0, LATEST);
consumer.close();
```

Offsets are **BigInt**. A partition can hold more than
`Number.MAX_SAFE_INTEGER` records, and silently losing precision on an
offset is the kind of bug that only shows up in production.

## Consume as a group

```js
const { GroupConsumer, Assignor, AutoOffsetReset } = require('brahmaputra');

const consumer = await GroupConsumer.connect('127.0.0.1', 9092, 'billing', {
  assignor: Assignor.STICKY,
  autoOffsetReset: AutoOffsetReset.EARLIEST,
  autoCommitIntervalMs: 0,        // commit explicitly
  groupInstanceId: 'worker-3',    // static membership
});
consumer.subscribe(['orders']);

for (;;) {
  for (const record of await consumer.poll(500)) {
    handle(record.value);
  }
  // At-least-once: commit after processing, never before.
  await consumer.commit();
}

// Commits, then leaves, so partitions move immediately rather than after
// a session timeout.
await consumer.close();
```

## Configuration

Options are the camelCase forms of Kafka's names; `defaultProducerConfig()`,
`defaultConsumerConfig()` and `defaultGroupConfig()` return the defaults.

| Producer | Kafka | Default |
|---|---|---|
| `acks` | `acks` | 1 (`0`, `1`, `-1` = all) |
| `batchSize` | `batch.size` | 16384 |
| `lingerMs` | `linger.ms` | 5 (0 sends each record at once) |
| `compressionType` | `compression.type` | `'none'` |
| `requestTimeoutMs` | `request.timeout.ms` | 30000 (broker-side ack wait) |
| `retries` / `retryBackoffMs` | `retries` / `retry.backoff.ms` | 5 / 100 (retriable errors only) |
| `deliveryTimeoutMs` | `delivery.timeout.ms` | 120000 |
| `bufferMemory` / `maxBlockMs` | `buffer.memory` / `max.block.ms` | 32 MiB / 60000 |
| `socketTimeoutMs` | client-side round-trip deadline | 120000 (0 disables) |

| Consumer | Kafka | Default |
|---|---|---|
| `fetchMaxBytes` / `fetchMinBytes` / `fetchMaxWaitMs` | `fetch.*` | 8 MiB / 1 / 500 |
| `maxPollRecords` | `max.poll.records` | 500 |
| `isolationLevel` / `rack` | `isolation.level` / `client.rack` | 0 / `''` |
| `socketTimeoutMs` | client-side round-trip deadline | 120000 |

| Group | Kafka | Default |
|---|---|---|
| `sessionTimeoutMs` | `session.timeout.ms` | 10000 |
| `heartbeatIntervalMs` | `heartbeat.interval.ms` | 0 (session timeout / 3) |
| `rebalanceTimeoutMs` | `rebalance.timeout.ms` | 3000 |
| `maxPollIntervalMs` | `max.poll.interval.ms` | 300000 (time inside `poll()` never counts) |
| `autoCommitIntervalMs` | `auto.commit.interval.ms` | 5000 (0 disables auto-commit) |
| `autoOffsetReset` | `auto.offset.reset` | `'earliest'` (`'latest'`, `'none'` throws `NoOffsetForPartition`) |
| `assignor` | `partition.assignment.strategy` | `'range'` (`'roundrobin'`, `'sticky'`) |
| `groupInstanceId` | `group.instance.id` | `''` (dynamic member) |
| `maxPollRecords` / `fetchMaxBytes` / `socketTimeoutMs` | | 500 / 8 MiB / 120000 |

A connection that fails or times out is closed and redialled on next use
(the seed included). A group member's `memberId`, `generation` and
`assignment` are readable properties; `close()` commits and sends
LeaveGroup, and a member the coordinator forgot (`UNKNOWN_MEMBER_ID`)
rejoins as a new one.

## Compression

`none` and `gzip` always work; `zstd` works on Node 22.15+, where `zlib`
gained it. Register anything else yourself:

```js
const { registerCodec, Compression } = require('brahmaputra');
registerCodec(Compression.SNAPPY, { compress, decompress });
```

If you register lz4, note that the broker expects a little-endian `uint32`
of the uncompressed length followed by a raw LZ4 **block** — not the LZ4
frame format, which a frame-format library would silently produce instead.

## Running the end-to-end test

```bash
brahmaputra-server --data-dir ./data --default-partitions 4
./test.sh 127.0.0.1 9092     # node --check every file, then node test_manual.js
```

It prints `83 passed, 0 failed` and exits non-zero on any failure.
