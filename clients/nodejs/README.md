# Brahmaputra client for Node.js

Requires Node 18+. Verified end to end against a live broker: **34/34
checks** (`node test_manual.js`).

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
