# Brahmaputra from TypeScript

There is no separate TypeScript driver. The Node.js driver in
[`../nodejs`](../nodejs) ships type declarations
([`src/index.d.ts`](../nodejs/src/index.d.ts), wired up through `"types"` in
its `package.json`) that describe every export exactly as the JavaScript
behaves at runtime. This directory holds the proof:

- `src/test_manual.ts` — the Node end-to-end suite (83 checks, same sections
  and names as `../nodejs/test_manual.js`) ported to strict TypeScript and
  compiled against the typings.
- `tests/types.test-d.ts` — compile-time tests of the typings: statements that
  must compile, exact-type assertions, and `// @ts-expect-error` lines (wrong
  `acks`, a missing group id, a dotted Kafka option name, an unnarrowed
  tombstone value, ...) that must fail. It is type-checked, never run.

Verified against a live broker: **83 passed, 0 failed**.

## Install

The driver has no runtime dependencies. A TypeScript project needs
`typescript` and `@types/node` (the typings use `Buffer`, `net.Socket` and
`NodeJS.Timeout`):

```sh
npm install --save-dev typescript @types/node
npm install /path/to/brahmaputra/clients/nodejs     # or "brahmaputra": "file:../nodejs"
```

Any `module`/`moduleResolution` that reads `package.json` `"types"` works;
this directory uses `node16` with `strict`, `noImplicitAny` and
`noUncheckedIndexedAccess`. The driver is CommonJS, so `import { Producer }
from 'brahmaputra'` works from CommonJS output, and from ESM via Node's CJS
interop.

## Produce

```ts
import { Producer, RecordHeader, type ProducerOptions } from 'brahmaputra';

const options: ProducerOptions = {
  acks: -1,                 // 0 | 1 | -1 — Kafka's "all" is -1; anything else is a type error
  lingerMs: 5,              // linger.ms
  batchSize: 16 * 1024,     // batch.size
  compressionType: 'gzip',  // 'none' | 'gzip' | 'zstd' | 'lz4' | 'snappy'
  bufferMemory: 32 << 20,   // buffer.memory
  maxBlockMs: 60_000,       // max.block.ms
};
const producer = await Producer.connect('127.0.0.1', 9092, options);

await producer.send('orders', Buffer.from('{"id":1}'), {
  key: 'user-7',                                    // Buffer | string | typed array | null
  headers: [new RecordHeader('trace-id', 'abc-123'), new RecordHeader('empty', null)],
});
await producer.send('orders', null, { key: 'user-7' });   // tombstone
await producer.send('orders', 'replayed', { timestamp: 1_700_000_000_000 });  // unix ms; default now
const offset: bigint = await producer.sendSync('orders', 'one at a time');
await producer.close();   // flushes; rejects if a background flush failed
```

## Consume

```ts
import { Consumer, EARLIEST, LATEST, type ConsumerRecord } from 'brahmaputra';

const consumer = await Consumer.connect('127.0.0.1', 9092, { fetchMaxWaitMs: 500 });
const records: ConsumerRecord[] = await consumer.fetch('orders', 0, 0n);
for (const record of records) {
  // offset/timestamp are bigint; key/value are Buffer | null — null is a
  // missing key or a tombstone, an empty Buffer is an empty one.
  console.log(record.offset, record.key?.toString(), record.value?.toString() ?? '<tombstone>');
  const trace: Buffer | null = record.header('trace-id');
}
const end: bigint = await consumer.listOffsets('orders', 0, LATEST);
const { highWatermark } = await consumer.fetchVerbose('orders', 0, EARLIEST);
consumer.close();
```

## Consume as a group

```ts
import { GroupConsumer, Assignor, AutoOffsetReset, NoOffsetForPartition } from 'brahmaputra';

const group = await GroupConsumer.connect('127.0.0.1', 9092, 'billing', {
  assignor: Assignor.STICKY,                  // 'range' | 'roundrobin' | 'sticky'
  autoOffsetReset: AutoOffsetReset.EARLIEST,  // 'earliest' | 'latest' | 'none'
  autoCommitIntervalMs: 0,                    // commit explicitly
  sessionTimeoutMs: 10_000,
  maxPollIntervalMs: 300_000,
  groupInstanceId: 'worker-3',                // static membership
});
group.subscribe(['orders']);
try {
  for (;;) {
    for (const record of await group.poll(500)) handle(record);
    await group.commit();        // at-least-once
  }
} catch (error) {
  if (error instanceof NoOffsetForPartition) { /* auto.offset.reset=none */ }
  throw error;
} finally {
  await group.close();           // commit + LeaveGroup
}
```

## Config reference

Option names are the camelCase forms of Kafka's, exactly as the JS reads them
(dotted names such as `'linger.ms'` are a type error). All are optional;
`defaultProducerConfig()`, `defaultConsumerConfig()` and
`defaultGroupConfig()` return the defaults, typed as the full
`ProducerConfig` / `ConsumerConfig` / `GroupConfig`.

| Producer            | Default        | Consumer          | Default | Group                  | Default    |
|---------------------|----------------|-------------------|---------|------------------------|------------|
| `clientId`          | `brahmaputra-node` | `clientId`    | same    | `clientId`             | same       |
| `acks`              | `1`            | `fetchMaxBytes`   | 8 MiB   | `sessionTimeoutMs`     | 10000      |
| `batchSize`         | 16384          | `fetchMinBytes`   | 1       | `rebalanceTimeoutMs`   | 3000       |
| `lingerMs`          | 5              | `fetchMaxWaitMs`  | 500     | `maxPollIntervalMs`    | 300000     |
| `compressionType`   | `'none'`       | `isolationLevel`  | 0       | `autoCommitIntervalMs` | 5000 (0 = off) |
| `requestTimeoutMs`  | 30000          | `rack`            | `''`    | `autoOffsetReset`      | `'earliest'` |
| `retries`           | 5              | `maxPollRecords`  | 500     | `assignor`             | `'range'`  |
| `retryBackoffMs`    | 100            |                   |         | `groupInstanceId`      | `''`       |
| `deliveryTimeoutMs` | 120000         |                   |         | `maxPollRecords`       | 500 (<=0 = no cap) |
| `bufferMemory`      | 32 MiB         |                   |         | `fetchMaxBytes`        | 8 MiB      |
| `maxBlockMs`        | 60000          | `socketTimeoutMs` | 120000  | `heartbeatIntervalMs`  | 0 (= session / 3) |
| `socketTimeoutMs`   | 120000         |                   |         | `socketTimeoutMs`      | 120000     |

`socketTimeoutMs` is the client-side per-request round-trip timeout on every
connection the client opens (0 disables it); `router.setRequestTimeout(ms)`
changes it later, and it is also the fifth argument of
`Connection.connect(host, port, clientId, connectTimeoutMs, requestTimeoutMs)`.
Per record, `send()`/`sendSync()` take `key`, `partition`, `headers` and
`timestamp` (unix ms, `number | bigint`).

Other codecs: `registerCodec(Compression.LZ4, { compress, decompress })`,
both `(payload: Buffer) => Buffer`.

## Run the tests

```sh
./test.sh HOST PORT        # e.g. ./test.sh 127.0.0.1 9092
```

It runs `npm ci`, then `tsc --noEmit` over the suite and the typings tests,
then compiles the suite to `out/` (gitignored) and runs
`node out/test_manual.js HOST PORT`. It works from any directory and exits
non-zero if the typings fail to check or any e2e check fails. Type-check only:
`npm ci && npx tsc -p tsconfig.json`.
