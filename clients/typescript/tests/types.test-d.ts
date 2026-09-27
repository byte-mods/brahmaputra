/**
 * Compile-time tests for clients/nodejs/src/index.d.ts. Never executed.
 *
 * `tsc --noEmit` must pass on this file, which proves two things:
 *   - every plain statement below type-checks (valid usage compiles, and
 *     `expectType` pins the exact declared types — no `any` leaking out);
 *   - every line after `// @ts-expect-error` FAILS to type-check: if one
 *     ever compiled, tsc reports "Unused '@ts-expect-error' directive".
 */

import {
  API_VERSION,
  ApiKey,
  Assignor,
  AutoOffsetReset,
  BrahmaputraError,
  Compression,
  Connection,
  Consumer,
  EARLIEST,
  ErrorCode,
  GroupConsumer,
  LATEST,
  NoOffsetForPartition,
  Producer,
  ProtocolError,
  READ_COMMITTED,
  RETRIABLE_ERRORS,
  RecordHeader,
  Router,
  SCHEMA_VERSION,
  ServerError,
  bodyReader,
  bodyWriter,
  crc32c,
  decodeRecordBatch,
  defaultConsumerConfig,
  defaultGroupConfig,
  defaultProducerConfig,
  encodeRecordBatch,
  murmur2,
  parseCompression,
  partitionForKey,
  rangeAssign,
  registerCodec,
  roundRobinAssign,
  stickyAssign,
  toBytes,
} from '../../nodejs';
import type {
  Acks,
  ClusterMetadata,
  CompressionCodec,
  ConsumerConfig,
  ConsumerRecord,
  GroupConfig,
  ProducerConfig,
  ProducerOptions,
  TopicPartition,
} from '../../nodejs';

// --- helpers ---------------------------------------------------------------

/** True only when A and B are the same type (not merely assignable). */
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
/** Compiles only when `Actual` is exactly `Expected`. */
declare function expectType<Expected>(): <Actual>(
  value: Actual,
  ...exact: Equal<Actual, Expected> extends true ? [] : [never]
) => void;
/** Compiles only for `true`. */
declare function assert<T extends true>(): T;

declare const buffer: Buffer;
declare const record: ConsumerRecord;
declare const producer: Producer;
declare const consumer: Consumer;
declare const group: GroupConsumer;

async function typings(): Promise<void> {
  // --- constants ------------------------------------------------------------
  expectType<'1.0.0'>()(SCHEMA_VERSION);
  expectType<4>()(API_VERSION);
  expectType<-2n>()(EARLIEST);
  expectType<-1n>()(LATEST);
  expectType<3>()(ApiKey.METADATA);
  expectType<13>()(ErrorCode.UNKNOWN_MEMBER_ID);
  expectType<4>()(Compression.GZIP);
  expectType<'earliest'>()(AutoOffsetReset.EARLIEST);
  expectType<'sticky'>()(Assignor.STICKY);
  expectType<boolean>()(RETRIABLE_ERRORS.has(ErrorCode.INTERNAL));
  // @ts-expect-error — the enums are frozen
  Compression.GZIP = 1;

  // --- producer config ------------------------------------------------------
  const full: ProducerConfig = defaultProducerConfig();
  expectType<Acks>()(full.acks);
  const kafkaStyle: ProducerOptions = {
    clientId: 'orders-api',
    acks: -1,
    batchSize: 32 * 1024,
    lingerMs: 10,
    compressionType: 'gzip',
    requestTimeoutMs: 15000,
    retries: 3,
    retryBackoffMs: 50,
    deliveryTimeoutMs: 60000,
    bufferMemory: 1 << 20,
    maxBlockMs: 1000,
  };
  const p1: Promise<Producer> = Producer.connect('localhost', 9092, kafkaStyle);
  const p2: Promise<Producer> = Producer.connect('localhost', 9092); // every option optional
  void p1;
  void p2;

  // @ts-expect-error — acks must be 0, 1 or -1 (Kafka's "all" is -1)
  await Producer.connect('localhost', 9092, { acks: 2 });
  // @ts-expect-error — the JS reads -1, not the string "all"
  await Producer.connect('localhost', 9092, { acks: 'all' });
  // @ts-expect-error — Kafka's dotted names are not what the JS reads
  await Producer.connect('localhost', 9092, { 'linger.ms': 5 });
  // @ts-expect-error — unknown compression codec
  await Producer.connect('localhost', 9092, { compressionType: 'brotli' });
  // @ts-expect-error — numbers are numbers
  await Producer.connect('localhost', 9092, { lingerMs: '5' });
  // @ts-expect-error — the port is required
  await Producer.connect('localhost');
  // @ts-expect-error — a full ProducerConfig needs every field
  const partial: ProducerConfig = { acks: 1 };
  void partial;

  // --- producing ------------------------------------------------------------
  expectType<Promise<void>>()(producer.send('t', buffer));
  expectType<Promise<void>>()(producer.send('t', 'a string is UTF-8'));
  expectType<Promise<void>>()(producer.send('t', new Uint8Array([1, 2, 3])));
  expectType<Promise<void>>()(producer.send('t', null, { key: 'k' })); // tombstone
  expectType<Promise<bigint>>()(producer.sendSync('t', buffer, { partition: 2 }));
  expectType<Promise<void>>()(producer.flush());
  expectType<Promise<void>>()(producer.close());
  await producer.send('t', buffer, {
    key: buffer,
    partition: null,
    headers: [
      new RecordHeader('trace', buffer),
      new RecordHeader('null-valued', null),
      new RecordHeader('no-value'),
      new RecordHeader('as-string', 'text'),
      { key: 'plain-object', value: buffer },
      { key: 'plain-object-null' },
    ],
  });
  // @ts-expect-error — values are bytes, strings or null, not numbers
  await producer.send('t', 42);
  // @ts-expect-error — nor plain objects
  await producer.send('t', { id: 1 });
  // @ts-expect-error — a header key is a string
  await producer.send('t', buffer, { headers: [{ key: 7, value: buffer }] });
  // @ts-expect-error — the partition is a number
  await producer.send('t', buffer, { partition: '0' });
  // @ts-expect-error — the topic is required
  await producer.send();

  // --- records --------------------------------------------------------------
  expectType<bigint>()(record.offset);
  expectType<bigint>()(record.timestamp);
  expectType<Buffer | null>()(record.key);
  expectType<Buffer | null>()(record.value);
  expectType<Buffer | null>()(record.header('trace'));
  expectType<RecordHeader<Buffer | null>[]>()(record.headers);
  expectType<Buffer | null>()(record.headers[0]!.value);
  // @ts-expect-error — a tombstone's value is null, so it must be narrowed
  record.value.length;
  // @ts-expect-error — offsets are bigint, not number
  const offsetAsNumber: number = record.offset;
  void offsetAsNumber;
  if (record.value !== null) expectType<number>()(record.value.length);
  expectType<RecordHeader<Buffer>>()(new RecordHeader('k', buffer));

  // --- consumer -------------------------------------------------------------
  const cc: ConsumerConfig = defaultConsumerConfig();
  expectType<0 | 1>()(cc.isolationLevel);
  await Consumer.connect('h', 1, { fetchMaxWaitMs: 100, isolationLevel: READ_COMMITTED, rack: 'r1' });
  // @ts-expect-error — isolation level is 0 or 1
  await Consumer.connect('h', 1, { isolationLevel: 2 });
  expectType<Promise<ConsumerRecord[]>>()(consumer.fetch('t', 0, 0n));
  expectType<Promise<ConsumerRecord[]>>()(consumer.fetch('t', 0, 17, 250));
  expectType<Promise<{ records: ConsumerRecord[]; highWatermark: bigint }>>()(
    consumer.fetchVerbose('t', 0, 0n)
  );
  expectType<Promise<bigint>>()(consumer.listOffsets('t', 0, EARLIEST));
  expectType<Promise<bigint>>()(consumer.listOffsets('t', 0, Date.now()));
  expectType<Promise<number[]>>()(consumer.partitions('t'));
  expectType<void>()(consumer.close());
  // @ts-expect-error — an offset is not a string
  await consumer.fetch('t', 0, '0');

  // --- routing --------------------------------------------------------------
  expectType<Router>()(consumer.router);
  expectType<Connection>()(consumer.router.seed);
  expectType<boolean>()(consumer.router.seed.closed);
  expectType<Promise<ClusterMetadata>>()(consumer.router.getMetadata([], true));
  expectType<Promise<Connection>>()(consumer.router.connectionFor('t', 0));
  const conn = await Connection.connect('h', 1, 'client', 2000, 300);
  expectType<Promise<Buffer>>()(conn.request(ApiKey.METADATA, bodyWriter().stringArray([]).bytes()));
  const versions = await conn.apiVersions();
  expectType<string>()(versions.brokerVersion);
  expectType<number>()(versions.versions[0]!.maxVersion);

  // --- groups ---------------------------------------------------------------
  const gc: GroupConfig = defaultGroupConfig();
  expectType<'earliest' | 'latest' | 'none'>()(gc.autoOffsetReset);
  expectType<'range' | 'roundrobin' | 'sticky'>()(gc.assignor);
  await GroupConsumer.connect('h', 1, 'billing', {
    sessionTimeoutMs: 10000,
    maxPollIntervalMs: 60000,
    autoCommitIntervalMs: 0,
    autoOffsetReset: 'latest',
    assignor: Assignor.ROUNDROBIN,
    groupInstanceId: 'worker-3',
    maxPollRecords: 100,
  });
  // @ts-expect-error — the group id is required
  await GroupConsumer.connect('h', 1);
  // @ts-expect-error — the group id is a string
  await GroupConsumer.connect('h', 1, 7);
  // @ts-expect-error — unknown reset policy
  await GroupConsumer.connect('h', 1, 'g', { autoOffsetReset: 'smallest' });
  // @ts-expect-error — unknown assignor
  await GroupConsumer.connect('h', 1, 'g', { assignor: 'cooperative-sticky' });
  expectType<void>()(group.subscribe(['a', 'b']));
  expectType<Promise<ConsumerRecord[]>>()(group.poll(500));
  expectType<Promise<void>>()(group.commit());
  expectType<Promise<Map<string, bigint>>>()(group.committed());
  expectType<Promise<void>>()(group.close());
  // @ts-expect-error — subscribe takes a list of topics
  group.subscribe('orders');

  // --- assignors ------------------------------------------------------------
  const topics = new Map<string, number[]>([['t', [0, 1, 2]]]);
  const members = [{ id: 'a', topics: ['t'] }];
  expectType<Map<string, TopicPartition[]>>()(rangeAssign(members, topics));
  expectType<Map<string, TopicPartition[]>>()(roundRobinAssign(members, topics));
  expectType<Map<string, TopicPartition[]>>()(
    stickyAssign(members, topics, new Map([['a', [{ topic: 't', partition: 0 }]]]))
  );

  // --- errors ---------------------------------------------------------------
  const error: unknown = new ServerError(ErrorCode.NOT_COORDINATOR, 'ctx');
  if (error instanceof ServerError) expectType<number>()(error.code);
  assert<Equal<ProtocolError extends BrahmaputraError ? true : false, true>>();
  assert<Equal<NoOffsetForPartition extends BrahmaputraError ? true : false, true>>();
  assert<Equal<BrahmaputraError extends Error ? true : false, true>>();

  // --- codecs and hashing ---------------------------------------------------
  registerCodec(Compression.LZ4, {
    compress: (payload: Buffer) => payload,
    decompress: (payload: Buffer) => payload,
  });
  // @ts-expect-error — a codec must provide both directions
  registerCodec(Compression.SNAPPY, { compress: (payload: Buffer) => payload });
  // @ts-expect-error — not a codec id
  registerCodec(9, { compress: (b: Buffer) => b, decompress: (b: Buffer) => b });
  expectType<CompressionCodec>()(parseCompression('gzip'));
  expectType<number>()(murmur2(buffer));
  expectType<number>()(partitionForKey('user-7', [0, 1, 2, 3]));
  // @ts-expect-error — keyed partitioning needs a key
  partitionForKey(null, [0, 1]);
  expectType<number>()(crc32c(buffer));
  expectType<null>()(toBytes(null));
  expectType<Buffer>()(toBytes('x'));

  // --- wire helpers ---------------------------------------------------------
  const batch = encodeRecordBatch([{ key: 'k', value: null }, { value: buffer }], Date.now());
  expectType<Buffer>()(batch);
  const decoded = decodeRecordBatch(batch, 0);
  expectType<number>()(decoded.next);
  expectType<bigint>()(decoded.batch.baseOffset);
  expectType<Buffer | null>()(decoded.batch.records[0]!.value);
  expectType<bigint>()(bodyReader(batch).int64());
  expectType<number>()(bodyReader(batch).int32());
}

void typings;
