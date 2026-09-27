/**
 * Manual end-to-end check of the Node driver, written in strict TypeScript
 * against its typings, run against a live broker.
 *
 *   brahmaputra-server --data-dir ./data --default-partitions 4
 *   ./test.sh HOST PORT          # or: npm run build && node out/test_manual.js HOST PORT
 *
 * A line-for-line port of clients/nodejs/test_manual.js (57 checks, same
 * sections, same names). Every check asserts a property of the system, not
 * that a function ran. Exits non-zero on any failure.
 */

import * as net from 'net';

// The driver itself, resolved through ../nodejs/package.json ("main" at
// runtime, "types" at compile time). This path is the same from src/ and
// from the compiled out/.
import {
  Assignor,
  AutoOffsetReset,
  Connection,
  Consumer,
  EARLIEST,
  GroupConsumer,
  LATEST,
  NoOffsetForPartition,
  Producer,
  RecordHeader,
  murmur2,
  partitionForKey,
  stickyAssign,
} from '../../nodejs';
import type {
  Acks,
  AssignorName,
  CompressionType,
  ConsumerRecord,
  TopicPartition,
} from '../../nodejs';

const HOST: string = process.argv[2] || '127.0.0.1';
const PORT: number = Number(process.argv[3] || 9092);

let passed = 0;
let failed = 0;

function check(name: string, condition: boolean, detail: string = ''): void {
  if (condition) {
    passed += 1;
    console.log(`  ok   ${name}`);
  } else {
    failed += 1;
    console.log(`  FAIL ${name}${detail ? `: ${detail}` : ''}`);
  }
}

const section = (title: string): void => console.log(`\n${title}`);
const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

let counter = 0;
const unique = (prefix: string): string => `${prefix}-${Date.now() % 1e9}-${counter++}`;

/** `String(error)` for the check details, whatever was thrown. */
const describe = (error: unknown): string => String(error);
const messageOf = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

/** Buffer equality that treats a null (tombstone) side as unequal. */
const same = (actual: Buffer | null, expected: Buffer): boolean =>
  actual !== null && actual.equals(expected);

async function main(): Promise<void> {
  section('connection and metadata');
  {
    const consumer = await Consumer.connect(HOST, PORT);
    const { versions, brokerVersion } = await consumer.router.seed.apiVersions();
    check('ApiVersions answers', versions.length > 0, `${versions.length} apis`);
    check('broker reports a version', brokerVersion !== '', brokerVersion);
    const metadata = await consumer.router.getMetadata([], true);
    check('metadata lists brokers', metadata.brokers.length >= 1);
    consumer.close();
  }

  section('produce and consume round trip');
  const topic = unique('ts-roundtrip');
  const payloads: Buffer[] = Array.from({ length: 50 }, (_, i) => Buffer.from(`record-${i}`));
  {
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    for (const payload of payloads) {
      await producer.send(topic, payload, { partition: 0 });
    }
    await producer.close();
  }
  {
    const consumer = await Consumer.connect(HOST, PORT);
    const got: ConsumerRecord[] = await consumer.fetch(topic, 0, 0n);
    check('every record comes back', got.length === payloads.length, `got ${got.length}`);
    const identical =
      got.length === payloads.length &&
      got.every((record, i) => same(record.value, payloads[i]!) && record.offset === BigInt(i));
    check('values byte-identical and offsets contiguous', identical);
    consumer.close();
  }

  section('compression codecs');
  const codecs: CompressionType[] = ['none', 'gzip', 'zstd'];
  for (const codec of codecs) {
    const codecTopic = unique(`ts-${codec}`);
    const body = Buffer.from('the same line over and over. '.repeat(40));
    try {
      const producer = await Producer.connect(HOST, PORT, {
        lingerMs: 0,
        compressionType: codec,
      });
      for (let i = 0; i < 20; i += 1) {
        await producer.send(codecTopic, Buffer.concat([body, Buffer.from(String(i))]), {
          partition: 0,
        });
      }
      await producer.close();

      const consumer = await Consumer.connect(HOST, PORT);
      const got = await consumer.fetch(codecTopic, 0, 0n);
      const first = got[0]?.value ?? null;
      check(
        `${codec}: round trips`,
        got.length === 20 && first !== null && first.subarray(0, body.length).equals(body),
        `got ${got.length} records`
      );
      consumer.close();
    } catch (error) {
      // A codec this runtime cannot provide is a skip, not a failure: the
      // driver is correct, the environment just lacks it.
      if (describe(error).includes('not available')) {
        console.log(`  skip ${codec}: ${messageOf(error)}`);
      } else {
        check(`${codec}: round trips`, false, describe(error));
      }
    }
  }

  section('keys, partitioning and ordering');
  {
    const keyTopic = unique('ts-keys');
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    const partitions: number[] = await producer.router.partitions(keyTopic);
    for (let i = 0; i < 30; i += 1) {
      await producer.send(keyTopic, Buffer.from(`v${i}`), { key: Buffer.from('user-7') });
    }
    await producer.close();

    const target = partitionForKey(Buffer.from('user-7'), partitions);
    const consumer = await Consumer.connect(HOST, PORT);
    const onTarget = await consumer.fetch(keyTopic, target, 0n);
    check(
      'a key pins every record to one partition',
      onTarget.length === 30,
      `partition ${target} holds ${onTarget.length} of 30`
    );
    check(
      'per-key order is preserved',
      onTarget.every((record, i) => record.value?.toString() === `v${i}`)
    );
    let strays = 0;
    for (const partition of partitions) {
      if (partition === target) continue;
      strays += (await consumer.fetch(keyTopic, partition, 0n)).length;
    }
    check('no keyed record landed elsewhere', strays === 0, `${strays} strays`);
    consumer.close();
  }

  section("murmur2 agrees with the broker's partitioner");
  check('murmur2("") is stable', murmur2(Buffer.from('')) === 275646681,
    String(murmur2(Buffer.from(''))));
  check('murmur2 is deterministic',
    murmur2(Buffer.from('user-7')) === murmur2(Buffer.from('user-7')));
  check('different keys hash differently',
    murmur2(Buffer.from('user-7')) !== murmur2(Buffer.from('user-8')));

  section('record headers and timestamps');
  {
    const headerTopic = unique('ts-headers');
    const before = Date.now() - 1000;
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    await producer.send(headerTopic, Buffer.from('annotated'), {
      partition: 0,
      headers: [
        new RecordHeader('trace-id', Buffer.from('abc-123')),
        new RecordHeader('content-type', Buffer.from('application/json')),
        new RecordHeader('tombstone-reason', null),
      ],
    });
    await producer.send(headerTopic, Buffer.from('plain'), { partition: 0 });
    await producer.close();
    const after = Date.now() + 1000;

    const consumer = await Consumer.connect(HOST, PORT);
    const got = await consumer.fetch(headerTopic, 0, 0n);
    check('both records arrive', got.length === 2, `got ${got.length}`);
    const [annotated, plain] = got;
    if (got.length === 2 && annotated && plain) {
      check('headers survive the round trip', annotated.headers.length === 3);
      check('header values are exact',
        same(annotated.header('trace-id'), Buffer.from('abc-123')));
      check('a null header value stays null', annotated.headers[2]?.value === null);
      check('a record with no headers gains none from its batch',
        plain.headers.length === 0);
      check(
        'timestamps are real wall-clock values',
        got.every((r) => Number(r.timestamp) >= before && Number(r.timestamp) <= after),
        `${got.map((r) => r.timestamp).join(',')} outside ${before}..${after}`
      );
    }
    consumer.close();
  }

  section('tombstones');
  {
    const tombTopic = unique('ts-tombstones');
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    await producer.send(tombTopic, Buffer.from('set'), {
      partition: 0,
      key: Buffer.from('k1'),
    });
    await producer.send(tombTopic, Buffer.alloc(0), {
      partition: 0,
      key: Buffer.from('k2'),
    });
    // A null value is a deletion, and must stay distinguishable from the
    // empty value above all the way through the round trip.
    await producer.send(tombTopic, null, { partition: 0, key: Buffer.from('k3') });
    await producer.close();

    const consumer = await Consumer.connect(HOST, PORT);
    const got = await consumer.fetch(tombTopic, 0, 0n);
    check('all three records arrive', got.length === 3, `got ${got.length}`);
    const [set, empty, tombstone] = got;
    if (got.length === 3 && set && empty && tombstone) {
      check('an ordinary value round-trips', same(set.value, Buffer.from('set')));
      check('an empty value is empty, not null',
        empty.value !== null && empty.value.length === 0);
      check('a tombstone arrives as a null value', tombstone.value === null,
        `${tombstone.value}`);
    }
    consumer.close();
  }

  section('offsets');
  {
    const consumer = await Consumer.connect(HOST, PORT);
    const earliest: bigint = await consumer.listOffsets(topic, 0, EARLIEST);
    const latest: bigint = await consumer.listOffsets(topic, 0, LATEST);
    check('earliest is 0 on a fresh topic', earliest === 0n, String(earliest));
    check('latest equals the record count', latest === 50n, String(latest));
    consumer.close();
  }

  section('acks');
  const acksValues: Acks[] = [0, 1, -1];
  for (const acks of acksValues) {
    const acksTopic = unique(`ts-acks${acks}`);
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0, acks });
    await producer.send(acksTopic, Buffer.from('durable'), { partition: 0 });
    await producer.close();
    await sleep(400);

    const consumer = await Consumer.connect(HOST, PORT);
    const got = await consumer.fetch(acksTopic, 0, 0n);
    check(`acks=${acks} stores the record`, got.length === 1, `got ${got.length}`);
    consumer.close();
  }

  section('consumer group: assignment, commit, resume');
  {
    const groupTopic = unique('ts-group');
    const groupId = unique('ts-billing');
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    for (let i = 0; i < 40; i += 1) {
      await producer.send(groupTopic, Buffer.from(`g${i}`));
    }
    await producer.close();

    const consumer = await GroupConsumer.connect(HOST, PORT, groupId, {
      autoCommitIntervalMs: 0,
    });
    consumer.subscribe([groupTopic]);
    const seen: ConsumerRecord[] = [];
    const deadline = Date.now() + 30000;
    while (seen.length < 40 && Date.now() < deadline) {
      seen.push(...(await consumer.poll(500)));
    }
    check('the group consumes every record', seen.length === 40, `got ${seen.length}`);
    check(
      'no record is delivered twice',
      new Set(seen.map((r) => `${r.partition}-${r.offset}`)).size === seen.length
    );
    await consumer.commit();
    const committed: Map<string, bigint> = await consumer.committed();
    const total = [...committed.values()].reduce((sum, offset) => sum + Number(offset), 0);
    check('commit records a position', total === 40, String(total));
    await consumer.close();

    // A second consumer in the same group must resume, not replay.
    const rejoined = await GroupConsumer.connect(HOST, PORT, groupId, {
      autoCommitIntervalMs: 0,
    });
    rejoined.subscribe([groupTopic]);
    const replayed: ConsumerRecord[] = [];
    const until = Date.now() + 5000;
    while (Date.now() < until) {
      replayed.push(...(await rejoined.poll(300)));
    }
    check(
      'a rejoining group resumes from its commit',
      replayed.length === 0,
      `replayed ${replayed.length} records it had already committed`
    );
    await rejoined.close();
  }

  section('auto.offset.reset');
  {
    const resetTopic = unique('ts-reset');
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    for (let i = 0; i < 10; i += 1) await producer.send(resetTopic, Buffer.from(`r${i}`));
    await producer.close();

    const latestConsumer = await GroupConsumer.connect(HOST, PORT, unique('ts-latest'), {
      autoCommitIntervalMs: 0,
      autoOffsetReset: AutoOffsetReset.LATEST,
    });
    latestConsumer.subscribe([resetTopic]);
    const skipped: ConsumerRecord[] = [];
    const until = Date.now() + 4000;
    while (Date.now() < until) skipped.push(...(await latestConsumer.poll(300)));
    check(
      'latest skips records produced before the group existed',
      skipped.length === 0,
      `saw ${skipped.length}`
    );
    await latestConsumer.close();

    const strict = await GroupConsumer.connect(HOST, PORT, unique('ts-none'), {
      autoCommitIntervalMs: 0,
      autoOffsetReset: AutoOffsetReset.NONE,
    });
    strict.subscribe([resetTopic]);
    let raised = false;
    const strictUntil = Date.now() + 5000;
    while (Date.now() < strictUntil && !raised) {
      try {
        await strict.poll(300);
      } catch (error) {
        raised = error instanceof NoOffsetForPartition;
      }
    }
    check('none refuses to guess a position', raised);
    await strict.close();
  }

  section('assignors');
  const assignors: AssignorName[] = [Assignor.RANGE, Assignor.ROUNDROBIN, Assignor.STICKY];
  for (const assignor of assignors) {
    const assignorTopic = unique(`ts-${assignor}`);
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    for (let i = 0; i < 20; i += 1) await producer.send(assignorTopic, Buffer.from(`a${i}`));
    await producer.close();

    const consumer = await GroupConsumer.connect(HOST, PORT, unique(`ts-grp-${assignor}`), {
      autoCommitIntervalMs: 0,
      assignor,
    });
    consumer.subscribe([assignorTopic]);
    const collected: ConsumerRecord[] = [];
    const deadline = Date.now() + 20000;
    while (collected.length < 20 && Date.now() < deadline) {
      collected.push(...(await consumer.poll(500)));
    }
    check(`${assignor}: consumes every record`, collected.length === 20,
      `got ${collected.length}`);
    await consumer.close();
  }

  section('bounded client buffer');
  {
    const bufferTopic = unique('ts-buffer');
    const producer = await Producer.connect(HOST, PORT, {
      lingerMs: 10000, // never flush on time during this check
      bufferMemory: 2048,
      maxBlockMs: 300,
    });
    let blocked = false;
    for (let i = 0; i < 500 && !blocked; i += 1) {
      try {
        await producer.send(bufferTopic, Buffer.alloc(256, 'x'), { partition: 0 });
      } catch (error) {
        blocked = describe(error).includes('buffer full');
      }
    }
    check('a full buffer blocks and then reports', blocked);
    // Tear down without flushing the (deliberately stuck) buffer.
    producer.closed = true;
    clearInterval(producer.ticker ?? undefined);
    producer.router.close();
  }

  section('wire edge cases');
  {
    const edgeTopic = unique('ts-edge');
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    const large = Buffer.alloc(1 << 20);
    for (let i = 0; i < large.length; i += 1) large[i] = (i * 7) & 0xff;
    const unicodeKey = Buffer.from('ключ-✓-🔑', 'utf8');
    const unicodeValue = Buffer.from('значение — 数据 — 🚀', 'utf8');
    let largeError: unknown = null;
    try {
      await producer.send(edgeTopic, large, { partition: 0 });
    } catch (error) {
      largeError = error;
    }
    await producer.send(edgeTopic, unicodeValue, {
      partition: 0,
      key: unicodeKey,
      headers: [new RecordHeader('ünïcødé-🏷', Buffer.from('✓'))],
    });
    // An empty key and an empty header value are values, not nulls.
    await producer.send(edgeTopic, Buffer.from('empty-key'), {
      partition: 0,
      key: Buffer.alloc(0),
      headers: [new RecordHeader('empty', Buffer.alloc(0)), new RecordHeader('null', null)],
    });
    // Strings are accepted and travel as UTF-8, as Buffers do.
    await producer.send(edgeTopic, 'héllo ✓ 🚀', {
      partition: 0,
      key: 'kéy',
      headers: [new RecordHeader('h', 'välue')],
    });
    await producer.close();

    const consumer = await Consumer.connect(HOST, PORT);
    const got: ConsumerRecord[] = [];
    for (let offset = 0n; got.length < 4; ) {
      const batch = await consumer.fetch(edgeTopic, 0, offset, 500);
      const last = batch[batch.length - 1];
      if (last === undefined) break;
      got.push(...batch);
      offset = last.offset + 1n;
    }
    check('a 1 MiB value is accepted', largeError === null, describe(largeError));
    const offsetOf = largeError === null ? 1 : 0;
    const expected = offsetOf + 3;
    check('edge records all arrive', got.length === expected, `got ${got.length}`);
    const head = got[0];
    if (largeError === null && head !== undefined) {
      check('a 1 MiB value round-trips byte-identical', same(head.value, large),
        `${head.value?.length ?? 'null'} bytes`);
    }
    const [uni, empty, str] = got.slice(offsetOf);
    if (got.length === expected && uni && empty && str) {
      check('unicode key, value and header key round-trip',
        uni.key !== null && uni.key.equals(unicodeKey) && same(uni.value, unicodeValue) &&
          uni.headers.length === 1 && uni.headers[0]?.key === 'ünïcødé-🏷');
      check('an empty key stays empty, not null', empty.key !== null && empty.key.length === 0);
      const [emptyHeader, nullHeader] = empty.headers;
      check('an empty header value stays empty, not null',
        empty.headers.length === 2 && emptyHeader !== undefined && emptyHeader.value !== null &&
          emptyHeader.value.length === 0 && nullHeader !== undefined && nullHeader.value === null);
      check('string key, value and header value travel as UTF-8',
        str.value?.toString('utf8') === 'héllo ✓ 🚀' && str.key?.toString('utf8') === 'kéy' &&
          str.headers[0]?.value?.toString('utf8') === 'välue',
        `${str.value?.toString()} / ${str.key?.toString()}`);
    }
    consumer.close();
  }

  section('ordering under linger flushes');
  {
    const orderTopic = unique('ts-order');
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 1, batchSize: 256 });
    const total = 5000;
    const sends: Promise<void>[] = [];
    for (let i = 0; i < total; i += 1) {
      sends.push(producer.send(orderTopic, Buffer.from(String(i)), { partition: 0 }));
    }
    await Promise.all(sends);
    await producer.close();
    const consumer = await Consumer.connect(HOST, PORT);
    const values: number[] = [];
    for (let offset = 0n; values.length < total; ) {
      const batch = await consumer.fetch(orderTopic, 0, offset, 500);
      const last = batch[batch.length - 1];
      if (last === undefined) break;
      for (const record of batch) values.push(Number(record.value?.toString()));
      offset = last.offset + 1n;
    }
    let inversions = 0;
    for (let i = 1; i < values.length; i += 1) {
      if (values[i]! < values[i - 1]!) inversions += 1;
    }
    check('every record of a partition arrives', values.length === total, `got ${values.length}`);
    check("a partition's records keep send order", inversions === 0, `${inversions} inversions`);
    consumer.close();
  }

  section('background flush failures are reported');
  {
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 20 });
    let sendError: unknown = null;
    try {
      // Partition 999 does not exist, so the linger ticker's flush fails.
      await producer.send(unique('ts-bgfail'), Buffer.from('lost'), { partition: 999 });
    } catch (error) {
      sendError = error;
    }
    await sleep(300);
    let flushError: unknown = null;
    try {
      await producer.flush();
    } catch (error) {
      flushError = error;
    }
    check('a failed linger flush surfaces on the next flush',
      sendError === null && flushError !== null,
      `send=${describe(sendError)} flush=${describe(flushError)}`);
    try {
      await producer.close();
    } catch {
      // Reported above; close must still release everything.
    }
    const ticker = producer.ticker;
    check('close releases the ticker and sockets', producer.router.seed.closed &&
      !(ticker !== null && ticker.hasRef()));
  }

  section('connection failures');
  {
    // A broker that accepts and never answers must cost an error, not a
    // promise that never settles.
    const silent = net.createServer((socket) => socket.on('data', () => {}));
    const silentPort = await listen(silent);
    const connection = await Connection.connect('127.0.0.1', silentPort, 'ts-test', 2000, 300);
    const started = Date.now();
    let timedOut: unknown = null;
    try {
      await connection.apiVersions();
    } catch (error) {
      timedOut = error;
    }
    check('a request to an unresponsive broker times out',
      timedOut !== null && Date.now() - started < 3000, describe(timedOut));
    connection.close();
    silent.close();

    // A connection the broker drops is redialled, not kept forever.
    const proxy = await startProxy(HOST, PORT);
    const dropTopic = unique('ts-drop');
    const producer = await Producer.connect('127.0.0.1', proxy.port, { lingerMs: 0 });
    await producer.send(dropTopic, Buffer.from('before'), { partition: 0 });
    await proxy.dropAll();
    let recovered: unknown = new Error('not attempted');
    for (let attempt = 0; attempt < 3 && recovered; attempt += 1) {
      try {
        await producer.send(dropTopic, Buffer.from('after'), { partition: 0 });
        recovered = null;
      } catch (error) {
        recovered = error;
      }
    }
    check('a producer recovers after its connection drops', recovered === null,
      describe(recovered));
    await producer.close().catch(() => {});
    const consumer = await Consumer.connect('127.0.0.1', proxy.port);
    await consumer.fetch(dropTopic, 0, 0n, 100);
    await proxy.dropAll();
    let fetchError: unknown = new Error('not attempted');
    let fetched: ConsumerRecord[] = [];
    for (let attempt = 0; attempt < 3 && fetchError; attempt += 1) {
      try {
        fetched = await consumer.fetch(dropTopic, 0, 0n, 100);
        fetchError = null;
      } catch (error) {
        fetchError = error;
      }
    }
    check('a consumer recovers after its connection drops',
      fetchError === null && fetched.length >= 1, describe(fetchError));
    consumer.close();
    await proxy.close();
  }

  section('consumer group: max.poll.interval and rejoin');
  {
    const slowTopic = unique('ts-slow');
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    for (let i = 0; i < 10; i += 1) await producer.send(slowTopic, Buffer.from(`s${i}`));
    const consumer = await GroupConsumer.connect(HOST, PORT, unique('ts-slow-grp'), {
      autoCommitIntervalMs: 0,
      maxPollIntervalMs: 1500,
      // 0 means no cap, as in the Go driver, not "return nothing".
      maxPollRecords: 0,
    });
    consumer.subscribe([slowTopic]);
    const first: ConsumerRecord[] = [];
    let deadline = Date.now() + 15000;
    while (first.length < 10 && Date.now() < deadline) first.push(...(await consumer.poll(300)));
    await consumer.commit();
    // Stall past max.poll.interval.ms: the member leaves the group.
    await sleep(2500);
    for (let i = 10; i < 20; i += 1) await producer.send(slowTopic, Buffer.from(`s${i}`));
    await producer.close();
    const second: ConsumerRecord[] = [];
    let pollError: unknown = null;
    deadline = Date.now() + 15000;
    while (second.length < 10 && Date.now() < deadline) {
      try {
        second.push(...(await consumer.poll(300)));
      } catch (error) {
        pollError = error;
        break;
      }
    }
    check('a member that stalled rejoins on its next poll',
      first.length === 10 && second.length === 10 && pollError === null,
      `first=${first.length} second=${second.length} err=${describe(pollError)}`);
    await consumer.close();
  }

  section('sticky assignor agrees with the Go and Rust drivers');
  {
    // With ten or more partitions a string sort puts "t 10" before "t 2";
    // every driver must order partitions numerically or a mixed-language
    // group reshuffles whenever leadership changes hands.
    const topics = new Map<string, number[]>([['t', Array.from({ length: 12 }, (_, i) => i)]]);
    const previous = new Map<string, TopicPartition[]>([
      ['a', Array.from({ length: 12 }, (_, i) => ({ topic: 't', partition: i }))],
      ['b', []],
    ]);
    const result = stickyAssign([{ id: 'a', topics: ['t'] }, { id: 'b', topics: ['t'] }], topics,
      previous);
    const kept = (result.get('a') ?? []).map((slot) => slot.partition).join(',');
    const moved = (result.get('b') ?? []).map((slot) => slot.partition).join(',');
    check('the member over quota keeps its lowest partitions', kept === '0,1,2,3,4,5', kept);
    check('assignments are listed in numeric order', moved === '6,7,8,9,10,11', moved);
  }

  section('consumer group: time inside poll does not count against max.poll.interval');
  {
    const joinTopic = unique('ts-inpoll');
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    await producer.router.partitions(joinTopic);
    const consumer = await GroupConsumer.connect(HOST, PORT, unique('ts-inpoll-grp'), {
      autoCommitIntervalMs: 0,
      // Far shorter than the poll below, which spends ~1s joining (the
      // broker's initial rebalance delay) and then waits for data.
      maxPollIntervalMs: 600,
    });
    consumer.subscribe([joinTopic]);
    const producing = (async (): Promise<void> => {
      await sleep(2000);
      for (let i = 0; i < 10; i += 1) await producer.send(joinTopic, Buffer.from(`j${i}`));
    })();
    let got: ConsumerRecord[] = [];
    let pollError: unknown = null;
    try {
      got = await consumer.poll(4000);
    } catch (error) {
      pollError = error;
    }
    // Committed straight away, before another poll could quietly rejoin:
    // this fails if the member left the group mid-poll.
    let commitError: unknown = null;
    try {
      await consumer.commit();
    } catch (error) {
      commitError = error;
    }
    check('a member is still in its group after a long poll',
      pollError === null && got.length > 0 && commitError === null,
      `got=${got.length} poll=${describe(pollError)} commit=${describe(commitError)}`);
    await producing;
    await producer.close();
    await consumer.close();
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

/** Listen on an ephemeral loopback port and return it. */
async function listen(server: net.Server): Promise<number> {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (address === null || typeof address === 'string') {
    throw new Error(`unexpected listen address ${String(address)}`);
  }
  return address.port;
}

interface Proxy {
  port: number;
  dropAll(): Promise<void>;
  close(): Promise<void>;
}

/** A TCP forwarder that can sever every live connection, which is how a
 * broker restart or an idle timeout looks to a client. */
async function startProxy(host: string, port: number): Promise<Proxy> {
  const live = new Set<net.Socket>();
  const server = net.createServer((client) => {
    const upstream = net.connect(port, host);
    live.add(client);
    live.add(upstream);
    client.pipe(upstream);
    upstream.pipe(client);
    const drop = (): void => {
      client.destroy();
      upstream.destroy();
      live.delete(client);
      live.delete(upstream);
    };
    client.on('error', drop);
    upstream.on('error', drop);
    client.on('close', drop);
    upstream.on('close', drop);
  });
  const proxyPort = await listen(server);
  const dropAll = async (): Promise<void> => {
    for (const socket of live) socket.destroy();
    live.clear();
    await sleep(50);
  };
  return {
    port: proxyPort,
    dropAll,
    async close(): Promise<void> {
      await dropAll();
      server.close();
    },
  };
}

main().catch((error: unknown) => {
  console.error('FATAL', error);
  process.exit(2);
});
