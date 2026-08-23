#!/usr/bin/env node
'use strict';

/**
 * Manual end-to-end check of the Node driver against a live broker.
 *
 *   brahmaputra-server --data-dir ./data --default-partitions 4
 *   node test_manual.js [host] [port]
 *
 * Every check asserts a property of the system, not that a function ran:
 * records come back byte-identical, keys pin partitions, headers survive,
 * offsets are contiguous, a group splits partitions and resumes from its
 * commit. Exits non-zero on any failure.
 */

const {
  Assignor,
  AutoOffsetReset,
  Consumer,
  EARLIEST,
  GroupConsumer,
  LATEST,
  NoOffsetForPartition,
  Producer,
  RecordHeader,
  murmur2,
  partitionForKey,
} = require('./src/index');

const HOST = process.argv[2] || '127.0.0.1';
const PORT = Number(process.argv[3] || 9092);

let passed = 0;
let failed = 0;

function check(name, condition, detail = '') {
  if (condition) {
    passed += 1;
    console.log(`  ok   ${name}`);
  } else {
    failed += 1;
    console.log(`  FAIL ${name}${detail ? `: ${detail}` : ''}`);
  }
}

const section = (title) => console.log(`\n${title}`);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let counter = 0;
const unique = (prefix) => `${prefix}-${Date.now() % 1e9}-${counter++}`;

async function main() {
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
  const topic = unique('node-roundtrip');
  const payloads = Array.from({ length: 50 }, (_, i) => Buffer.from(`record-${i}`));
  {
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    for (const payload of payloads) {
      await producer.send(topic, payload, { partition: 0 });
    }
    await producer.close();
  }
  {
    const consumer = await Consumer.connect(HOST, PORT);
    const got = await consumer.fetch(topic, 0, 0n);
    check('every record comes back', got.length === payloads.length, `got ${got.length}`);
    const identical =
      got.length === payloads.length &&
      got.every((record, i) => record.value.equals(payloads[i]) && record.offset === BigInt(i));
    check('values byte-identical and offsets contiguous', identical);
    consumer.close();
  }

  section('compression codecs');
  for (const codec of ['none', 'gzip', 'zstd']) {
    const codecTopic = unique(`node-${codec}`);
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
      check(
        `${codec}: round trips`,
        got.length === 20 && got[0].value.subarray(0, body.length).equals(body),
        `got ${got.length} records`
      );
      consumer.close();
    } catch (error) {
      // A codec this runtime cannot provide is a skip, not a failure: the
      // driver is correct, the environment just lacks it.
      if (String(error).includes('not available')) {
        console.log(`  skip ${codec}: ${error.message}`);
      } else {
        check(`${codec}: round trips`, false, String(error));
      }
    }
  }

  section('keys, partitioning and ordering');
  {
    const keyTopic = unique('node-keys');
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    const partitions = await producer.router.partitions(keyTopic);
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
      onTarget.every((record, i) => record.value.toString() === `v${i}`)
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
    const headerTopic = unique('node-headers');
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
    if (got.length === 2) {
      const [annotated, plain] = got;
      check('headers survive the round trip', annotated.headers.length === 3);
      check('header values are exact',
        annotated.header('trace-id').equals(Buffer.from('abc-123')));
      check('a null header value stays null', annotated.headers[2].value === null);
      check('a record with no headers gains none from its batch',
        plain.headers.length === 0);
      check(
        'timestamps are real wall-clock values',
        got.every((r) => Number(r.timestamp) >= before && Number(r.timestamp) <= after),
        `${got.map((r) => r.timestamp)} outside ${before}..${after}`
      );
    }
    consumer.close();
  }

  section('tombstones');
  {
    const tombTopic = unique('node-tombstones');
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
    if (got.length === 3) {
      check('an ordinary value round-trips', got[0].value.equals(Buffer.from('set')));
      check('an empty value is empty, not null',
        got[1].value !== null && got[1].value.length === 0);
      check('a tombstone arrives as a null value', got[2].value === null,
        `${got[2].value}`);
    }
    await consumer.close();
  }

  section('offsets');
  {
    const consumer = await Consumer.connect(HOST, PORT);
    const earliest = await consumer.listOffsets(topic, 0, EARLIEST);
    const latest = await consumer.listOffsets(topic, 0, LATEST);
    check('earliest is 0 on a fresh topic', earliest === 0n, String(earliest));
    check('latest equals the record count', latest === 50n, String(latest));
    consumer.close();
  }

  section('acks');
  for (const acks of [0, 1, -1]) {
    const acksTopic = unique(`node-acks${acks}`);
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
    const groupTopic = unique('node-group');
    const groupId = unique('node-billing');
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    for (let i = 0; i < 40; i += 1) {
      await producer.send(groupTopic, Buffer.from(`g${i}`));
    }
    await producer.close();

    const consumer = await GroupConsumer.connect(HOST, PORT, groupId, {
      autoCommitIntervalMs: 0,
    });
    consumer.subscribe([groupTopic]);
    const seen = [];
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
    const committed = await consumer.committed();
    const total = [...committed.values()].reduce((sum, offset) => sum + Number(offset), 0);
    check('commit records a position', total === 40, String(total));
    await consumer.close();

    // A second consumer in the same group must resume, not replay.
    const rejoined = await GroupConsumer.connect(HOST, PORT, groupId, {
      autoCommitIntervalMs: 0,
    });
    rejoined.subscribe([groupTopic]);
    const replayed = [];
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
    const resetTopic = unique('node-reset');
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    for (let i = 0; i < 10; i += 1) await producer.send(resetTopic, Buffer.from(`r${i}`));
    await producer.close();

    const latestConsumer = await GroupConsumer.connect(HOST, PORT, unique('node-latest'), {
      autoCommitIntervalMs: 0,
      autoOffsetReset: AutoOffsetReset.LATEST,
    });
    latestConsumer.subscribe([resetTopic]);
    const skipped = [];
    const until = Date.now() + 4000;
    while (Date.now() < until) skipped.push(...(await latestConsumer.poll(300)));
    check(
      'latest skips records produced before the group existed',
      skipped.length === 0,
      `saw ${skipped.length}`
    );
    await latestConsumer.close();

    const strict = await GroupConsumer.connect(HOST, PORT, unique('node-none'), {
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
  for (const assignor of [Assignor.RANGE, Assignor.ROUNDROBIN, Assignor.STICKY]) {
    const assignorTopic = unique(`node-${assignor}`);
    const producer = await Producer.connect(HOST, PORT, { lingerMs: 0 });
    for (let i = 0; i < 20; i += 1) await producer.send(assignorTopic, Buffer.from(`a${i}`));
    await producer.close();

    const consumer = await GroupConsumer.connect(HOST, PORT, unique(`node-grp-${assignor}`), {
      autoCommitIntervalMs: 0,
      assignor,
    });
    consumer.subscribe([assignorTopic]);
    const collected = [];
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
    const bufferTopic = unique('node-buffer');
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
        blocked = String(error).includes('buffer full');
      }
    }
    check('a full buffer blocks and then reports', blocked);
    producer.closed = true;
    clearInterval(producer.ticker);
    producer.router.close();
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((error) => {
  console.error('FATAL', error);
  process.exit(2);
});
