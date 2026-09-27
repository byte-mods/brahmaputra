// End-to-end suite for the Dart driver against a live broker.
//
//   brahmaputra-server --data-dir ./data --default-partitions 4
//   dart run bin/manual_test.dart [host] [port]
//
// A port of clients/go/cmd/manualtest with the same sections and checks.
// Every check asserts a property of the system, not that a function ran.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:brahmaputra/brahmaputra.dart';

int passed = 0;
int failed = 0;

void check(String name, bool ok, [String detail = '']) {
  if (ok) {
    passed++;
    stdout.writeln('  ok   $name');
    return;
  }
  failed++;
  stdout.writeln(detail.isEmpty ? '  FAIL $name' : '  FAIL $name: $detail');
}

void section(String title) => stdout.writeln('\n$title');

String unique(String prefix) =>
    '$prefix-${DateTime.now().microsecondsSinceEpoch * 1000 % 1000000000}';

Future<T> must<T>(Future<T> future) async {
  try {
    return await future;
  } catch (error) {
    stdout.writeln('  FATAL $error');
    exit(2);
  }
}

List<int> b(String s) => utf8.encode(s);

bool bytesEqual(List<int>? a, List<int>? c) {
  if (a == null || c == null) return a == c;
  if (a.length != c.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (a[i] != c[i]) return false;
  }
  return true;
}

bool startsWith(List<int>? data, List<int> prefix) =>
    data != null &&
    data.length >= prefix.length &&
    bytesEqual(data.sublist(0, prefix.length), prefix);

Future<void> sleepMs(int ms) => Future<void>.delayed(Duration(milliseconds: ms));

int nowMs() => DateTime.now().millisecondsSinceEpoch;

ProducerConfig noLinger() => ProducerConfig(lingerMs: 0);

Future<void> main(List<String> args) async {
  final host = args.isNotEmpty ? args[0] : '127.0.0.1';
  final port = args.length > 1 ? int.parse(args[1]) : 9092;

  section('connection and metadata');
  {
    final consumer = await must(Consumer.connect(host, port));
    try {
      final (versions, brokerVersion) =
          await consumer.router.seed.apiVersions();
      check('ApiVersions answers', versions.isNotEmpty);
      check('broker reports a version', brokerVersion.isNotEmpty, brokerVersion);
    } catch (error) {
      check('ApiVersions answers', false, '$error');
      check('broker reports a version', false, '$error');
    }
    final metadata = await must(consumer.router.metadata(refresh: true));
    check('metadata lists brokers', metadata.brokers.isNotEmpty,
        '${metadata.brokers.length} brokers');
    consumer.close();
  }

  section('produce and consume round trip');
  final topic = unique('dart-roundtrip');
  final payloads = [for (var i = 0; i < 50; i++) b('record-$i')];
  {
    final producer = await must(Producer.connect(host, port, noLinger()));
    for (final payload in payloads) {
      await must(producer.send(topic, payload, partition: 0));
    }
    await must(producer.flush());
    await must(producer.close());
  }
  {
    final consumer = await must(Consumer.connect(host, port));
    final got = await must(consumer.fetch(topic, 0, 0, 500));
    check('every record comes back', got.length == payloads.length,
        'got ${got.length}');
    var identical = got.length == payloads.length;
    for (var i = 0; identical && i < got.length; i++) {
      if (!bytesEqual(got[i].value, payloads[i]) || got[i].offset != i) {
        identical = false;
      }
    }
    check('values byte-identical and offsets contiguous', identical);
    consumer.close();
  }

  section('compression codecs');
  for (final codec in ['none', 'gzip']) {
    final codecTopic = unique('dart-$codec');
    final body = b('the same line over and over. ' * 40);
    final producer = await must(Producer.connect(
        host, port, ProducerConfig(lingerMs: 0, compressionType: codec)));
    for (var i = 0; i < 20; i++) {
      await must(producer.send(codecTopic, [...body, 0x30 + i % 10], partition: 0));
    }
    await must(producer.flush());
    await must(producer.close());
    final consumer = await must(Consumer.connect(host, port));
    final got = await must(consumer.fetch(codecTopic, 0, 0, 500));
    check('$codec: round trips',
        got.length == 20 && startsWith(got[0].value, body),
        'got ${got.length} records');
    consumer.close();
  }

  section('keys, partitioning and ordering');
  {
    final keyTopic = unique('dart-keys');
    final producer = await must(Producer.connect(host, port, noLinger()));
    final partitions = await must(producer.router.partitions(keyTopic));
    for (var i = 0; i < 30; i++) {
      await must(producer.send(keyTopic, b('v$i'), key: b('user-7')));
    }
    await must(producer.flush());
    await must(producer.close());

    final target = partitionForKey(b('user-7'), partitions);
    final consumer = await must(Consumer.connect(host, port));
    final onTarget = await must(consumer.fetch(keyTopic, target, 0, 500));
    check('a key pins every record to one partition', onTarget.length == 30,
        'partition $target holds ${onTarget.length} of 30');
    var ordered = onTarget.length == 30;
    for (var i = 0; ordered && i < onTarget.length; i++) {
      if (utf8.decode(onTarget[i].value!) != 'v$i') ordered = false;
    }
    check('per-key order is preserved', ordered);
    var strays = 0;
    for (final partition in partitions) {
      if (partition == target) continue;
      strays += (await must(consumer.fetch(keyTopic, partition, 0, 200))).length;
    }
    check('no keyed record landed elsewhere', strays == 0, '$strays strays');
    consumer.close();
  }

  section("murmur2 agrees with the broker's partitioner");
  check('murmur2("") is stable', murmur2(const []) == 275646681,
      '${murmur2(const [])}');
  check('murmur2 is deterministic', murmur2(b('user-7')) == murmur2(b('user-7')));
  check('different keys hash differently',
      murmur2(b('user-7')) != murmur2(b('user-8')));

  section('record headers and timestamps');
  {
    final headerTopic = unique('dart-headers');
    final before = nowMs() - 1000;
    final producer = await must(Producer.connect(host, port, noLinger()));
    await must(producer.send(headerTopic, b('annotated'), partition: 0, headers: [
      RecordHeader('trace-id', b('abc-123')),
      RecordHeader('content-type', b('application/json')),
      const RecordHeader('tombstone-reason', null),
    ]));
    await must(producer.send(headerTopic, b('plain'), partition: 0));
    await must(producer.flush());
    await must(producer.close());
    final after = nowMs() + 1000;

    final consumer = await must(Consumer.connect(host, port));
    final got = await must(consumer.fetch(headerTopic, 0, 0, 500));
    check('both records arrive', got.length == 2, 'got ${got.length}');
    if (got.length == 2) {
      final annotated = got[0], plain = got[1];
      check('headers survive the round trip', annotated.headers.length == 3,
          '${annotated.headers.length} headers');
      check('header values are exact',
          bytesEqual(annotated.header('trace-id'), b('abc-123')));
      check('a null header value stays null',
          annotated.headers.length == 3 && annotated.headers[2].value == null);
      check('a record with no headers gains none from its batch',
          plain.headers.isEmpty, '${plain.headers.length} headers');
      final inWindow =
          got.every((r) => r.timestamp >= before && r.timestamp <= after);
      check('timestamps are real wall-clock values', inWindow,
          '${got[0].timestamp},${got[1].timestamp} outside $before..$after');
    }
    consumer.close();
  }

  section('tombstones');
  {
    final tombTopic = unique('dart-tombstones');
    final producer = await must(Producer.connect(host, port, noLinger()));
    await must(producer.send(tombTopic, b('set'), key: b('k1'), partition: 0));
    await must(producer.send(tombTopic, const [], key: b('k2'), partition: 0));
    // A null value is a deletion and must stay distinct from empty.
    await must(producer.send(tombTopic, null, key: b('k3'), partition: 0));
    await must(producer.flush());
    await must(producer.close());

    final consumer = await must(Consumer.connect(host, port));
    final got = await must(consumer.fetch(tombTopic, 0, 0, 500));
    check('all three records arrive', got.length == 3, 'got ${got.length}');
    if (got.length == 3) {
      check('an ordinary value round-trips', bytesEqual(got[0].value, b('set')));
      check('an empty value is empty, not null',
          got[1].value != null && got[1].value!.isEmpty, '${got[1].value}');
      check('a tombstone arrives as a null value', got[2].value == null,
          '${got[2].value}');
    }
    consumer.close();
  }

  section('offsets');
  {
    final consumer = await must(Consumer.connect(host, port));
    final first = await must(consumer.listOffsets(topic, 0, earliest));
    final last = await must(consumer.listOffsets(topic, 0, latest));
    check('earliest is 0 on a fresh topic', first == 0, '$first');
    check('latest equals the record count', last == 50, '$last');
    consumer.close();
  }

  section('acks');
  for (final acks in [0, 1, -1]) {
    final acksTopic = unique('dart-acks$acks');
    final producer = await must(
        Producer.connect(host, port, ProducerConfig(lingerMs: 0, acks: acks)));
    await must(producer.send(acksTopic, b('durable'), partition: 0));
    await must(producer.flush());
    await must(producer.close());
    await sleepMs(400);
    final consumer = await must(Consumer.connect(host, port));
    final got = await must(consumer.fetch(acksTopic, 0, 0, 500));
    check('acks=$acks stores the record', got.length == 1, 'got ${got.length}');
    consumer.close();
  }

  section('consumer group: assignment, commit, resume');
  {
    final groupTopic = unique('dart-group');
    final groupId = unique('dart-billing');
    final producer = await must(Producer.connect(host, port, noLinger()));
    for (var i = 0; i < 40; i++) {
      await must(producer.send(groupTopic, b('g$i')));
    }
    await must(producer.flush());
    await must(producer.close());

    GroupConfig groupConfig() => GroupConfig(autoCommitIntervalMs: 0);
    final consumer =
        await must(GroupConsumer.connect(host, port, groupId, groupConfig()));
    consumer.subscribe([groupTopic]);
    final seen = <ConsumedRecord>[];
    final deadline = nowMs() + 30000;
    while (seen.length < 40 && nowMs() < deadline) {
      seen.addAll(await must(consumer.poll(const Duration(milliseconds: 500))));
    }
    check('the group consumes every record', seen.length == 40,
        'got ${seen.length}');
    final ids = {for (final r in seen) '${r.partition}-${r.offset}'};
    check('no record is delivered twice', ids.length == seen.length);

    await must(consumer.commit());
    final committed = await must(consumer.committed());
    final total = committed.values.fold<int>(0, (s, o) => s + o);
    check('commit records a position', total == 40, '$total');
    await must(consumer.close());

    // A second consumer in the same group must resume, not replay.
    final rejoined =
        await must(GroupConsumer.connect(host, port, groupId, groupConfig()));
    rejoined.subscribe([groupTopic]);
    final replayed = <ConsumedRecord>[];
    final until = nowMs() + 5000;
    while (nowMs() < until) {
      try {
        replayed.addAll(await rejoined.poll(const Duration(milliseconds: 300)));
      } catch (_) {}
    }
    check('a rejoining group resumes from its commit', replayed.isEmpty,
        'replayed ${replayed.length} records it had already committed');
    await must(rejoined.close());
  }

  section('auto.offset.reset');
  {
    final resetTopic = unique('dart-reset');
    final producer = await must(Producer.connect(host, port, noLinger()));
    for (var i = 0; i < 10; i++) {
      await must(producer.send(resetTopic, b('r$i')));
    }
    await must(producer.flush());
    await must(producer.close());

    final consumer = await must(GroupConsumer.connect(
        host,
        port,
        unique('dart-latest'),
        GroupConfig(
            autoCommitIntervalMs: 0, autoOffsetReset: AutoOffsetReset.latest)));
    consumer.subscribe([resetTopic]);
    final skipped = <ConsumedRecord>[];
    var until = nowMs() + 4000;
    while (nowMs() < until) {
      try {
        skipped.addAll(await consumer.poll(const Duration(milliseconds: 300)));
      } catch (_) {}
    }
    check('latest skips records produced before the group existed',
        skipped.isEmpty, 'saw ${skipped.length}');
    await must(consumer.close());

    final strict = await must(GroupConsumer.connect(
        host,
        port,
        unique('dart-none'),
        GroupConfig(
            autoCommitIntervalMs: 0, autoOffsetReset: AutoOffsetReset.none)));
    strict.subscribe([resetTopic]);
    var raised = false;
    until = nowMs() + 5000;
    while (nowMs() < until && !raised) {
      try {
        await strict.poll(const Duration(milliseconds: 300));
      } catch (error) {
        raised = error is NoOffsetForPartitionException ||
            '$error'.contains('no committed offset');
      }
    }
    check('none refuses to guess a position', raised);
    await must(strict.close());
  }

  section('assignors');
  for (final assignor in Assignor.values) {
    final assignorTopic = unique('dart-${assignor.name}');
    final producer = await must(Producer.connect(host, port, noLinger()));
    for (var i = 0; i < 20; i++) {
      await must(producer.send(assignorTopic, b('a$i')));
    }
    await must(producer.flush());
    await must(producer.close());

    final consumer = await must(GroupConsumer.connect(
        host,
        port,
        unique('dart-grp-${assignor.name}'),
        GroupConfig(autoCommitIntervalMs: 0, assignor: assignor)));
    consumer.subscribe([assignorTopic]);
    final collected = <ConsumedRecord>[];
    final deadline = nowMs() + 20000;
    while (collected.length < 20 && nowMs() < deadline) {
      try {
        collected.addAll(await consumer.poll(const Duration(milliseconds: 500)));
      } catch (_) {}
    }
    check('${assignor.name}: consumes every record', collected.length == 20,
        'got ${collected.length}');
    await must(consumer.close());
  }

  section('bounded client buffer');
  {
    final bufferTopic = unique('dart-buffer');
    final producer = await must(Producer.connect(
        host,
        port,
        ProducerConfig(
            lingerMs: 10000, // never flush on time during this check
            bufferMemory: 2048,
            maxBlockMs: 300)));
    var blocked = false;
    for (var i = 0; i < 500 && !blocked; i++) {
      try {
        await producer.send(bufferTopic, List<int>.filled(256, 0x78), partition: 0);
      } catch (error) {
        blocked = '$error'.contains('buffer full');
      }
    }
    check('a full buffer blocks and then reports', blocked);
    try {
      await producer.close();
    } catch (_) {}
  }

  section('wire edge cases');
  {
    final edgeTopic = unique('dart-edge');
    final producer = await must(Producer.connect(host, port, noLinger()));
    final large = Uint8List(1 << 20);
    for (var i = 0; i < large.length; i++) {
      large[i] = (i * 7) & 0xff;
    }
    final unicodeKey = b('ключ-✓-🔑');
    final unicodeValue = b('значение — 数据 — 🚀');
    await must(producer.send(edgeTopic, large, partition: 0));
    await must(producer.send(edgeTopic, unicodeValue,
        key: unicodeKey,
        partition: 0,
        headers: [RecordHeader('ünïcødé-🏷', b('✓'))]));
    // An empty key and an empty header value are values, not nulls.
    await must(producer.send(edgeTopic, b('empty-key'),
        key: const [],
        partition: 0,
        headers: [
          const RecordHeader('empty', []),
          const RecordHeader('null', null)
        ]));
    await must(producer.send(edgeTopic, b('null-key'), partition: 0));
    await must(producer.close());

    final consumer = await must(Consumer.connect(host, port));
    final got = <ConsumedRecord>[];
    for (var offset = 0; got.length < 4;) {
      List<ConsumedRecord> batch;
      try {
        batch = await consumer.fetch(edgeTopic, 0, offset, 500);
      } catch (_) {
        break;
      }
      if (batch.isEmpty) break;
      got.addAll(batch);
      offset = batch.last.offset + 1;
    }
    check('edge records all arrive', got.length == 4, 'got ${got.length}');
    if (got.length == 4) {
      check('a 1 MiB value round-trips byte-identical',
          bytesEqual(got[0].value, large), '${got[0].value?.length} bytes');
      check(
          'unicode key, value and header key round-trip',
          bytesEqual(got[1].key, unicodeKey) &&
              bytesEqual(got[1].value, unicodeValue) &&
              got[1].headers.length == 1 &&
              got[1].headers[0].key == 'ünïcødé-🏷');
      check('an empty key stays empty, not null',
          got[2].key != null && got[2].key!.isEmpty, '${got[2].key}');
      check(
          'an empty header value stays empty, not null',
          got[2].headers.length == 2 &&
              got[2].headers[0].value != null &&
              got[2].headers[0].value!.isEmpty &&
              got[2].headers[1].value == null,
          '${got[2].headers}');
      check('a null key stays null', got[3].key == null, '${got[3].key}');
    }
    consumer.close();
  }

  section('ordering under linger flushes');
  {
    final orderTopic = unique('dart-order');
    final producer = await must(Producer.connect(
        host, port, ProducerConfig(lingerMs: 1, batchSize: 256)));
    const total = 5000;
    for (var i = 0; i < total; i++) {
      await must(producer.send(orderTopic, b('$i'), partition: 0));
    }
    await must(producer.close());
    final consumer = await must(Consumer.connect(host, port));
    final values = <int>[];
    for (var offset = 0; values.length < total;) {
      List<ConsumedRecord> batch;
      try {
        batch = await consumer.fetch(orderTopic, 0, offset, 500);
      } catch (_) {
        break;
      }
      if (batch.isEmpty) break;
      for (final r in batch) {
        values.add(int.tryParse(utf8.decode(r.value!)) ?? -1);
      }
      offset = batch.last.offset + 1;
    }
    var inversions = 0;
    for (var i = 1; i < values.length; i++) {
      if (values[i] < values[i - 1]) inversions++;
    }
    check('every record of a partition arrives', values.length == total,
        'got ${values.length}');
    check("a partition's records keep send order", inversions == 0,
        '$inversions inversions');
    consumer.close();
  }

  section('background flush failures are reported');
  {
    final producer =
        await must(Producer.connect(host, port, ProducerConfig(lingerMs: 20)));
    // Partition 999 does not exist, so the linger timer's flush fails.
    Object? sendErr;
    try {
      await producer.send(unique('dart-bgfail'), b('lost'), partition: 999);
    } catch (error) {
      sendErr = error;
    }
    await sleepMs(300);
    Object? flushErr;
    try {
      await producer.flush();
    } catch (error) {
      flushErr = error;
    }
    check('a failed linger flush surfaces on the next Flush',
        sendErr == null && flushErr != null, 'send=$sendErr flush=$flushErr');
    var returned = false;
    try {
      await producer.close().timeout(const Duration(seconds: 5));
      returned = true;
    } on TimeoutException {
      returned = false;
    } catch (_) {
      returned = true;
    }
    check('Close returns after a failed flush', returned, returned ? '' : 'hung');
  }

  section('connection failures');
  {
    // A broker that accepts and never answers must cost an error, not a
    // Future that never completes.
    final silent = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final silentClients = <Socket>[];
    silent.listen((socket) {
      silentClients.add(socket);
      socket.listen((_) {}, onError: (_) {}, onDone: () {});
    });
    final conn = await must(Connection.connect('127.0.0.1', silent.port,
        clientId: 'dart-test', connectTimeout: const Duration(seconds: 1)));
    conn.requestTimeout = const Duration(milliseconds: 300);
    final started = nowMs();
    Object? requestErr;
    try {
      await conn.apiVersions();
    } catch (error) {
      requestErr = error;
    }
    check('a request to an unresponsive broker times out',
        requestErr != null && nowMs() - started < 3000, '$requestErr');
    check('a timed-out connection is not reused', conn.broken);
    conn.close();
    for (final s in silentClients) {
      s.destroy();
    }
    await silent.close();

    // A connection the broker drops is redialled, not kept forever.
    final proxy = await _Proxy.start(host, port);
    final dropTopic = unique('dart-drop');
    final producer =
        await must(Producer.connect('127.0.0.1', proxy.port, noLinger()));
    await must(producer.send(dropTopic, b('before'), partition: 0));
    await proxy.dropAll();
    Object? recovered = 'not attempted';
    for (var attempt = 0; attempt < 3 && recovered != null; attempt++) {
      try {
        await producer.send(dropTopic, b('after'), partition: 0);
        recovered = null;
      } catch (error) {
        recovered = error;
      }
    }
    check('a producer recovers after its connection drops', recovered == null,
        '$recovered');
    try {
      await producer.close();
    } catch (_) {}
    final consumer = await must(Consumer.connect('127.0.0.1', proxy.port));
    await must(consumer.fetch(dropTopic, 0, 0, 100));
    await proxy.dropAll();
    Object? fetchErr = 'not attempted';
    var fetched = <ConsumedRecord>[];
    for (var attempt = 0; attempt < 3 && fetchErr != null; attempt++) {
      try {
        fetched = await consumer.fetch(dropTopic, 0, 0, 100);
        fetchErr = null;
      } catch (error) {
        fetchErr = error;
      }
    }
    check('a consumer recovers after its connection drops',
        fetchErr == null && fetched.isNotEmpty, '$fetchErr');
    consumer.close();
    await proxy.close();
  }

  section('consumer group: max.poll.interval and rejoin');
  {
    final slowTopic = unique('dart-slow');
    final producer = await must(Producer.connect(host, port, noLinger()));
    for (var i = 0; i < 10; i++) {
      await must(producer.send(slowTopic, b('s$i')));
    }
    final consumer = await must(GroupConsumer.connect(host, port,
        unique('dart-slow-grp'),
        GroupConfig(autoCommitIntervalMs: 0, maxPollIntervalMs: 1500)));
    consumer.subscribe([slowTopic]);
    final first = <ConsumedRecord>[];
    var deadline = nowMs() + 15000;
    while (first.length < 10 && nowMs() < deadline) {
      try {
        first.addAll(await consumer.poll(const Duration(milliseconds: 300)));
      } catch (_) {
        break;
      }
    }
    await must(consumer.commit());
    // Stall past max.poll.interval.ms: the member leaves the group.
    await sleepMs(2500);
    for (var i = 10; i < 20; i++) {
      await must(producer.send(slowTopic, b('s$i')));
    }
    await must(producer.close());
    final second = <ConsumedRecord>[];
    Object? pollErr;
    deadline = nowMs() + 15000;
    while (second.length < 10 && nowMs() < deadline) {
      try {
        second.addAll(await consumer.poll(const Duration(milliseconds: 300)));
      } catch (error) {
        pollErr = error;
        break;
      }
    }
    check(
        'a member that stalled rejoins on its next poll',
        first.length == 10 && second.length == 10 && pollErr == null,
        'first=${first.length} second=${second.length} err=$pollErr');
    await must(consumer.close());
  }

  section(
      'consumer group: time inside poll does not count against max.poll.interval');
  {
    final joinTopic = unique('dart-inpoll');
    final producer = await must(Producer.connect(host, port, noLinger()));
    await must(producer.router.partitions(joinTopic));
    // Far shorter than the first poll below, which spends ~1s joining (the
    // broker's initial rebalance delay) and then waits for data.
    final consumer = await must(GroupConsumer.connect(host, port,
        unique('dart-inpoll-grp'),
        GroupConfig(autoCommitIntervalMs: 0, maxPollIntervalMs: 600)));
    consumer.subscribe([joinTopic]);
    final background = sleepMs(2000).then((_) async {
      for (var i = 0; i < 10; i++) {
        try {
          await producer.send(joinTopic, b('j$i'));
        } catch (_) {}
      }
    });
    // One long poll: it joins, then waits for the records above.
    var got = <ConsumedRecord>[];
    Object? pollErr;
    try {
      got = await consumer.poll(const Duration(seconds: 4));
    } catch (error) {
      pollErr = error;
    }
    // Committed straight away, before another poll could quietly rejoin:
    // this fails if the member left the group mid-poll.
    Object? commitErr;
    try {
      await consumer.commit();
    } catch (error) {
      commitErr = error;
    }
    check('a member is still in its group after a long poll',
        pollErr == null && got.isNotEmpty && commitErr == null,
        'got=${got.length} poll=$pollErr commit=$commitErr');
    await background;
    await must(consumer.close());
    await must(producer.close());
  }

  stdout.writeln('\n$passed passed, $failed failed');
  await stdout.flush();
  exit(failed > 0 ? 1 : 0);
}

/// Forwards TCP to the broker and can sever every live connection, which is
/// how a broker restart or an idle timeout looks to a client.
class _Proxy {
  _Proxy(this._server, this._host, this._port) {
    _server.listen((client) async {
      Socket upstream;
      try {
        upstream = await Socket.connect(_host, _port);
      } catch (_) {
        client.destroy();
        return;
      }
      _live
        ..add(client)
        ..add(upstream);
      void pipe(Socket from, Socket to) {
        from.listen((data) {
          try {
            to.add(data);
          } catch (_) {}
        }, onError: (_) => to.destroy(), onDone: to.destroy, cancelOnError: true);
        from.done.then((_) {}, onError: (_) {});
      }

      pipe(client, upstream);
      pipe(upstream, client);
    });
  }

  static Future<_Proxy> start(String host, int port) async => _Proxy(
      await ServerSocket.bind(InternetAddress.loopbackIPv4, 0), host, port);

  final ServerSocket _server;
  final String _host;
  final int _port;
  final List<Socket> _live = [];

  int get port => _server.port;

  Future<void> dropAll() async {
    for (final s in _live) {
      s.destroy();
    }
    _live.clear();
    await sleepMs(50);
  }

  Future<void> close() async {
    await _server.close();
    await dropAll();
  }
}
