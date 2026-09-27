# Brahmaputra client for Dart

A native Dart driver: `async`/`Future` API over `dart:io` sockets, no
dependencies beyond the Dart SDK (≥ 3.0). gzip comes from `dart:io`'s
`GZipCodec`; linger and heartbeats run on `Timer`s.

```yaml
# pubspec.yaml
dependencies:
  brahmaputra:
    path: path/to/brahmaputra/clients/dart
```

Verified end to end against a live broker: **54/54 checks**
(`./test.sh HOST PORT`, which runs `dart analyze --fatal-infos` and then
`dart run bin/manual_test.dart HOST PORT`).

## Produce

```dart
import 'dart:convert';
import 'package:brahmaputra/brahmaputra.dart';

final producer = await Producer.connect('127.0.0.1', 9092, ProducerConfig(
  acks: 1,
  lingerMs: 5,
  compressionType: 'gzip',
));

// Keyed: murmur2(key) % partitions, so records sharing a key keep order.
await producer.send('orders', utf8.encode('{"id":1}'),
    key: utf8.encode('user-7'),
    headers: [RecordHeader('trace-id', utf8.encode('abc-123'))]);

// A null value is a tombstone; an empty list is an empty value.
await producer.send('orders', null, key: utf8.encode('user-7'));

// Or wait for one record's offset. A full round trip — correct, and slow.
final offset = await producer.sendSync('orders', utf8.encode('{"id":2}'));

await producer.flush();   // also throws a failed background (linger) flush
await producer.close();
```

## Consume one partition

```dart
final consumer = await Consumer.connect('127.0.0.1', 9092);
final records = await consumer.fetch('orders', 0, 0, 500);
for (final record in records) {
  print('${record.offset} ${record.key} ${record.value}');
}
final end = await consumer.listOffsets('orders', 0, latest);
final result = await consumer.fetchVerbose('orders', 0, end); // + highWatermark
consumer.close();
```

## Consume as a group

```dart
final consumer = await GroupConsumer.connect('127.0.0.1', 9092, 'billing',
    GroupConfig(
      assignor: Assignor.sticky,
      autoOffsetReset: AutoOffsetReset.earliest,
      autoCommitIntervalMs: 0,        // commit explicitly
      groupInstanceId: 'worker-3',    // static membership
    ));
consumer.subscribe(['orders']);
try {
  while (running) {
    final records = await consumer.poll(const Duration(milliseconds: 500));
    for (final record in records) {
      handle(record.value);
    }
    // At-least-once: commit after processing, never before.
    await consumer.commit();
  }
} finally {
  await consumer.close();   // commits, then leaves so partitions move at once
}
```

Every config class also takes Kafka's dotted names:

```dart
ProducerConfig.fromProperties({'acks': 'all', 'linger.ms': 10});
GroupConfig.fromProperties({'enable.auto.commit': 'false',
                            'partition.assignment.strategy': 'sticky'});
```

## Configuration

| Kafka name | Dart field | Default | Notes |
|---|---|---|---|
| **Producer** | | | |
| `acks` | `acks` | `1` | `0`, `1`, `-1` (`all`) |
| `batch.size` | `batchSize` | 16 KiB | per-partition flush threshold |
| `linger.ms` | `lingerMs` | `5` | `0` sends every record immediately |
| `compression.type` | `compressionType` | `none` | `none`, `gzip`, or a registered codec |
| `request.timeout.ms` | `requestTimeoutMs` | 30000 | broker-side replication wait |
| `retries` | `retries` | 5 | retriable broker errors only |
| `retry.backoff.ms` | `retryBackoffMs` | 100 | |
| `delivery.timeout.ms` | `deliveryTimeoutMs` | 120000 | caps first attempt through last retry |
| `buffer.memory` | `bufferMemory` | 32 MiB | unflushed bytes held client-side |
| `max.block.ms` | `maxBlockMs` | 60000 | then `send` throws "producer buffer full" |
| **Consumer** | | | |
| `fetch.max.bytes` | `fetchMaxBytes` | 8 MiB | |
| `fetch.min.bytes` | `fetchMinBytes` | 1 | |
| `fetch.max.wait.ms` | `fetchMaxWaitMs` | 500 | |
| `max.poll.records` | `maxPollRecords` | 500 | |
| `isolation.level` | `isolationLevel` | `readUncommitted` | |
| `client.rack` | `clientRack` | `''` | |
| **Group** | | | |
| `session.timeout.ms` | `sessionTimeoutMs` | 10000 | heartbeat every third of it |
| `max.poll.interval.ms` | `maxPollIntervalMs` | 300000 | time inside `poll` never counts |
| `auto.commit.interval.ms` | `autoCommitIntervalMs` | 5000 | `0` disables auto-commit |
| `auto.offset.reset` | `autoOffsetReset` | `earliest` | `none` throws `NoOffsetForPartitionException` |
| `partition.assignment.strategy` | `assignor` | `range` | `range`, `roundrobin`, `sticky` |
| `group.instance.id` | `groupInstanceId` | `''` | static membership |

Every config also has `socketRequestTimeout` (default 2 minutes): the
client-side bound on one request's round trip. A request that exceeds it,
an I/O error, or a response with an unexpected correlation id closes that
connection and marks it `broken`; the router redials on next use, the seed
connection included. `Connection.requestTimeout` changes it on a single
connection.

## Compression

`none` and `gzip` are built in. The rest are opt-in, so this package has
no dependencies of its own:

```dart
registerCodec(Compression.zstd, Codec(myZstdCompress, myZstdDecompress));
```

If you register lz4, the broker expects a little-endian `uint32` of the
uncompressed length followed by a raw LZ4 **block**, not the LZ4 frame
format.

## Running the tests

```bash
brahmaputra-server --data-dir ./data --default-partitions 4
clients/dart/test.sh 127.0.0.1 9092
```

## Not implemented

As with the other non-Rust drivers: no TLS/QUIC, and no idempotent or
transactional producer. `Connection.authenticate` (SCRAM-SHA-256) is
implemented but the broker refuses credentials on a plaintext listener.
