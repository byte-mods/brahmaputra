import 'dart:typed_data';

import 'connection.dart';
import 'protocol.dart';

/// ListOffsets sentinel: the oldest retained offset.
const int earliest = -2;

/// ListOffsets sentinel: the next offset to be written (the high watermark).
const int latest = -1;

/// A topic and partition, ordered by topic then partition *as an integer*.
class TopicPartition implements Comparable<TopicPartition> {
  const TopicPartition(this.topic, this.partition);
  final String topic;
  final int partition;

  @override
  bool operator ==(Object other) =>
      other is TopicPartition &&
      other.topic == topic &&
      other.partition == partition;

  @override
  int get hashCode => Object.hash(topic, partition);

  @override
  int compareTo(TopicPartition other) {
    final byTopic = topic.compareTo(other.topic);
    return byTopic != 0 ? byTopic : partition.compareTo(other.partition);
  }

  @override
  String toString() => '$topic-$partition';
}

/// One record delivered to the application.
class ConsumedRecord {
  ConsumedRecord(this.topic, this.partition, this.offset, this.key, this.value,
      this.timestamp, this.headers);
  final String topic;
  final int partition;
  final int offset;

  /// Null when the record has no key; empty when its key is empty.
  final Uint8List? key;

  /// Null for a tombstone; empty for an empty value.
  final Uint8List? value;

  /// Absolute unix milliseconds.
  final int timestamp;
  final List<RecordHeader> headers;

  TopicPartition get topicPartition => TopicPartition(topic, partition);

  /// The first header named [name], or null.
  List<int>? header(String name) {
    for (final h in headers) {
      if (h.key == name) return h.value;
    }
    return null;
  }
}

/// A fetch's records plus the partition's high watermark.
class FetchResult {
  FetchResult(this.records, this.highWatermark);
  final List<ConsumedRecord> records;
  final int highWatermark;
}

/// Consumer settings (Kafka names, camel-cased).
class ConsumerConfig {
  ConsumerConfig({
    this.clientId = 'brahmaputra-dart',
    this.fetchMaxBytes = 8 * 1024 * 1024,
    this.fetchMinBytes = 1,
    this.fetchMaxWaitMs = 500,
    this.maxPollRecords = 500,
    this.isolationLevel = readUncommitted,
    this.clientRack = '',
    this.socketRequestTimeout = defaultRequestTimeout,
  });

  /// Build from Kafka-style properties such as `{'fetch.max.wait.ms': 100}`.
  factory ConsumerConfig.fromProperties(Map<String, Object> props) {
    int i(String k, int d) =>
        props.containsKey(k) ? int.parse(props[k].toString()) : d;
    return ConsumerConfig(
      clientId: props['client.id']?.toString() ?? 'brahmaputra-dart',
      fetchMaxBytes: i('fetch.max.bytes', 8 * 1024 * 1024),
      fetchMinBytes: i('fetch.min.bytes', 1),
      fetchMaxWaitMs: i('fetch.max.wait.ms', 500),
      maxPollRecords: i('max.poll.records', 500),
      isolationLevel: props['isolation.level']?.toString() == 'read_committed'
          ? readCommitted
          : readUncommitted,
      clientRack: props['client.rack']?.toString() ?? '',
    );
  }

  String clientId;

  /// `fetch.max.bytes`.
  int fetchMaxBytes;

  /// `fetch.min.bytes`.
  int fetchMinBytes;

  /// `fetch.max.wait.ms`: longest the broker holds a fetch waiting for data.
  int fetchMaxWaitMs;

  /// `max.poll.records` (used by the group consumer).
  int maxPollRecords;

  /// `isolation.level`: [readUncommitted] or [readCommitted].
  int isolationLevel;

  /// `client.rack`.
  String clientRack;

  /// Client-side round-trip bound on every request's socket wait.
  Duration socketRequestTimeout;
}

/// Reads one partition at a time, with no group coordination.
class Consumer {
  Consumer._(this.router, this.config);

  static Future<Consumer> connect(String host, int port,
      [ConsumerConfig? config]) async {
    final c = config ?? ConsumerConfig();
    final router = await Router.connect(host, port,
        clientId: c.clientId, requestTimeout: c.socketRequestTimeout);
    return Consumer._(router, c);
  }

  final Router router;
  final ConsumerConfig config;

  void close() => router.close();

  Future<List<int>> partitions(String topic) => router.partitions(topic);

  /// Resolve [earliest], [latest] or a unix-ms timestamp to an offset.
  Future<int> listOffsets(String topic, int partition, int timestamp) async {
    final w = bodyWriter()
      ..string(topic)
      ..int32(partition)
      ..int64(timestamp);
    final connection = await router.connectionFor(topic, partition);
    final r = bodyReader(await connection.request(ApiKey.listOffsets, w.bytes()));
    r.string();
    r.int32();
    final code = r.int32();
    final offset = r.int64();
    r.int64();
    if (code != ErrorCode.none) {
      throw ServerException(code, 'list_offsets $topic-$partition');
    }
    return offset;
  }

  /// Records of [topic]-[partition] from [offset] on.
  Future<List<ConsumedRecord>> fetch(String topic, int partition, int offset,
          [int? maxWaitMs]) async =>
      (await fetchVerbose(topic, partition, offset, maxWaitMs)).records;

  /// Fetch, also returning the partition's high watermark.
  Future<FetchResult> fetchVerbose(String topic, int partition, int offset,
      [int? maxWaitMs]) async {
    var wait = maxWaitMs ?? config.fetchMaxWaitMs;
    if (wait > config.fetchMaxWaitMs) wait = config.fetchMaxWaitMs;
    if (wait < 0) wait = 0;
    final w = bodyWriter()
      ..string(topic)
      ..int32(partition)
      ..int64(offset)
      ..int32(config.fetchMaxBytes)
      ..int32(wait)
      ..int32(config.fetchMinBytes)
      ..int32(config.isolationLevel)
      ..string(config.clientRack);
    final body = w.bytes();

    var connection = await router.connectionFor(topic, partition);
    var result = _decodeFetch(await connection.request(ApiKey.fetch, body));
    if (result.$1 == ErrorCode.notLeaderOrFollower) {
      await router.refresh(topic);
      connection = await router.connectionFor(topic, partition);
      result = _decodeFetch(await connection.request(ApiKey.fetch, body));
    }
    final (code, highWatermark, batches) = result;
    if (code != ErrorCode.none) {
      throw ServerException(code, 'fetch $topic-$partition');
    }
    final records = <ConsumedRecord>[];
    for (final batch in batches) {
      for (var i = 0; i < batch.records.length; i++) {
        final recordOffset = batch.baseOffset + i;
        // A batch can start before the requested offset.
        if (recordOffset < offset) continue;
        final rec = batch.records[i];
        records.add(ConsumedRecord(
            topic,
            partition,
            recordOffset,
            rec.key as Uint8List?,
            rec.value as Uint8List?,
            batch.maxTimestamp + rec.timestampDelta,
            rec.headers));
      }
    }
    return FetchResult(records, highWatermark);
  }

  (int, int, List<RecordBatch>) _decodeFetch(Uint8List body) {
    final r = bodyReader(body);
    r.string(); // topic
    r.int32(); // partition
    final code = r.int32();
    final highWatermark = r.int64();
    r.int64(); // last_stable_offset
    final batchesLength = r.int64();
    r.int32(); // preferred_read_replica
    final trailing = r.rest();
    if (batchesLength < 0) {
      throw ProtocolException('negative batch length $batchesLength');
    }
    if (batchesLength > trailing.length) {
      throw ProtocolException(
          'fetch response claims more batch bytes than it carries');
    }
    final raw = Uint8List.sublistView(trailing, 0, batchesLength);
    final batches = <RecordBatch>[];
    var pos = 0;
    while (pos < raw.length) {
      final (batch, next) = decodeRecordBatch(raw, pos);
      batches.add(batch);
      pos = next;
    }
    return (code, highWatermark, batches);
  }
}
