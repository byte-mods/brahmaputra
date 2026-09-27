import 'dart:async';
import 'dart:convert';

import 'connection.dart';
import 'protocol.dart';

/// Producer settings. Field names are Kafka's, camel-cased; see
/// [ProducerConfig.fromProperties] for the dotted spelling.
class ProducerConfig {
  ProducerConfig({
    this.clientId = 'brahmaputra-dart',
    this.acks = 1,
    this.batchSize = 16 * 1024,
    this.lingerMs = 5,
    this.compressionType = 'none',
    this.requestTimeoutMs = 30000,
    this.retries = 5,
    this.retryBackoffMs = 100,
    this.deliveryTimeoutMs = 120000,
    this.bufferMemory = 32 * 1024 * 1024,
    this.maxBlockMs = 60000,
    this.socketRequestTimeout = defaultRequestTimeout,
  });

  /// Build from Kafka-style properties such as `{'linger.ms': 5}`.
  factory ProducerConfig.fromProperties(Map<String, Object> props) {
    int i(String k, int d) => props.containsKey(k) ? _asInt(props[k]!) : d;
    return ProducerConfig(
      clientId: props['client.id']?.toString() ?? 'brahmaputra-dart',
      acks: props['acks']?.toString() == 'all' ? -1 : i('acks', 1),
      batchSize: i('batch.size', 16 * 1024),
      lingerMs: i('linger.ms', 5),
      compressionType: props['compression.type']?.toString() ?? 'none',
      requestTimeoutMs: i('request.timeout.ms', 30000),
      retries: i('retries', 5),
      retryBackoffMs: i('retry.backoff.ms', 100),
      deliveryTimeoutMs: i('delivery.timeout.ms', 120000),
      bufferMemory: i('buffer.memory', 32 * 1024 * 1024),
      maxBlockMs: i('max.block.ms', 60000),
    );
  }

  /// `client.id`.
  String clientId;

  /// `acks`: 0 fire-and-forget, 1 leader append, -1 (all) every in-sync replica.
  int acks;

  /// `batch.size`: flush a partition's buffer once it holds this many bytes.
  int batchSize;

  /// `linger.ms`: flush every non-empty buffer at least this often; 0 sends
  /// each record immediately. Defaults to 5 (Kafka: 0).
  int lingerMs;

  /// `compression.type`: none, gzip, or one added with [registerCodec].
  String compressionType;

  /// `request.timeout.ms`: how long the broker may wait for replication.
  int requestTimeoutMs;

  /// `retries` of a send refused with a retriable error.
  int retries;

  /// `retry.backoff.ms`.
  int retryBackoffMs;

  /// `delivery.timeout.ms`: caps a send, first attempt through last retry.
  int deliveryTimeoutMs;

  /// `buffer.memory`: caps unflushed record bytes held client-side.
  int bufferMemory;

  /// `max.block.ms`: how long send() may block on a full buffer.
  int maxBlockMs;

  /// Client-side round-trip bound on every request's socket wait.
  Duration socketRequestTimeout;
}

int _asInt(Object value) =>
    value is int ? value : int.parse(value.toString());

class _Buffered {
  _Buffered(this.record, this.createdMs);
  final Record record;
  final int createdMs;
}

class _Slot {
  _Slot(this.topic, this.partition);
  final String topic;
  final int partition;
  final List<_Buffered> records = [];
  int size = 0;

  /// Tail of this partition's chain of sends: one batch in flight at a time.
  Future<void> chain = Future.value();
}

/// A batching producer. Share one across the application: the batching is
/// the point.
class Producer {
  Producer._(this.router, this.config)
      : _codec = Compression.parse(config.compressionType) {
    if (config.lingerMs > 0) {
      _ticker = Timer.periodic(Duration(milliseconds: config.lingerMs), (_) {
        if (_ticking || _closed) return;
        _ticking = true;
        // A background flush that fails must not kill the ticker; the next
        // flush()/close() reports it.
        _flushAll().catchError((Object error) {
          _backgroundError ??= error;
        }).whenComplete(() => _ticking = false);
      });
    }
  }

  static Future<Producer> connect(String host, int port,
      [ProducerConfig? config]) async {
    final c = config ?? ProducerConfig();
    final router = await Router.connect(host, port,
        clientId: c.clientId, requestTimeout: c.socketRequestTimeout);
    return Producer._(router, c);
  }

  final Router router;
  final ProducerConfig config;
  final int _codec;
  final Map<String, _Slot> _slots = {};
  int _bufferedBytes = 0;
  int _roundRobin = 0;
  bool _closed = false;
  bool _ticking = false;
  Timer? _ticker;
  Object? _backgroundError;

  /// Buffer one record for [topic]. A null [value] is a tombstone; an empty
  /// list is an empty value. Without [partition], a [key] picks the
  /// partition by murmur2 and no key goes round-robin. Completes once the
  /// record is buffered (or sent, with linger.ms=0 or a full batch); call
  /// [flush] to await delivery.
  Future<void> send(String topic, List<int>? value,
      {List<int>? key, int? partition, List<RecordHeader> headers = const []}) async {
    if (_closed) throw BrahmaputraException('producer is closed');
    final target = partition ?? await _choosePartition(topic, key);
    final record = Record(key: key, value: value, headers: headers);
    var size = (value?.length ?? 0) + (key?.length ?? 0) + 16;
    for (final h in headers) {
      size += utf8.encode(h.key).length + (h.value?.length ?? 0) + 4;
    }
    await _reserve(size);

    final slot =
        _slots.putIfAbsent('$topic\u0000$target', () => _Slot(topic, target));
    slot.records.add(_Buffered(record, DateTime.now().millisecondsSinceEpoch));
    slot.size += size;
    if (config.lingerMs == 0 || slot.size >= config.batchSize) {
      await _flushSlot(slot);
    }
  }

  /// Send one record on its own and return its offset. Slow by design.
  Future<int> sendSync(String topic, List<int>? value,
      {List<int>? key, int? partition, List<RecordHeader> headers = const []}) async {
    final target = partition ?? await _choosePartition(topic, key);
    return _produce(topic, target, [
      _Buffered(Record(key: key, value: value, headers: headers),
          DateTime.now().millisecondsSinceEpoch)
    ]);
  }

  Future<int> _choosePartition(String topic, List<int>? key) async {
    final partitions = await router.partitions(topic);
    if (key == null) return partitions[_roundRobin++ % partitions.length];
    return partitionForKey(key, partitions);
  }

  /// Send every buffered record and wait for acknowledgement. Also throws
  /// the failure of any background (linger) flush since the last call,
  /// because those records are gone and nothing else would say so.
  Future<void> flush() async {
    await _flushAll();
    final background = _backgroundError;
    _backgroundError = null;
    if (background != null) throw background;
  }

  /// Flush, then release the timer and sockets, even if the flush fails.
  Future<void> close() async {
    try {
      await flush();
    } finally {
      _closed = true;
      _ticker?.cancel();
      _ticker = null;
      router.close();
    }
  }

  Future<void> _flushAll() async {
    Object? first;
    for (final slot in _slots.values.toList()) {
      if (slot.records.isEmpty) continue;
      try {
        await _flushSlot(slot);
      } catch (error) {
        first ??= error;
      }
    }
    if (first != null) throw first;
  }

  Future<void> _reserve(int size) async {
    final limit = config.bufferMemory;
    if (limit <= 0 || size >= limit) {
      // Larger than the whole budget: admitted rather than waiting forever
      // on a condition that can never hold.
      _bufferedBytes += size;
      return;
    }
    final deadline =
        DateTime.now().millisecondsSinceEpoch + config.maxBlockMs;
    while (_bufferedBytes + size > limit) {
      if (DateTime.now().millisecondsSinceEpoch >= deadline) {
        throw BrahmaputraException(
            'producer buffer full: $_bufferedBytes of $limit bytes unflushed '
            'after max.block.ms=${config.maxBlockMs}');
      }
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
    _bufferedBytes += size;
  }

  /// Queue a send of [slot] behind the previous one. The batch is taken when
  /// this send's turn comes, so a batch never overtakes records buffered
  /// before it.
  Future<void> _flushSlot(_Slot slot) {
    final run = slot.chain.then((_) => _sendSlot(slot));
    slot.chain = run.then((_) {}, onError: (_) {});
    return run;
  }

  Future<void> _sendSlot(_Slot slot) async {
    if (slot.records.isEmpty) return;
    final batch = List<_Buffered>.of(slot.records);
    slot.records.clear();
    final size = slot.size;
    slot.size = 0;
    _bufferedBytes = _bufferedBytes - size < 0 ? 0 : _bufferedBytes - size;
    await _produce(slot.topic, slot.partition, batch);
  }

  Future<int> _produce(String topic, int partition, List<_Buffered> batch) async {
    if (batch.isEmpty) return -1;
    // One base timestamp per batch and a delta per record.
    var maxTimestamp = batch.first.createdMs;
    for (final b in batch) {
      if (b.createdMs > maxTimestamp) maxTimestamp = b.createdMs;
    }
    final records = [
      for (final b in batch) b.record..timestampDelta = b.createdMs - maxTimestamp
    ];
    final encoded = encodeRecordBatch(records, maxTimestamp, _codec);
    final w = bodyWriter()
      ..string(topic)
      ..int32(partition)
      ..int32(config.acks)
      ..int32(config.requestTimeoutMs)
      ..int64(encoded.length)
      ..raw(encoded);
    final body = w.bytes();

    if (config.acks == 0) {
      final connection = await router.connectionFor(topic, partition);
      await connection.sendOneway(ApiKey.produce, body);
      return -1;
    }

    final deadline =
        DateTime.now().millisecondsSinceEpoch + config.deliveryTimeoutMs;
    var attemptsLeft = config.retries;
    while (true) {
      final connection = await router.connectionFor(topic, partition);
      final r = bodyReader(await connection.request(ApiKey.produce, body));
      r.string(); // topic
      r.int32(); // partition
      final code = r.int32();
      final baseOffset = r.int64();
      r.int64(); // log_append_time_ms
      if (code == ErrorCode.none) return baseOffset;
      final outOfTime = DateTime.now().millisecondsSinceEpoch >= deadline;
      if (!ErrorCode.retriable.contains(code) || attemptsLeft <= 0 || outOfTime) {
        throw ServerException(code, 'produce to $topic-$partition');
      }
      attemptsLeft--;
      if (code == ErrorCode.notLeaderOrFollower ||
          code == ErrorCode.fencedLeaderEpoch ||
          code == ErrorCode.unknownLeaderEpoch) {
        await router.refresh(topic);
      }
      await Future<void>.delayed(Duration(milliseconds: config.retryBackoffMs));
    }
  }
}
