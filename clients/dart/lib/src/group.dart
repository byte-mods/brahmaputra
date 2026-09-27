import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'connection.dart';
import 'consumer.dart';
import 'protocol.dart';

const String _offsetsTopic = '__consumer_offsets';
const int _coordinatorAttempts = 4;
const int _joinAttempts = 4;

/// `auto.offset.reset` policies.
enum AutoOffsetReset {
  /// Oldest retained record: reprocesses, never silently skips.
  earliest,

  /// The end: skips whatever was missed, never reprocesses.
  latest,

  /// Refuse to guess: [GroupConsumer.poll] throws
  /// [NoOffsetForPartitionException].
  none,
}

/// `partition.assignment.strategy`.
enum Assignor {
  range,
  roundrobin,

  /// Keeps members on the partitions they already hold.
  sticky,
}

/// Group consumer settings (Kafka names, camel-cased).
class GroupConfig {
  GroupConfig({
    this.clientId = 'brahmaputra-dart',
    this.sessionTimeoutMs = 10000,
    this.rebalanceTimeoutMs = 3000,
    this.maxPollIntervalMs = 300000,
    this.autoCommitIntervalMs = 5000,
    this.autoOffsetReset = AutoOffsetReset.earliest,
    this.assignor = Assignor.range,
    this.groupInstanceId = '',
    this.maxPollRecords = 500,
    this.fetchMaxBytes = 8 * 1024 * 1024,
    this.socketRequestTimeout = defaultRequestTimeout,
  });

  /// Build from Kafka-style properties such as `{'session.timeout.ms': 6000}`.
  factory GroupConfig.fromProperties(Map<String, Object> props) {
    int i(String k, int d) =>
        props.containsKey(k) ? int.parse(props[k].toString()) : d;
    final enableAutoCommit =
        props['enable.auto.commit']?.toString() != 'false';
    return GroupConfig(
      clientId: props['client.id']?.toString() ?? 'brahmaputra-dart',
      sessionTimeoutMs: i('session.timeout.ms', 10000),
      rebalanceTimeoutMs: i('rebalance.timeout.ms', 3000),
      maxPollIntervalMs: i('max.poll.interval.ms', 300000),
      autoCommitIntervalMs:
          enableAutoCommit ? i('auto.commit.interval.ms', 5000) : 0,
      autoOffsetReset: AutoOffsetReset.values
          .byName(props['auto.offset.reset']?.toString() ?? 'earliest'),
      assignor: Assignor.values.byName(
          props['partition.assignment.strategy']?.toString() ?? 'range'),
      groupInstanceId: props['group.instance.id']?.toString() ?? '',
      maxPollRecords: i('max.poll.records', 500),
      fetchMaxBytes: i('fetch.max.bytes', 8 * 1024 * 1024),
    );
  }

  String clientId;

  /// `session.timeout.ms`: the coordinator evicts a silent member after this.
  /// Defaults to 10 s (Kafka: 45 s).
  int sessionTimeoutMs;

  /// `rebalance.timeout.ms`.
  int rebalanceTimeoutMs;

  /// `max.poll.interval.ms`: longest gap between polls before this member is
  /// presumed stuck and leaves. Time spent inside poll does not count.
  int maxPollIntervalMs;

  /// `auto.commit.interval.ms`; 0 disables auto-commit.
  int autoCommitIntervalMs;

  /// `auto.offset.reset`.
  AutoOffsetReset autoOffsetReset;

  /// `partition.assignment.strategy`.
  Assignor assignor;

  /// `group.instance.id`: static membership (KIP-345); empty is dynamic.
  String groupInstanceId;

  /// `max.poll.records`; 0 means no cap.
  int maxPollRecords;

  /// `fetch.max.bytes`.
  int fetchMaxBytes;

  Duration socketRequestTimeout;
}

class _Member {
  _Member(this.id, this.topics, this.held);
  final String id;
  final List<String> topics;
  final List<TopicPartition> held;
}

int _now() => DateTime.now().millisecondsSinceEpoch;

/// A consumer that shares a topic's partitions with its group.
///
/// Single-instance by design, as Kafka's consumer is: use one per worker.
class GroupConsumer {
  GroupConsumer._(this.consumer, this.groupId, this.config) {
    // One timer enforces two deadlines, so it fires often enough for the
    // shorter of them.
    final heartbeatEvery = config.sessionTimeoutMs ~/ 3;
    final pollCheckEvery = config.maxPollIntervalMs ~/ 3;
    var every = heartbeatEvery < pollCheckEvery ? heartbeatEvery : pollCheckEvery;
    if (every < 1) every = 1;
    _timer = Timer.periodic(Duration(milliseconds: every), (_) {
      if (_ticking) return; // one heartbeat at a time
      _ticking = true;
      _tick().catchError((Object _) {}).whenComplete(() => _ticking = false);
    });
  }

  static Future<GroupConsumer> connect(String host, int port, String groupId,
      [GroupConfig? config]) async {
    final c = config ?? GroupConfig();
    final consumer = await Consumer.connect(
        host,
        port,
        ConsumerConfig(
            clientId: c.clientId,
            fetchMaxBytes: c.fetchMaxBytes,
            maxPollRecords: c.maxPollRecords,
            socketRequestTimeout: c.socketRequestTimeout));
    return GroupConsumer._(consumer, groupId, c);
  }

  final Consumer consumer;
  final String groupId;
  final GroupConfig config;

  List<String> _subscribed = [];
  String _memberId = '';
  int _generation = -1;
  bool _joined = false;
  List<TopicPartition> _assignment = [];

  /// Next offset to *deliver*: what gets committed.
  final Map<TopicPartition, int> _positions = {};

  /// Next offset to *fetch*; runs ahead of positions by the buffer.
  Map<TopicPartition, int> _fetchPositions = {};
  List<ConsumedRecord> _buffered = [];
  int _lastPollMs = _now();
  int _lastCommitMs = _now();
  bool _closed = false;
  bool _leftForSlowPoll = false;
  bool _inPoll = false;
  bool _ticking = false;
  late final Timer _timer;

  String get memberId => _memberId;
  int get generation => _generation;
  List<TopicPartition> get assignment => List.unmodifiable(_assignment);

  void subscribe(List<String> topics) {
    _subscribed = List.of(topics);
    _joined = false;
  }

  /// Commit, leave the group, then stop. Leaving lets the coordinator
  /// reassign at once instead of waiting out session.timeout.ms.
  Future<void> close() async {
    _closed = true;
    _timer.cancel();
    try {
      if (_joined) await commit();
    } catch (_) {}
    try {
      if (_memberId.isNotEmpty) await _leave();
    } catch (_) {}
    consumer.close();
  }

  /// Up to max.poll.records records, joining the group first if needed.
  Future<List<ConsumedRecord>> poll(
      [Duration timeout = const Duration(seconds: 1)]) async {
    if (_closed) throw BrahmaputraException('consumer is closed');
    if (_subscribed.isEmpty) {
      throw BrahmaputraException(
          'subscribe to at least one topic before polling');
    }
    // Stamped on entry and exit, never enforced in between: the interval
    // bounds time the *application* spends between polls.
    _lastPollMs = _now();
    _inPoll = true;
    try {
      return await _poll(timeout.inMilliseconds);
    } finally {
      _inPoll = false;
      _lastPollMs = _now();
    }
  }

  Future<List<ConsumedRecord>> _poll(int timeoutMs) async {
    final deadline = _now() + timeoutMs;
    while (true) {
      // Checked every sweep: a rebalance the heartbeat learns of mid-poll
      // must stop this member fetching partitions it may no longer own.
      if (!_joined) await _join();
      if (_buffered.isNotEmpty) return _takeBuffered();
      if (_assignment.isEmpty) {
        if (_now() >= deadline) return [];
        await Future<void>.delayed(const Duration(milliseconds: 50));
        continue;
      }
      var gotAny = false;
      for (final slot in List.of(_assignment)) {
        if (!_joined) break;
        var remaining = deadline - _now();
        if (remaining < 0) remaining = 0;
        final offset = _fetchPositions[slot] ?? 0;
        List<ConsumedRecord> records;
        try {
          records = await consumer.fetch(slot.topic, slot.partition, offset,
              remaining < 500 ? remaining : 500);
        } on ServerException catch (error) {
          if (error.code == ErrorCode.offsetOutOfRange) {
            final reset = await _resetOffset(slot);
            _fetchPositions[slot] = reset;
            _positions[slot] = reset;
            _buffered.removeWhere((r) => r.topicPartition == slot);
            continue;
          }
          if (error.code == ErrorCode.notLeaderOrFollower) {
            await consumer.router.refresh(slot.topic);
            continue;
          }
          rethrow;
        }
        if (records.isNotEmpty && _fetchPositions.containsKey(slot)) {
          gotAny = true;
          _fetchPositions[slot] = records.last.offset + 1;
          _buffered.addAll(records);
        }
      }
      await _maybeAutoCommit();
      if (_buffered.isNotEmpty) return _takeBuffered();
      if (!gotAny && _now() >= deadline) return [];
    }
  }

  List<ConsumedRecord> _takeBuffered() {
    final limit = config.maxPollRecords > 0 &&
            config.maxPollRecords < _buffered.length
        ? config.maxPollRecords
        : _buffered.length;
    final delivered = _buffered.sublist(0, limit);
    _buffered = _buffered.sublist(limit);
    for (final r in delivered) {
      // Only what was handed to the caller counts as consumed.
      _positions[r.topicPartition] = r.offset + 1;
    }
    return delivered;
  }

  /// Commit delivered positions. At-least-once: call after processing.
  Future<void> commit() async {
    if (_positions.isEmpty) return;
    final entries = _positions.entries.toList()
      ..sort((a, b) => a.key.compareTo(b.key));
    final w = bodyWriter()
      ..string(groupId)
      ..int32(_generation)
      ..string(_memberId)
      ..int32(entries.length);
    for (final e in entries) {
      w
        ..string(e.key.topic)
        ..int32(e.key.partition)
        ..int64(e.value);
    }
    final r = bodyReader(
        await _coordinatorRequest(ApiKey.offsetCommit, w.bytes()));
    final code = r.int32();
    if (code != ErrorCode.none) throw ServerException(code, 'offset_commit');
    _lastCommitMs = _now();
  }

  /// Committed offsets; an empty list asks for every partition.
  Future<Map<TopicPartition, int>> committed(
      [List<TopicPartition> partitions = const []]) async {
    final w = bodyWriter()
      ..string(groupId)
      ..int32(partitions.length);
    for (final p in partitions) {
      w
        ..string(p.topic)
        ..int32(p.partition);
    }
    final r =
        bodyReader(await _coordinatorRequest(ApiKey.offsetFetch, w.bytes()));
    final code = r.int32();
    if (code != ErrorCode.none) throw ServerException(code, 'offset_fetch');
    final out = <TopicPartition, int>{};
    for (var n = r.count(); n > 0; n--) {
      final topic = r.string();
      final partition = r.int32();
      out[TopicPartition(topic, partition)] = r.int64();
    }
    return out;
  }

  Future<void> _maybeAutoCommit() async {
    final interval = config.autoCommitIntervalMs;
    if (interval <= 0 || _positions.isEmpty) return;
    if (_now() - _lastCommitMs < interval) return;
    try {
      await commit();
    } catch (_) {
      // Retried on the next poll; an explicit commit is what callers rely on.
    }
  }

  Future<int> _resetOffset(TopicPartition slot) {
    switch (config.autoOffsetReset) {
      case AutoOffsetReset.earliest:
        return consumer.listOffsets(slot.topic, slot.partition, earliest);
      case AutoOffsetReset.latest:
        return consumer.listOffsets(slot.topic, slot.partition, latest);
      case AutoOffsetReset.none:
        throw NoOffsetForPartitionException(
            'no committed offset for ${slot.topic}-${slot.partition}');
    }
  }

  Future<void> _join() async {
    for (var attempt = 0; attempt < _joinAttempts; attempt++) {
      final w = bodyWriter()
        ..string(groupId)
        ..int32(config.sessionTimeoutMs)
        ..int32(config.rebalanceTimeoutMs)
        ..string(_memberId)
        ..stringArray(_subscribed)
        ..string(config.groupInstanceId);
      final r =
          bodyReader(await _coordinatorRequest(ApiKey.joinGroup, w.bytes()));
      final code = r.int32();
      if (code == ErrorCode.rebalanceInProgress) {
        await Future<void>.delayed(const Duration(milliseconds: 100));
        continue;
      }
      if (code == ErrorCode.unknownMemberId) {
        // Dropped by the coordinator: rejoin as a new member.
        _memberId = '';
        continue;
      }
      if (code != ErrorCode.none) throw ServerException(code, 'join_group');
      final generation = r.int32();
      final memberId = r.string();
      final leaderId = r.string();
      final members = <_Member>[];
      for (var n = r.count(); n > 0; n--) {
        final id = r.string();
        final topics = r.stringArray();
        final held = <TopicPartition>[];
        for (var h = r.count(); h > 0; h--) {
          held.add(TopicPartition(r.string(), r.int32()));
        }
        members.add(_Member(id, topics, held));
      }
      _memberId = memberId;
      _generation = generation;
      final assignments = memberId == leaderId
          ? await _computeAssignments(members)
          : <String, List<TopicPartition>>{};
      if (await _sync(assignments)) {
        _joined = true;
        _leftForSlowPoll = false;
        return;
      }
    }
    throw BrahmaputraException(
        'consumer group failed to stabilise after $_joinAttempts join attempts');
  }

  Future<bool> _sync(Map<String, List<TopicPartition>> assignments) async {
    final ids = assignments.keys.toList()..sort();
    final w = bodyWriter()
      ..string(groupId)
      ..int32(_generation)
      ..string(_memberId)
      ..int32(ids.length);
    for (final id in ids) {
      final parts = assignments[id]!;
      w
        ..string(id)
        ..int32(parts.length);
      for (final p in parts) {
        w
          ..string(p.topic)
          ..int32(p.partition);
      }
    }
    final r = bodyReader(await _coordinatorRequest(ApiKey.syncGroup, w.bytes()));
    final code = r.int32();
    if (code == ErrorCode.rebalanceInProgress ||
        code == ErrorCode.illegalGeneration) {
      return false;
    }
    if (code == ErrorCode.unknownMemberId) {
      _memberId = '';
      return false;
    }
    if (code != ErrorCode.none) throw ServerException(code, 'sync_group');
    final assignment = <TopicPartition>[];
    for (var n = r.count(); n > 0; n--) {
      assignment.add(TopicPartition(r.string(), r.int32()));
    }
    await _applyAssignment(assignment);
    return true;
  }

  Future<void> _applyAssignment(List<TopicPartition> assignment) async {
    _assignment = assignment;
    final owned = assignment.toSet();
    _positions.removeWhere((slot, _) => !owned.contains(slot));
    // Buffered records were never delivered; a new assignment drops them.
    _buffered = [];
    final needed =
        assignment.where((slot) => !_positions.containsKey(slot)).toList();
    if (needed.isNotEmpty) {
      final committedOffsets = await committed(needed);
      for (final slot in needed) {
        var offset = committedOffsets[slot];
        if (offset == null || offset < 0) offset = await _resetOffset(slot);
        _positions[slot] = offset;
      }
    }
    _fetchPositions = Map.of(_positions);
  }

  Future<Map<String, List<TopicPartition>>> _computeAssignments(
      List<_Member> members) async {
    final topicPartitions = <String, List<int>>{};
    for (final m in members) {
      for (final t in m.topics) {
        if (!topicPartitions.containsKey(t)) {
          topicPartitions[t] = await consumer.partitions(t);
        }
      }
    }
    final subscriptions = {for (final m in members) m.id: m.topics};
    switch (config.assignor) {
      case Assignor.range:
        return rangeAssign(subscriptions, topicPartitions);
      case Assignor.roundrobin:
        return roundRobinAssign(subscriptions, topicPartitions);
      case Assignor.sticky:
        return stickyAssign(subscriptions, topicPartitions,
            {for (final m in members) m.id: m.held});
    }
  }

  Future<void> _leave() async {
    final w = bodyWriter()
      ..string(groupId)
      ..string(_memberId);
    final r =
        bodyReader(await _coordinatorRequest(ApiKey.leaveGroup, w.bytes()));
    final code = r.int32();
    _joined = false;
    if (code != ErrorCode.none) throw ServerException(code, 'leave_group');
  }

  Future<void> _tick() async {
    if (_closed || !_joined || _memberId.isEmpty) return;
    final idle = _now() - _lastPollMs;
    if (!_inPoll && idle >= config.maxPollIntervalMs) {
      // The application stopped consuming; heartbeating on would hold its
      // partitions away from a member that could make progress.
      if (!_leftForSlowPoll) {
        _leftForSlowPoll = true;
        _joined = false;
        try {
          await _leave();
        } catch (_) {}
      }
      return;
    }
    final generation = _generation;
    final memberId = _memberId;
    final w = bodyWriter()
      ..string(groupId)
      ..int32(generation)
      ..string(memberId);
    final r = bodyReader(await _coordinatorRequest(ApiKey.heartbeat, w.bytes()));
    final code = r.int32();
    if ((code == ErrorCode.rebalanceInProgress ||
            code == ErrorCode.unknownMemberId ||
            code == ErrorCode.illegalGeneration) &&
        // An answer about a generation this member already left behind
        // must not send it round again.
        _generation == generation &&
        _memberId == memberId) {
      _joined = false;
    }
  }

  Future<int> _coordinatorPartition() async {
    final partitions = await consumer.partitions(_offsetsTopic);
    return crc32c(utf8.encode(groupId)) % partitions.length;
  }

  Future<Uint8List> _coordinatorRequest(int apiKey, List<int> body) async {
    for (var attempt = 0; attempt < _coordinatorAttempts; attempt++) {
      final partition = await _coordinatorPartition();
      final connection =
          await consumer.router.connectionFor(_offsetsTopic, partition);
      final response = await connection.request(apiKey, body);
      final code = _peekErrorCode(response);
      if (code == ErrorCode.coordinatorLoadInProgress) {
        await Future<void>.delayed(const Duration(milliseconds: 100));
        continue;
      }
      if (code == ErrorCode.notCoordinator ||
          code == ErrorCode.notLeaderOrFollower) {
        await consumer.router.refresh(_offsetsTopic);
        continue;
      }
      return response;
    }
    throw BrahmaputraException(
        'group coordinator unavailable after $_coordinatorAttempts attempts');
  }
}

int _peekErrorCode(Uint8List body) {
  try {
    return bodyReader(body).int32();
  } on BrahmaputraException {
    return ErrorCode.none;
  }
}

// ---------------------------------------------------------------------------
// Assignors. Every member computes nothing but the leader; the algorithms
// match the Go and Rust drivers so a mixed-language group agrees.
// ---------------------------------------------------------------------------

Map<String, List<TopicPartition>> _empty(Map<String, List<String>> members) =>
    {for (final id in members.keys) id: <TopicPartition>[]};

/// Contiguous ranges per topic; the first (n % members) take one extra.
Map<String, List<TopicPartition>> rangeAssign(
    Map<String, List<String>> members, Map<String, List<int>> topicPartitions) {
  final out = _empty(members);
  for (final topic in topicPartitions.keys.toList()..sort()) {
    final partitions = topicPartitions[topic]!;
    final subscribers = members.entries
        .where((e) => e.value.contains(topic))
        .map((e) => e.key)
        .toList()
      ..sort();
    if (subscribers.isEmpty) continue;
    final base = partitions.length ~/ subscribers.length;
    final extra = partitions.length % subscribers.length;
    var cursor = 0;
    for (var i = 0; i < subscribers.length; i++) {
      final n = base + (i < extra ? 1 : 0);
      for (final p in partitions.sublist(cursor, cursor + n)) {
        out[subscribers[i]]!.add(TopicPartition(topic, p));
      }
      cursor += n;
    }
  }
  return out;
}

/// Deal every partition around the circle of members sorted by id.
Map<String, List<TopicPartition>> roundRobinAssign(
    Map<String, List<String>> members, Map<String, List<int>> topicPartitions) {
  final out = _empty(members);
  final circle = members.keys.toList()..sort();
  if (circle.isEmpty) return out;
  var cursor = 0;
  for (final topic in topicPartitions.keys.toList()..sort()) {
    for (final p in topicPartitions[topic]!) {
      final start = cursor;
      while (true) {
        final id = circle[cursor % circle.length];
        cursor++;
        if (members[id]!.contains(topic)) {
          out[id]!.add(TopicPartition(topic, p));
          break;
        }
        if (cursor - start >= circle.length) break; // nobody subscribes
      }
    }
  }
  return out;
}

/// Keep members on what they hold; move only what balance requires.
/// Slots compare as (topic, partition-as-integer), never as strings.
Map<String, List<TopicPartition>> stickyAssign(
    Map<String, List<String>> members,
    Map<String, List<int>> topicPartitions,
    Map<String, List<TopicPartition>> previous) {
  final out = _empty(members);
  if (members.isEmpty) return out;
  bool subscribes(String id, String topic) =>
      members[id]?.contains(topic) ?? false;

  final unassigned = <TopicPartition>[];
  final claimed = <TopicPartition, String>{};
  final previousIds = previous.keys.toList()..sort();
  for (final topic in topicPartitions.keys.toList()..sort()) {
    for (final p in topicPartitions[topic]!) {
      final slot = TopicPartition(topic, p);
      String? holder;
      for (final id in previousIds) {
        if ((previous[id] ?? const []).contains(slot) && subscribes(id, topic)) {
          holder = id;
          break;
        }
      }
      if (holder == null) {
        unassigned.add(slot);
      } else {
        claimed[slot] = holder;
      }
    }
  }

  final eligible = members.entries
      .where((e) => e.value.any(topicPartitions.containsKey))
      .map((e) => e.key)
      .toList()
    ..sort();
  if (eligible.isEmpty) return out;
  final total = topicPartitions.values.fold<int>(0, (s, l) => s + l.length);
  final base = total ~/ eligible.length;
  final extra = total % eligible.length;
  final quota = {
    for (var i = 0; i < eligible.length; i++)
      eligible[i]: base + (i < extra ? 1 : 0)
  };

  final kept = <String, List<TopicPartition>>{};
  final claimedSlots = claimed.keys.toList()..sort();
  for (final slot in claimedSlots) {
    final id = claimed[slot]!;
    final held = kept.putIfAbsent(id, () => []);
    if (held.length < (quota[id] ?? 0)) {
      held.add(slot);
    } else {
      unassigned.add(slot);
    }
  }
  kept.forEach((id, held) {
    if (out.containsKey(id)) out[id] = held;
  });

  unassigned.sort();
  for (final slot in unassigned) {
    String? taker;
    for (final id in eligible) {
      if (subscribes(id, slot.topic) && out[id]!.length < (quota[id] ?? 0)) {
        taker = id;
        break;
      }
    }
    if (taker == null) {
      // Quotas exhausted (uneven subscriptions): fall back to any subscriber
      // rather than leave the partition stalled.
      for (final id in eligible) {
        if (subscribes(id, slot.topic)) {
          taker = id;
          break;
        }
      }
    }
    if (taker != null) out[taker]!.add(slot);
  }
  for (final held in out.values) {
    held.sort();
  }
  return out;
}
