import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'package:web_socket_channel/web_socket_channel.dart';

/// Where a [BrahmaputraClient] is in its life.
enum ConnectionStatus { idle, connecting, open, reconnecting, closed }

/// Opens a WebSocket. The default works on every platform Flutter runs on
/// (the token travels as a subprotocol, which browsers also allow).
typedef WebSocketConnector = WebSocketChannel Function(
    Uri uri, Iterable<String>? protocols);

WebSocketChannel _defaultConnector(Uri uri, Iterable<String>? protocols) =>
    WebSocketChannel.connect(uri, protocols: protocols);

class BrahmaputraException implements Exception {
  BrahmaputraException(this.code, this.message, {this.retryable = false});

  /// The gateway's error code (`TOPIC_NOT_ALLOWED`, `RATE_LIMITED`, ...) or
  /// the client's own (`CONNECTION`, `TIMEOUT`, `CLOSED`, `QUEUE_FULL`).
  final String code;
  final String message;
  final bool retryable;

  @override
  String toString() => 'BrahmaputraException($code: $message)';
}

/// What the gateway said when the connection opened.
class Welcome {
  Welcome(Map<String, dynamic> f)
      : user = f['user'] as String,
        topic = f['topic'] as String?,
        key = f['key'] as String,
        maxMessageBytes = f['max_message_bytes'] as int,
        maxInflight = f['max_inflight'] as int,
        subscribe = f['subscribe'] as bool? ?? false;

  final String user;
  final String? topic;
  final String key;
  final int maxMessageBytes;
  final int maxInflight;

  /// Whether this token may subscribe to anything.
  final bool subscribe;
}

/// The broker's acknowledgement of a publish.
class Ack {
  Ack(this.id, this.topic, this.partition, this.offset);
  final int id;
  final String topic;
  final int partition;
  final int offset;
}

/// One record delivered to a subscription.
class FeedRecord {
  FeedRecord._(Map<String, dynamic> f, this.snapshot)
      : topic = f['topic'] as String,
        partition = f['partition'] as int,
        offset = f['offset'] as int,
        timestamp = f['timestamp'] as int,
        key = f['key'] as String?,
        _keyB64 = f['key_b64'] as String?,
        value = f['value'] as String?,
        _valueB64 = f['value_b64'] as String?,
        headers = Map.unmodifiable(
            (f['headers'] as Map<String, dynamic>?)?.cast<String, String?>() ??
                const <String, String?>{});

  final String topic;
  final int partition;
  final int offset;
  final int timestamp;

  /// The key as text; null without a key or for a binary one ([keyBytes]).
  final String? key;

  /// The value as text; null for a tombstone or a binary value ([valueBytes]).
  final String? value;
  final Map<String, String?> headers;

  /// True for records sent as the subscription's snapshot.
  final bool snapshot;
  final String? _keyB64;
  final String? _valueB64;

  /// A delete marker: the key no longer has a value.
  bool get tombstone => value == null && _valueB64 == null;

  Uint8List? get keyBytes => _keyB64 != null
      ? base64.decode(_keyB64)
      : key == null
          ? null
          : Uint8List.fromList(utf8.encode(key!));

  Uint8List? get valueBytes => _valueB64 != null
      ? base64.decode(_valueB64)
      : value == null
          ? null
          : Uint8List.fromList(utf8.encode(value!));

  /// The value parsed as JSON.
  dynamic json() {
    final v = value;
    if (v == null) throw StateError('record has no text value');
    return jsonDecode(v);
  }

  /// Map key for "latest per key" views.
  String get keyId => key ?? _keyB64 ?? '';

  @override
  String toString() => 'FeedRecord($topic/$partition@$offset $key=$value)';
}

/// A live subscription; [cancel] ends it.
class Subscription {
  Subscription._(this.topic, this._cancel, this.ready);
  final String topic;

  /// Completes when the gateway first confirms the subscription.
  final Future<void> ready;
  final void Function() _cancel;
  bool _cancelled = false;

  void cancel() {
    if (_cancelled) return;
    _cancelled = true;
    _cancel();
  }
}

class _Listener {
  factory _Listener(
          Set<String>? keys,
          bool wantsSnapshot,
          void Function(FeedRecord) onRecord,
          void Function(int skipped)? onLag,
          void Function(BrahmaputraException)? onError,
          void Function(int snapshotSize)? onSubscribed) =>
      _Listener._init(
          keys, wantsSnapshot, onRecord, onLag, onError, onSubscribed);

  final Set<String>? keys;
  final bool wantsSnapshot;
  final void Function(FeedRecord) onRecord;
  final void Function(int skipped)? onLag;
  final void Function(BrahmaputraException)? onError;
  final void Function(int snapshotSize)? onSubscribed;
  final ready = Completer<void>();

  _Listener._init(this.keys, this.wantsSnapshot, this.onRecord, this.onLag,
      this.onError, this.onSubscribed) {
    // A refusal reaches whoever awaits `ready` (and onError), but is not
    // an unhandled error when nobody does.
    ready.future.ignore();
  }
}

class _Request {
  _Request(this.confirm, this.snapshot);
  final Set<_Listener> confirm;
  final Set<_Listener> snapshot;
}

class _Topic {
  final listeners = <_Listener>{};
  final requests = <int, _Request>{};
  int snapshotLeft = 0;
  Set<_Listener> snapshotTo = {};
}

class _Pending {
  _Pending(this.frame, this.completer, this.timer);
  final String frame;
  final Completer<Ack> completer;
  final Timer timer;
  int retries = 0;
  bool sent = false;
}

/// One connection to the gateway: publishes with acknowledgements, topic
/// subscriptions, and a reconnect loop that restores both.
class BrahmaputraClient {
  BrahmaputraClient({
    required this.url,
    required FutureOr<String> Function() token,
    this.topic,
    this.key,
    this.reconnect = true,
    this.minReconnectDelay = const Duration(milliseconds: 250),
    this.maxReconnectDelay = const Duration(seconds: 15),
    this.publishTimeout = const Duration(seconds: 30),
    this.maxPending = 1000,
    this.maxRetries = 5,
    WebSocketConnector? connector,
  })  : _token = token,
        _connector = connector ?? _defaultConnector;

  /// Gateway endpoint, e.g. `wss://gw.example.com/ws`.
  final Uri url;

  /// Called before every connection attempt, so an expiring token is
  /// refreshed on reconnect.
  final FutureOr<String> Function() _token;

  /// The connection's default topic for publishes.
  final String? topic;

  /// The connection's default key (the gateway defaults it to the user).
  final String? key;
  final bool reconnect;
  final Duration minReconnectDelay;
  final Duration maxReconnectDelay;
  final Duration publishTimeout;
  final int maxPending;
  final int maxRetries;
  final WebSocketConnector _connector;

  static const subprotocol = 'brahmaputra.v1';

  WebSocketChannel? _channel;
  StreamSubscription<dynamic>? _incoming;
  ConnectionStatus _state = ConnectionStatus.idle;
  final _states = StreamController<ConnectionStatus>.broadcast(sync: true);
  Welcome? _welcome;
  Object? _lastError;
  int _nextId = 1;
  int _attempt = 0;
  bool _stopped = false;
  Timer? _reconnectTimer;
  final _pending = <int, _Pending>{};
  final _topics = <String, _Topic>{};
  final _subscribeIds = <int, String>{};
  final _connectWaiters = <Completer<void>>[];
  final _random = Random();

  ConnectionStatus get state => _state;

  /// Every state change.
  Stream<ConnectionStatus> get states => _states.stream;

  /// The gateway's welcome for the current connection, once open.
  Welcome? get welcome => _welcome;

  /// Why the last connection attempt failed, while not open.
  Object? get lastError => _lastError;

  /// Open the connection; completes once the gateway welcomes it. With
  /// [reconnect] on it keeps trying until then or [close].
  Future<void> connect() {
    if (_state == ConnectionStatus.open) return Future.value();
    _stopped = false;
    final waiter = Completer<void>();
    _connectWaiters.add(waiter);
    if (_state == ConnectionStatus.idle || _state == ConnectionStatus.closed) {
      _open();
    }
    return waiter.future;
  }

  /// Close for good: pending publishes fail and subscriptions end.
  Future<void> close() async {
    _stopped = true;
    _reconnectTimer?.cancel();
    final channel = _channel;
    _channel = null;
    await _incoming?.cancel();
    _incoming = null;
    await channel?.sink.close(1000, 'client closing');
    final closed = BrahmaputraException('CLOSED', 'client closed');
    for (final p in _pending.values) {
      p.timer.cancel();
      if (!p.completer.isCompleted) p.completer.completeError(closed);
    }
    _pending.clear();
    for (final w in _connectWaiters) {
      w.completeError(closed);
    }
    _connectWaiters.clear();
    _setState(ConnectionStatus.closed);
  }

  /// Publish one record; completes with its partition and offset once the
  /// broker has it. Publishes made while disconnected are held and sent on
  /// reconnect, and unacknowledged ones are sent again (at least once).
  ///
  /// [key] and [value] are a `String` or bytes (`List<int>`); a null
  /// [value] is a tombstone.
  Future<Ack> publish(
      {String? topic,
      Object? key,
      required Object? value,
      Map<String, String?>? headers}) {
    if (_stopped && _state == ConnectionStatus.closed) {
      return Future.error(BrahmaputraException('CLOSED', 'client closed'));
    }
    if (_pending.length >= maxPending) {
      return Future.error(BrahmaputraException(
          'QUEUE_FULL', '${_pending.length} publishes pending',
          retryable: true));
    }
    final id = _nextId++;
    final frame = <String, Object?>{'id': id};
    if (topic != null) frame['topic'] = topic;
    if (key is String) {
      frame['key'] = key;
    } else if (key is List<int>) {
      frame['key_b64'] = base64.encode(key);
    } else if (key != null) {
      throw ArgumentError.value(key, 'key', 'a String or List<int>');
    }
    if (value == null || value is String) {
      frame['value'] = value;
    } else if (value is List<int>) {
      frame['value_b64'] = base64.encode(value);
    } else {
      throw ArgumentError.value(value, 'value', 'a String, List<int> or null');
    }
    if (headers != null) frame['headers'] = headers;
    final completer = Completer<Ack>();
    final timer = Timer(publishTimeout, () {
      if (_pending.remove(id) != null && !completer.isCompleted) {
        completer.completeError(BrahmaputraException(
            'TIMEOUT', 'no acknowledgement in time',
            retryable: true));
      }
    });
    final p = _Pending(jsonEncode(frame), completer, timer);
    _pending[id] = p;
    if (_state == ConnectionStatus.open) _sendPublish(p);
    return completer.future;
  }

  /// Publish [value] encoded as JSON.
  Future<Ack> publishJson(Object? value,
          {String? topic, Object? key, Map<String, String?>? headers}) =>
      publish(
          topic: topic, key: key, value: jsonEncode(value), headers: headers);

  /// Receive [topic]'s records as they are written. Survives reconnects:
  /// renewed on every new connection, with a fresh snapshot if asked for.
  Subscription subscribe(
    String topic, {
    required void Function(FeedRecord record) onRecord,
    List<String>? keys,
    bool snapshot = false,
    void Function(int skipped)? onLag,
    void Function(BrahmaputraException error)? onError,
    void Function(int snapshotSize)? onSubscribed,
  }) {
    final listener = _Listener(
        keys?.toSet(), snapshot, onRecord, onLag, onError, onSubscribed);
    final state = _topics.putIfAbsent(topic, _Topic.new);
    state.listeners.add(listener);
    if (_state == ConnectionStatus.open) {
      _sendSubscribe(topic, state, [listener]);
    }
    return Subscription._(topic, () {
      final current = _topics[topic];
      if (current == null) return;
      current.listeners.remove(listener);
      current.snapshotTo.remove(listener);
      if (current.listeners.isEmpty) {
        _topics.remove(topic);
        _sendFrame({'op': 'unsubscribe', 'topic': topic});
      } else {
        _sendSubscribe(topic, current, const []);
      }
    }, listener.ready.future);
  }

  /// [topic]'s records as a stream; cancelling it ends the subscription.
  Stream<FeedRecord> records(String topic,
      {List<String>? keys, bool snapshot = false}) {
    late StreamController<FeedRecord> controller;
    Subscription? sub;
    controller = StreamController<FeedRecord>(
      onListen: () {
        sub = subscribe(topic,
            keys: keys,
            snapshot: snapshot,
            onRecord: controller.add,
            onError: controller.addError);
      },
      onCancel: () => sub?.cancel(),
    );
    return controller.stream;
  }

  // -- internals -----------------------------------------------------------

  void _setState(ConnectionStatus s) {
    if (s == _state) return;
    _state = s;
    _states.add(s);
  }

  Future<void> _open() async {
    _setState(_attempt == 0 && _welcome == null
        ? ConnectionStatus.connecting
        : ConnectionStatus.reconnecting);
    String token;
    try {
      token = await _token();
    } catch (e) {
      _lastError = e;
      _scheduleReconnect(false);
      return;
    }
    if (_stopped) return;
    final params = Map<String, String>.of(url.queryParameters);
    if (topic != null) params['topic'] = topic!;
    if (key != null) params['key'] = key!;
    final uri = url.replace(queryParameters: params.isEmpty ? null : params);
    final WebSocketChannel channel;
    try {
      channel = _connector(uri, [subprotocol, 'bearer.$token']);
    } catch (e) {
      _lastError = e;
      _scheduleReconnect(false);
      return;
    }
    _channel = channel;
    var ended = false;
    void closed() {
      if (ended || _channel != channel) return;
      ended = true;
      _channel = null;
      final wasOpen = _state == ConnectionStatus.open;
      for (final p in _pending.values) {
        p.sent = false;
      }
      for (final t in _topics.values) {
        t.requests.clear();
        t.snapshotLeft = 0;
        t.snapshotTo = {};
      }
      if (!wasOpen) {
        _lastError = BrahmaputraException('CONNECTION',
            'connection refused or lost before welcome (${channel.closeCode})',
            retryable: true);
      }
      // 1001: the gateway instance is draining; go elsewhere at once.
      _scheduleReconnect(channel.closeCode == 1001);
    }

    _incoming = channel.stream.listen(
      (data) {
        if (data is String) _onFrame(data);
      },
      onError: (Object e) {
        _lastError = e;
        closed();
      },
      onDone: closed,
      cancelOnError: true,
    );
    // A refused handshake surfaces here (and as a stream error).
    channel.ready.catchError((Object e) {
      _lastError = e;
      closed();
    });
  }

  void _scheduleReconnect(bool immediate) {
    if (_stopped) return;
    if (!reconnect) {
      final error = _lastError ??
          BrahmaputraException('CONNECTION', 'connection closed',
              retryable: true);
      for (final w in _connectWaiters) {
        w.completeError(error);
      }
      _connectWaiters.clear();
      for (final p in _pending.values) {
        p.timer.cancel();
        if (!p.completer.isCompleted) p.completer.completeError(error);
      }
      _pending.clear();
      _setState(ConnectionStatus.closed);
      return;
    }
    final ceiling = min(maxReconnectDelay.inMicroseconds,
        minReconnectDelay.inMicroseconds * (1 << min(_attempt, 16)));
    // Full jitter: phones reconnecting after a network blip spread out.
    final delay = Duration(
        microseconds: (_random.nextDouble() *
                (immediate ? minReconnectDelay.inMicroseconds : ceiling))
            .round());
    _attempt++;
    _setState(ConnectionStatus.reconnecting);
    _reconnectTimer = Timer(delay, _open);
  }

  void _onFrame(String text) {
    final Map<String, dynamic> f;
    try {
      f = jsonDecode(text) as Map<String, dynamic>;
    } catch (_) {
      return;
    }
    switch (f['type']) {
      case 'welcome':
        _welcome = Welcome(f);
        _attempt = 0;
        _lastError = null;
        _setState(ConnectionStatus.open);
        _topics.forEach((topic, state) =>
            _sendSubscribe(topic, state, state.listeners.toList()));
        for (final p in _pending.values.toList()) {
          if (!p.sent) _sendPublish(p);
        }
        for (final w in _connectWaiters) {
          w.complete();
        }
        _connectWaiters.clear();
      case 'ack':
        final id = f['id'] as int;
        final p = _pending.remove(id);
        if (p == null) return;
        p.timer.cancel();
        p.completer.complete(Ack(id, f['topic'] as String,
            f['partition'] as int, f['offset'] as int));
      case 'error':
        _onError(
            f['id'] as int?,
            BrahmaputraException(f['code'] as String, f['message'] as String,
                retryable: f['retryable'] as bool? ?? false));
      case 'subscribed':
        final state = _topics[f['topic']];
        if (state == null) return;
        final id = f['id'] as int?;
        final request = id == null ? null : state.requests.remove(id);
        if (id != null) _subscribeIds.remove(id);
        final size = f['snapshot'] as int;
        state.snapshotLeft = size;
        state.snapshotTo = request?.snapshot ?? {};
        for (final l in request?.confirm ?? const <_Listener>{}) {
          if (!state.listeners.contains(l)) continue;
          if (!l.ready.isCompleted) l.ready.complete();
          l.onSubscribed?.call(request!.snapshot.contains(l) ? size : 0);
        }
      case 'record':
        final state = _topics[f['topic']];
        if (state == null) return;
        final isSnapshot = state.snapshotLeft > 0;
        final targets = isSnapshot ? state.snapshotTo : state.listeners;
        if (isSnapshot) state.snapshotLeft--;
        if (targets.isEmpty) return;
        final record = FeedRecord._(f, isSnapshot);
        for (final l in targets.toList()) {
          if (l.keys != null && !l.keys!.contains(record.keyId)) continue;
          l.onRecord(record);
        }
      case 'lagged':
        for (final l in _topics[f['topic']]?.listeners.toList() ?? const []) {
          l.onLag?.call(f['skipped'] as int);
        }
    }
  }

  void _onError(int? id, BrahmaputraException error) {
    if (id == null) {
      _lastError = error;
      return;
    }
    final p = _pending[id];
    if (p != null) {
      if (error.retryable && p.retries < maxRetries) {
        p.retries++;
        p.sent = false;
        final backoff = Duration(
            milliseconds:
                (100 * (1 << p.retries) * (0.5 + _random.nextDouble()))
                    .round());
        Timer(backoff, () {
          if (identical(_pending[id], p) && _state == ConnectionStatus.open) {
            _sendPublish(p);
          }
        });
        return;
      }
      _pending.remove(id);
      p.timer.cancel();
      if (!p.completer.isCompleted) p.completer.completeError(error);
      return;
    }
    // A refused subscribe: tell its topic's listeners and forget them.
    final topic = _subscribeIds.remove(id);
    if (topic == null) return;
    final state = _topics.remove(topic);
    for (final l in state?.listeners ?? const <_Listener>{}) {
      l.onError?.call(error);
      if (!l.ready.isCompleted) l.ready.completeError(error);
    }
  }

  void _sendSubscribe(String topic, _Topic state, List<_Listener> confirming) {
    final id = _nextId++;
    final frame = <String, Object?>{
      'op': 'subscribe',
      'id': id,
      'topic': topic
    };
    Set<String>? keys = {};
    for (final l in state.listeners) {
      if (l.keys == null) {
        keys = null;
        break;
      }
      keys!.addAll(l.keys!);
    }
    if (keys != null) frame['keys'] = keys.toList();
    final owed = confirming.where((l) => l.wantsSnapshot).toSet();
    if (owed.isNotEmpty) frame['snapshot'] = true;
    state.requests[id] = _Request(confirming.toSet(), owed);
    _subscribeIds[id] = topic;
    if (_subscribeIds.length > 10000) {
      _subscribeIds.remove(_subscribeIds.keys.first);
    }
    _sendFrame(frame);
  }

  void _sendPublish(_Pending p) {
    final channel = _channel;
    if (channel != null && _state == ConnectionStatus.open) {
      channel.sink.add(p.frame);
      p.sent = true;
    }
  }

  void _sendFrame(Map<String, Object?> frame) {
    final channel = _channel;
    if (channel != null && _state == ConnectionStatus.open) {
      channel.sink.add(jsonEncode(frame));
    }
  }
}
