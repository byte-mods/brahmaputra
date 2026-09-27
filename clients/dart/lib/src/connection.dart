import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'scram.dart';

import 'protocol.dart';

/// How long one request may wait for its response by default. It must exceed
/// the longest the broker may legitimately hold a request (a fetch
/// long-poll, an acks=all wait, a JoinGroup waiting out a rebalance); its job
/// is to turn a wedged broker into an error instead of a Future that never
/// completes.
const Duration defaultRequestTimeout = Duration(minutes: 2);

/// One API a broker supports, with its version range.
class ApiVersionRange {
  const ApiVersionRange(this.apiKey, this.minVersion, this.maxVersion);
  final int apiKey;
  final int minVersion;
  final int maxVersion;
}

class _Pending {
  _Pending(this.completer, this.timer);
  final Completer<Uint8List> completer;
  final Timer? timer;
}

/// One TCP connection to one broker, multiplexed by correlation id.
///
/// Any I/O failure, timeout or unexpected correlation id leaves the byte
/// stream at an unknown position, so the connection is closed and marked
/// [broken] rather than reused. The [Router] notices and redials.
class Connection {
  Connection._(this._socket, this.address, this.clientId, this.requestTimeout) {
    _socket.setOption(SocketOption.tcpNoDelay, true);
    _socket.listen(_onData,
        onError: (Object error) => _fail(BrahmaputraException('$error')),
        onDone: () => _fail(BrahmaputraException('connection closed by broker')),
        cancelOnError: true);
    // Write errors surface here; without a handler they would be unhandled.
    _socket.done.then((_) {}, onError: (Object error) {
      _fail(BrahmaputraException('$error'));
    });
  }

  /// Dial one broker.
  static Future<Connection> connect(String host, int port,
      {String clientId = 'brahmaputra-dart',
      Duration connectTimeout = const Duration(seconds: 30),
      Duration requestTimeout = defaultRequestTimeout}) async {
    final socket = await Socket.connect(host, port, timeout: connectTimeout);
    return Connection._(socket, '$host:$port', clientId, requestTimeout);
  }

  final Socket _socket;

  /// host:port this connection was dialled to.
  final String address;
  final String clientId;

  /// Round-trip bound for each request. Zero disables it.
  Duration requestTimeout;

  final Map<int, _Pending> _pending = {};
  final BytesBuilder _buffer = BytesBuilder(copy: false);
  Uint8List _carry = Uint8List(0);
  int _next = 0;
  bool _broken = false;

  /// True once this connection failed or was closed; it must not be reused.
  bool get broken => _broken;

  void close() => _fail(BrahmaputraException('connection closed'));

  void _fail(BrahmaputraException error) {
    if (!_broken) {
      _broken = true;
      _socket.destroy();
    }
    final waiters = _pending.values.toList();
    _pending.clear();
    for (final waiter in waiters) {
      waiter.timer?.cancel();
      if (!waiter.completer.isCompleted) waiter.completer.completeError(error);
    }
  }

  int _nextCorrelationId() {
    _next = _next >= 0x7fffffff ? 1 : _next + 1;
    return _next;
  }

  /// Send one request and complete with the matching response body.
  Future<Uint8List> request(int apiKey, List<int> body) {
    if (_broken) {
      return Future.error(BrahmaputraException(
          'connection to $address is broken; the router will redial'));
    }
    final correlationId = _nextCorrelationId();
    final completer = Completer<Uint8List>();
    Timer? timer;
    if (requestTimeout > Duration.zero) {
      final timeout = requestTimeout;
      timer = Timer(timeout, () {
        if (completer.isCompleted) return;
        // The response may still be on its way; the stream is no longer
        // trustworthy, so the whole connection goes.
        _fail(RequestTimeoutException(
            'request to $address timed out after ${timeout.inMilliseconds} ms'));
      });
    }
    _pending[correlationId] = _Pending(completer, timer);
    try {
      _socket.add(encodeFrame(apiKey, correlationId, clientId, body));
    } catch (error) {
      _fail(BrahmaputraException('write to $address failed: $error'));
    }
    return completer.future;
  }

  /// Send without awaiting a response (acks=0).
  Future<void> sendOneway(int apiKey, List<int> body) async {
    if (_broken) {
      throw BrahmaputraException(
          'connection to $address is broken; the router will redial');
    }
    try {
      _socket.add(encodeFrame(apiKey, _nextCorrelationId(), clientId, body));
      await _socket.flush();
    } catch (error) {
      _fail(BrahmaputraException('write to $address failed: $error'));
      rethrow;
    }
  }

  void _onData(Uint8List chunk) {
    Uint8List data;
    if (_carry.isEmpty) {
      data = chunk;
    } else {
      _buffer.add(_carry);
      _buffer.add(chunk);
      data = _buffer.takeBytes();
    }
    var pos = 0;
    while (!_broken) {
      if (data.length - pos < 4) break;
      final length = ByteData.sublistView(data, pos, pos + 4).getInt32(0);
      if (length < 0) {
        _fail(ProtocolException('negative frame length $length'));
        return;
      }
      if (data.length - pos - 4 < length) break;
      final payload = Uint8List.sublistView(data, pos + 4, pos + 4 + length);
      pos += 4 + length;
      try {
        final (correlationId, body) = decodeFramePayload(payload);
        final waiter = _pending.remove(correlationId);
        if (waiter == null) {
          // A response nobody is waiting for means the stream has
          // desynchronised; every later response would be misattributed.
          _fail(ProtocolException(
              'correlation id mismatch: unexpected response $correlationId'));
          return;
        }
        waiter.timer?.cancel();
        waiter.completer.complete(Uint8List.fromList(body));
      } on BrahmaputraException catch (error) {
        _fail(error);
        return;
      }
    }
    _carry = pos >= data.length
        ? Uint8List(0)
        : Uint8List.fromList(Uint8List.sublistView(data, pos));
  }

  /// ApiVersions: the supported ranges and the broker's version string.
  Future<(List<ApiVersionRange>, String)> apiVersions() async {
    final w = bodyWriter()
      ..string('brahmaputra-dart')
      ..string('0.1.0');
    final r = bodyReader(await request(ApiKey.apiVersions, w.bytes()));
    final code = r.int32();
    if (code != ErrorCode.none) throw ServerException(code, 'api_versions');
    final n = r.count();
    final versions = [
      for (var i = 0; i < n; i++) ApiVersionRange(r.int32(), r.int32(), r.int32())
    ];
    return (versions, r.string());
  }

  /// Bind a principal to this connection using SCRAM-SHA-256.
  Future<(String principal, String role)> authenticate(
      String username, String password) async {
    final nonce = base64
        .encode(List<int>.generate(18, (_) => Random.secure().nextInt(256)))
        .replaceAll(',', '.');
    final bare = 'n=$username,r=$nonce';
    final first = await _authStep(username, '', 'SCRAM-SHA-256', 'n,,$bare');
    if (first.$4) {
      throw ProtocolException('broker ended the SCRAM exchange before it began');
    }
    final serverFirst = first.$3;
    final serverNonce = scramField(serverFirst, 'r');
    final salt = scramField(serverFirst, 's');
    final iterations = int.tryParse(scramField(serverFirst, 'i') ?? '') ?? 0;
    if (serverNonce == null || salt == null || iterations <= 0) {
      throw ProtocolException('malformed SCRAM server-first message');
    }
    if (!serverNonce.startsWith(nonce)) {
      throw ProtocolException(
          'SCRAM server nonce does not extend the client nonce');
    }
    final withoutProof = 'c=biws,r=$serverNonce';
    final authMessage = '$bare,$serverFirst,$withoutProof';
    final proof = scramClientProof(password, salt, iterations, authMessage);
    final last = await _authStep(
        username, '', 'SCRAM-SHA-256', '$withoutProof,p=$proof');
    return (last.$1, last.$2);
  }

  /// SASL/PLAIN-style: sends the password. Refused on a plaintext listener.
  Future<(String principal, String role)> authenticatePlain(
      String username, String password) async {
    final result = await _authStep(username, password, 'PLAIN', '');
    return (result.$1, result.$2);
  }

  Future<(String, String, String, bool)> _authStep(String username,
      String password, String mechanism, String payload) async {
    final w = bodyWriter()
      ..string(username)
      ..string(password)
      ..string(mechanism)
      ..string(payload);
    final r = bodyReader(await request(ApiKey.authenticate, w.bytes()));
    final code = r.int32();
    final principal = r.string();
    final role = r.string();
    final responsePayload = r.string();
    final done = r.boolean();
    if (code != ErrorCode.none) throw ServerException(code, 'authenticate');
    return (principal, role, responsePayload, done);
  }
}

// ---------------------------------------------------------------------------
// Metadata and routing
// ---------------------------------------------------------------------------

class BrokerInfo {
  const BrokerInfo(this.nodeId, this.host, this.port, this.rack);
  final int nodeId;
  final String host;
  final int port;
  final String rack;
}

class PartitionInfo {
  const PartitionInfo(
      this.partition, this.leader, this.replicas, this.isr, this.leaderEpoch);
  final int partition;
  final int leader;
  final List<int> replicas;
  final List<int> isr;
  final int leaderEpoch;
}

class TopicInfo {
  const TopicInfo(this.name, this.errorCode, this.partitions);
  final String name;
  final int errorCode;
  final List<PartitionInfo> partitions;
}

class ClusterMetadata {
  ClusterMetadata(this.brokers, this.controllerId, this.topics);
  final List<BrokerInfo> brokers;
  final int controllerId;
  final List<TopicInfo> topics;

  TopicInfo? topic(String name) {
    for (final t in topics) {
      if (t.name == name) return t;
    }
    return null;
  }
}

ClusterMetadata decodeMetadata(Reader r) {
  final requestError = r.int32();
  if (requestError != ErrorCode.none) {
    throw ServerException(requestError, 'metadata');
  }
  final brokerCount = r.count();
  final brokers = [
    for (var i = 0; i < brokerCount; i++)
      BrokerInfo(r.int32(), r.string(), r.int32(), r.string())
  ];
  final controllerId = r.int32();
  final topics = <TopicInfo>[];
  for (var t = r.count(); t > 0; t--) {
    final name = r.string();
    final topicError = r.int32();
    final partitions = <PartitionInfo>[];
    for (var p = r.count(); p > 0; p--) {
      final partition = r.int32();
      final leader = r.int32();
      final rc = r.count();
      final replicas = [for (var i = 0; i < rc; i++) r.int32()];
      final ic = r.count();
      final isr = [for (var i = 0; i < ic; i++) r.int32()];
      partitions.add(
          PartitionInfo(partition, leader, replicas, isr, r.int32()));
    }
    if (topicError != ErrorCode.none &&
        topicError != ErrorCode.unknownTopicOrPartition) {
      throw ServerException(topicError, 'metadata for $name');
    }
    topics.add(TopicInfo(name, topicError, partitions));
  }
  return ClusterMetadata(brokers, controllerId, topics);
}

/// Keeps connections to every broker and routes by partition leader.
///
/// Metadata is cached per topic and refreshed only when a request says the
/// route was stale. A connection that broke is redialled on its next use,
/// the seed included.
class Router {
  Router._(this.host, this.port, this.clientId, this.connectTimeout,
      this.requestTimeout, this._seed);

  static Future<Router> connect(String host, int port,
      {String clientId = 'brahmaputra-dart',
      Duration connectTimeout = const Duration(seconds: 30),
      Duration requestTimeout = defaultRequestTimeout}) async {
    final seed = await Connection.connect(host, port,
        clientId: clientId,
        connectTimeout: connectTimeout,
        requestTimeout: requestTimeout);
    return Router._(host, port, clientId, connectTimeout, requestTimeout, seed);
  }

  final String host;
  final int port;
  final String clientId;
  final Duration connectTimeout;
  final Duration requestTimeout;
  Connection _seed;
  final Map<int, Connection> _connections = {};
  final Map<String, Future<Connection>> _dialing = {};
  List<BrokerInfo> _brokers = const [];
  final Map<String, TopicInfo> _topics = {};
  bool _closed = false;

  /// The seed connection as it is now (may be broken; see [liveSeed]).
  Connection get seed => _seed;

  void close() {
    _closed = true;
    for (final c in _connections.values) {
      c.close();
    }
    _connections.clear();
    _seed.close();
  }

  Future<Connection> _dial(String key, String host, int port) {
    if (_closed) return Future.error(BrahmaputraException('router is closed'));
    return _dialing.putIfAbsent(key, () {
      final future = Connection.connect(host, port,
          clientId: clientId,
          connectTimeout: connectTimeout,
          requestTimeout: requestTimeout);
      future.then((_) {}, onError: (_) {}).whenComplete(() => _dialing.remove(key));
      return future;
    });
  }

  /// The seed connection, redialled if it has failed.
  Future<Connection> liveSeed() async {
    if (!_seed.broken) return _seed;
    final fresh = await _dial('seed', host, port);
    if (_seed.broken) _seed = fresh;
    return _seed;
  }

  /// Fetch metadata for [topics] (empty: all topics). With [refresh] false,
  /// answers from the cache when every requested topic is in it.
  Future<ClusterMetadata> metadata(
      {List<String> topics = const [], bool refresh = false}) async {
    if (!refresh &&
        _brokers.isNotEmpty &&
        topics.isNotEmpty &&
        topics.every(_topics.containsKey)) {
      return ClusterMetadata(
          _brokers, -1, [for (final t in topics) _topics[t]!]);
    }
    final w = bodyWriter()..stringArray(topics);
    final seed = await liveSeed();
    final result =
        decodeMetadata(bodyReader(await seed.request(ApiKey.metadata, w.bytes())));
    _brokers = result.brokers;
    for (final t in result.topics) {
      if (t.partitions.isEmpty) {
        _topics.remove(t.name);
      } else {
        _topics[t.name] = t;
      }
    }
    return result;
  }

  Future<ClusterMetadata> refresh(String topic) =>
      metadata(topics: [topic], refresh: true);

  /// Sorted partition ids of [topic]. Auto-creates the topic on a broker
  /// that does so.
  Future<List<int>> partitions(String topic) async {
    var found = (await metadata(topics: [topic])).topic(topic);
    if (found == null || found.partitions.isEmpty) {
      found = (await refresh(topic)).topic(topic);
    }
    if (found == null || found.partitions.isEmpty) {
      throw BrahmaputraException('topic $topic has no partitions');
    }
    return found.partitions.map((p) => p.partition).toList()..sort();
  }

  int _leaderOf(String topic, int partition) {
    final t = _topics[topic];
    if (t == null) return -1;
    for (final p in t.partitions) {
      if (p.partition == partition) return p.leader;
    }
    return -1;
  }

  /// A live connection to the leader of [topic]-[partition].
  Future<Connection> connectionFor(String topic, int partition) async {
    await metadata(topics: [topic]);
    var leader = _leaderOf(topic, partition);
    if (leader < 0) {
      await refresh(topic);
      leader = _leaderOf(topic, partition);
    }
    if (leader < 0) {
      throw BrahmaputraException('no leader for $topic-$partition');
    }
    // A single-broker cluster advertises the address it was configured
    // with, which may not be the one we dialled (a proxy, a NAT); reuse the
    // seed rather than dialling that.
    if (_brokers.length == 1) return liveSeed();

    final existing = _connections[leader];
    if (existing != null && !existing.broken) return existing;
    _connections.remove(leader);
    BrokerInfo? broker;
    for (final b in _brokers) {
      if (b.nodeId == leader) broker = b;
    }
    if (broker == null) {
      throw BrahmaputraException('broker $leader is not in the metadata');
    }
    final connection = await _dial('broker $leader', broker.host, broker.port);
    final current = _connections[leader];
    if (current != null && !current.broken) {
      if (!identical(current, connection)) connection.close();
      return current;
    }
    _connections[leader] = connection;
    return connection;
  }
}
