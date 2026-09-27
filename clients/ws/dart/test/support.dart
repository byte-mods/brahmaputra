// Fixtures: real gateway processes, HS256 tokens, and a back end writing
// prices straight into Brahmaputra with the Dart broker driver.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:brahmaputra/brahmaputra.dart' as brp;
import 'package:crypto/crypto.dart';

const secret = 'ws-sdk-e2e-secret-0123456789';
final brokerHost = Platform.environment['BRP_HOST'] ?? '127.0.0.1';
final brokerPort = int.parse(Platform.environment['BRP_PORT'] ?? '9092');
final gatewayBin = Platform.environment['GW_BIN'] ??
    '${Directory.current.path}/../../../target/release/brahmaputra-ws-gateway';

String _b64(List<int> bytes) => base64Url.encode(bytes).replaceAll('=', '');

String mintToken(String sub,
    {List<String>? topics,
    List<String>? subscribe,
    int ttlSecs = 600,
    String key = secret}) {
  final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
  final claims = <String, Object>{'sub': sub, 'iat': now, 'exp': now + ttlSecs};
  if (topics != null) claims['topics'] = topics;
  if (subscribe != null) claims['subscribe'] = subscribe;
  final head = _b64(utf8.encode(jsonEncode({'alg': 'HS256', 'typ': 'JWT'})));
  final body = _b64(utf8.encode(jsonEncode(claims)));
  final sig =
      Hmac(sha256, utf8.encode(key)).convert(utf8.encode('$head.$body'));
  return '$head.$body.${_b64(sig.bytes)}';
}

Future<int> _freePort() async {
  final s = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final port = s.port;
  await s.close();
  return port;
}

class Gateway {
  Gateway._(this.process, this.port, this.httpPort);
  final Process process;
  final int port;
  final int httpPort;

  Uri get url => Uri.parse('ws://127.0.0.1:$port/ws');

  static Future<Gateway> start({int? port, int? httpPort}) async {
    port ??= await _freePort();
    httpPort ??= await _freePort();
    final process = await Process.start(gatewayBin, [
      '--listen',
      '127.0.0.1:$port',
      '--http-listen',
      '127.0.0.1:$httpPort',
      '--broker',
      '$brokerHost:$brokerPort',
      '--jwt-secret',
      secret,
      '--allow-topic',
      'orders.*,chat.*',
      '--allow-subscribe',
      'prices.*,orders.*,chat.*',
      '--linger-ms',
      '2',
      '--compression',
      'gzip',
    ], environment: {
      'RUST_LOG': 'warn'
    });
    process.stdout.drain<void>();
    process.stderr.drain<void>();
    final client = HttpClient();
    final deadline = DateTime.now().add(const Duration(seconds: 15));
    while (true) {
      try {
        final req = await client.get('127.0.0.1', httpPort, '/readyz');
        final res = await req.close();
        await res.drain<void>();
        if (res.statusCode == 200) break;
      } catch (_) {}
      if (DateTime.now().isAfter(deadline)) {
        process.kill(ProcessSignal.sigkill);
        throw StateError('gateway not ready');
      }
      await Future<void>.delayed(const Duration(milliseconds: 100));
    }
    client.close();
    return Gateway._(process, port, httpPort);
  }

  Future<void> stop() async {
    process.kill(ProcessSignal.sigterm);
    await process.exitCode;
  }

  Future<Gateway> restart() async {
    await stop();
    return start(port: port, httpPort: httpPort);
  }
}

class Prices {
  Prices._(this._producer);
  final brp.Producer _producer;

  static Future<Prices> connect() async =>
      Prices._(await brp.Producer.connect(brokerHost, brokerPort));

  Future<int> tick(String topic, String symbol, num price) =>
      _producer.sendSync(
          topic, utf8.encode(jsonEncode({'symbol': symbol, 'price': price})),
          key: utf8.encode(symbol));

  Future<int> tombstone(String topic, String symbol) =>
      _producer.sendSync(topic, null, key: utf8.encode(symbol));

  Future<void> close() => _producer.close();
}

/// Every record of [topic], read straight from the broker.
Future<List<brp.ConsumedRecord>> readTopic(String topic,
    {int partitions = 4}) async {
  final consumer = await brp.Consumer.connect(brokerHost, brokerPort);
  final out = <brp.ConsumedRecord>[];
  try {
    for (var p = 0; p < partitions; p++) {
      var offset = 0;
      while (true) {
        final batch = await consumer.fetch(topic, p, offset, 50);
        if (batch.isEmpty) break;
        out.addAll(batch);
        offset = batch.last.offset + 1;
      }
    }
  } finally {
    consumer.close();
  }
  return out;
}

final _rand = Random();
String uniqueTopic(String prefix) =>
    '$prefix.${DateTime.now().microsecondsSinceEpoch.toRadixString(36)}${_rand.nextInt(1 << 20)}';

Future<T> eventually<T>(FutureOr<T?> Function() check,
    {Duration timeout = const Duration(seconds: 10),
    String what = 'condition'}) async {
  final deadline = DateTime.now().add(timeout);
  while (true) {
    final v = await check();
    if (v != null && v != false) return v;
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('timed out waiting for $what');
    }
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
}
