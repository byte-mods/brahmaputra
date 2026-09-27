// Flutter/Dart client <-> brahmaputra-ws-gateway <-> Brahmaputra, for real.
import 'dart:convert';
import 'dart:typed_data';

import 'package:brahmaputra_ws/brahmaputra_ws.dart';
import 'package:test/test.dart';

import 'support.dart';

void main() {
  late Gateway gw;
  late Prices prices;
  final clients = <BrahmaputraClient>[];

  BrahmaputraClient client(
      {String? token, String? topic, bool reconnect = true}) {
    final t = token ?? mintToken('viewer', topics: [], subscribe: ['prices.*']);
    final c = BrahmaputraClient(
      url: gw.url,
      token: () => t,
      topic: topic,
      reconnect: reconnect,
      minReconnectDelay: const Duration(milliseconds: 50),
      maxReconnectDelay: const Duration(milliseconds: 500),
    );
    clients.add(c);
    return c;
  }

  setUpAll(() async {
    gw = await Gateway.start();
    prices = await Prices.connect();
  });

  tearDownAll(() async {
    for (final c in clients) {
      await c.close();
    }
    await prices.close();
    await gw.stop();
  });

  test('publishes are acknowledged, land in Brahmaputra and reach subscribers',
      () async {
    final topic = uniqueTopic('chat');
    final alice = client(
        token: mintToken('alice', topics: ['chat.*'], subscribe: ['chat.*']),
        topic: topic);
    final bob =
        client(token: mintToken('bob', topics: [], subscribe: ['chat.*']));
    await Future.wait([alice.connect(), bob.connect()]);
    expect(alice.welcome!.user, 'alice');
    expect(alice.welcome!.subscribe, isTrue);

    final got = <FeedRecord>[];
    final sub = bob.subscribe(topic, onRecord: got.add);
    await sub.ready;
    final a1 = await alice.publish(key: 'room', value: 'hello');
    final a2 =
        await alice.publish(key: 'room', value: Uint8List.fromList([0, 255]));
    final a3 = await alice.publishJson({'text': 'hi'},
        key: 'room', headers: {'lang': 'en'});
    await eventually(() => got.length == 3, what: 'three records');
    expect(got[0].value, 'hello');
    expect(got[0].offset, a1.offset);
    expect(got[0].partition, a1.partition);
    expect(got[0].headers['x-gw-user'], 'alice');
    expect(got[1].valueBytes, [0, 255]);
    expect(got[1].offset, a2.offset);
    expect(got[2].json(), {'text': 'hi'});
    expect(got[2].headers['lang'], 'en');
    expect(got[2].offset, a3.offset);

    final stored = await readTopic(topic);
    expect(
        stored.map((r) => r.value == null
            ? null
            : utf8.decode(r.value!, allowMalformed: true)),
        contains('hello'));
    sub.cancel();
  });

  test('a price board: snapshot, live ticks, key filters, deletes', () async {
    final topic = uniqueTopic('prices');
    await prices.tick(topic, 'AAPL', 100);
    await prices.tick(topic, 'MSFT', 200);
    await prices.tick(topic, 'AAPL', 101);

    final ui = client();
    await ui.connect();
    final board = LatestByKey(ui, topic);
    final msft = LatestByKey(ui, topic, keys: ['MSFT']);
    final tape = RecentRecords(ui, topic, limit: 2);
    await Future.wait([board.ready, msft.ready, tape.ready]);
    await eventually(() => board.value.length == 2, what: 'snapshot');
    expect(board.value['AAPL']!.json()['price'], 101);
    expect(board.value['AAPL']!.snapshot, isTrue);
    await eventually(() => msft.value.length == 1);
    expect(msft.value.keys, ['MSFT']);
    expect(tape.value, isEmpty);

    final emissions = <Map<String, FeedRecord>>[];
    board.changes.listen(emissions.add);
    await prices.tick(topic, 'GOOG', 300);
    await prices.tick(topic, 'MSFT', 201);
    await eventually(() => board.value['MSFT']?.json()['price'] == 201,
        what: 'live tick');
    expect(board.value['GOOG']!.snapshot, isFalse);
    expect(msft.value['MSFT']!.json()['price'], 201);
    expect(msft.value.length, 1);
    await eventually(() => tape.value.length == 2);
    expect(tape.value.map((r) => r.key), ['GOOG', 'MSFT']);

    await prices.tombstone(topic, 'GOOG');
    await eventually(() => !board.value.containsKey('GOOG'), what: 'delete');
    expect(emissions, isNotEmpty);
    expect(() => board.value.remove('AAPL'), throwsUnsupportedError,
        reason: 'values are immutable snapshots');
    for (final FeedView<Object> v in [board, msft, tape]) {
      v.close();
    }
  });

  test('records() is a stream; cancelling it unsubscribes', () async {
    final topic = uniqueTopic('prices');
    final ui = client();
    await ui.connect();
    final stream = ui.records(topic).take(2);
    final collected = stream.toList();
    await Future<void>.delayed(const Duration(milliseconds: 300));
    await prices.tick(topic, 'A', 1);
    await prices.tick(topic, 'B', 2);
    final list = await collected.timeout(const Duration(seconds: 10));
    expect(list.map((r) => r.key), ['A', 'B']);
  });

  test('authentication and authorization', () async {
    final forged = client(
        token: mintToken('mallory', key: 'an-entirely-different-secret-value'),
        reconnect: false);
    await expectLater(forged.connect(), throwsA(isA<Object>()));
    expect(forged.state, ConnectionStatus.closed);

    final viewer = client();
    await viewer.connect();
    await expectLater(
        viewer.publish(topic: uniqueTopic('orders'), value: 'buy'),
        throwsA(isA<BrahmaputraException>()
            .having((e) => e.code, 'code', 'TOPIC_NOT_ALLOWED')
            .having((e) => e.retryable, 'retryable', false)));
    final denied = LatestByKey(viewer, uniqueTopic('orders'));
    await expectLater(denied.ready, throwsA(isA<BrahmaputraException>()));
    expect(denied.error!.code, 'TOPIC_NOT_ALLOWED');
  });

  test(
      'a gateway restart: subscriptions resume and unacked publishes are resent',
      () async {
    final topic = uniqueTopic('orders');
    var calls = 0;
    final trader = BrahmaputraClient(
      url: gw.url,
      token: () {
        calls++;
        return mintToken('trader',
            topics: ['orders.*'], subscribe: ['orders.*']);
      },
      topic: topic,
      minReconnectDelay: const Duration(milliseconds: 50),
      maxReconnectDelay: const Duration(milliseconds: 500),
    );
    clients.add(trader);
    final states = <ConnectionStatus>[];
    trader.states.listen(states.add);
    await trader.connect();
    final got = <String?>[];
    await trader.subscribe(topic, onRecord: (r) => got.add(r.value)).ready;
    await trader.publish(key: 'o', value: 'before');
    await eventually(() => got.contains('before'));

    await gw.stop();
    final during = trader.publish(key: 'o', value: 'during');
    await eventually(() => trader.state == ConnectionStatus.reconnecting);
    gw = await Gateway.start(port: gw.port, httpPort: gw.httpPort);
    final ack = await during.timeout(const Duration(seconds: 15));
    expect(ack.offset, greaterThanOrEqualTo(1));
    await eventually(() => got.contains('during'), what: 'resubscribed');
    expect(
        states,
        containsAllInOrder(
            [ConnectionStatus.reconnecting, ConnectionStatus.open]));
    expect(calls, greaterThanOrEqualTo(2),
        reason: 'a fresh token per connection');
  });
}
