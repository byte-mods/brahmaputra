// The Flutter ticker app, pumped for real: widgets -> brahmaputra_ws ->
// brahmaputra-ws-gateway (a real process) -> Brahmaputra, and back.
import 'dart:convert';
import 'dart:io';

import 'package:brahmaputra_ws_flutter/brahmaputra_ws_flutter.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

import '../../dart/test/support.dart';
// The example app is what these tests drive.
// ignore: avoid_relative_lib_imports
import '../example/lib/main.dart';

late Gateway gw;
late Prices prices;

/// Let real I/O happen, pumping frames, until [done] holds.
/// Unmount the app and let the socket close: dart:io's WebSocket.close
/// arms a timer for the closing handshake, which must elapse on the
/// test's fake clock before the test may end.
Future<void> unmount(WidgetTester tester) async {
  await tester.pumpWidget(const SizedBox());
  await tester.runAsync(() => Future<void>.delayed(const Duration(milliseconds: 100)));
  await tester.pump(const Duration(seconds: 10));
}

Future<void> pumpUntil(WidgetTester tester, bool Function() done,
    {Duration timeout = const Duration(seconds: 15), String what = 'condition'}) async {
  final deadline = DateTime.now().add(timeout);
  while (!done()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('timed out waiting for $what');
    }
    await tester.runAsync(() => Future<void>.delayed(const Duration(milliseconds: 30)));
    await tester.pump(const Duration(milliseconds: 30));
  }
}

String? textOf(Key key) {
  final f = find.byKey(key);
  if (f.evaluate().isEmpty) return null;
  return (f.evaluate().single.widget as Text).data;
}

BrahmaputraClient Function() clientFor(String token) => () => BrahmaputraClient(
      url: gw.url,
      token: () => token,
      minReconnectDelay: const Duration(milliseconds: 50),
      maxReconnectDelay: const Duration(milliseconds: 500),
    );

void main() {
  setUpAll(() async {
    // flutter_test answers every HTTP request with 400 by default; these
    // tests talk to a real gateway.
    HttpOverrides.global = null;
    gw = await Gateway.start();
    prices = await Prices.connect();
  });

  tearDownAll(() async {
    await prices.close();
    await gw.stop();
  });

  testWidgets('prices from Brahmaputra render live; Buy places an order in Brahmaputra',
      (tester) async {
    final pricesTopic = uniqueTopic('prices');
    final ordersTopic = uniqueTopic('orders');
    await tester.runAsync(() async {
      await prices.tick(pricesTopic, 'AAPL', 100);
      await prices.tick(pricesTopic, 'MSFT', 200);
      await prices.tick(pricesTopic, 'AAPL', 100.5);
    });
    final token = mintToken('flutter-trader', topics: ['orders.*'], subscribe: ['prices.*']);
    await tester.pumpWidget(TickerApp(
        create: clientFor(token), pricesTopic: pricesTopic, ordersTopic: ordersTopic));

    await pumpUntil(tester, () => textOf(const Key('state')) == 'open', what: 'open');
    await pumpUntil(tester, () => textOf(const Key('price-AAPL')) == '100.50',
        what: 'snapshot price');
    expect(textOf(const Key('price-MSFT')), '200.00');

    await tester.runAsync(() => prices.tick(pricesTopic, 'AAPL', 101.25));
    await pumpUntil(tester, () => textOf(const Key('price-AAPL')) == '101.25',
        what: 'live tick');
    await tester.runAsync(() => prices.tick(pricesTopic, 'NVDA', 950));
    await pumpUntil(tester, () => textOf(const Key('price-NVDA')) == '950.00',
        what: 'new symbol');

    await tester.tap(find.byKey(const Key('buy-MSFT')));
    await pumpUntil(tester, () => RegExp(r'^\d+:\d+$').hasMatch(textOf(const Key('last-order')) ?? ''),
        what: 'order ack');
    final orders = (await tester.runAsync(() => readTopic(ordersTopic)))!;
    expect(orders, hasLength(1));
    expect(jsonDecode(utf8.decode(orders.single.value!)), {'symbol': 'MSFT', 'qty': 1});
    final user = orders.single.headers.firstWhere((h) => h.key == 'x-gw-user');
    expect(utf8.decode(user.value!), 'flutter-trader');

    // Unmounting closes the client the scope created.
    await unmount(tester);
  });

  testWidgets('a token without the price feed sees the refusal; forged tokens never connect',
      (tester) async {
    final token = mintToken('flutter-limited', topics: [], subscribe: ['orders.*']);
    await tester.pumpWidget(TickerApp(
        create: clientFor(token), pricesTopic: uniqueTopic('prices'), ordersTopic: 'orders.x'));
    await pumpUntil(tester, () => textOf(const Key('feed-error')) == 'TOPIC_NOT_ALLOWED',
        what: 'refusal');
    await unmount(tester);

    final forged = mintToken('mallory', key: 'an-entirely-different-secret-value');
    await tester.pumpWidget(TickerApp(
        create: clientFor(forged), pricesTopic: uniqueTopic('prices'), ordersTopic: 'orders.x'));
    await pumpUntil(tester, () => textOf(const Key('state')) == 'reconnecting',
        what: 'refused connection');
    for (var i = 0; i < 20; i++) {
      await tester.runAsync(() => Future<void>.delayed(const Duration(milliseconds: 30)));
      await tester.pump(const Duration(milliseconds: 30));
      expect(textOf(const Key('state')), isNot('open'));
    }
    await unmount(tester);
  });

  testWidgets('a gateway restart under the running app: it reconnects and catches up',
      (tester) async {
    final pricesTopic = uniqueTopic('prices');
    await tester.runAsync(() => prices.tick(pricesTopic, 'AAPL', 10));
    final token = mintToken('flutter-resilient', topics: ['orders.*'], subscribe: ['prices.*']);
    await tester.pumpWidget(TickerApp(
        create: clientFor(token), pricesTopic: pricesTopic, ordersTopic: 'orders.y'));
    await pumpUntil(tester, () => textOf(const Key('price-AAPL')) == '10.00', what: 'first');

    await tester.runAsync(() => gw.stop());
    await pumpUntil(tester, () => textOf(const Key('state')) == 'reconnecting',
        what: 'reconnecting');
    await tester.runAsync(() async {
      await prices.tick(pricesTopic, 'AAPL', 11);
      gw = await Gateway.start(port: gw.port, httpPort: gw.httpPort);
    });
    await pumpUntil(tester, () => textOf(const Key('state')) == 'open', what: 'reopened');
    await pumpUntil(tester, () => textOf(const Key('price-AAPL')) == '11.00',
        what: 'caught up via snapshot');
    await tester.runAsync(() => prices.tick(pricesTopic, 'AAPL', 12));
    await pumpUntil(tester, () => textOf(const Key('price-AAPL')) == '12.00', what: 'live again');
    await unmount(tester);
  });
}
