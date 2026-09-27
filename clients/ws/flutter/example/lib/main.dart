// A stock ticker: live prices from Brahmaputra through the gateway, and a
// Buy button that publishes an order. Also what the widget tests pump.
//
//   flutter run --dart-define=GW=wss://gw.example.com/ws --dart-define=TOKEN=...
import 'package:brahmaputra_ws_flutter/brahmaputra_ws_flutter.dart';
import 'package:flutter/material.dart';

void main() {
  const gw = String.fromEnvironment('GW', defaultValue: 'ws://127.0.0.1:8090/ws');
  const token = String.fromEnvironment('TOKEN');
  runApp(TickerApp(
    create: () => BrahmaputraClient(url: Uri.parse(gw), token: () => token),
    pricesTopic: 'prices.us',
    ordersTopic: 'orders.us',
  ));
}

class TickerApp extends StatelessWidget {
  const TickerApp({
    super.key,
    required this.create,
    required this.pricesTopic,
    required this.ordersTopic,
  });

  final BrahmaputraClient Function() create;
  final String pricesTopic;
  final String ordersTopic;

  @override
  Widget build(BuildContext context) => BrahmaputraScope(
        create: create,
        child: MaterialApp(
          home: Scaffold(
            appBar: AppBar(
              title: const Text('Prices'),
              actions: [
                ConnectionStatusBuilder(
                  builder: (context, state) => Padding(
                    padding: const EdgeInsets.all(16),
                    child: Text(state.name, key: const Key('state')),
                  ),
                ),
              ],
            ),
            body: PriceBoard(pricesTopic: pricesTopic, ordersTopic: ordersTopic),
          ),
        ),
      );
}

class PriceBoard extends StatefulWidget {
  const PriceBoard({super.key, required this.pricesTopic, required this.ordersTopic});
  final String pricesTopic;
  final String ordersTopic;

  @override
  State<PriceBoard> createState() => _PriceBoardState();
}

class _PriceBoardState extends State<PriceBoard> {
  String _lastOrder = '';

  Future<void> _buy(String symbol) async {
    final client = BrahmaputraScope.of(context);
    try {
      final ack = await client.publishJson({'symbol': symbol, 'qty': 1},
          topic: widget.ordersTopic);
      setState(() => _lastOrder = '${ack.partition}:${ack.offset}');
    } on BrahmaputraException catch (e) {
      setState(() => _lastOrder = e.code);
    }
  }

  @override
  Widget build(BuildContext context) => LatestByKeyBuilder(
        topic: widget.pricesTopic,
        builder: (context, prices, error) {
          final symbols = prices.keys.toList()..sort();
          return Column(children: [
            if (error != null) Text(error.code, key: const Key('feed-error')),
            Text(_lastOrder, key: const Key('last-order')),
            Expanded(
              child: ListView(children: [
                for (final s in symbols)
                  ListTile(
                    key: Key('row-$s'),
                    title: Text(s),
                    subtitle: Text(
                      (prices[s]!.json()['price'] as num).toStringAsFixed(2),
                      key: Key('price-$s'),
                    ),
                    trailing: TextButton(
                      key: Key('buy-$s'),
                      onPressed: () => _buy(s),
                      child: const Text('Buy'),
                    ),
                  ),
              ]),
            ),
          ]);
        },
      );
}
