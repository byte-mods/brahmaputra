/// Client for the Brahmaputra WebSocket gateway, for Flutter and Dart.
///
/// ```dart
/// final client = BrahmaputraClient(
///   url: Uri.parse('wss://gw.example.com/ws'),
///   token: () => auth.freshToken(),
/// );
/// await client.connect();
/// final prices = LatestByKey(client, 'prices.us', keys: ['AAPL', 'MSFT']);
/// prices.changes.listen((board) => print(board['AAPL']?.json()));
/// final ack = await client.publishJson({'symbol': 'AAPL', 'qty': 1},
///     topic: 'orders.eu');
/// ```
library;

export 'src/client.dart'
    show
        Ack,
        BrahmaputraClient,
        BrahmaputraException,
        ConnectionStatus,
        FeedRecord,
        Subscription,
        WebSocketConnector,
        Welcome;
export 'src/stores.dart' show FeedView, LatestByKey, RecentRecords;
