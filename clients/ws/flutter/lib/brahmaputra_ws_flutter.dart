/// Flutter widgets for the Brahmaputra WebSocket gateway.
///
/// ```dart
/// BrahmaputraScope(
///   create: () => BrahmaputraClient(url: gatewayUrl, token: auth.freshToken),
///   child: MaterialApp(home: PriceBoard()),
/// );
///
/// class PriceBoard extends StatelessWidget {
///   Widget build(BuildContext context) => LatestByKeyBuilder(
///         topic: 'prices.us',
///         builder: (context, prices, error) => ListView(children: [
///           for (final p in prices.values) Text('${p.key} ${p.json()['price']}'),
///         ]),
///       );
/// }
/// ```
library;

import 'dart:async';

import 'package:brahmaputra_ws/brahmaputra_ws.dart';
import 'package:flutter/widgets.dart';

export 'package:brahmaputra_ws/brahmaputra_ws.dart';

/// Provides a [BrahmaputraClient] to the subtree and keeps it connected.
///
/// With [create], the scope owns the client: it connects it when inserted
/// and closes it when removed. With [client], you own it.
class BrahmaputraScope extends StatefulWidget {
  const BrahmaputraScope({super.key, required this.create, required this.child})
      : client = null;

  const BrahmaputraScope.value(
      {super.key, required BrahmaputraClient this.client, required this.child})
      : create = null;

  final BrahmaputraClient Function()? create;
  final BrahmaputraClient? client;
  final Widget child;

  /// The nearest scope's client.
  static BrahmaputraClient of(BuildContext context) {
    final scope = context.dependOnInheritedWidgetOfExactType<_Inherited>();
    assert(scope != null, 'no BrahmaputraScope above this widget');
    return scope!.client;
  }

  @override
  State<BrahmaputraScope> createState() => _BrahmaputraScopeState();
}

class _BrahmaputraScopeState extends State<BrahmaputraScope> {
  late final BrahmaputraClient _client = widget.client ?? widget.create!();

  @override
  void initState() {
    super.initState();
    // Failures show through ConnectionStatusBuilder; the client retries.
    _client.connect().catchError((Object _) {});
  }

  @override
  void dispose() {
    if (widget.client == null) _client.close();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => _Inherited(_client, widget.child);
}

class _Inherited extends InheritedWidget {
  const _Inherited(this.client, Widget child) : super(child: child);
  final BrahmaputraClient client;

  @override
  bool updateShouldNotify(_Inherited old) => old.client != client;
}

/// Rebuilds on every connection status change.
class ConnectionStatusBuilder extends StatelessWidget {
  const ConnectionStatusBuilder({super.key, required this.builder});
  final Widget Function(BuildContext context, ConnectionStatus status) builder;

  @override
  Widget build(BuildContext context) {
    final client = BrahmaputraScope.of(context);
    return StreamBuilder<ConnectionStatus>(
      stream: client.states,
      initialData: client.state,
      builder: (context, snap) => builder(context, snap.data ?? client.state),
    );
  }
}

typedef FeedWidgetBuilder<T> = Widget Function(
    BuildContext context, T data, BrahmaputraException? error);

abstract class _FeedBuilder<T> extends StatefulWidget {
  const _FeedBuilder({super.key, required this.topic, required this.builder});
  final String topic;
  final FeedWidgetBuilder<T> builder;

  FeedView<T> open(BrahmaputraClient client);

  /// Whether a new widget configuration needs a new subscription.
  bool differs(covariant _FeedBuilder<T> old);

  @override
  State<_FeedBuilder<T>> createState() => _FeedBuilderState<T>();
}

class _FeedBuilderState<T> extends State<_FeedBuilder<T>> {
  FeedView<T>? _view;
  StreamSubscription<T>? _changes;
  BrahmaputraClient? _client;

  void _subscribe() {
    _changes?.cancel();
    _view?.close();
    final view = widget.open(_client!);
    _view = view;
    _changes = view.changes.listen((_) {
      if (mounted) setState(() {});
    });
  }

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    final client = BrahmaputraScope.of(context);
    if (!identical(client, _client)) {
      _client = client;
      _subscribe();
    }
  }

  @override
  void didUpdateWidget(_FeedBuilder<T> old) {
    super.didUpdateWidget(old);
    if (widget.differs(old)) _subscribe();
  }

  @override
  void dispose() {
    _changes?.cancel();
    _view?.close();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final view = _view!;
    return widget.builder(context, view.value, view.error);
  }
}

bool _sameKeys(List<String>? a, List<String>? b) =>
    a == null ? b == null : b != null && a.join('\u0000') == b.join('\u0000');

/// The latest record per key of [topic] (a price board), starting from the
/// gateway's snapshot unless [snapshot] is false.
class LatestByKeyBuilder extends _FeedBuilder<Map<String, FeedRecord>> {
  const LatestByKeyBuilder({
    super.key,
    required super.topic,
    required super.builder,
    this.keys,
    this.snapshot = true,
    this.throttle,
  });

  final List<String>? keys;
  final bool snapshot;
  final Duration? throttle;

  @override
  FeedView<Map<String, FeedRecord>> open(BrahmaputraClient client) =>
      LatestByKey(client, topic, keys: keys, snapshot: snapshot, throttle: throttle);

  @override
  bool differs(LatestByKeyBuilder old) =>
      old.topic != topic || !_sameKeys(old.keys, keys) || old.snapshot != snapshot;
}

/// The last [limit] records of [topic], oldest first (a tape, a chat).
class RecentRecordsBuilder extends _FeedBuilder<List<FeedRecord>> {
  const RecentRecordsBuilder({
    super.key,
    required super.topic,
    required super.builder,
    this.limit = 100,
    this.keys,
    this.snapshot = false,
    this.throttle,
  });

  final int limit;
  final List<String>? keys;
  final bool snapshot;
  final Duration? throttle;

  @override
  FeedView<List<FeedRecord>> open(BrahmaputraClient client) => RecentRecords(
      client, topic,
      limit: limit, keys: keys, snapshot: snapshot, throttle: throttle);

  @override
  bool differs(RecentRecordsBuilder old) =>
      old.topic != topic ||
      old.limit != limit ||
      !_sameKeys(old.keys, keys) ||
      old.snapshot != snapshot;
}
