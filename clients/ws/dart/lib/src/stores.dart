import 'dart:async';

import 'client.dart';

/// A live view over a subscription: [value] is always the current state
/// (an unmodifiable snapshot, replaced on every change) and [changes]
/// emits each new one. Changes arriving together (a burst of ticks, a
/// snapshot) are coalesced into one emission per microtask, or per
/// [throttle] for very busy feeds.
abstract class FeedView<T> {
  FeedView(this._value, this.throttle);

  T _value;
  final Duration? throttle;
  final _changes = StreamController<T>.broadcast(sync: true);
  bool _scheduled = false;
  late final Subscription _subscription;

  /// Records skipped because this client fell behind the feed.
  int lagged = 0;

  /// Set if the gateway refused the subscription.
  BrahmaputraException? error;

  T get value => _value;
  Stream<T> get changes => _changes.stream;

  /// Completes when the gateway confirms the subscription.
  Future<void> get ready => _subscription.ready;

  void close() {
    _subscription.cancel();
    _changes.close();
  }

  T build();

  void changed() {
    if (_scheduled) return;
    _scheduled = true;
    void flush() {
      _scheduled = false;
      if (_changes.isClosed) return;
      _value = build();
      _changes.add(_value);
    }

    final t = throttle;
    if (t == null || t == Duration.zero) {
      scheduleMicrotask(flush);
    } else {
      Timer(t, flush);
    }
  }
}

/// The latest record per key of a topic: a price board. Starts from the
/// gateway's snapshot unless `snapshot: false`.
class LatestByKey extends FeedView<Map<String, FeedRecord>> {
  LatestByKey(BrahmaputraClient client, String topic,
      {List<String>? keys, bool snapshot = true, Duration? throttle})
      : super(const {}, throttle) {
    _subscription = client.subscribe(
      topic,
      keys: keys,
      snapshot: snapshot,
      onRecord: (r) {
        final previous = _latest[r.keyId];
        // After a reconnect the snapshot may be older than a live record
        // already seen on the same partition; keep the newer one.
        if (previous != null &&
            previous.partition == r.partition &&
            previous.offset > r.offset) {
          return;
        }
        if (r.tombstone) {
          _latest.remove(r.keyId);
        } else {
          _latest[r.keyId] = r;
        }
        changed();
      },
      onLag: (n) => lagged += n,
      onError: (e) {
        error = e;
        changed();
      },
    );
  }

  final _latest = <String, FeedRecord>{};

  @override
  Map<String, FeedRecord> build() => Map.unmodifiable(_latest);
}

/// The last [limit] records of a topic, oldest first: a trade tape, a chat.
class RecentRecords extends FeedView<List<FeedRecord>> {
  RecentRecords(BrahmaputraClient client, String topic,
      {this.limit = 100,
      List<String>? keys,
      bool snapshot = false,
      Duration? throttle})
      : super(const [], throttle) {
    _subscription = client.subscribe(
      topic,
      keys: keys,
      snapshot: snapshot,
      onRecord: (r) {
        _buffer.add(r);
        if (_buffer.length > limit) {
          _buffer.removeRange(0, _buffer.length - limit);
        }
        changed();
      },
      onLag: (n) => lagged += n,
      onError: (e) {
        error = e;
        changed();
      },
    );
  }

  final int limit;
  final _buffer = <FeedRecord>[];

  @override
  List<FeedRecord> build() => List.unmodifiable(_buffer);
}
