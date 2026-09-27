# brahmaputra_ws

The Dart client for the Brahmaputra WebSocket gateway. It works in Flutter on iOS, Android, web and desktop, and in plain Dart. It covers acknowledged publishes, subscriptions with snapshots, reconnects with backoff, and the `LatestByKey` / `RecentRecords` views.

Usage and the rest of the guide are in [../README.md](../README.md). Widgets are in [../flutter](../flutter).

```bash
dart pub get
../test.sh dart     # end to end against a real gateway
```
