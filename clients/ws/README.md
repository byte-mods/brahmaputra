# UI SDKs for the WebSocket gateway

Browsers, phones and desktop apps talk to Brahmaputra through
[`brahmaputra-ws-gateway`](../../crates/gateway). These SDKs are the UI side
of that connection: **publish** with acknowledgements, **subscribe** to live
topics (a stock price feed, order status, chat) with a snapshot of the
current state, and keep both working across network drops, gateway
restarts and token expiry.

```
 market data / order service ──▶ Brahmaputra ◀──────────────────────────┐
   (any of the 24 drivers)          │                                   │
                                    │ one fetch per topic per instance  │ batched produce
                                    ▼                                   │
                           brahmaputra-ws-gateway × N  (stateless, behind an L4 LB)
                                    │  ▲
                        records ────┘  └──── publishes (orders, chat, telemetry)
                                    ▼  │
     React · Vue · Angular · Svelte · Flutter (iOS/Android/web/desktop) · React Native
```

| Package | For | Folder |
|---|---|---|
| `@brahmaputra/ws-client` | Any JS runtime with `WebSocket`: browsers, React Native, Node 22+, Deno, Bun. No dependencies. Includes the Svelte store adapter | [`js/`](js) |
| `@brahmaputra/ws-react` | React 18/19 hooks | [`react/`](react) |
| `@brahmaputra/ws-vue` | Vue 3 plugin and composables | [`vue/`](vue) |
| `@brahmaputra/ws-angular` | Angular 17+ providers, signals and RxJS observables (no decorators, so AOT, JIT and zoneless apps all work) | [`angular/`](angular) |
| `brahmaputra_ws` | Dart and Flutter on every platform | [`dart/`](dart) |
| `brahmaputra_ws_flutter` | Flutter widgets: `BrahmaputraScope`, `LatestByKeyBuilder`, ... and a stock-ticker example | [`flutter/`](flutter) |

All of them are the same client with the same behaviour:

- **Publish:** resolves with the record's partition and offset once the
  broker has it. Publishes made while offline are queued (bounded), and
  anything unacknowledged when a connection drops is sent again, so
  delivery is at least once. Retryable refusals (`RATE_LIMITED`,
  `OVERLOADED`) are retried with backoff.
- **Subscribe:** `latestByKey` gives the latest record per key, i.e. a
  price board. It starts from the gateway's snapshot, so the current price
  of every symbol shows immediately. `recentRecords` gives the last N (a
  tape or a chat). Both are immutable values replaced on each change, and
  bursts are coalesced into one update.
- **Reconnect:** exponential backoff with full jitter, so a million phones
  coming back after an outage do not arrive as one wave. After a gateway
  drain (close code 1001) the client reconnects at once. Subscriptions
  are renewed, with a fresh snapshot, so a screen that was offline catches
  up.
- **Tokens:** pass a function and it is called before every connection,
  so an expiring JWT is refreshed on reconnect.

## Quick start

```bash
brahmaputra-server --data-dir ./data --default-partitions 16

SECRET=$(openssl rand -hex 32)
brahmaputra-ws-gateway --broker 127.0.0.1:9092 --jwt-secret "$SECRET" \
    --allow-topic 'orders.*' --allow-subscribe 'prices.*,orders.*'

# Tokens come from your identity provider; for trying things out:
brahmaputra-ws-gateway mint-token --secret "$SECRET" --sub viewer-1 --read-only --subscribe 'prices.*'
brahmaputra-ws-gateway mint-token --secret "$SECRET" --sub trader-7 --topic 'orders.*' --subscribe 'prices.*'
```

Anything that writes to Brahmaputra feeds the UIs. For example, a
market-data service using any driver writes prices keyed by symbol:

```bash
brahmaputra-cli produce --topic prices.us --key AAPL --value '{"price":189.12}'
```

### Plain JavaScript / TypeScript

```ts
import { BrahmaputraClient, latestByKey } from "@brahmaputra/ws-client";

const client = new BrahmaputraClient({
  url: "wss://gw.example.com/ws",
  token: () => auth.getAccessToken(),       // called on every (re)connect
});
await client.connect();

const board = latestByKey(client, "prices.us", { keys: ["AAPL", "MSFT"] });
board.subscribe(() => render(board.get()));  // Map<symbol, FeedRecord>
// board.get().get("AAPL")?.json() -> { price: 189.12 }

const ack = await client.publishJson({ symbol: "AAPL", qty: 10 }, { topic: "orders.us" });
console.log(`order stored at ${ack.partition}:${ack.offset}`);
```

### React

```tsx
import { BrahmaputraProvider, useConnectionState, useLatestByKey, usePublish } from "@brahmaputra/ws-react";

<BrahmaputraProvider options={{ url: GATEWAY, token: getToken }}>
  <PriceBoard />
</BrahmaputraProvider>;

function PriceBoard() {
  const state = useConnectionState();                  // "open" | "reconnecting" | ...
  const { data: prices, error } = useLatestByKey("prices.us");
  const publish = usePublish();
  if (error) return <p>{error.code}</p>;               // e.g. TOPIC_NOT_ALLOWED
  return (
    <table>{[...prices].map(([symbol, r]) => (
      <tr key={symbol}>
        <td>{symbol}</td><td>{r.json<{ price: number }>().price}</td>
        <td><button onClick={() => publish({ topic: "orders.us", value: JSON.stringify({ symbol, qty: 1 }) })}>Buy</button></td>
      </tr>))}
    </table>
  );
}
```

Also `useRecentRecords(topic, { limit })`, `useSubscription(topic, onRecord)`
and `useBrahmaputra()`. The hooks read through `useSyncExternalStore`, so
concurrent rendering never tears.

### Vue 3

```ts
app.use(createBrahmaputra({ url: GATEWAY, token: getToken }));
```

```vue
<script setup lang="ts">
import { useConnectionState, useLatestByKey, usePublish } from "@brahmaputra/ws-vue";
const props = defineProps<{ symbols: string[] }>();
const state = useConnectionState();
const { data: prices } = useLatestByKey("prices.us", () => ({ keys: props.symbols })); // follows the prop
const publish = usePublish();
</script>
<template>
  <p>{{ state }}</p>
  <div v-for="[symbol, r] in prices" :key="symbol">{{ symbol }} {{ r.json().price }}</div>
</template>
```

### Angular

```ts
bootstrapApplication(App, {
  providers: [provideBrahmaputra(() => ({ url: GATEWAY, token: () => inject(Auth).token() }))],
});

@Component({
  imports: [KeyValuePipe],
  template: `
    <p>{{ state() }}</p>
    @for (entry of prices.data() | keyvalue; track entry.key) {
      <div>{{ entry.key }} {{ entry.value.json().price }}</div>
    }`,
})
export class PriceBoard {
  state = injectConnectionState();                 // Signal<ConnectionState>
  prices = injectLatestByKey("prices.us");         // { data, error, lagged } signals
  publish = injectPublish();
}
```

For RxJS pipelines there are `records$`, `latestByKey$`, `recentRecords$`
and `connectionState$`.

### Svelte

```svelte
<script>
  import { latestByKey } from "@brahmaputra/ws-client";
  import { readable } from "@brahmaputra/ws-client/svelte";
  const prices = readable(latestByKey(client, "prices.us"));
</script>
{#each [...$prices] as [symbol, r]}<div>{symbol} {r.json().price}</div>{/each}
```

### Flutter

```dart
BrahmaputraScope(
  create: () => BrahmaputraClient(url: Uri.parse(gateway), token: auth.freshToken),
  child: MaterialApp(home: Scaffold(
    appBar: AppBar(actions: [
      ConnectionStatusBuilder(builder: (context, status) => Text(status.name)),
    ]),
    body: LatestByKeyBuilder(
      topic: 'prices.us',
      builder: (context, prices, error) => ListView(children: [
        for (final p in prices.values)
          ListTile(title: Text(p.key!), subtitle: Text('${p.json()['price']}')),
      ]),
    ),
  )),
);

// Publishing, anywhere below the scope:
final ack = await BrahmaputraScope.of(context)
    .publishJson({'symbol': 'AAPL', 'qty': 1}, topic: 'orders.us');
```

[`flutter/example/lib/main.dart`](flutter/example/lib/main.dart) is a
complete ticker app. In plain Dart, use `LatestByKey(client, topic)` (a
`value` plus a `changes` stream) and `client.records(topic)`.

## Authentication and authorization

The gateway verifies an HS256 JWT during the WebSocket upgrade. The SDKs
send it as a subprotocol (`brahmaputra.v1`, `bearer.<jwt>`), which is the
one place browsers allow. A connection is bound to the token's `sub` for
its lifetime.

| Gateway flag | Token claim | Governs |
|---|---|---|
| `--allow-topic` (default `*`) | `topics` | where the connection may **publish** |
| `--allow-subscribe` (default: none, so subscriptions are off) | `subscribe` | what it may **subscribe** to |

A claim narrows the gateway's list and never widens it. `"topics": []` is
a read-only token (`mint-token --read-only`). A price-screen user
typically gets `subscribe: ["prices.*"]` and no publish rights, and a
trader gets `topics: ["orders.*"]` as well. Topics starting with `__` are
never reachable.

The client sees authorization as data, not as exceptions to catch
somewhere:

- a refused publish rejects with `TOPIC_NOT_ALLOWED` (not retryable);
- a refused subscription sets the store's `error` (`useLatestByKey(...).error`,
  `FeedView.error`, the Angular `error` signal);
- a forged or expired token never gets a connection, so the state stays
  `connecting`/`reconnecting` and `client.error` says why.

Every record a UI publishes carries an `x-gw-user` header that clients
cannot set, so back ends can trust who placed an order.

## Building a price feed

- **Topic:** key every record by instrument (`AAPL`), so one symbol's
  ticks stay in order in one partition. Make it a compacted topic and
  the log itself holds the last price of every symbol:
  `brahmaputra-cli topic create --name prices.us --partitions 16 --replication-factor 3 --config cleanup.policy=compact`.
- **Snapshot:** each gateway instance keeps the latest record per key for
  every topic it serves (`--snapshot-max-keys`, default 100,000). When its
  feed starts it reads back `--snapshot-warmup-records` per partition. A
  subscriber that asks for the snapshot (the default for `latestByKey`)
  gets it, then the live stream, with nothing missed or repeated in
  between. Tombstones delete a key (delisting a symbol).
- **Slow screens:** a subscriber that falls more than `--feed-buffer`
  records (default 4096) behind skips ahead to the newest and is told how
  many it skipped (`lagged`, counted in the store's `lagged`). For prices
  this is the right trade: a stale quote is worth less than a current one.
  Nobody else waits for it. A client that stops reading entirely is
  disconnected after `--write-timeout-secs`.
- **Busy feeds:** pass `throttleMs` (`throttle` in Dart) to update a
  screen at most every N ms however fast ticks arrive.
- **Key filters:** `keys: ["AAPL", "MSFT"]` has the gateway send only
  those symbols, which saves both bandwidth and phone battery.

## Scaling

Each gateway instance reads a subscribed topic **once**, encodes each
record into its frame once, and broadcasts it to every subscribed socket.
The broker's load follows topics × gateway instances, never the number of
screens. Measured with [`scripts/verify-ws-fanout.sh`](../../scripts/verify-ws-fanout.sh)
on one 4-core machine running the broker, two gateway instances and the
load generator side by side:

| | 10,000 screens, 20 ticks/s | 18,000 screens, 10 ticks/s | 500 screens, 20 ticks/s |
|---|---|---|---|
| Deliveries | 4,010,000 of 4,010,000 (≈200,000/s) | 3,618,000 of 3,618,000 (≈180,000/s) | 100,500 of 100,500 |
| Skipped / lost | 0 / 0 | 0 / 0 | 0 / 0 |
| Broker write → screen | p50 58 ms, p99 133 ms | p50 106 ms, p99 256 ms | p50 5 ms, p99 7 ms |
| Broker connections | 8 | 8 | 8 |
| Gateway memory | ~6 KB per subscribed socket | ~6 KB per subscribed socket | |

At 180–200k frames/s this machine has no idle CPU left (the load
generator alone takes about 1.4 cores), so latency there measures the
box, not the design. On an idle path a tick reaches the screen in about
5 ms. To scale, add gateway instances behind the load balancer: each new
instance costs the broker one fetch connection per topic it serves.

## Testing

[`test.sh`](test.sh) runs every suite against a real broker and real
gateway processes:

```bash
clients/ws/test.sh                  # js browser dart flutter
clients/ws/test.sh browser          # just the real-browser suite
```

| Suite | What it proves |
|---|---|
| `js` (11 tests) | publish → Brahmaputra → subscribers; price board snapshot, live ticks, key filters, tombstones, coalescing; forged, expired and malformed tokens; read-only tokens; token rotation across a gateway restart; unacked publishes resent after a restart; offline queueing; Svelte adapter |
| `browser` (10 tests) | React, Vue and Angular trading screens in Chromium ([`e2e/`](e2e)): snapshot and live prices written straight into Brahmaputra, orders placed from the UI found in Brahmaputra with the right user, refusals shown to the user, a gateway restart under a live page, 12 screens across two gateway instances updating together |
| `dart` (5 tests) | the Dart client against real gateways, with the Dart broker driver as the back end |
| `flutter` (3 tests) | the example ticker app's widgets against a real gateway: prices render and update, Buy places an order in Brahmaputra, refusals render, a gateway restart is ridden out |

The wire protocol these SDKs speak is documented in
[`crates/gateway/README.md`](../../crates/gateway/README.md#subscriptions-fan-out).
