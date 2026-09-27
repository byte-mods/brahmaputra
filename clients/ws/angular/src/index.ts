// Angular bindings for @brahmaputra/ws-client.
//
//   bootstrapApplication(App, {
//     providers: [provideBrahmaputra({ url, token: () => auth.token() })],
//   });
//
//   @Component({ template: `@for (p of prices.data() | keyvalue; track p.key) {...}` })
//   class PriceBoard {
//     prices = injectLatestByKey("prices.us", { keys: ["AAPL", "MSFT"] });
//     state = injectConnectionState();
//     publish = injectPublish();
//   }
//
// The inject* functions return signals and must run in an injection
// context (a field initializer or constructor); their subscriptions end
// when the component is destroyed. For RxJS pipelines use the `$`
// observables instead. No decorators here, so the package needs no
// Angular compiler and works in AOT, JIT and zoneless apps alike.

import {
  DestroyRef,
  InjectionToken,
  inject,
  makeEnvironmentProviders,
  signal,
  type EnvironmentProviders,
  type Signal,
} from "@angular/core";
import { Observable } from "rxjs";
import {
  BrahmaputraClient,
  latestByKey,
  recentRecords,
  type Ack,
  type BrahmaputraError,
  type ClientOptions,
  type ConnectionState,
  type FeedRecord,
  type FeedStore,
  type PublishOptions,
  type StoreOptions,
  type SubscribeOptions,
} from "@brahmaputra/ws-client";

export const BRAHMAPUTRA = new InjectionToken<BrahmaputraClient>("BrahmaputraClient");

/**
 * Provide one client to the application, connected on first use and
 * closed with the application. `options` may be a factory, which runs in an
 * injection context (so it can inject your auth service).
 */
export function provideBrahmaputra(
  options: ClientOptions | (() => ClientOptions),
): EnvironmentProviders {
  return makeEnvironmentProviders([
    {
      provide: BRAHMAPUTRA,
      useFactory: () => {
        const client = new BrahmaputraClient(typeof options === "function" ? options() : options);
        client.connect().catch(() => {
          // Surfaced through injectConnectionState / connectionState$.
        });
        inject(DestroyRef).onDestroy(() => client.close());
        return client;
      },
    },
  ]);
}

export function injectBrahmaputra(): BrahmaputraClient {
  return inject(BRAHMAPUTRA);
}

export function injectConnectionState(): Signal<ConnectionState> {
  const client = inject(BRAHMAPUTRA);
  const state = signal(client.state);
  const stop = client.onState((s) => state.set(s));
  inject(DestroyRef).onDestroy(stop);
  return state.asReadonly();
}

export interface FeedSignals<T> {
  data: Signal<T>;
  error: Signal<BrahmaputraError | null>;
  lagged: Signal<number>;
}

function injectFeed<T>(store: FeedStore<T>): FeedSignals<T> {
  const data = signal(store.get());
  const error = signal<BrahmaputraError | null>(null);
  const lagged = signal(0);
  store.subscribe(() => {
    data.set(store.get());
    error.set(store.error);
    lagged.set(store.lagged);
  });
  inject(DestroyRef).onDestroy(() => store.close());
  return { data: data.asReadonly(), error: error.asReadonly(), lagged: lagged.asReadonly() };
}

/** The latest record per key as a signal: a price board, starting from the snapshot. */
export function injectLatestByKey(
  topic: string,
  options: StoreOptions = {},
): FeedSignals<ReadonlyMap<string, FeedRecord>> {
  return injectFeed(latestByKey(inject(BRAHMAPUTRA), topic, options));
}

/** The last `limit` records, oldest first, as a signal: a trade tape, a chat. */
export function injectRecentRecords(
  topic: string,
  options: StoreOptions & { limit?: number } = {},
): FeedSignals<readonly FeedRecord[]> {
  return injectFeed(recentRecords(inject(BRAHMAPUTRA), topic, options));
}

export function injectPublish(): (message: PublishOptions) => Promise<Ack> {
  const client = inject(BRAHMAPUTRA);
  return (message) => client.publish(message);
}

// -- RxJS -------------------------------------------------------------------

function store$<T>(make: () => FeedStore<T>): Observable<T> {
  return new Observable<T>((subscriber) => {
    const store = make();
    const unsubscribe = store.subscribe(() => {
      if (store.error) subscriber.error(store.error);
      else subscriber.next(store.get());
    });
    return () => {
      unsubscribe();
      store.close();
    };
  });
}

/** Every record of `topic` as it arrives. Unsubscribing ends the subscription. */
export function records$(
  client: BrahmaputraClient,
  topic: string,
  options: SubscribeOptions = {},
): Observable<FeedRecord> {
  return new Observable<FeedRecord>((subscriber) => {
    const sub = client.subscribe(
      topic,
      {
        onRecord: (r) => subscriber.next(r),
        onError: (e) => subscriber.error(e),
      },
      options,
    );
    return () => sub.unsubscribe();
  });
}

/** The latest record per key, emitted on every change. */
export function latestByKey$(
  client: BrahmaputraClient,
  topic: string,
  options: StoreOptions = {},
): Observable<ReadonlyMap<string, FeedRecord>> {
  return store$(() => latestByKey(client, topic, options));
}

export function recentRecords$(
  client: BrahmaputraClient,
  topic: string,
  options: StoreOptions & { limit?: number } = {},
): Observable<readonly FeedRecord[]> {
  return store$(() => recentRecords(client, topic, options));
}

export function connectionState$(client: BrahmaputraClient): Observable<ConnectionState> {
  return new Observable<ConnectionState>((subscriber) => {
    subscriber.next(client.state);
    return client.onState((s) => subscriber.next(s));
  });
}

export {
  BrahmaputraClient,
  type Ack,
  type ClientOptions,
  type ConnectionState,
  type FeedRecord,
  type PublishOptions,
} from "@brahmaputra/ws-client";
