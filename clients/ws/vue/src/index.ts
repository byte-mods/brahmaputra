// Vue 3 bindings for @brahmaputra/ws-client.
//
//   app.use(createBrahmaputra({ url, token: getToken }));
//
//   // in <script setup>
//   const { data: prices, error } = useLatestByKey("prices.us", { keys: ["AAPL"] });
//   const state = useConnectionState();
//   const publish = usePublish();
//
// Topics and keys may be refs or getters: the subscription follows them.
// Subscriptions end with the component (or effect scope) that made them.

import {
  inject,
  onScopeDispose,
  readonly,
  shallowRef,
  toValue,
  watch,
  type App,
  type InjectionKey,
  type MaybeRefOrGetter,
  type Ref,
  type ShallowRef,
} from "vue";
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
} from "@brahmaputra/ws-client";

export const BRAHMAPUTRA: InjectionKey<BrahmaputraClient> = Symbol("brahmaputra");

export interface BrahmaputraPlugin {
  client: BrahmaputraClient;
  install(app: App): void;
}

/**
 * A plugin that provides one connected client to the app. Pass options to
 * create it, or a client you already have. `app.unmount()` closes a client
 * the plugin created.
 */
export function createBrahmaputra(config: ClientOptions | BrahmaputraClient): BrahmaputraPlugin {
  const owned = !(config instanceof BrahmaputraClient);
  const client = config instanceof BrahmaputraClient ? config : new BrahmaputraClient(config);
  return {
    client,
    install(app: App) {
      app.provide(BRAHMAPUTRA, client);
      client.connect().catch(() => {
        // Surfaced through useConnectionState.
      });
      if (owned) {
        const unmount = app.unmount.bind(app);
        app.unmount = () => {
          client.close();
          unmount();
        };
      }
    },
  };
}

export function useBrahmaputra(): BrahmaputraClient {
  const client = inject(BRAHMAPUTRA, null);
  if (!client) throw new Error("useBrahmaputra needs app.use(createBrahmaputra(...))");
  return client;
}

export function useConnectionState(): Readonly<Ref<ConnectionState>> {
  const client = useBrahmaputra();
  const state = shallowRef(client.state);
  const stop = client.onState((s) => (state.value = s));
  onScopeDispose(stop);
  return readonly(state);
}

export interface FeedRefs<T> {
  data: Readonly<ShallowRef<T>>;
  error: Readonly<ShallowRef<BrahmaputraError | null>>;
  lagged: Readonly<ShallowRef<number>>;
}

function useFeed<T>(
  make: (client: BrahmaputraClient, topic: string, options: StoreOptions) => FeedStore<T>,
  topic: MaybeRefOrGetter<string>,
  options: MaybeRefOrGetter<StoreOptions>,
  empty: T,
): FeedRefs<T> {
  const client = useBrahmaputra();
  const data = shallowRef<T>(empty);
  const error = shallowRef<BrahmaputraError | null>(null);
  const lagged = shallowRef(0);
  let store: FeedStore<T> | null = null;
  const stop = watch(
    () => {
      const o = toValue(options);
      return [toValue(topic), o.keys?.join("\u0000") ?? null, o.snapshot, o.throttleMs] as const;
    },
    () => {
      store?.close();
      data.value = empty;
      error.value = null;
      const s = make(client, toValue(topic), toValue(options));
      store = s;
      s.subscribe(() => {
        data.value = s.get();
        error.value = s.error;
        lagged.value = s.lagged;
      });
    },
    { immediate: true },
  );
  onScopeDispose(() => {
    stop();
    store?.close();
  });
  return { data, error, lagged };
}

const EMPTY_MAP: ReadonlyMap<string, FeedRecord> = new Map();
const EMPTY_LIST: readonly FeedRecord[] = [];

/** The latest record per key: a price board, starting from the gateway's snapshot. */
export function useLatestByKey(
  topic: MaybeRefOrGetter<string>,
  options: MaybeRefOrGetter<StoreOptions> = {},
): FeedRefs<ReadonlyMap<string, FeedRecord>> {
  return useFeed(latestByKey, topic, options, EMPTY_MAP);
}

/** The last `limit` records, oldest first: a trade tape, a chat. */
export function useRecentRecords(
  topic: MaybeRefOrGetter<string>,
  options: MaybeRefOrGetter<StoreOptions & { limit?: number }> = {},
): FeedRefs<readonly FeedRecord[]> {
  return useFeed(recentRecords, topic, options, EMPTY_LIST);
}

export function usePublish(): (message: PublishOptions) => Promise<Ack> {
  const client = useBrahmaputra();
  return (message) => client.publish(message);
}

export {
  BrahmaputraClient,
  type Ack,
  type ClientOptions,
  type ConnectionState,
  type FeedRecord,
  type PublishOptions,
} from "@brahmaputra/ws-client";
