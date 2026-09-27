// React bindings for @brahmaputra/ws-client.
//
//   <BrahmaputraProvider options={{ url, token: getToken }}>
//     <PriceBoard />
//   </BrahmaputraProvider>
//
//   function PriceBoard() {
//     const { data: prices } = useLatestByKey("prices.us", { keys: ["AAPL", "MSFT"] });
//     const publish = usePublish();
//     ...
//   }
//
// Every hook reads through useSyncExternalStore, so concurrent rendering
// never tears, and every subscription ends when its component unmounts.

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
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
  type SubscriptionHandlers,
} from "@brahmaputra/ws-client";

const Context = createContext<BrahmaputraClient | null>(null);

export type ProviderProps =
  | { client: BrahmaputraClient; options?: undefined; children?: ReactNode }
  | { options: ClientOptions; client?: undefined; children?: ReactNode };

/**
 * Makes a client available to the tree and keeps it connected while
 * mounted. Pass `options` to have the provider own the client (closed on
 * unmount), or a `client` you manage yourself.
 */
export function BrahmaputraProvider(props: ProviderProps) {
  const [client] = useState(() => props.client ?? new BrahmaputraClient(props.options!));
  const owned = props.client === undefined;
  useEffect(() => {
    client.connect().catch(() => {
      // Surfaced through useConnectionState / useConnectionError.
    });
    return () => {
      if (owned) client.close();
    };
  }, [client, owned]);
  return <Context.Provider value={client}>{props.children}</Context.Provider>;
}

/** The client from the nearest BrahmaputraProvider. */
export function useBrahmaputra(): BrahmaputraClient {
  const client = useContext(Context);
  if (!client) throw new Error("useBrahmaputra must be used inside <BrahmaputraProvider>");
  return client;
}

/** "connecting" | "open" | "reconnecting" | "closed" (and "idle" before connect). */
export function useConnectionState(): ConnectionState {
  const client = useBrahmaputra();
  return useSyncExternalStore(
    (onChange) => client.onState(onChange),
    () => client.state,
    () => client.state,
  );
}

/** Why the connection last failed, while it is not open. */
export function useConnectionError(): unknown {
  const state = useConnectionState();
  const client = useBrahmaputra();
  return state === "open" ? null : client.error;
}

export interface FeedResult<T> {
  data: T;
  /** Set if the gateway refused the subscription (e.g. TOPIC_NOT_ALLOWED). */
  error: BrahmaputraError | null;
  /** Records skipped because this client fell behind the feed. */
  lagged: number;
}

const noSubscribe = () => () => {};

function useFeed<T>(
  make: (client: BrahmaputraClient) => FeedStore<T>,
  deps: unknown[],
  empty: T,
): FeedResult<T> {
  const client = useBrahmaputra();
  const [store, setStore] = useState<FeedStore<T> | null>(null);
  useEffect(() => {
    const created = make(client);
    setStore(created);
    return () => created.close();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, ...deps]);
  const data = useSyncExternalStore(
    store ? (onChange) => store.subscribe(onChange) : noSubscribe,
    () => (store ? store.get() : empty),
    () => empty,
  );
  return { data, error: store?.error ?? null, lagged: store?.lagged ?? 0 };
}

const EMPTY_MAP: ReadonlyMap<string, FeedRecord> = new Map();
const EMPTY_LIST: readonly FeedRecord[] = [];

const keysDep = (keys?: string[]) => (keys ? keys.join("\u0000") : null);

/**
 * The latest record per key of `topic`: a price board. Starts from the
 * gateway's snapshot (the current price of every symbol) unless
 * `snapshot: false`.
 */
export function useLatestByKey(
  topic: string,
  options: StoreOptions = {},
): FeedResult<ReadonlyMap<string, FeedRecord>> {
  return useFeed(
    (client) => latestByKey(client, topic, options),
    [topic, keysDep(options.keys), options.snapshot, options.throttleMs],
    EMPTY_MAP,
  );
}

/** The last `limit` records of `topic`, oldest first: a trade tape, a chat. */
export function useRecentRecords(
  topic: string,
  options: StoreOptions & { limit?: number } = {},
): FeedResult<readonly FeedRecord[]> {
  return useFeed(
    (client) => recentRecords(client, topic, options),
    [topic, keysDep(options.keys), options.snapshot, options.throttleMs, options.limit],
    EMPTY_LIST,
  );
}

/**
 * Call `onRecord` for each record of `topic`, for as long as the component
 * is mounted. The latest handler is always used; changing it does not
 * resubscribe.
 */
export function useSubscription(
  topic: string,
  onRecord: (record: FeedRecord) => void,
  options: SubscribeOptions & Omit<SubscriptionHandlers, "onRecord"> = {},
): void {
  const client = useBrahmaputra();
  const handler = useRef(onRecord);
  handler.current = onRecord;
  const rest = useRef(options);
  rest.current = options;
  useEffect(() => {
    const sub = client.subscribe(
      topic,
      {
        onRecord: (r) => handler.current(r),
        onLag: (n) => rest.current.onLag?.(n),
        onSubscribed: (n) => rest.current.onSubscribed?.(n),
        onError: (e) => rest.current.onError?.(e),
      },
      { keys: options.keys, snapshot: options.snapshot },
    );
    return () => sub.unsubscribe();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, topic, keysDep(options.keys), options.snapshot]);
}

/** A stable publish function: resolves with the record's partition and offset. */
export function usePublish(): (message: PublishOptions) => Promise<Ack> {
  const client = useBrahmaputra();
  return useCallback((message: PublishOptions) => client.publish(message), [client]);
}

export {
  BrahmaputraClient,
  type Ack,
  type ClientOptions,
  type ConnectionState,
  type FeedRecord,
  type PublishOptions,
} from "@brahmaputra/ws-client";
