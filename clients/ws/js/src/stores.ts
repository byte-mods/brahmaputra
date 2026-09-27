// Framework-neutral reactive views over subscriptions. Each store has
// get() and subscribe(onChange): exactly what React's useSyncExternalStore,
// Vue refs, Angular signals/observables and Svelte stores are built from.
//
// Values are immutable snapshots: a change produces a new Map or array, so
// UI frameworks can compare by identity. Changes that arrive together (a
// burst of ticks, a snapshot) are coalesced into one notification, per
// microtask by default or per `throttleMs` for very busy feeds.

import type {
  BrahmaputraClient,
  BrahmaputraError,
  ConnectionState,
  FeedRecord,
  SubscribeOptions,
} from "./client.js";

export interface ReadableStore<T> {
  get(): T;
  subscribe(onChange: () => void): () => void;
}

export interface FeedStore<T> extends ReadableStore<T> {
  /** Resolves when the gateway confirms the subscription. */
  readonly ready: Promise<void>;
  /** Records skipped because this client fell behind the feed. */
  readonly lagged: number;
  /** Set if the gateway refused the subscription. */
  readonly error: BrahmaputraError | null;
  /** Stop the subscription. */
  close(): void;
}

export interface StoreOptions extends SubscribeOptions {
  /** Coalesce notifications over this many milliseconds (0: per microtask). */
  throttleMs?: number;
}

abstract class BaseStore<T> implements FeedStore<T> {
  protected value: T;
  private readonly listeners = new Set<() => void>();
  private scheduled = false;
  private dirty = false;
  lagged = 0;
  error: BrahmaputraError | null = null;
  ready: Promise<void> = Promise.resolve();
  protected unsubscribe: () => void = () => {};

  constructor(
    initial: T,
    private readonly throttleMs: number,
  ) {
    this.value = initial;
  }

  get(): T {
    return this.value;
  }

  subscribe(onChange: () => void): () => void {
    this.listeners.add(onChange);
    return () => this.listeners.delete(onChange);
  }

  close(): void {
    this.unsubscribe();
    this.listeners.clear();
  }

  /** Something changed; publish a new value soon. */
  protected changed(): void {
    this.dirty = true;
    if (this.scheduled) return;
    this.scheduled = true;
    const flush = () => {
      this.scheduled = false;
      if (!this.dirty) return;
      this.dirty = false;
      this.value = this.build();
      for (const l of [...this.listeners]) l();
    };
    if (this.throttleMs > 0) setTimeout(flush, this.throttleMs);
    else queueMicrotask(flush);
  }

  protected abstract build(): T;
}

class LatestByKeyStore extends BaseStore<ReadonlyMap<string, FeedRecord>> {
  private readonly latest = new Map<string, FeedRecord>();

  constructor(client: BrahmaputraClient, topic: string, options: StoreOptions) {
    super(new Map(), options.throttleMs ?? 0);
    const sub = client.subscribe(
      topic,
      {
        onRecord: (record) => {
          const key = record.keyId;
          const previous = this.latest.get(key);
          // A snapshot after a reconnect may be older than what arrived
          // live meanwhile on the same partition; keep the newer one.
          if (
            previous &&
            previous.partition === record.partition &&
            previous.offset > record.offset
          ) {
            return;
          }
          if (record.tombstone) this.latest.delete(key);
          else this.latest.set(key, record);
          this.changed();
        },
        onLag: (skipped) => {
          this.lagged += skipped;
        },
        onError: (error) => {
          this.error = error;
          this.changed();
        },
      },
      { keys: options.keys, snapshot: options.snapshot ?? true },
    );
    this.ready = sub.ready;
    this.unsubscribe = () => sub.unsubscribe();
  }

  protected build(): ReadonlyMap<string, FeedRecord> {
    return new Map(this.latest);
  }
}

class RecentStore extends BaseStore<readonly FeedRecord[]> {
  private readonly buffer: FeedRecord[] = [];

  constructor(
    client: BrahmaputraClient,
    topic: string,
    private readonly limit: number,
    options: StoreOptions,
  ) {
    super([], options.throttleMs ?? 0);
    const sub = client.subscribe(
      topic,
      {
        onRecord: (record) => {
          this.buffer.push(record);
          if (this.buffer.length > this.limit) this.buffer.splice(0, this.buffer.length - this.limit);
          this.changed();
        },
        onLag: (skipped) => {
          this.lagged += skipped;
        },
        onError: (error) => {
          this.error = error;
          this.changed();
        },
      },
      { keys: options.keys, snapshot: options.snapshot ?? false },
    );
    this.ready = sub.ready;
    this.unsubscribe = () => sub.unsubscribe();
  }

  protected build(): readonly FeedRecord[] {
    return this.buffer.slice();
  }
}

/**
 * The latest record per key: a price board, a presence list, the state of
 * every order. Starts from the gateway's snapshot unless `snapshot: false`.
 */
export function latestByKey(
  client: BrahmaputraClient,
  topic: string,
  options: StoreOptions = {},
): FeedStore<ReadonlyMap<string, FeedRecord>> {
  return new LatestByKeyStore(client, topic, options);
}

/** The last `limit` records, oldest first: a trade tape, a chat, an activity log. */
export function recentRecords(
  client: BrahmaputraClient,
  topic: string,
  options: StoreOptions & { limit?: number } = {},
): FeedStore<readonly FeedRecord[]> {
  return new RecentStore(client, topic, options.limit ?? 100, options);
}

/** The client's connection state as a store. */
export function connectionState(client: BrahmaputraClient): ReadableStore<ConnectionState> {
  return {
    get: () => client.state,
    subscribe: (onChange) => client.onState(() => onChange()),
  };
}
