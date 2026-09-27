// The Brahmaputra WebSocket gateway client: one socket, publishes with
// acknowledgements, topic subscriptions, and a reconnect loop that
// restores both without the application noticing more than a state change.

import { fromBase64, toBase64 } from "./base64.js";

export type ConnectionState = "idle" | "connecting" | "open" | "reconnecting" | "closed";

/** Anything shaped like the browser WebSocket constructor. */
export type WebSocketLike = {
  new (url: string, protocols?: string | string[]): WebSocket;
};

export interface ReconnectOptions {
  /** First retry delay; doubles per failure (with full jitter). Default 250. */
  minDelayMs?: number;
  /** Ceiling on the retry delay. Default 15000. */
  maxDelayMs?: number;
}

export interface ClientOptions {
  /** Gateway endpoint, e.g. `wss://gw.example.com/ws`. */
  url: string;
  /**
   * The JWT, or a function returning one. A function is called before every
   * connection attempt, so an expiring token is refreshed on reconnect.
   */
  token: string | (() => string | Promise<string>);
  /**
   * How the token travels. `subprotocol` (default) works in every browser,
   * which cannot set headers on a WebSocket; `query` puts it in the URL
   * (`?access_token=`), which ends up in access logs.
   */
  auth?: "subprotocol" | "query";
  /** The connection's default topic for publishes (`?topic=`). */
  topic?: string;
  /** The connection's default key (`?key=`); the gateway defaults it to the token subject. */
  key?: string;
  /** Reconnect after an unexpected close. Default on. */
  reconnect?: boolean | ReconnectOptions;
  /** How long a publish may wait for its acknowledgement, retries included. Default 30000. */
  publishTimeoutMs?: number;
  /** Publishes held while disconnected or awaiting acknowledgement. Default 1000. */
  maxPending?: number;
  /** Retries for publishes the gateway refused as retryable (rate limited, overloaded). Default 5. */
  maxRetries?: number;
  /** WebSocket implementation; defaults to the global one (browsers, Node 22+, React Native). */
  WebSocket?: WebSocketLike;
}

/** What the gateway said when the connection opened. */
export interface Welcome {
  user: string;
  topic: string | null;
  key: string;
  max_message_bytes: number;
  max_inflight: number;
  /** Whether this token may subscribe to anything. */
  subscribe: boolean;
}

export interface Ack {
  id: number;
  topic: string;
  partition: number;
  offset: number;
}

export interface PublishOptions {
  /** Defaults to the connection's topic. */
  topic?: string;
  /** Picks the partition; defaults to the connection's key (the user). */
  key?: string | Uint8Array;
  /** `null` publishes a tombstone (deletes the key on a compacted topic). */
  value: string | Uint8Array | null;
  headers?: Record<string, string | null>;
}

export interface SubscribeOptions {
  /** Only records with one of these keys (e.g. the symbols on screen). */
  keys?: string[];
  /** Start with the latest record of each key the gateway has cached. */
  snapshot?: boolean;
}

export interface SubscriptionHandlers {
  onRecord: (record: FeedRecord) => void;
  /** The subscription fell behind the feed and skipped `skipped` records. */
  onLag?: (skipped: number) => void;
  /** The gateway confirmed the subscription (again after each reconnect). */
  onSubscribed?: (snapshotSize: number) => void;
  /** The gateway refused the subscription (e.g. not permitted). */
  onError?: (error: BrahmaputraError) => void;
}

export interface Subscription {
  readonly topic: string;
  /** Resolves when the gateway first confirms the subscription. */
  readonly ready: Promise<void>;
  unsubscribe(): void;
}

export class BrahmaputraError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly retryable: boolean,
  ) {
    super(`${code}: ${message}`);
    this.name = "BrahmaputraError";
  }
}

/** One record delivered to a subscription. */
export class FeedRecord {
  readonly topic: string;
  readonly partition: number;
  readonly offset: number;
  readonly timestamp: number;
  /** The key as text, or null (no key, or a binary key: see keyBytes). */
  readonly key: string | null;
  /** The value as text; null for a tombstone or a binary value (see valueBytes). */
  readonly value: string | null;
  readonly headers: Readonly<Record<string, string | null>>;
  /** True for records sent as the subscription's snapshot. */
  readonly snapshot: boolean;
  private readonly keyB64: string | undefined;
  private readonly valueB64: string | undefined;

  constructor(frame: RecordFrame, snapshot: boolean) {
    this.topic = frame.topic;
    this.partition = frame.partition;
    this.offset = frame.offset;
    this.timestamp = frame.timestamp;
    this.key = frame.key ?? null;
    this.value = frame.value ?? null;
    this.headers = frame.headers ?? {};
    this.snapshot = snapshot;
    this.keyB64 = frame.key_b64;
    this.valueB64 = frame.value_b64;
  }

  /** A delete marker: the key no longer has a value. */
  get tombstone(): boolean {
    return this.value === null && this.valueB64 === undefined;
  }

  get keyBytes(): Uint8Array | null {
    if (this.keyB64 !== undefined) return fromBase64(this.keyB64);
    return this.key === null ? null : new TextEncoder().encode(this.key);
  }

  get valueBytes(): Uint8Array | null {
    if (this.valueB64 !== undefined) return fromBase64(this.valueB64);
    return this.value === null ? null : new TextEncoder().encode(this.value);
  }

  /** The value parsed as JSON. */
  json<T = unknown>(): T {
    if (this.value === null) throw new Error("record has no text value");
    return JSON.parse(this.value) as T;
  }

  /** Map key for "latest per key" views: the text key, or the base64 of a binary one. */
  get keyId(): string {
    return this.key ?? this.keyB64 ?? "";
  }
}

interface RecordFrame {
  type: "record";
  topic: string;
  partition: number;
  offset: number;
  timestamp: number;
  key?: string;
  key_b64?: string;
  value?: string | null;
  value_b64?: string;
  headers?: Record<string, string | null>;
}

type ServerFrame =
  | ({ type: "welcome" } & Welcome)
  | { type: "ack"; id: number; topic: string; partition: number; offset: number }
  | { type: "error"; id?: number; code: string; message: string; retryable: boolean }
  | { type: "subscribed"; id?: number; topic: string; snapshot: number }
  | { type: "unsubscribed"; id?: number; topic: string; reason?: string }
  | { type: "lagged"; topic: string; skipped: number }
  | RecordFrame;

interface PendingPublish {
  frame: string;
  resolve: (ack: Ack) => void;
  reject: (error: unknown) => void;
  retries: number;
  timer: ReturnType<typeof setTimeout>;
  sent: boolean;
}

interface Listener {
  keys: Set<string> | null;
  wantsSnapshot: boolean;
  handlers: SubscriptionHandlers;
  confirmed: boolean;
  confirm: () => void;
}

interface TopicState {
  listeners: Set<Listener>;
  /** Subscribe requests in flight: whom each confirms, and who gets its snapshot. */
  requests: Map<number, { confirm: Set<Listener>; snapshot: Set<Listener> }>;
  /** Records still to come as snapshot, and who gets them. */
  snapshotLeft: number;
  snapshotTo: Set<Listener>;
}

type StateListener = (state: ConnectionState) => void;

const SUBPROTOCOL = "brahmaputra.v1";

export class BrahmaputraClient {
  private readonly options: Required<
    Pick<ClientOptions, "auth" | "publishTimeoutMs" | "maxPending" | "maxRetries">
  > &
    ClientOptions;
  private readonly WS: WebSocketLike;
  private ws: WebSocket | null = null;
  private stateValue: ConnectionState = "idle";
  private readonly stateListeners = new Set<StateListener>();
  private welcomeValue: Welcome | null = null;
  private nextId = 1;
  private readonly pending = new Map<number, PendingPublish>();
  private readonly topics = new Map<string, TopicState>();
  private attempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private connectWaiters: Array<{ resolve: () => void; reject: (e: unknown) => void }> = [];
  private stopped = false;
  private lastError: unknown = null;

  constructor(options: ClientOptions) {
    this.options = {
      auth: "subprotocol",
      publishTimeoutMs: 30_000,
      maxPending: 1000,
      maxRetries: 5,
      ...options,
    };
    const WS = options.WebSocket ?? (globalThis as { WebSocket?: WebSocketLike }).WebSocket;
    if (!WS) throw new Error("no WebSocket implementation: pass options.WebSocket");
    this.WS = WS;
  }

  get state(): ConnectionState {
    return this.stateValue;
  }

  /** The gateway's welcome for the current connection, once open. */
  get welcome(): Welcome | null {
    return this.welcomeValue;
  }

  /** Why the last connection attempt or connection failed, if it did. */
  get error(): unknown {
    return this.lastError;
  }

  /** Be told of every state change. Returns the function that stops it. */
  onState(listener: StateListener): () => void {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  /**
   * Open the connection. Resolves once the gateway welcomes it; with
   * reconnect on (the default) it keeps trying until then or close().
   */
  connect(): Promise<void> {
    if (this.stateValue === "open") return Promise.resolve();
    this.stopped = false;
    const opened = new Promise<void>((resolve, reject) => {
      this.connectWaiters.push({ resolve, reject });
    });
    if (this.stateValue === "idle" || this.stateValue === "closed") {
      void this.open();
    }
    return opened;
  }

  /** Close for good: pending publishes are rejected and subscriptions dropped. */
  close(): void {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    const ws = this.ws;
    this.ws = null;
    if (ws) {
      ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null;
      try {
        ws.close(1000, "client closing");
      } catch {
        // already closing
      }
    }
    const closed = new BrahmaputraError("CLOSED", "client closed", false);
    for (const [id, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(closed);
      this.pending.delete(id);
    }
    for (const w of this.connectWaiters.splice(0)) w.reject(closed);
    this.setState("closed");
  }

  /**
   * Publish one record. Resolves with its partition and offset once the
   * broker has it. Publishes made while disconnected are held and sent on
   * reconnect, and anything unacknowledged when a connection drops is sent
   * again (at least once: a consumer that must not see duplicates should
   * deduplicate on an id of its own, e.g. a header).
   */
  publish(message: PublishOptions): Promise<Ack> {
    if (this.stopped && this.stateValue === "closed") {
      return Promise.reject(new BrahmaputraError("CLOSED", "client closed", false));
    }
    if (this.pending.size >= this.options.maxPending) {
      return Promise.reject(
        new BrahmaputraError("QUEUE_FULL", `${this.pending.size} publishes pending`, true),
      );
    }
    const id = this.nextId++;
    const frame: Record<string, unknown> = { id };
    if (message.topic !== undefined) frame.topic = message.topic;
    if (message.key !== undefined) {
      if (typeof message.key === "string") frame.key = message.key;
      else frame.key_b64 = toBase64(message.key);
    }
    if (message.value === null || typeof message.value === "string") frame.value = message.value;
    else frame.value_b64 = toBase64(message.value);
    if (message.headers) frame.headers = message.headers;
    const text = JSON.stringify(frame);
    return new Promise<Ack>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new BrahmaputraError("TIMEOUT", "no acknowledgement in time", true));
      }, this.options.publishTimeoutMs);
      const entry: PendingPublish = { frame: text, resolve, reject, retries: 0, timer, sent: false };
      this.pending.set(id, entry);
      if (this.stateValue === "open") this.sendPublish(entry);
    });
  }

  /** Publish a value as JSON. */
  publishJson(value: unknown, options: Omit<PublishOptions, "value"> = {}): Promise<Ack> {
    return this.publish({ ...options, value: JSON.stringify(value) });
  }

  /**
   * Receive a topic's records as they are written. Survives reconnects:
   * the subscription is renewed on every new connection (with a fresh
   * snapshot if asked for, so a view that missed ticks while offline is
   * brought up to date).
   */
  subscribe(
    topic: string,
    handlers: SubscriptionHandlers | ((record: FeedRecord) => void),
    options: SubscribeOptions = {},
  ): Subscription {
    const h: SubscriptionHandlers =
      typeof handlers === "function" ? { onRecord: handlers } : handlers;
    let confirm!: () => void;
    const ready = new Promise<void>((resolve) => (confirm = resolve));
    const listener: Listener = {
      keys: options.keys ? new Set(options.keys) : null,
      wantsSnapshot: options.snapshot ?? false,
      handlers: h,
      confirmed: false,
      confirm,
    };
    let state = this.topics.get(topic);
    if (!state) {
      state = { listeners: new Set(), requests: new Map(), snapshotLeft: 0, snapshotTo: new Set() };
      this.topics.set(topic, state);
    }
    state.listeners.add(listener);
    if (this.stateValue === "open") {
      this.sendSubscribe(topic, state, [listener]);
    }
    let done = false;
    return {
      topic,
      ready,
      unsubscribe: () => {
        if (done) return;
        done = true;
        const current = this.topics.get(topic);
        if (!current) return;
        current.listeners.delete(listener);
        current.snapshotTo.delete(listener);
        if (current.listeners.size === 0) {
          this.topics.delete(topic);
          this.sendFrame({ op: "unsubscribe", topic });
        } else {
          // Narrow the gateway's filter to what the rest still want.
          this.sendSubscribe(topic, current, [], []);
        }
      },
    };
  }

  // -- internals -------------------------------------------------------

  private setState(state: ConnectionState): void {
    if (state === this.stateValue) return;
    this.stateValue = state;
    for (const l of [...this.stateListeners]) {
      try {
        l(state);
      } catch (e) {
        console.error("brahmaputra state listener failed", e);
      }
    }
  }

  private async open(): Promise<void> {
    this.setState(this.attempt === 0 && !this.welcomeValue ? "connecting" : "reconnecting");
    let token: string;
    try {
      token = typeof this.options.token === "function" ? await this.options.token() : this.options.token;
    } catch (e) {
      this.lastError = e;
      this.scheduleReconnect(false);
      return;
    }
    if (this.stopped) return;
    const url = new URL(this.options.url);
    if (this.options.topic) url.searchParams.set("topic", this.options.topic);
    if (this.options.key) url.searchParams.set("key", this.options.key);
    let protocols: string[] | undefined;
    if (this.options.auth === "query") url.searchParams.set("access_token", token);
    else protocols = [SUBPROTOCOL, `bearer.${token}`];
    let ws: WebSocket;
    try {
      ws = new this.WS(url.toString(), protocols);
    } catch (e) {
      this.lastError = e;
      this.scheduleReconnect(false);
      return;
    }
    this.ws = ws;
    ws.onmessage = (event: MessageEvent) => {
      if (typeof event.data === "string") this.onFrame(event.data);
    };
    const closed = (code: number) => {
      if (this.ws !== ws) return;
      this.ws = null;
      const wasOpen = this.stateValue === "open";
      for (const p of this.pending.values()) p.sent = false;
      for (const t of this.topics.values()) {
        t.requests.clear();
        t.snapshotLeft = 0;
        t.snapshotTo.clear();
      }
      if (!wasOpen) {
        this.lastError = new BrahmaputraError(
          "CONNECTION",
          `connection refused or lost before welcome (code ${code})`,
          true,
        );
      }
      // 1001: the gateway instance is draining; go to another at once.
      this.scheduleReconnect(code === 1001);
    };
    ws.onerror = () => {
      this.lastError = new BrahmaputraError("CONNECTION", "WebSocket error", true);
      // Browsers follow a failed handshake's error with a close event, but
      // Node's WebSocket leaves the socket CONNECTING and never closes it.
      // An error on a socket that is not OPEN is the end of it either way;
      // `closed` ignores whichever report comes second.
      if (ws.readyState !== 1) {
        closed(1006);
        try {
          ws.close();
        } catch {
          // not closable in this state
        }
      }
    };
    ws.onclose = (event: CloseEvent) => closed(event.code);
  }

  private scheduleReconnect(immediate: boolean): void {
    if (this.stopped) return;
    const reconnect = this.options.reconnect ?? true;
    if (reconnect === false) {
      const error = this.lastError ?? new BrahmaputraError("CONNECTION", "connection closed", true);
      for (const w of this.connectWaiters.splice(0)) w.reject(error);
      for (const [id, p] of this.pending) {
        clearTimeout(p.timer);
        p.reject(error);
        this.pending.delete(id);
      }
      this.setState("closed");
      return;
    }
    const { minDelayMs = 250, maxDelayMs = 15_000 } = reconnect === true ? {} : reconnect;
    const ceiling = Math.min(maxDelayMs, minDelayMs * 2 ** Math.min(this.attempt, 16));
    // Full jitter: a fleet of phones reconnecting after a network blip
    // spreads out instead of arriving as one wave.
    const delay = immediate ? Math.random() * minDelayMs : Math.random() * ceiling;
    this.attempt++;
    this.setState("reconnecting");
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.open();
    }, delay);
  }

  private onFrame(text: string): void {
    let frame: ServerFrame;
    try {
      frame = JSON.parse(text) as ServerFrame;
    } catch {
      return;
    }
    switch (frame.type) {
      case "welcome": {
        const { type: _type, ...welcome } = frame;
        this.welcomeValue = welcome;
        this.attempt = 0;
        this.lastError = null;
        this.setState("open");
        for (const [topic, state] of this.topics) {
          this.sendSubscribe(topic, state, [...state.listeners]);
        }
        for (const p of [...this.pending.values()]) if (!p.sent) this.sendPublish(p);
        for (const w of this.connectWaiters.splice(0)) w.resolve();
        return;
      }
      case "ack": {
        const p = this.pending.get(frame.id);
        if (!p) return;
        this.pending.delete(frame.id);
        clearTimeout(p.timer);
        p.resolve({ id: frame.id, topic: frame.topic, partition: frame.partition, offset: frame.offset });
        return;
      }
      case "error": {
        this.onError(frame.id, new BrahmaputraError(frame.code, frame.message, frame.retryable));
        return;
      }
      case "subscribed": {
        const state = this.topics.get(frame.topic);
        if (!state) return;
        const request = frame.id !== undefined ? state.requests.get(frame.id) : undefined;
        if (frame.id !== undefined) {
          state.requests.delete(frame.id);
          this.subscribeIds.delete(frame.id);
        }
        state.snapshotLeft = frame.snapshot;
        state.snapshotTo = request?.snapshot ?? new Set();
        for (const l of request?.confirm ?? []) {
          if (!state.listeners.has(l)) continue;
          if (!l.confirmed) {
            l.confirmed = true;
            l.confirm();
          }
          l.handlers.onSubscribed?.(request?.snapshot.has(l) ? frame.snapshot : 0);
        }
        return;
      }
      case "record": {
        const state = this.topics.get(frame.topic);
        if (!state) return;
        const isSnapshot = state.snapshotLeft > 0;
        const targets = isSnapshot ? state.snapshotTo : state.listeners;
        if (isSnapshot) state.snapshotLeft--;
        if (targets.size === 0) return;
        const record = new FeedRecord(frame, isSnapshot);
        for (const l of targets) {
          if (l.keys && !l.keys.has(record.keyId)) continue;
          try {
            l.handlers.onRecord(record);
          } catch (e) {
            console.error("brahmaputra subscription handler failed", e);
          }
        }
        return;
      }
      case "lagged": {
        for (const l of this.topics.get(frame.topic)?.listeners ?? []) l.handlers.onLag?.(frame.skipped);
        return;
      }
      case "unsubscribed":
        return;
    }
  }

  private onError(id: number | undefined, error: BrahmaputraError): void {
    if (id === undefined) {
      this.lastError = error;
      return;
    }
    const p = this.pending.get(id);
    if (p) {
      if (error.retryable && p.retries < this.options.maxRetries) {
        p.retries++;
        p.sent = false;
        setTimeout(() => {
          if (this.pending.get(id) === p && this.stateValue === "open") this.sendPublish(p);
        }, 100 * 2 ** p.retries * (0.5 + Math.random()));
        return;
      }
      this.pending.delete(id);
      clearTimeout(p.timer);
      p.reject(error);
      return;
    }
    // A refused subscribe: tell its topic's listeners, and forget them.
    const topic = this.subscribeIds.get(id);
    if (topic === undefined) return;
    this.subscribeIds.delete(id);
    const state = this.topics.get(topic);
    if (!state) return;
    this.topics.delete(topic);
    for (const l of state.listeners) l.handlers.onError?.(error);
  }

  /** Subscribe request id -> topic, to route a refusal to its listeners. */
  private readonly subscribeIds = new Map<number, string>();

  /**
   * (Re)send the topic's subscription with the union of its listeners' key
   * filters. `confirming` are the listeners this request is for; those of
   * them that want a snapshot get this request's.
   */
  private sendSubscribe(
    topic: string,
    state: TopicState,
    confirming: Listener[],
    owedSnapshot: Listener[] = confirming.filter((l) => l.wantsSnapshot),
  ): void {
    const id = this.nextId++;
    const frame: Record<string, unknown> = { op: "subscribe", id, topic };
    let keys: Set<string> | null = new Set();
    for (const l of state.listeners) {
      if (!l.keys) {
        keys = null;
        break;
      }
      for (const k of l.keys) keys.add(k);
    }
    if (keys) frame.keys = [...keys];
    if (owedSnapshot.length > 0) frame.snapshot = true;
    state.requests.set(id, { confirm: new Set(confirming), snapshot: new Set(owedSnapshot) });
    this.subscribeIds.set(id, topic);
    if (this.subscribeIds.size > 10_000) {
      // Refusals arrive promptly; ids this old were confirmed long ago.
      const oldest = this.subscribeIds.keys().next().value;
      if (oldest !== undefined) this.subscribeIds.delete(oldest);
    }
    this.sendFrame(frame);
  }

  private sendPublish(p: PendingPublish): void {
    if (this.ws && this.stateValue === "open") {
      this.ws.send(p.frame);
      p.sent = true;
    }
  }

  private sendFrame(frame: Record<string, unknown>): void {
    if (this.ws && this.stateValue === "open") this.ws.send(JSON.stringify(frame));
  }
}
