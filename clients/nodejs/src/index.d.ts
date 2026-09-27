/// <reference types="node" />
/**
 * Type declarations for the Brahmaputra Node.js driver (src/index.js).
 *
 * These describe the JavaScript exactly as it behaves at runtime:
 *
 *  - Configuration names are the camelCase forms of Kafka's (`lingerMs` for
 *    `linger.ms`, `bufferMemory` for `buffer.memory`, ...), which is how the
 *    JS reads them. Every override is optional; omitted keys take the
 *    defaults returned by `defaultProducerConfig()` and friends.
 *  - Offsets and timestamps read from the broker are `bigint`.
 *  - A record key, value or header value that the broker sent as null comes
 *    back as `null` (a null key, a tombstone, a null header value); an empty
 *    one comes back as an empty `Buffer`, never as `null`.
 *
 * Members whose names start with an underscore are internal and are not
 * declared here.
 */

import type { Socket } from 'net';

// ===========================================================================
// Shared value types
// ===========================================================================

/**
 * Anything the driver turns into bytes: a Buffer as-is, a string as UTF-8,
 * any other typed array / DataView viewed as bytes, or an ArrayBuffer.
 */
export type BytesInput = Buffer | string | ArrayBufferView | ArrayBuffer;

/** A key, value or header value to send; `null`/`undefined` means null. */
export type NullableBytesInput = BytesInput | null | undefined;

/** A header as `send()` accepts it: a `RecordHeader` or any `{ key, value }`. */
export interface HeaderInput {
  key: string;
  /** Omitted, `null` or `undefined` sends a null header value. */
  value?: NullableBytesInput;
}

/** One partition of one topic. */
export interface TopicPartition {
  topic: string;
  partition: number;
}

// ===========================================================================
// protocol.js
// ===========================================================================

/** The BitPacker schema version every request/response body starts with. */
export declare const SCHEMA_VERSION: '1.0.0';
/** The wire version this client speaks; the broker requires an exact match. */
export declare const API_VERSION: 4;
/** Fetch isolation level: see every record (the default). */
export declare const READ_UNCOMMITTED: 0;
/** Fetch isolation level: stop at the last stable offset. */
export declare const READ_COMMITTED: 1;
/** Size of the fixed big-endian batch prefix (base_offset + batch_length). */
export declare const BATCH_HEADER_LEN: 12;

export type IsolationLevel = typeof READ_UNCOMMITTED | typeof READ_COMMITTED;

export declare const ApiKey: Readonly<{
  PRODUCE: 0;
  FETCH: 1;
  LIST_OFFSETS: 2;
  METADATA: 3;
  REPLICA_FETCH: 4;
  OFFSETS_FOR_LEADER_EPOCH: 5;
  INIT_PRODUCER_ID: 6;
  JOIN_GROUP: 7;
  SYNC_GROUP: 8;
  HEARTBEAT: 9;
  OFFSET_COMMIT: 10;
  OFFSET_FETCH: 11;
  LIST_GROUPS: 12;
  DESCRIBE_GROUP: 13;
  API_VERSIONS: 14;
  PRODUCE_MULTI: 15;
  FETCH_MULTI: 16;
  AUTHENTICATE: 17;
  LEAVE_GROUP: 18;
}>;
export type ApiKeyCode = (typeof ApiKey)[keyof typeof ApiKey];

export declare const ErrorCode: Readonly<{
  NONE: 0;
  UNKNOWN_TOPIC_OR_PARTITION: 1;
  OFFSET_OUT_OF_RANGE: 2;
  INVALID_REQUEST: 3;
  UNSUPPORTED_VERSION: 4;
  INTERNAL: 5;
  NOT_LEADER_OR_FOLLOWER: 6;
  FENCED_BROKER_EPOCH: 7;
  FENCED_LEADER_EPOCH: 8;
  UNKNOWN_LEADER_EPOCH: 9;
  NOT_ENOUGH_REPLICAS: 10;
  FENCED_PRODUCER_EPOCH: 11;
  OUT_OF_ORDER_SEQUENCE: 12;
  UNKNOWN_MEMBER_ID: 13;
  REBALANCE_IN_PROGRESS: 14;
  NOT_COORDINATOR: 15;
  ILLEGAL_GENERATION: 16;
  COORDINATOR_LOAD_IN_PROGRESS: 17;
  SASL_AUTHENTICATION_FAILED: 18;
  AUTHORIZATION_FAILED: 19;
}>;
export type ErrorCodeValue = (typeof ErrorCode)[keyof typeof ErrorCode];

/** Broker error codes a producer may safely retry (nothing was appended). */
export declare const RETRIABLE_ERRORS: Set<number>;

/** Base class of every error this driver raises itself. */
export declare class BrahmaputraError extends Error {}

/** The broker sent bytes this client cannot decode (or vice versa). */
export declare class ProtocolError extends BrahmaputraError {}

/** The broker answered with a non-zero error code. */
export declare class ServerError extends BrahmaputraError {
  constructor(code: number, context?: string);
  /** The broker's error code; compare against `ErrorCode`. */
  code: number;
}

/** auto.offset.reset=none and the group has no usable committed offset. */
export declare class NoOffsetForPartition extends BrahmaputraError {}

/** Builds a BitPacker body: zigzag varints, varint-prefixed strings/arrays. */
export declare class Writer {
  constructor();
  chunks: Buffer[];
  raw(buffer: Buffer): this;
  /** Plain (non-zigzag) unsigned varint. */
  uvarint(value: number | bigint): this;
  int32(value: number | bigint): this;
  int64(value: number | bigint): this;
  bool(value: boolean): this;
  string(value: string): this;
  stringArray(values: readonly string[]): this;
  bytes(): Buffer;
}

/** Reads a BitPacker body. Every method throws `ProtocolError` on truncation. */
export declare class Reader {
  constructor(data: Buffer);
  data: Buffer;
  pos: number;
  readonly remaining: number;
  uvarint(): bigint;
  int32(): number;
  /** A BigInt: an offset can exceed Number.MAX_SAFE_INTEGER. */
  int64(): bigint;
  bool(): boolean;
  string(): string;
  stringArray(): string[];
  rest(): Buffer;
  skipString(): void;
  skipInt32(): void;
  skipInt64(): void;
}

/** A `Writer` that has already written `SCHEMA_VERSION`. */
export declare function bodyWriter(): Writer;
/** A `Reader` positioned past a verified `SCHEMA_VERSION`. */
export declare function bodyReader(data: Buffer): Reader;

/** One complete request frame, int32 length prefix included. */
export declare function encodeFrame(
  apiKey: number,
  correlationId: number,
  clientId: string | null,
  body: Buffer
): Buffer;

/** Split a response frame payload (length prefix removed) into id and body. */
export declare function decodeFramePayload(payload: Buffer): {
  correlationId: number;
  body: Buffer;
};

/** CRC32C (Castagnoli), as an unsigned 32-bit number. */
export declare function crc32c(data: Uint8Array): number;

/** Record-batch compression codec ids, as the broker numbers them. */
export declare const Compression: Readonly<{
  NONE: 0;
  LZ4: 1;
  ZSTD: 2;
  SNAPPY: 3;
  GZIP: 4;
}>;
export type CompressionCodec = (typeof Compression)[keyof typeof Compression];

/** The `compressionType` names a producer accepts. */
export type CompressionType = 'none' | 'lz4' | 'zstd' | 'snappy' | 'gzip';

/**
 * Map a `compressionType` name to its codec id. Throws `BrahmaputraError`
 * for an unknown name.
 */
export declare function parseCompression(name: string): CompressionCodec;
/** The name of a codec id, or `unknown(<id>)`. */
export declare function compressionName(codec: number): string;

/**
 * A codec implementation for `registerCodec`. Both directions are
 * synchronous and work on whole payloads.
 */
export interface CodecImplementation {
  compress(payload: Buffer): Buffer;
  decompress(payload: Buffer): Buffer;
}

/**
 * Supply a codec the runtime lacks (lz4, snappy; zstd before Node 22.15).
 * none and gzip are always built in and cannot be overridden. For lz4 the
 * broker expects a little-endian uint32 of the uncompressed length followed
 * by a raw LZ4 *block*, not the LZ4 frame format.
 */
export declare function registerCodec(
  codec: CompressionCodec,
  implementation: CodecImplementation
): void;

/**
 * An ordered, possibly repeating annotation on a record.
 *
 * On records read from the broker `value` is always `Buffer | null` (the
 * default type argument). When building one to send, any `BytesInput` may be
 * given; `send()` converts it.
 */
export declare class RecordHeader<V extends NullableBytesInput = Buffer | null> {
  /** `value` defaults to `null`: a null header value. */
  constructor(key: string, value?: V);
  key: string;
  value: V;
}

/**
 * Normalise a key/value/header value to bytes. `null`/`undefined` become
 * `null`; anything that is not bytes-like throws `TypeError`.
 */
export declare function toBytes(value: null | undefined, what?: string): null;
export declare function toBytes(value: BytesInput, what?: string): Buffer;
export declare function toBytes(value: NullableBytesInput, what?: string): Buffer | null;

/** A record as `encodeRecordBatch` takes it. */
export interface BatchRecordInput {
  key?: NullableBytesInput;
  /** `null`/`undefined` is a tombstone; an empty buffer is not. */
  value?: NullableBytesInput;
  /** Milliseconds relative to the batch's `maxTimestamp` (usually <= 0). */
  timestampDelta?: number | bigint;
  headers?: readonly HeaderInput[];
}

/** One record of a decoded batch, before it gets an offset. */
export interface DecodedRecord {
  key: Buffer | null;
  value: Buffer | null;
  timestampDelta: bigint;
  headers: RecordHeader[];
}

export interface DecodedBatch {
  baseOffset: bigint;
  maxTimestamp: bigint;
  records: DecodedRecord[];
}

/** Encode one record batch (magic 1, CRC32C) exactly as the broker stores it. */
export declare function encodeRecordBatch(
  records: readonly BatchRecordInput[],
  maxTimestamp: number | bigint,
  codec?: CompressionCodec
): Buffer;

/**
 * Decode the batch starting at `offset`; `next` is where the following one
 * starts. Throws `ProtocolError` on a bad length, magic or CRC.
 */
export declare function decodeRecordBatch(
  data: Buffer,
  offset: number
): { batch: DecodedBatch; next: number };

/** Kafka's murmur2 as an unsigned 32-bit number; `murmur2(empty) === 275646681`. */
export declare function murmur2(data: Uint8Array): number;

/**
 * Kafka's default keyed partitioner:
 * `partitions[(murmur2(key) & 0x7fffffff) % partitions.length]`.
 */
export declare function partitionForKey(key: BytesInput, partitions: readonly number[]): number;

// ===========================================================================
// client.js
// ===========================================================================

/** `listOffsets` sentinel: the oldest retained offset. */
export declare const EARLIEST: -2n;
/** `listOffsets` sentinel: the next offset to be written (log end). */
export declare const LATEST: -1n;
export declare const OFFSETS_TOPIC: '__consumer_offsets';
export declare const COORDINATOR_ATTEMPTS: 4;
export declare const JOIN_ATTEMPTS: 4;
/** Default client-side bound on one request/response round trip (ms). */
export declare const DEFAULT_REQUEST_TIMEOUT_MS: 120000;

/** `Date.now()`. */
export declare function nowMs(): number;
export declare function sleep(ms: number): Promise<void>;

export interface ApiVersionRange {
  apiKey: number;
  minVersion: number;
  maxVersion: number;
}

export interface AuthenticationResult {
  principal: string;
  role: string;
}

/**
 * One TCP connection to one broker, multiplexed by correlation id.
 *
 * After a request timeout, a socket error, the peer closing, or an
 * undecodable frame the connection is `closed` for good and every pending
 * request rejects; `Router` redials a closed connection on its next use.
 */
export declare class Connection {
  constructor(socket: Socket, clientId: string | null, requestTimeoutMs?: number);

  /**
   * @param clientId  defaults to `'brahmaputra-node'`; `null` sends none.
   * @param timeoutMs connect timeout, default 30000.
   * @param requestTimeoutMs per-request round-trip timeout, default 120000;
   *        0 or less disables it.
   */
  static connect(
    host: string,
    port: number,
    clientId?: string | null,
    timeoutMs?: number,
    requestTimeoutMs?: number
  ): Promise<Connection>;

  socket: Socket;
  clientId: string | null;
  requestTimeoutMs: number;
  /** The last correlation id used. */
  correlation: number;
  /** True once closed locally or broken by a failure; never reopens. */
  closed: boolean;

  close(): void;
  /** Send one request frame and resolve with the response body. */
  request(apiKey: number, body: Buffer): Promise<Buffer>;
  /** Send without awaiting a response (acks=0). */
  sendOneway(apiKey: number, body: Buffer): Promise<void>;
  /** SASL SCRAM-SHA-256. */
  authenticate(username: string, password: string): Promise<AuthenticationResult>;
  /** SASL PLAIN; the broker refuses it on a plaintext listener. */
  authenticatePlain(username: string, password: string): Promise<AuthenticationResult>;
  apiVersions(): Promise<{ versions: ApiVersionRange[]; brokerVersion: string }>;
}

export interface BrokerMetadata {
  nodeId: number;
  host: string;
  port: number;
  /** Empty when the broker was started without --rack. */
  rack: string;
}

export interface PartitionMetadata {
  partition: number;
  /** Leader node id, or negative when there is none. */
  leader: number;
  replicas: number[];
  isr: number[];
  leaderEpoch: number;
}

export interface TopicMetadata {
  name: string;
  partitions: PartitionMetadata[];
}

export interface ClusterMetadata {
  brokers: BrokerMetadata[];
  topics: TopicMetadata[];
}

/**
 * Keeps connections to every broker and routes requests to partition
 * leaders, from cached metadata refreshed when a route turns out stale.
 */
export declare class Router {
  constructor(
    host: string,
    port: number,
    clientId: string | null | undefined,
    timeoutMs: number,
    requestTimeoutMs?: number
  );
  /**
   * @param timeoutMs connect timeout, default 30000.
   * @param requestTimeoutMs per-request round-trip timeout for every
   *   connection the router opens, default 120000; 0 disables it.
   */
  static connect(
    host: string,
    port: number,
    clientId?: string | null,
    timeoutMs?: number,
    requestTimeoutMs?: number
  ): Promise<Router>;

  host: string;
  port: number;
  clientId: string | null | undefined;
  timeoutMs: number;
  requestTimeoutMs: number;
  /** Apply a round-trip timeout to every current and future connection. */
  setRequestTimeout(requestTimeoutMs: number): void;
  /** The bootstrap connection (redialled by `liveSeed()` once closed). */
  seed: Connection;
  /** Leader node id -> connection. */
  connections: Map<number, Connection>;
  /** The cached metadata image, `null` until first fetched. */
  metadata: ClusterMetadata | null;
  closed: boolean;

  close(): void;
  /** The seed connection, redialled if it has failed. */
  liveSeed(): Promise<Connection>;
  /**
   * Cached metadata, or a fresh fetch when `refresh` is true or nothing is
   * cached. An empty `topics` list asks for every topic.
   */
  getMetadata(topics?: readonly string[], refresh?: boolean): Promise<ClusterMetadata>;
  refresh(topic: string): Promise<ClusterMetadata>;
  /** Sorted partition ids; auto-creates the topic on a broker that allows it. */
  partitions(topic: string): Promise<number[]>;
  /** The live connection to the leader of `topic`-`partition`. */
  connectionFor(topic: string, partition: number): Promise<Connection>;
}

/** `acks`: 0 fire-and-forget, 1 leader append, -1 (`all`) every in-sync replica. */
export type Acks = 0 | 1 | -1;

/** Producer settings (Kafka name in brackets) and their defaults. */
export interface ProducerConfig {
  /** [client.id] default `'brahmaputra-node'`. */
  clientId: string;
  /** [acks] default 1. Use -1 for Kafka's `all`. */
  acks: Acks;
  /** [batch.size] bytes buffered per partition before a flush, default 16384. */
  batchSize: number;
  /** [linger.ms] default 5; 0 sends every record immediately. */
  lingerMs: number;
  /** [compression.type] default `'none'`. */
  compressionType: CompressionType;
  /** [request.timeout.ms] how long the broker may take (e.g. acks=-1), default 30000. */
  requestTimeoutMs: number;
  /** [retries] of retriable broker errors, default 5. */
  retries: number;
  /** [retry.backoff.ms] default 100. */
  retryBackoffMs: number;
  /** [delivery.timeout.ms] caps first attempt through last retry, default 120000. */
  deliveryTimeoutMs: number;
  /** [buffer.memory] unflushed bytes held client-side, default 32 MiB; <= 0 unbounded. */
  bufferMemory: number;
  /** [max.block.ms] how long `send()` waits on a full buffer, default 60000. */
  maxBlockMs: number;
  /** Client-side round-trip bound per request, default 120000; 0 disables. */
  socketTimeoutMs: number;
}

/** What `Producer.connect` accepts: any subset of `ProducerConfig`. */
export type ProducerOptions = Partial<ProducerConfig>;

export declare function defaultProducerConfig(): ProducerConfig;

export interface SendOptions {
  /** Keyed records go to `murmur2(key) % partitions`; default null (round robin). */
  key?: NullableBytesInput;
  /** Explicit partition; overrides the key. Default null. */
  partition?: number | null;
  headers?: readonly HeaderInput[];
  /** Record timestamp in unix milliseconds; default null (the wall clock). */
  timestamp?: number | bigint | null;
}

/** A batching producer. Share one; the batching is the point. */
export declare class Producer {
  constructor(router: Router, config: ProducerConfig);
  static connect(host: string, port: number, overrides?: ProducerOptions): Promise<Producer>;

  router: Router;
  config: ProducerConfig;
  codec: CompressionCodec;
  /** Bytes currently reserved against `bufferMemory`. */
  bufferedBytes: number;
  closed: boolean;
  /** The linger timer; `null` when `lingerMs` is 0 or after `close()`. */
  ticker: NodeJS.Timeout | null;
  /** First failure of a background (linger) flush not yet reported. */
  backgroundError: unknown;

  /**
   * Buffer one record. Resolves once it is buffered — or, when `lingerMs`
   * is 0 or the batch filled up, once that batch is acknowledged. Rejects
   * with `BrahmaputraError('producer buffer full ...')` after `maxBlockMs`
   * on a full buffer. A `null`/`undefined` value is a tombstone.
   */
  send(topic: string, value: NullableBytesInput, options?: SendOptions): Promise<void>;
  /**
   * Send one record on its own, unbatched, and resolve with its offset
   * (`-1n` with acks=0).
   */
  sendSync(topic: string, value: NullableBytesInput, options?: SendOptions): Promise<bigint>;
  /**
   * Send every buffered record and wait for acknowledgement. Also rejects
   * with the first background-flush failure since the last call.
   */
  flush(): Promise<void>;
  /** Flush, then release the timer and sockets even if the flush fails. */
  close(): Promise<void>;
}

/** Consumer settings (Kafka name in brackets) and their defaults. */
export interface ConsumerConfig {
  /** [client.id] default `'brahmaputra-node'`. */
  clientId: string;
  /** [fetch.max.bytes] default 8 MiB. */
  fetchMaxBytes: number;
  /** [fetch.min.bytes] default 1. */
  fetchMinBytes: number;
  /** [fetch.max.wait.ms] default 500; also caps any per-call `maxWaitMs`. */
  fetchMaxWaitMs: number;
  /** [isolation.level] `READ_UNCOMMITTED` (default) or `READ_COMMITTED`. */
  isolationLevel: IsolationLevel;
  /** [client.rack] default `''`. */
  rack: string;
  /** [max.poll.records] default 500 (used by `GroupConsumer`). */
  maxPollRecords: number;
  /** Client-side round-trip bound per request, default 120000; keep it above fetchMaxWaitMs. */
  socketTimeoutMs: number;
}

export type ConsumerOptions = Partial<ConsumerConfig>;

export declare function defaultConsumerConfig(): ConsumerConfig;

/** A record read from the broker. */
export interface ConsumerRecord {
  topic: string;
  partition: number;
  offset: bigint;
  /** `null` for a record sent without a key; empty Buffer for an empty key. */
  key: Buffer | null;
  /** `null` for a tombstone; empty Buffer for an empty value. */
  value: Buffer | null;
  /** Unix milliseconds. */
  timestamp: bigint;
  headers: RecordHeader[];
  /** The first header with this key's value, or `null` if absent (or null). */
  header(name: string): Buffer | null;
}

/** Reads one partition at a time, with no group coordination. */
export declare class Consumer {
  constructor(router: Router, config: ConsumerConfig);
  static connect(host: string, port: number, overrides?: ConsumerOptions): Promise<Consumer>;

  router: Router;
  config: ConsumerConfig;

  close(): void;
  partitions(topic: string): Promise<number[]>;
  /** Resolve `EARLIEST`, `LATEST` or a unix-ms timestamp to an offset. */
  listOffsets(topic: string, partition: number, timestamp: bigint | number): Promise<bigint>;
  /**
   * Records at or after `offset`. `maxWaitMs` (default/capped by
   * `fetchMaxWaitMs`) is how long the broker may wait for `fetchMinBytes`.
   */
  fetch(
    topic: string,
    partition: number,
    offset: bigint | number,
    maxWaitMs?: number | null
  ): Promise<ConsumerRecord[]>;
  /** `fetch`, also returning the partition's high watermark. */
  fetchVerbose(
    topic: string,
    partition: number,
    offset: bigint | number,
    maxWaitMs?: number | null
  ): Promise<{ records: ConsumerRecord[]; highWatermark: bigint }>;
}

// ===========================================================================
// group.js
// ===========================================================================

export declare const AutoOffsetReset: Readonly<{
  EARLIEST: 'earliest';
  LATEST: 'latest';
  NONE: 'none';
}>;
export type AutoOffsetResetPolicy = (typeof AutoOffsetReset)[keyof typeof AutoOffsetReset];

export declare const Assignor: Readonly<{
  RANGE: 'range';
  ROUNDROBIN: 'roundrobin';
  STICKY: 'sticky';
}>;
export type AssignorName = (typeof Assignor)[keyof typeof Assignor];

/** Group consumer settings (Kafka name in brackets) and their defaults. */
export interface GroupConfig {
  /** [client.id] default `'brahmaputra-node'`. */
  clientId: string;
  /** [session.timeout.ms] default 10000. */
  sessionTimeoutMs: number;
  /** [rebalance.timeout.ms] default 3000. */
  rebalanceTimeoutMs: number;
  /** [max.poll.interval.ms] default 300000; time inside poll() never counts. */
  maxPollIntervalMs: number;
  /** [auto.commit.interval.ms] default 5000; 0 disables auto-commit. */
  autoCommitIntervalMs: number;
  /** [auto.offset.reset] default `'earliest'`. */
  autoOffsetReset: AutoOffsetResetPolicy;
  /** [partition.assignment.strategy] default `'range'`. */
  assignor: AssignorName;
  /** [group.instance.id] static membership; default `''` (dynamic). */
  groupInstanceId: string;
  /** [max.poll.records] default 500; 0 or less means no cap. */
  maxPollRecords: number;
  /** [fetch.max.bytes] default 8 MiB. */
  fetchMaxBytes: number;
  /** [heartbeat.interval.ms] default 0, meaning sessionTimeoutMs / 3. */
  heartbeatIntervalMs: number;
  /** Client-side round-trip bound per request, default 120000; keep it above rebalanceTimeoutMs. */
  socketTimeoutMs: number;
}

export type GroupConsumerOptions = Partial<GroupConfig>;

export declare function defaultGroupConfig(): GroupConfig;

/** A consumer that shares its topics' partitions with its group. */
export declare class GroupConsumer {
  constructor(consumer: Consumer, groupId: string, config: GroupConfig);
  static connect(
    host: string,
    port: number,
    groupId: string,
    overrides?: GroupConsumerOptions
  ): Promise<GroupConsumer>;

  consumer: Consumer;
  groupId: string;
  config: GroupConfig;
  subscribed: string[];
  /** `''` until the first join. */
  memberId: string;
  /** `-1` until the first join. */
  generation: number;
  joined: boolean;
  assignment: TopicPartition[];
  /** Next offset to deliver per `"topic partition"`: what gets committed. */
  positions: Map<string, bigint>;
  closed: boolean;

  /** Replace the subscription; the next poll() (re)joins. */
  subscribe(topics: readonly string[]): void;
  /**
   * Join if needed, then return up to `maxPollRecords` records, or `[]`
   * after `timeoutMs` (default 1000). Throws `NoOffsetForPartition` under
   * auto.offset.reset=none, and `BrahmaputraError` before `subscribe()`.
   */
  poll(timeoutMs?: number): Promise<ConsumerRecord[]>;
  /** Commit delivered positions. At-least-once: call after processing. */
  commit(): Promise<void>;
  /**
   * Committed offsets keyed `"<topic> <partition>"`. An empty list (the
   * default) asks for every partition the group has committed.
   */
  committed(partitions?: readonly TopicPartition[]): Promise<Map<string, bigint>>;
  /** Commit, send LeaveGroup, stop the heartbeat and close sockets. */
  close(): Promise<void>;
}

/** A group member as the assignors see it. */
export interface AssignorMember {
  id: string;
  topics: readonly string[];
}

/** Member id -> the partitions it gets. */
export type Assignment = Map<string, TopicPartition[]>;

/** Contiguous ranges per topic; the first (n % members) take one extra. */
export declare function rangeAssign(
  members: readonly AssignorMember[],
  topicPartitions: ReadonlyMap<string, readonly number[]>
): Assignment;

/** Deal every partition around the circle of members sorted by id. */
export declare function roundRobinAssign(
  members: readonly AssignorMember[],
  topicPartitions: ReadonlyMap<string, readonly number[]>
): Assignment;

/**
 * Keep members on what they held (`previous`: member id -> partitions),
 * moving only what balance requires. Identical to the Go and Rust drivers.
 */
export declare function stickyAssign(
  members: readonly AssignorMember[],
  topicPartitions: ReadonlyMap<string, readonly number[]>,
  previous: ReadonlyMap<string, readonly TopicPartition[]>
): Assignment;
