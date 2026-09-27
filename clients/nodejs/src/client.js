'use strict';

/**
 * Producer, consumer and group consumer.
 *
 * Configuration names mirror Kafka's, because the point of a client
 * library is that someone who knows Kafka does not have to learn a new
 * vocabulary. Where a default differs from Kafka's it is called out.
 */

const crypto = require('crypto');
const net = require('net');

const {
  ApiKey,
  BrahmaputraError,
  ErrorCode,
  NoOffsetForPartition,
  ProtocolError,
  RETRIABLE_ERRORS,
  RecordHeader,
  ServerError,
  bodyReader,
  bodyWriter,
  crc32c,
  decodeFramePayload,
  decodeRecordBatch,
  encodeFrame,
  encodeRecordBatch,
  parseCompression,
  partitionForKey,
  toBytes,
} = require('./protocol');

const EARLIEST = -2n;
const LATEST = -1n;
const OFFSETS_TOPIC = '__consumer_offsets';
const COORDINATOR_ATTEMPTS = 4;
const JOIN_ATTEMPTS = 4;

const nowMs = () => Date.now();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/**
 * One TCP connection to one broker, multiplexed by correlation id.
 *
 * The broker may answer out of order, so responses are matched by
 * correlation id rather than by arrival order — which is also what lets
 * several requests be in flight at once on one socket.
 */

const SCRAM_MECHANISM = 'SCRAM-SHA-256';

/** One `key=value` field out of a SCRAM message. */
function scramField(message, key) {
  for (const part of message.split(',')) {
    if (part.startsWith(`${key}=`)) return part.slice(key.length + 1);
  }
  return null;
}

/**
 * The client half of RFC 5802: prove knowledge of the password without
 * sending it.
 *
 * Node's PBKDF2 is used directly rather than hand-rolled — it is the same
 * construction the broker derives its stored key with.
 */
function scramClientProof(password, salt, iterations, authMessage) {
  const salted = crypto.pbkdf2Sync(
    Buffer.from(password, 'utf8'),
    Buffer.from(salt, 'base64'),
    iterations,
    32,
    'sha256'
  );
  const clientKey = crypto.createHmac('sha256', salted).update('Client Key').digest();
  const storedKey = crypto.createHash('sha256').update(clientKey).digest();
  const signature = crypto.createHmac('sha256', storedKey).update(authMessage).digest();
  const proof = Buffer.alloc(clientKey.length);
  for (let i = 0; i < clientKey.length; i += 1) proof[i] = clientKey[i] ^ signature[i];
  return proof.toString('base64');
}

/**
 * How long one request may wait for its response. It must exceed the
 * longest the broker may legitimately hold a request (a fetch long-poll, an
 * acks=all wait, a JoinGroup waiting out a rebalance); its job is to turn a
 * wedged broker into a rejection instead of a promise that never settles.
 * A response that arrives after its request timed out is simply dropped:
 * responses are matched by correlation id, so it cannot be mistaken for
 * the answer to a later request.
 */
const DEFAULT_REQUEST_TIMEOUT_MS = 120000;

class Connection {
  constructor(socket, clientId, requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS) {
    this.socket = socket;
    this.clientId = clientId;
    this.requestTimeoutMs = requestTimeoutMs;
    this.correlation = 0;
    this.pending = new Map();
    this.buffer = Buffer.alloc(0);
    this.closed = false;

    socket.on('data', (chunk) => this._onData(chunk));
    // Any socket failure ends the connection: after it the byte stream is
    // at an unknown position, and the Router must redial rather than reuse
    // it.
    socket.on('error', (error) => this._fatal(error));
    socket.on('close', () => this._fatal(new BrahmaputraError('connection closed by broker')));
  }

  static connect(
    host,
    port,
    clientId = 'brahmaputra-node',
    timeoutMs = 30000,
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS
  ) {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection({ host, port });
      // Responses are small and latency matters more than packet count;
      // without this every request pays Nagle plus the peer's delayed ACK.
      socket.setNoDelay(true);
      const timer = setTimeout(() => {
        socket.destroy();
        reject(new BrahmaputraError(`connect to ${host}:${port} timed out`));
      }, timeoutMs);
      socket.once('connect', () => {
        clearTimeout(timer);
        resolve(new Connection(socket, clientId, requestTimeoutMs));
      });
      socket.once('error', (error) => {
        clearTimeout(timer);
        reject(error);
      });
    });
  }

  close() {
    this.closed = true;
    this.socket.destroy();
    this._failAll(new BrahmaputraError('connection closed'));
  }

  _fatal(error) {
    this.closed = true;
    this.socket.destroy();
    this._failAll(error);
  }

  _nextCorrelationId() {
    // Wrapped inside int32: writeInt32BE throws past 2^31-1, which would
    // kill a long-lived connection after two billion requests.
    this.correlation = this.correlation >= 0x7fffffff ? 1 : this.correlation + 1;
    return this.correlation;
  }

  request(apiKey, body) {
    if (this.closed) return Promise.reject(new BrahmaputraError('connection is closed'));
    const correlationId = this._nextCorrelationId();
    return new Promise((resolve, reject) => {
      let timer = null;
      const settle = (fn) => (value) => {
        if (timer) clearTimeout(timer);
        this.pending.delete(correlationId);
        fn(value);
      };
      this.pending.set(correlationId, { resolve: settle(resolve), reject: settle(reject) });
      if (this.requestTimeoutMs > 0) {
        timer = setTimeout(() => {
          const waiter = this.pending.get(correlationId);
          if (waiter) {
            waiter.reject(
              new BrahmaputraError(`request timed out after ${this.requestTimeoutMs} ms`)
            );
          }
        }, this.requestTimeoutMs);
      }
      this.socket.write(encodeFrame(apiKey, correlationId, this.clientId, body), (error) => {
        if (error) {
          const waiter = this.pending.get(correlationId);
          if (waiter) waiter.reject(error);
        }
      });
    });
  }

  /** Send without awaiting a response (acks=0). */
  sendOneway(apiKey, body) {
    if (this.closed) return Promise.reject(new BrahmaputraError('connection is closed'));
    this._nextCorrelationId();
    return new Promise((resolve, reject) => {
      this.socket.write(encodeFrame(apiKey, this.correlation, this.clientId, body), (error) =>
        error ? reject(error) : resolve()
      );
    });
  }

  _onData(chunk) {
    this.buffer = this.buffer.length ? Buffer.concat([this.buffer, chunk]) : chunk;
    for (;;) {
      if (this.buffer.length < 4) return;
      const length = this.buffer.readInt32BE(0);
      if (length < 0) {
        this._fatal(new ProtocolError(`negative frame length ${length}`));
        return;
      }
      if (this.buffer.length < 4 + length) return;
      const payload = this.buffer.subarray(4, 4 + length);
      this.buffer = this.buffer.subarray(4 + length);
      try {
        const { correlationId, body } = decodeFramePayload(payload);
        const waiter = this.pending.get(correlationId);
        if (waiter) waiter.resolve(Buffer.from(body));
      } catch (error) {
        this._fatal(error);
        return;
      }
    }
  }

  _failAll(error) {
    const waiters = [...this.pending.values()];
    this.pending.clear();
    for (const waiter of waiters) waiter.reject(error);
  }

  /**
   * Bind a principal to this connection using SCRAM-SHA-256.
   *
   * The password never crosses the wire: the broker sends a challenge and
   * this answers with a proof derived from the password, which is what
   * makes authentication meaningful on a plaintext listener. Use
   * `authenticatePlain` only where the connection is already encrypted.
   */
  async authenticate(username, password) {
    const clientNonce = crypto.randomBytes(18).toString('base64').replace(/,/g, '.');
    const bare = `n=${username},r=${clientNonce}`;
    const first = await this._authenticateStep(username, '', SCRAM_MECHANISM, `n,,${bare}`);
    if (first.done) throw new ProtocolError('broker ended the SCRAM exchange before it began');

    const serverFirst = first.payload;
    const nonce = scramField(serverFirst, 'r');
    const salt = scramField(serverFirst, 's');
    const iterations = Number(scramField(serverFirst, 'i'));
    if (!nonce || !salt || !Number.isInteger(iterations) || iterations <= 0) {
      throw new ProtocolError('malformed SCRAM server-first message');
    }
    // The server must have kept this client's nonce, which is what makes
    // the exchange this one rather than a replay of an earlier one.
    if (!nonce.startsWith(clientNonce)) {
      throw new ProtocolError('SCRAM server nonce does not extend the client nonce');
    }
    // `biws` is base64 of the GS2 header "n,,", echoed so the server can
    // see it was not altered in flight.
    const withoutProof = `c=biws,r=${nonce}`;
    const authMessage = `${bare},${serverFirst},${withoutProof}`;
    const proof = scramClientProof(password, salt, iterations, authMessage);
    const final = await this._authenticateStep(
      username,
      '',
      SCRAM_MECHANISM,
      `${withoutProof},p=${proof}`
    );
    return { principal: final.principal, role: final.role };
  }

  /** Send the password itself, as SASL/PLAIN does. Refused on a plaintext listener. */
  async authenticatePlain(username, password) {
    const result = await this._authenticateStep(username, password, 'PLAIN', '');
    return { principal: result.principal, role: result.role };
  }

  async _authenticateStep(username, password, mechanism, payload) {
    const writer = bodyWriter()
      .string(username)
      .string(password)
      .string(mechanism)
      .string(payload);
    const reader = bodyReader(await this.request(ApiKey.AUTHENTICATE, writer.bytes()));
    const code = reader.int32();
    const principal = reader.string();
    const role = reader.string();
    const responsePayload = reader.string();
    const done = reader.bool();
    if (code !== ErrorCode.NONE) throw new ServerError(code, 'authenticate');
    return { principal, role, payload: responsePayload, done };
  }

  async apiVersions() {
    const writer = bodyWriter().string('brahmaputra-node').string('0.1.0');
    const reader = bodyReader(await this.request(ApiKey.API_VERSIONS, writer.bytes()));
    const code = reader.int32();
    if (code !== ErrorCode.NONE) throw new ServerError(code, 'api_versions');
    const count = reader.int32();
    const versions = [];
    for (let index = 0; index < count; index += 1) {
      versions.push({
        apiKey: reader.int32(),
        minVersion: reader.int32(),
        maxVersion: reader.int32(),
      });
    }
    return { versions, brokerVersion: reader.string() };
  }
}

// ---------------------------------------------------------------------------
// Metadata and routing
// ---------------------------------------------------------------------------

function decodeMetadata(reader) {
  // Field order is exactly the schema's: error_code, brokers,
  // controller_id, topics. The leading code is request-level — an
  // authorization denial, say — and is distinct from the per-topic one,
  // which is what "no such topic" uses.
  const requestError = reader.int32();
  if (requestError !== 0) {
    throw serverError(requestError, 'metadata');
  }
  const brokers = [];
  for (let count = reader.int32(); count > 0; count -= 1) {
    brokers.push({
      nodeId: reader.int32(),
      host: reader.string(),
      port: reader.int32(),
      // Empty when the broker was started without --rack.
      rack: reader.string(),
    });
  }
  reader.skipInt32(); // controller_id
  const topics = [];
  for (let count = reader.int32(); count > 0; count -= 1) {
    const name = reader.string();
    const topicError = reader.int32();
    const partitions = [];
    for (let pcount = reader.int32(); pcount > 0; pcount -= 1) {
      const partition = reader.int32();
      const leader = reader.int32();
      const replicas = [];
      for (let rc = reader.int32(); rc > 0; rc -= 1) replicas.push(reader.int32());
      const isr = [];
      for (let ic = reader.int32(); ic > 0; ic -= 1) isr.push(reader.int32());
      const leaderEpoch = reader.int32();
      partitions.push({ partition, leader, replicas, isr, leaderEpoch });
    }
    if (topicError !== ErrorCode.NONE && topicError !== ErrorCode.UNKNOWN_TOPIC_OR_PARTITION) {
      throw new ServerError(topicError, `metadata for ${name}`);
    }
    topics.push({ name, partitions });
  }
  return { brokers, topics };
}

/**
 * Keeps connections to every broker and routes by partition leader.
 *
 * Metadata is cached and refreshed only when a request says the route was
 * stale, because refreshing per request would put the control plane on the
 * data path.
 */
class Router {
  constructor(host, port, clientId, timeoutMs) {
    this.host = host;
    this.port = port;
    this.clientId = clientId;
    this.timeoutMs = timeoutMs;
    this.seed = null;
    this.connections = new Map();
    this.metadata = null;
    // In-progress dials, so concurrent callers that all find a dead
    // connection share one replacement instead of opening one each.
    this.dialing = new Map();
    this.closed = false;
  }

  static async connect(host, port, clientId, timeoutMs = 30000) {
    const router = new Router(host, port, clientId, timeoutMs);
    router.seed = await Connection.connect(host, port, clientId, timeoutMs);
    return router;
  }

  close() {
    this.closed = true;
    for (const connection of this.connections.values()) {
      if (connection !== this.seed) connection.close();
    }
    this.connections.clear();
    this.seed.close();
  }

  /**
   * Dial `host:port` once for all concurrent callers. A connection that
   * failed is replaced on its next use rather than kept: without that, one
   * dropped socket — a broker restart, a load balancer's idle timeout —
   * would fail every later request for the life of the client.
   */
  _dial(key, host, port) {
    if (this.closed) return Promise.reject(new BrahmaputraError('router is closed'));
    let pending = this.dialing.get(key);
    if (!pending) {
      pending = Connection.connect(host, port, this.clientId, this.timeoutMs).finally(() =>
        this.dialing.delete(key)
      );
      this.dialing.set(key, pending);
    }
    return pending;
  }

  /** The seed connection, redialled if it has failed. */
  async liveSeed() {
    if (!this.seed.closed) return this.seed;
    const old = this.seed;
    const fresh = await this._dial(`seed ${this.host}:${this.port}`, this.host, this.port);
    if (this.seed === old) this.seed = fresh;
    for (const [leader, connection] of this.connections) {
      if (connection === old) this.connections.set(leader, this.seed);
    }
    return this.seed;
  }

  async getMetadata(topics = [], refresh = false) {
    if (!refresh && this.metadata) return this.metadata;
    const writer = bodyWriter().stringArray(topics);
    const seed = await this.liveSeed();
    const reader = bodyReader(await seed.request(ApiKey.METADATA, writer.bytes()));
    this.metadata = decodeMetadata(reader);
    return this.metadata;
  }

  refresh(topic) {
    return this.getMetadata([topic], true);
  }

  async partitions(topic) {
    let metadata = await this.getMetadata([topic]);
    let found = metadata.topics.find((entry) => entry.name === topic);
    if (!found || found.partitions.length === 0) {
      // A topic auto-created on first produce is not in the cached image
      // yet; one refresh distinguishes "new" from "absent".
      metadata = await this.refresh(topic);
      found = metadata.topics.find((entry) => entry.name === topic);
    }
    if (!found || found.partitions.length === 0) {
      throw new BrahmaputraError(`topic ${topic} has no partitions`);
    }
    return found.partitions.map((entry) => entry.partition).sort((a, b) => a - b);
  }

  async connectionFor(topic, partition) {
    let metadata = await this.getMetadata([topic]);
    let leader = this._leaderOf(metadata, topic, partition);
    if (leader < 0) {
      metadata = await this.refresh(topic);
      leader = this._leaderOf(metadata, topic, partition);
    }
    if (leader < 0) throw new BrahmaputraError(`no leader for ${topic}-${partition}`);

    const existing = this.connections.get(leader);
    if (existing && !existing.closed) return existing;
    if (existing) this.connections.delete(leader);

    const broker = metadata.brokers.find((entry) => entry.nodeId === leader);
    if (!broker) throw new BrahmaputraError(`broker ${leader} is not in the metadata`);
    // A single-broker cluster advertises the address it was configured
    // with, which may not be the one we dialled; reuse the seed rather
    // than opening a second connection to ourselves.
    if (metadata.brokers.length === 1) {
      const seed = await this.liveSeed();
      this.connections.set(leader, seed);
      return seed;
    }
    const connection = await this._dial(`broker ${leader}`, broker.host, broker.port);
    const current = this.connections.get(leader);
    if (current && !current.closed) {
      if (current !== connection) connection.close();
      return current;
    }
    this.connections.set(leader, connection);
    return connection;
  }

  _leaderOf(metadata, topic, partition) {
    const found = metadata.topics.find((entry) => entry.name === topic);
    if (!found) return -1;
    const info = found.partitions.find((entry) => entry.partition === partition);
    return info ? info.leader : -1;
  }
}

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

/** Producer settings, named as Kafka names them. */
const defaultProducerConfig = () => ({
  clientId: 'brahmaputra-node',
  /** 0 fire-and-forget, 1 leader append, -1 every in-sync replica. */
  acks: 1,
  /** Flush a partition buffer once it holds this many bytes. */
  batchSize: 16 * 1024,
  /**
   * Flush every non-empty buffer at least this often; 0 sends each record
   * immediately. Kafka defaults to 0; this defaults to 5 because an
   * unbatched producer is slow enough to look broken.
   */
  lingerMs: 5,
  /** none, gzip, zstd (Node 22+), or a codec registered via registerCodec. */
  compressionType: 'none',
  requestTimeoutMs: 30000,
  /**
   * Retries of a send the broker refused with a retriable error — one it
   * returns before appending, so a retry cannot duplicate.
   */
  retries: 5,
  retryBackoffMs: 100,
  /** Caps the whole send, first attempt through last retry. */
  deliveryTimeoutMs: 120000,
  /** Caps unflushed record bytes held client-side. */
  bufferMemory: 32 * 1024 * 1024,
  /** How long send() may block on a full buffer before rejecting. */
  maxBlockMs: 60000,
});

/**
 * A batching producer. Share one across your application rather than
 * creating one per message: the batching is the point.
 */
class Producer {
  constructor(router, config) {
    this.router = router;
    this.config = config;
    this.codec = parseCompression(config.compressionType);
    this.buffers = new Map();
    this.sizes = new Map();
    this.bufferedBytes = 0;
    this.waiters = [];
    this.roundRobin = 0;
    this.closed = false;
    // Per partition, the tail of the chain of sends to it. Every flush of a
    // partition waits for the previous one, so a partition has one batch in
    // flight and batches reach the broker in the order they were taken.
    // Without this the ticker and a send that fills a batch both put a
    // batch for the same partition on the wire, the broker handles them
    // concurrently, and the log ends up in a different order from the one
    // the application sent.
    this.sendChains = new Map();
    // The first failure of a ticker-driven flush. Those records have left
    // the buffer, so this is the only trace of them; the next flush() or
    // close() rejects with it instead of reporting a success.
    this.backgroundError = null;
    this.ticker =
      config.lingerMs > 0
        ? setInterval(() => {
            // A background flush that fails must not kill the ticker.
            this._flushAll().catch((error) => {
              if (!this.backgroundError) this.backgroundError = error;
            });
          }, config.lingerMs)
        : null;
  }

  static async connect(host, port, overrides = {}) {
    const config = { ...defaultProducerConfig(), ...overrides };
    const router = await Router.connect(host, port, config.clientId);
    return new Producer(router, config);
  }

  /** Flush, then release the ticker and sockets — even if the flush fails. */
  async close() {
    try {
      await this.flush();
    } finally {
      this.closed = true;
      if (this.ticker) clearInterval(this.ticker);
      this.ticker = null;
      this.router.close();
    }
  }

  /** Buffer one record. Call flush() to await delivery. */
  async send(topic, value, { key = null, partition = null, headers = [] } = {}) {
    // Strings travel as UTF-8; null stays null (a tombstone, a null key).
    value = toBytes(value);
    key = toBytes(key, 'key');
    headers = headers.map((header) => new RecordHeader(header.key, toBytes(header.value)));
    let target = partition;
    if (target === null) {
      const partitions = await this.router.partitions(topic);
      if (key === null) {
        target = partitions[this.roundRobin % partitions.length];
        this.roundRobin += 1;
      } else {
        target = partitionForKey(key, partitions);
      }
    }

    const record = { key, value, timestampDelta: 0, headers };
    // A null value is a tombstone and carries no payload bytes.
    let size = (value ? value.length : 0) + (key ? key.length : 0) + 16;
    for (const header of headers) {
      size += header.key.length + (header.value ? header.value.length : 0) + 4;
    }
    await this._reserve(size);

    const slot = `${topic} ${target}`;
    if (!this.buffers.has(slot)) this.buffers.set(slot, []);
    this.buffers.get(slot).push({ record, createdMs: nowMs(), topic, partition: target });
    this.sizes.set(slot, (this.sizes.get(slot) || 0) + size);

    if (this.config.lingerMs === 0 || this.sizes.get(slot) >= this.config.batchSize) {
      await this._flushSlot(slot);
    }
  }

  /** Send one record on its own and return its offset. Slow by design. */
  async sendSync(topic, value, options = {}) {
    const key = toBytes(options.key, 'key');
    const partitions = await this.router.partitions(topic);
    const partition =
      options.partition !== undefined && options.partition !== null
        ? options.partition
        : key !== null
          ? partitionForKey(key, partitions)
          : partitions[this.roundRobin++ % partitions.length];
    return this._produce(topic, partition, [
      {
        record: {
          key,
          value: toBytes(value),
          timestampDelta: 0,
          headers: (options.headers || []).map(
            (header) => new RecordHeader(header.key, toBytes(header.value))
          ),
        },
        createdMs: nowMs(),
      },
    ]);
  }

  /**
   * Send every buffered record and wait for acknowledgement. Also rejects
   * with the failure of any ticker-driven flush since the last call,
   * because those records are gone and nothing else would say so.
   */
  async flush() {
    await this._flushAll();
    const background = this.backgroundError;
    this.backgroundError = null;
    if (background) throw background;
  }

  async _flushAll() {
    const slots = [...this.buffers.keys()].filter((slot) => this.buffers.get(slot).length > 0);
    for (const slot of slots) {
      await this._flushSlot(slot);
    }
  }

  /**
   * Wait until size more bytes may be buffered.
   *
   * This is what makes bufferMemory real: a producer faster than its
   * broker is slowed down here rather than allowed to grow without limit
   * and die holding records nobody has acknowledged.
   */
  async _reserve(size) {
    const limit = this.config.bufferMemory;
    if (limit <= 0 || size >= limit) {
      // A record larger than the whole budget is admitted rather than
      // waiting forever on a condition that can never hold; refusing
      // oversized records is the broker's job (max.message.bytes).
      this.bufferedBytes += size;
      return;
    }
    const deadline = Date.now() + this.config.maxBlockMs;
    while (this.bufferedBytes + size > limit) {
      if (Date.now() >= deadline) {
        throw new BrahmaputraError(
          `producer buffer full: ${this.bufferedBytes} of ${limit} bytes unflushed ` +
            `after maxBlockMs=${this.config.maxBlockMs}`
        );
      }
      await new Promise((resolve) => {
        this.waiters.push(resolve);
        setTimeout(resolve, 20);
      });
    }
    this.bufferedBytes += size;
  }

  _release(size) {
    this.bufferedBytes = Math.max(0, this.bufferedBytes - size);
    const waiters = this.waiters;
    this.waiters = [];
    for (const resolve of waiters) resolve();
  }

  _flushSlot(slot) {
    const previous = this.sendChains.get(slot) || Promise.resolve();
    // The batch is taken when this flush's turn comes, not now, so a batch
    // never overtakes records buffered before it.
    const run = previous.then(() => this._sendSlot(slot));
    const tail = run.catch(() => {});
    this.sendChains.set(slot, tail);
    tail.then(() => {
      if (this.sendChains.get(slot) === tail) this.sendChains.delete(slot);
    });
    return run;
  }

  async _sendSlot(slot) {
    const batch = this.buffers.get(slot);
    if (!batch || batch.length === 0) return;
    this.buffers.set(slot, []);
    const size = this.sizes.get(slot) || 0;
    this.sizes.delete(slot);
    this._release(size);
    await this._produce(batch[0].topic, batch[0].partition, batch);
  }

  async _produce(topic, partition, buffered) {
    if (buffered.length === 0) return -1n;
    // The batch stores one base timestamp and a delta per record, so the
    // rebasing happens here; maxTimestamp becomes the newest record's
    // time, which makes it a truthful answer to "how recent is this batch".
    const maxTimestamp = buffered.reduce((max, item) => Math.max(max, item.createdMs), 0);
    const records = buffered.map((item) => ({
      ...item.record,
      timestampDelta: item.createdMs - maxTimestamp,
    }));

    const encoded = encodeRecordBatch(records, maxTimestamp, this.codec);
    const writer = bodyWriter()
      .string(topic)
      .int32(partition)
      .int32(this.config.acks)
      .int32(this.config.requestTimeoutMs)
      .int64(encoded.length)
      .raw(encoded);
    const body = writer.bytes();

    if (this.config.acks === 0) {
      const connection = await this.router.connectionFor(topic, partition);
      await connection.sendOneway(ApiKey.PRODUCE, body);
      return -1n;
    }

    const deadline = Date.now() + this.config.deliveryTimeoutMs;
    let attemptsLeft = this.config.retries;
    for (;;) {
      const connection = await this.router.connectionFor(topic, partition);
      const reader = bodyReader(await connection.request(ApiKey.PRODUCE, body));
      reader.skipString(); // topic
      reader.skipInt32(); // partition
      const code = reader.int32();
      const baseOffset = reader.int64();
      reader.skipInt64(); // log_append_time_ms
      if (code === ErrorCode.NONE) return baseOffset;

      const outOfTime = Date.now() >= deadline;
      if (!RETRIABLE_ERRORS.has(code) || attemptsLeft <= 0 || outOfTime) {
        throw new ServerError(code, `produce to ${topic}-${partition}`);
      }
      attemptsLeft -= 1;
      if (
        code === ErrorCode.NOT_LEADER_OR_FOLLOWER ||
        code === ErrorCode.FENCED_LEADER_EPOCH ||
        code === ErrorCode.UNKNOWN_LEADER_EPOCH
      ) {
        // A stale route is the most common retriable cause, and resending
        // to the same broker would just repeat it.
        await this.router.refresh(topic);
      }
      await sleep(this.config.retryBackoffMs);
    }
  }
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

const defaultConsumerConfig = () => ({
  clientId: 'brahmaputra-node',
  fetchMaxBytes: 8 * 1024 * 1024,
  fetchMinBytes: 1,
  fetchMaxWaitMs: 500,
  // READ_UNCOMMITTED (0) or READ_COMMITTED (1). A committed read stops at
  // the last stable offset and never sees an aborted transaction's records.
  isolationLevel: 0,
  // This consumer's failure domain (`client.rack`), empty when it has none.
  rack: "",
  maxPollRecords: 500,
});

/** Reads one partition at a time, with no group coordination. */
class Consumer {
  constructor(router, config) {
    this.router = router;
    this.config = config;
  }

  static async connect(host, port, overrides = {}) {
    const config = { ...defaultConsumerConfig(), ...overrides };
    const router = await Router.connect(host, port, config.clientId);
    return new Consumer(router, config);
  }

  close() {
    this.router.close();
  }

  partitions(topic) {
    return this.router.partitions(topic);
  }

  /** Resolve EARLIEST, LATEST or a unix-ms timestamp to an offset. */
  async listOffsets(topic, partition, timestamp) {
    const writer = bodyWriter().string(topic).int32(partition).int64(timestamp);
    const connection = await this.router.connectionFor(topic, partition);
    const reader = bodyReader(await connection.request(ApiKey.LIST_OFFSETS, writer.bytes()));
    reader.skipString(); // topic
    reader.skipInt32(); // partition
    const code = reader.int32();
    const offset = reader.int64();
    reader.skipInt64(); // timestamp
    if (code !== ErrorCode.NONE) {
      throw new ServerError(code, `list_offsets ${topic}-${partition}`);
    }
    return offset;
  }

  async fetch(topic, partition, offset, maxWaitMs = null) {
    const { records } = await this.fetchVerbose(topic, partition, offset, maxWaitMs);
    return records;
  }

  /** Fetch, also returning the partition's high watermark. */
  async fetchVerbose(topic, partition, offset, maxWaitMs = null) {
    const wait = Math.min(
      maxWaitMs === null ? this.config.fetchMaxWaitMs : maxWaitMs,
      this.config.fetchMaxWaitMs
    );
    const writer = bodyWriter()
      .string(topic)
      .int32(partition)
      .int64(offset)
      .int32(this.config.fetchMaxBytes)
      .int32(wait)
      .int32(this.config.fetchMinBytes)
      .int32(this.config.isolationLevel)
      // `client.rack`: with it set the leader names an in-sync replica in
      // the same rack, and this client reads from that instead.
      .string(this.config.rack || "");
    const body = writer.bytes();

    let connection = await this.router.connectionFor(topic, partition);
    let result = this._decodeFetch(await connection.request(ApiKey.FETCH, body));
    if (result.code === ErrorCode.NOT_LEADER_OR_FOLLOWER) {
      await this.router.refresh(topic);
      connection = await this.router.connectionFor(topic, partition);
      result = this._decodeFetch(await connection.request(ApiKey.FETCH, body));
    }
    if (result.code !== ErrorCode.NONE) {
      throw new ServerError(result.code, `fetch ${topic}-${partition}`);
    }

    const records = [];
    for (const batch of result.batches) {
      batch.records.forEach((record, index) => {
        const recordOffset = batch.baseOffset + BigInt(index);
        // A batch can start before the requested offset; skip what the
        // caller has already seen.
        if (recordOffset < BigInt(offset)) return;
        records.push({
          topic,
          partition,
          offset: recordOffset,
          key: record.key,
          value: record.value,
          timestamp: batch.maxTimestamp + record.timestampDelta,
          headers: record.headers,
          header(name) {
            const found = this.headers.find((entry) => entry.key === name);
            return found ? found.value : null;
          },
        });
      });
    }
    return { records, highWatermark: result.highWatermark };
  }

  _decodeFetch(body) {
    const reader = bodyReader(body);
    reader.skipString(); // topic
    reader.skipInt32(); // partition
    const code = reader.int32();
    const highWatermark = reader.int64();
    reader.skipInt64(); // last_stable_offset
    const batchesLength = Number(reader.int64());
    // Read even though this client does not act on it: the batches trail
    // the whole struct, so skipping a field would take them from the
    // wrong offset and every batch after it would fail to decode.
    reader.skipInt32(); // preferred_read_replica
    const trailing = reader.rest();
    if (batchesLength > trailing.length) {
      throw new ProtocolError('fetch response claims more batch bytes than it carries');
    }
    const raw = trailing.subarray(0, batchesLength);

    const batches = [];
    let pos = 0;
    while (pos < raw.length) {
      const { batch, next } = decodeRecordBatch(raw, pos);
      batches.push(batch);
      pos = next;
    }
    return { code, highWatermark, batches };
  }
}

module.exports = {
  COORDINATOR_ATTEMPTS,
  Connection,
  Consumer,
  EARLIEST,
  JOIN_ATTEMPTS,
  LATEST,
  OFFSETS_TOPIC,
  Producer,
  Router,
  defaultConsumerConfig,
  defaultProducerConfig,
  nowMs,
  sleep,
};
