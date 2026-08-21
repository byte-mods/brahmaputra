'use strict';

/**
 * Brahmaputra wire protocol: framing, BitPacker bodies, record batches.
 *
 * Three encodings live in one connection and they do not agree with each
 * other, so keeping them straight is most of the work:
 *
 *  - The frame header is fixed big-endian — an int32 length prefix, then
 *    apiKey/apiVersion/correlationId and a length-prefixed client id.
 *  - A request body is BitPacker: every integer is a zigzag varint, every
 *    string and array is a varint count followed by its contents, and the
 *    whole body is prefixed with the schema version string.
 *  - A record batch is neither. Fixed big-endian header fields and plain
 *    (non-zigzag) varints inside each record, because the broker stamps
 *    offsets into it in place and validates its CRC without decoding it.
 *
 * Mixing those up produces a frame the broker rejects with no useful
 * error, so each encoder here is explicit about which one it is.
 */

const zlib = require('zlib');

/** The BitPacker schema version every body carries as its first field. */
const SCHEMA_VERSION = '1.0.0';
/** Wire version this client speaks. The broker requires an exact match. */
const API_VERSION = 2;

const BATCH_HEADER_LEN = 12;
const MIN_BATCH_LENGTH = 4 + 1 + 4 + 2 + 4 + 8;
const PRODUCER_EXTENSION_LEN = 8 + 2 + 4;

const MAGIC_V1 = 1;
const MAGIC_V2 = 2;

const COMPRESSION_MASK = 0x0007;
const HEADERS_BIT = 0x0008;

const MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024;

const ApiKey = Object.freeze({
  PRODUCE: 0,
  FETCH: 1,
  LIST_OFFSETS: 2,
  METADATA: 3,
  REPLICA_FETCH: 4,
  OFFSETS_FOR_LEADER_EPOCH: 5,
  INIT_PRODUCER_ID: 6,
  JOIN_GROUP: 7,
  SYNC_GROUP: 8,
  HEARTBEAT: 9,
  OFFSET_COMMIT: 10,
  OFFSET_FETCH: 11,
  LIST_GROUPS: 12,
  DESCRIBE_GROUP: 13,
  API_VERSIONS: 14,
  PRODUCE_MULTI: 15,
  FETCH_MULTI: 16,
  AUTHENTICATE: 17,
  LEAVE_GROUP: 18,
});

const ErrorCode = Object.freeze({
  NONE: 0,
  UNKNOWN_TOPIC_OR_PARTITION: 1,
  OFFSET_OUT_OF_RANGE: 2,
  INVALID_REQUEST: 3,
  UNSUPPORTED_VERSION: 4,
  INTERNAL: 5,
  NOT_LEADER_OR_FOLLOWER: 6,
  FENCED_BROKER_EPOCH: 7,
  FENCED_LEADER_EPOCH: 8,
  UNKNOWN_LEADER_EPOCH: 9,
  NOT_ENOUGH_REPLICAS: 10,
  FENCED_PRODUCER_EPOCH: 11,
  OUT_OF_ORDER_SEQUENCE: 12,
  UNKNOWN_MEMBER_ID: 13,
  REBALANCE_IN_PROGRESS: 14,
  NOT_COORDINATOR: 15,
  ILLEGAL_GENERATION: 16,
  COORDINATOR_LOAD_IN_PROGRESS: 17,
  SASL_AUTHENTICATION_FAILED: 18,
  AUTHORIZATION_FAILED: 19,
});

const ERROR_NAMES = Object.fromEntries(
  Object.entries(ErrorCode).map(([name, code]) => [code, name])
);

/**
 * Codes the broker only ever returns *before* it appends anything, so a
 * retry cannot duplicate a record. Anything not here is returned to the
 * caller as-is: a malformed request or a failed authorization fails
 * identically however often it is sent, and the idempotence errors mean
 * the producer's sequence state is already broken.
 */
const RETRIABLE_ERRORS = new Set([
  ErrorCode.NOT_LEADER_OR_FOLLOWER,
  ErrorCode.FENCED_LEADER_EPOCH,
  ErrorCode.UNKNOWN_LEADER_EPOCH,
  ErrorCode.NOT_ENOUGH_REPLICAS,
  ErrorCode.COORDINATOR_LOAD_IN_PROGRESS,
  ErrorCode.INTERNAL,
]);

class BrahmaputraError extends Error {}

class ProtocolError extends BrahmaputraError {}

class ServerError extends BrahmaputraError {
  constructor(code, context = '') {
    const name = ERROR_NAMES[code] || 'UNKNOWN';
    super(`broker returned ${name}[${code}]${context ? ` (${context})` : ''}`);
    this.code = code;
  }
}

class NoOffsetForPartition extends BrahmaputraError {}

// ---------------------------------------------------------------------------
// BitPacker primitives
// ---------------------------------------------------------------------------

/**
 * Builds a BitPacker body. Every integer goes out zigzag-varint encoded,
 * which is why this cannot share code with the record-batch encoder below.
 */
class Writer {
  constructor() {
    this.chunks = [];
  }

  raw(buffer) {
    this.chunks.push(buffer);
    return this;
  }

  uvarint(value) {
    // BigInt throughout: an int64 offset does not fit a JS number once a
    // partition passes 2^53 records, and silently losing precision on an
    // offset is the kind of bug that only shows up in production.
    let remaining = BigInt(value);
    const bytes = [];
    while (remaining >= 0x80n) {
      bytes.push(Number((remaining & 0x7fn) | 0x80n));
      remaining >>= 7n;
    }
    bytes.push(Number(remaining));
    this.chunks.push(Buffer.from(bytes));
    return this;
  }

  int32(value) {
    const v = BigInt.asIntN(32, BigInt(value));
    return this.uvarint(BigInt.asUintN(32, (v << 1n) ^ (v >> 31n)));
  }

  int64(value) {
    const v = BigInt.asIntN(64, BigInt(value));
    return this.uvarint(BigInt.asUintN(64, (v << 1n) ^ (v >> 63n)));
  }

  bool(value) {
    this.chunks.push(Buffer.from([value ? 1 : 0]));
    return this;
  }

  string(value) {
    const encoded = Buffer.from(value, 'utf8');
    this.int32(encoded.length);
    this.chunks.push(encoded);
    return this;
  }

  stringArray(values) {
    this.int32(values.length);
    for (const value of values) this.string(value);
    return this;
  }

  bytes() {
    return Buffer.concat(this.chunks);
  }
}

/** Reads a BitPacker body. */
class Reader {
  constructor(data) {
    this.data = data;
    this.pos = 0;
  }

  get remaining() {
    return this.data.length - this.pos;
  }

  uvarint() {
    let result = 0n;
    let shift = 0n;
    for (;;) {
      if (this.pos >= this.data.length) throw new ProtocolError('truncated varint');
      const byte = this.data[this.pos++];
      result |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) return result;
      shift += 7n;
      if (shift > 63n) throw new ProtocolError('varint overflows 64 bits');
    }
  }

  int32() {
    const raw = this.uvarint();
    return Number(BigInt.asIntN(32, (raw >> 1n) ^ -(raw & 1n)));
  }

  /** Returns a BigInt: an offset can exceed Number.MAX_SAFE_INTEGER. */
  int64() {
    const raw = this.uvarint();
    return BigInt.asIntN(64, (raw >> 1n) ^ -(raw & 1n));
  }

  bool() {
    if (this.pos >= this.data.length) throw new ProtocolError('truncated bool');
    return this.data[this.pos++] !== 0;
  }

  string() {
    const length = this.int32();
    if (length < 0 || this.pos + length > this.data.length) {
      throw new ProtocolError('truncated string');
    }
    const value = this.data.toString('utf8', this.pos, this.pos + length);
    this.pos += length;
    return value;
  }

  stringArray() {
    const count = this.int32();
    const out = [];
    for (let i = 0; i < count; i += 1) out.push(this.string());
    return out;
  }

  rest() {
    const value = this.data.subarray(this.pos);
    this.pos = this.data.length;
    return value;
  }

  /** Read and discard a field. The encoding is positional, so a skipped
   * field must still be *read* or everything after it misaligns. */
  skipString() { this.string(); }
  skipInt32() { this.int32(); }
  skipInt64() { this.int64(); }
}

/** A writer already carrying the schema version every body starts with. */
function bodyWriter() {
  return new Writer().string(SCHEMA_VERSION);
}

/**
 * A reader positioned past the schema version, which is verified. A
 * mismatch means broker and client disagree about the message shapes
 * themselves, so failing loudly beats decoding garbage into
 * plausible-looking fields.
 */
function bodyReader(data) {
  const reader = new Reader(data);
  const version = reader.string();
  if (version !== SCHEMA_VERSION) {
    throw new ProtocolError(
      `schema version mismatch: broker speaks ${version}, this client speaks ${SCHEMA_VERSION}`
    );
  }
  return reader;
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/**
 * One complete frame, length prefix included.
 *
 * The header is fixed big-endian while the body is BitPacker: the broker
 * has to read the header before it knows which body decoder to use, so the
 * header cannot depend on the schema.
 */
function encodeFrame(apiKey, correlationId, clientId, body) {
  const client = clientId === null ? null : Buffer.from(clientId, 'utf8');
  const clientLen = client === null ? 0 : client.length;
  const payload = Buffer.allocUnsafe(10 + clientLen);
  payload.writeInt16BE(apiKey, 0);
  payload.writeInt16BE(API_VERSION, 2);
  payload.writeInt32BE(correlationId, 4);
  payload.writeInt16BE(client === null ? -1 : clientLen, 8);
  if (client !== null) client.copy(payload, 10);

  const prefix = Buffer.allocUnsafe(4);
  prefix.writeInt32BE(payload.length + body.length, 0);
  return Buffer.concat([prefix, payload, body]);
}

/** Split a frame payload into its correlation id and body. */
function decodeFramePayload(payload) {
  if (payload.length < 10) throw new ProtocolError('frame payload shorter than its header');
  const correlationId = payload.readInt32BE(4);
  const clientLen = payload.readInt16BE(8);
  let offset = 10;
  if (clientLen >= 0) offset += clientLen;
  if (offset > payload.length) throw new ProtocolError('frame client id runs past the payload');
  return { correlationId, body: payload.subarray(offset) };
}

// ---------------------------------------------------------------------------
// CRC32C (Castagnoli)
// ---------------------------------------------------------------------------

const CRC32C_TABLE = (() => {
  // Castagnoli polynomial, reflected. Record batches use CRC32C rather
  // than the zlib CRC32, so zlib.crc32 is no help here.
  const poly = 0x82f63b78;
  const table = new Int32Array(256);
  for (let index = 0; index < 256; index += 1) {
    let crc = index;
    for (let bit = 0; bit < 8; bit += 1) {
      crc = crc & 1 ? (crc >>> 1) ^ poly : crc >>> 1;
    }
    table[index] = crc;
  }
  return table;
})();

function crc32c(data) {
  let crc = 0xffffffff;
  for (let index = 0; index < data.length; index += 1) {
    crc = CRC32C_TABLE[(crc ^ data[index]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

const Compression = Object.freeze({
  NONE: 0,
  LZ4: 1,
  ZSTD: 2,
  SNAPPY: 3,
  GZIP: 4,
});

const COMPRESSION_NAMES = {
  none: Compression.NONE,
  lz4: Compression.LZ4,
  zstd: Compression.ZSTD,
  snappy: Compression.SNAPPY,
  gzip: Compression.GZIP,
};

function parseCompression(name) {
  const codec = COMPRESSION_NAMES[name];
  if (codec === undefined) {
    throw new BrahmaputraError(
      `unknown compression ${name} (${Object.keys(COMPRESSION_NAMES).join(', ')})`
    );
  }
  return codec;
}

function compressionName(codec) {
  const found = Object.entries(COMPRESSION_NAMES).find(([, value]) => value === codec);
  return found ? found[0] : `unknown(${codec})`;
}

/**
 * Codecs beyond none/gzip/zstd are opt-in, so an application that does not
 * want an extra dependency does not carry one.
 *
 * The lz4 payload the broker expects is a little-endian uint32 of the
 * uncompressed length followed by a raw LZ4 block — not the LZ4 frame
 * format — so a frame-format library will not interoperate.
 */
const externalCodecs = new Map();

function registerCodec(codec, { compress: compressFn, decompress: decompressFn }) {
  externalCodecs.set(codec, { compress: compressFn, decompress: decompressFn });
}

function compress(codec, payload) {
  switch (codec) {
    case Compression.NONE:
      return payload;
    case Compression.GZIP:
      return zlib.gzipSync(payload);
    case Compression.ZSTD:
      // Node 22 ships zstd in zlib; older runtimes fall back to a
      // registered codec if the application supplied one.
      if (typeof zlib.zstdCompressSync === 'function') return zlib.zstdCompressSync(payload);
      break;
    default:
      break;
  }
  const external = externalCodecs.get(codec);
  if (external) return external.compress(payload);
  throw new BrahmaputraError(
    `${compressionName(codec)} compression is not available; ` +
      'register it with registerCodec() or use none/gzip'
  );
}

function decompress(codec, payload) {
  switch (codec) {
    case Compression.NONE:
      return payload;
    case Compression.GZIP:
      // Capped so a corrupt or hostile batch cannot name gigabytes of
      // output this process allocates before it can reject it.
      return zlib.gunzipSync(payload, { maxOutputLength: MAX_DECOMPRESSED_BYTES });
    case Compression.ZSTD:
      if (typeof zlib.zstdDecompressSync === 'function') {
        return zlib.zstdDecompressSync(payload, { maxOutputLength: MAX_DECOMPRESSED_BYTES });
      }
      break;
    default:
      break;
  }
  const external = externalCodecs.get(codec);
  if (external) return external.decompress(payload);
  throw new BrahmaputraError(
    `${compressionName(codec)} decompression is not available; register it with registerCodec()`
  );
}

// ---------------------------------------------------------------------------
// Record batches
// ---------------------------------------------------------------------------

/** An ordered, possibly repeating annotation on a record. */
class RecordHeader {
  constructor(key, value = null) {
    this.key = key;
    this.value = value;
  }
}

function putUvarint(bytes, value) {
  let remaining = BigInt(value);
  while (remaining >= 0x80n) {
    bytes.push(Number((remaining & 0x7fn) | 0x80n));
    remaining >>= 7n;
  }
  bytes.push(Number(remaining));
}

function getUvarint(data, pos) {
  let result = 0n;
  let shift = 0n;
  for (;;) {
    if (pos >= data.length) throw new ProtocolError('truncated varint in record');
    const byte = data[pos];
    pos += 1;
    result |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) return [result, pos];
    shift += 7n;
    if (shift > 63n) throw new ProtocolError('varint overflows 64 bits');
  }
}

/**
 * Encode one record batch exactly as the broker expects it.
 *
 * The broker never re-encodes this: it validates the header, stamps
 * baseOffset and leaderEpoch in place (both sit before the CRC, so it
 * stays valid), and writes these bytes to disk. Getting this wrong
 * corrupts the log rather than merely failing a request.
 */
function encodeRecordBatch(records, maxTimestamp, codec = Compression.NONE) {
  const hasHeaders = records.some((record) => record.headers && record.headers.length > 0);

  const payloadParts = [];
  for (const record of records) {
    const rec = [];
    if (record.key === null || record.key === undefined) {
      putUvarint(rec, 0);
    } else {
      putUvarint(rec, record.key.length + 1);
      rec.push(...record.key);
    }
    putUvarint(rec, record.value.length);
    rec.push(...record.value);
    const delta = BigInt.asIntN(64, BigInt(record.timestampDelta || 0));
    putUvarint(rec, BigInt.asUintN(64, (delta << 1n) ^ (delta >> 63n)));
    if (hasHeaders) {
      const headers = record.headers || [];
      putUvarint(rec, headers.length);
      for (const header of headers) {
        const key = Buffer.from(header.key, 'utf8');
        putUvarint(rec, key.length);
        rec.push(...key);
        if (header.value === null || header.value === undefined) {
          putUvarint(rec, 0);
        } else {
          putUvarint(rec, header.value.length + 1);
          rec.push(...header.value);
        }
      }
    }
    const lengthPrefix = [];
    putUvarint(lengthPrefix, rec.length);
    payloadParts.push(Buffer.from(lengthPrefix), Buffer.from(rec));
  }

  const compressed = compress(codec, Buffer.concat(payloadParts));
  let attributes = codec & COMPRESSION_MASK;
  if (hasHeaders) attributes |= HEADERS_BIT;

  const batchLength = MIN_BATCH_LENGTH + compressed.length;
  const head = Buffer.allocUnsafe(BATCH_HEADER_LEN + MIN_BATCH_LENGTH);
  head.writeBigInt64BE(0n, 0); // base_offset, stamped by the broker
  head.writeInt32BE(batchLength, 8);
  head.writeInt32BE(0, 12); // leader_epoch, likewise
  head.writeUInt8(MAGIC_V1, 16);
  head.writeUInt32BE(0, 17); // crc placeholder
  head.writeUInt16BE(attributes, 21);
  head.writeInt32BE(Math.max(records.length - 1, 0), 23);
  head.writeBigInt64BE(BigInt(maxTimestamp), 27);

  const out = Buffer.concat([head, compressed]);
  out.writeUInt32BE(crc32c(out.subarray(21)), 17);
  return out;
}

/** Decode one batch starting at offset; returns it and the next offset. */
function decodeRecordBatch(data, offset) {
  if (data.length - offset < BATCH_HEADER_LEN) throw new ProtocolError('truncated batch header');
  const baseOffset = data.readBigInt64BE(offset);
  const batchLength = data.readInt32BE(offset + 8);
  if (batchLength < MIN_BATCH_LENGTH) throw new ProtocolError('batch_length too small');
  const bodyAt = offset + BATCH_HEADER_LEN;
  const end = bodyAt + batchLength;
  if (end > data.length) throw new ProtocolError('truncated batch body');

  const magic = data.readUInt8(bodyAt + 4);
  if (magic !== MAGIC_V1 && magic !== MAGIC_V2) {
    throw new ProtocolError(`unsupported magic ${magic}`);
  }
  const crcAt = bodyAt + 5;
  const stored = data.readUInt32BE(crcAt);
  const computed = crc32c(data.subarray(crcAt + 4, end));
  if (stored !== computed) {
    throw new ProtocolError(
      `crc mismatch: stored 0x${stored.toString(16)}, computed 0x${computed.toString(16)}`
    );
  }

  let cursor = crcAt + 4;
  const attributes = data.readUInt16BE(cursor);
  const maxTimestamp = data.readBigInt64BE(cursor + 6);
  cursor += 14;
  if (magic === MAGIC_V2) cursor += PRODUCER_EXTENSION_LEN;

  const payload = decompress(attributes & COMPRESSION_MASK, data.subarray(cursor, end));
  const records = decodeRecords(payload, (attributes & HEADERS_BIT) !== 0);
  return { batch: { baseOffset, maxTimestamp, records }, next: end };
}

function decodeRecords(payload, hasHeaders) {
  const records = [];
  let pos = 0;
  while (pos < payload.length) {
    let length;
    [length, pos] = getUvarint(payload, pos);
    const size = Number(length);
    if (pos + size > payload.length) throw new ProtocolError('truncated record');
    const end = pos + size;

    let keyLenPlusOne;
    [keyLenPlusOne, pos] = getUvarint(payload, pos);
    let key = null;
    if (keyLenPlusOne > 0n) {
      const keyLen = Number(keyLenPlusOne) - 1;
      key = Buffer.from(payload.subarray(pos, pos + keyLen));
      pos += keyLen;
    }

    let valueLen;
    [valueLen, pos] = getUvarint(payload, pos);
    const value = Buffer.from(payload.subarray(pos, pos + Number(valueLen)));
    pos += Number(valueLen);

    let rawDelta;
    [rawDelta, pos] = getUvarint(payload, pos);
    const timestampDelta = BigInt.asIntN(64, (rawDelta >> 1n) ^ -(rawDelta & 1n));

    const headers = [];
    if (hasHeaders) {
      let count;
      [count, pos] = getUvarint(payload, pos);
      // A count is a promise about bytes that follow; if it exceeds what
      // is left it is corrupt, and allocating on it would let a two-byte
      // record ask for gigabytes.
      if (count > BigInt(end - pos)) {
        throw new ProtocolError('record header count exceeds record');
      }
      for (let index = 0n; index < count; index += 1n) {
        let keyLen;
        [keyLen, pos] = getUvarint(payload, pos);
        const headerKey = payload.toString('utf8', pos, pos + Number(keyLen));
        pos += Number(keyLen);
        let valuePlusOne;
        [valuePlusOne, pos] = getUvarint(payload, pos);
        let headerValue = null;
        if (valuePlusOne > 0n) {
          const headerLen = Number(valuePlusOne) - 1;
          headerValue = Buffer.from(payload.subarray(pos, pos + headerLen));
          pos += headerLen;
        }
        headers.push(new RecordHeader(headerKey, headerValue));
      }
    }

    if (pos !== end) throw new ProtocolError('trailing bytes in record');
    records.push({ key, value, timestampDelta, headers });
  }
  return records;
}

// ---------------------------------------------------------------------------
// Partitioning
// ---------------------------------------------------------------------------

/**
 * Kafka's 32-bit murmur2, so a key lands on the same partition here.
 *
 * Reproduced rather than imported because the point is that a Node
 * producer and a Rust producer writing the same key must agree, and "some
 * murmur2" is not good enough — it has to be this one.
 */
function murmur2(data) {
  const seed = 0x9747b28c;
  const m = 0x5bd1e995;
  const r = 24;

  const length = data.length;
  let h = (seed ^ length) | 0;
  const chunks = Math.floor(length / 4);

  for (let index = 0; index < chunks; index += 1) {
    const offset = index * 4;
    let k =
      (data[offset] |
        (data[offset + 1] << 8) |
        (data[offset + 2] << 16) |
        (data[offset + 3] << 24)) |
      0;
    k = Math.imul(k, m) | 0;
    k ^= k >>> r;
    k = Math.imul(k, m) | 0;
    h = Math.imul(h, m) | 0;
    h ^= k;
  }

  const tail = chunks * 4;
  switch (length - tail) {
    case 3:
      h ^= data[tail + 2] << 16;
      h ^= data[tail + 1] << 8;
      h ^= data[tail];
      h = Math.imul(h, m) | 0;
      break;
    case 2:
      h ^= data[tail + 1] << 8;
      h ^= data[tail];
      h = Math.imul(h, m) | 0;
      break;
    case 1:
      h ^= data[tail];
      h = Math.imul(h, m) | 0;
      break;
    default:
      break;
  }

  h ^= h >>> 13;
  h = Math.imul(h, m) | 0;
  h ^= h >>> 15;
  return h >>> 0;
}

/** murmur2(key) % partitions, matching Kafka's default partitioner. */
function partitionForKey(key, partitions) {
  return partitions[(murmur2(key) & 0x7fffffff) % partitions.length];
}

module.exports = {
  API_VERSION,
  ApiKey,
  BATCH_HEADER_LEN,
  BrahmaputraError,
  Compression,
  ErrorCode,
  NoOffsetForPartition,
  ProtocolError,
  RETRIABLE_ERRORS,
  Reader,
  RecordHeader,
  SCHEMA_VERSION,
  ServerError,
  Writer,
  bodyReader,
  bodyWriter,
  compressionName,
  crc32c,
  decodeFramePayload,
  decodeRecordBatch,
  encodeFrame,
  encodeRecordBatch,
  murmur2,
  parseCompression,
  partitionForKey,
  registerCodec,
};
