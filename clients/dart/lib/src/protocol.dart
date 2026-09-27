/// Brahmaputra wire protocol: framing, BitPacker bodies, record batches.
///
/// Three encodings share one connection and none of them agrees with the
/// others, so each encoder here is explicit about which one it is:
///
///  * The frame header is fixed big-endian: an int32 length prefix, then
///    apiKey/apiVersion/correlationId and an int16-prefixed client id.
///  * A request/response body is BitPacker: every integer is a zigzag
///    varint, every string and array is a varint count followed by its
///    contents, and the whole body starts with the schema version string.
///  * A record batch is neither: fixed big-endian header fields and plain
///    (non-zigzag) varints inside each record, because the broker stamps
///    offsets into it in place and validates its CRC without decoding it.
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

/// The BitPacker schema version every body carries as its first field.
const String schemaVersion = '1.0.0';

/// Wire version this client speaks. The broker requires an exact match.
const int apiVersion = 4;

/// Fetch isolation levels.
const int readUncommitted = 0;
const int readCommitted = 1;

const int _batchHeaderLen = 12;
const int _minBatchLength = 4 + 1 + 4 + 2 + 4 + 8;
const int _producerExtensionLen = 8 + 2 + 4;
const int _magicV1 = 1;
const int _magicV2 = 2;
const int _compressionMask = 0x0007;
const int _headersBit = 0x0008;
const int _nullValueBit = 0x0040;

/// Upper bound on a decompressed batch, so a corrupt or hostile batch cannot
/// make this process allocate gigabytes before rejecting it.
const int maxDecompressedBytes = 256 * 1024 * 1024;

/// API keys, as numbered in schemas/protocol.buff.
abstract final class ApiKey {
  static const int produce = 0;
  static const int fetch = 1;
  static const int listOffsets = 2;
  static const int metadata = 3;
  static const int joinGroup = 7;
  static const int syncGroup = 8;
  static const int heartbeat = 9;
  static const int offsetCommit = 10;
  static const int offsetFetch = 11;
  static const int apiVersions = 14;
  static const int authenticate = 17;
  static const int leaveGroup = 18;
}

/// Broker error codes.
abstract final class ErrorCode {
  static const int none = 0;
  static const int unknownTopicOrPartition = 1;
  static const int offsetOutOfRange = 2;
  static const int invalidRequest = 3;
  static const int unsupportedVersion = 4;
  static const int internal = 5;
  static const int notLeaderOrFollower = 6;
  static const int fencedBrokerEpoch = 7;
  static const int fencedLeaderEpoch = 8;
  static const int unknownLeaderEpoch = 9;
  static const int notEnoughReplicas = 10;
  static const int fencedProducerEpoch = 11;
  static const int outOfOrderSequence = 12;
  static const int unknownMemberId = 13;
  static const int rebalanceInProgress = 14;
  static const int notCoordinator = 15;
  static const int illegalGeneration = 16;
  static const int coordinatorLoadInProgress = 17;
  static const int saslAuthenticationFailed = 18;
  static const int authorizationFailed = 19;

  static const Map<int, String> names = {
    0: 'NONE',
    1: 'UNKNOWN_TOPIC_OR_PARTITION',
    2: 'OFFSET_OUT_OF_RANGE',
    3: 'INVALID_REQUEST',
    4: 'UNSUPPORTED_VERSION',
    5: 'INTERNAL',
    6: 'NOT_LEADER_OR_FOLLOWER',
    7: 'FENCED_BROKER_EPOCH',
    8: 'FENCED_LEADER_EPOCH',
    9: 'UNKNOWN_LEADER_EPOCH',
    10: 'NOT_ENOUGH_REPLICAS',
    11: 'FENCED_PRODUCER_EPOCH',
    12: 'OUT_OF_ORDER_SEQUENCE',
    13: 'UNKNOWN_MEMBER_ID',
    14: 'REBALANCE_IN_PROGRESS',
    15: 'NOT_COORDINATOR',
    16: 'ILLEGAL_GENERATION',
    17: 'COORDINATOR_LOAD_IN_PROGRESS',
    18: 'SASL_AUTHENTICATION_FAILED',
    19: 'AUTHORIZATION_FAILED',
  };

  /// Codes the broker only returns *before* appending, so a retry cannot
  /// duplicate a record.
  static const Set<int> retriable = {
    notLeaderOrFollower,
    fencedLeaderEpoch,
    unknownLeaderEpoch,
    notEnoughReplicas,
    coordinatorLoadInProgress,
    internal,
  };
}

/// Base class of every error this driver throws.
class BrahmaputraException implements Exception {
  BrahmaputraException(this.message);
  final String message;
  @override
  String toString() => 'BrahmaputraException: $message';
}

/// The bytes on the wire could not be decoded.
class ProtocolException extends BrahmaputraException {
  ProtocolException(super.message);
  @override
  String toString() => 'ProtocolException: $message';
}

/// The broker answered with a non-zero error code.
class ServerException extends BrahmaputraException {
  ServerException(this.code, [String context = ''])
      : super('broker returned ${ErrorCode.names[code] ?? 'UNKNOWN'}[$code]'
            '${context.isEmpty ? '' : ' ($context)'}');
  final int code;
  @override
  String toString() => 'ServerException: $message';
}

/// auto.offset.reset=none and the group has no committed position.
class NoOffsetForPartitionException extends BrahmaputraException {
  NoOffsetForPartitionException(super.message);
  @override
  String toString() => 'NoOffsetForPartitionException: $message';
}

/// A request did not get its response within the request timeout.
class RequestTimeoutException extends BrahmaputraException {
  RequestTimeoutException(super.message);
  @override
  String toString() => 'RequestTimeoutException: $message';
}

// ---------------------------------------------------------------------------
// BitPacker
// ---------------------------------------------------------------------------

void _putUvarint(BytesBuilder out, int value) {
  // Unsigned 64-bit in a Dart int: `>>>` so a "negative" (high-bit) value
  // still terminates.
  var v = value;
  while ((v & ~0x7f) != 0) {
    out.addByte((v & 0x7f) | 0x80);
    v = v >>> 7;
  }
  out.addByte(v);
}

int _zigzag64(int v) => (v << 1) ^ (v >> 63);
int _unzigzag(int raw) => (raw >>> 1) ^ -(raw & 1);

/// Builds a BitPacker body. Every integer is zigzag-varint encoded.
class Writer {
  final BytesBuilder _out = BytesBuilder(copy: false);

  void raw(List<int> bytes) => _out.add(bytes);

  void uvarint(int value) => _putUvarint(_out, value);

  void int32(int value) {
    final v = value.toSigned(32);
    uvarint(((v << 1) ^ (v >> 31)) & 0xffffffff);
  }

  void int64(int value) => uvarint(_zigzag64(value));

  void boolean(bool value) => _out.addByte(value ? 1 : 0);

  void string(String value) {
    final encoded = utf8.encode(value);
    int32(encoded.length);
    _out.add(encoded);
  }

  void stringArray(List<String> values) {
    int32(values.length);
    for (final value in values) {
      string(value);
    }
  }

  Uint8List bytes() => _out.toBytes();
}

/// Reads a BitPacker body. Every read is bounds-checked.
class Reader {
  Reader(this.data);
  final Uint8List data;
  int pos = 0;

  int get remaining => data.length - pos;

  int uvarint() {
    var result = 0;
    var shift = 0;
    while (true) {
      if (pos >= data.length) throw ProtocolException('truncated varint');
      final byte = data[pos++];
      result |= (byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) return result;
      shift += 7;
      if (shift > 63) throw ProtocolException('varint overflows 64 bits');
    }
  }

  int int32() => _unzigzag(uvarint()).toSigned(32);

  int int64() => _unzigzag(uvarint());

  bool boolean() {
    if (pos >= data.length) throw ProtocolException('truncated bool');
    return data[pos++] != 0;
  }

  String string() {
    final length = int32();
    if (length < 0 || length > remaining) {
      throw ProtocolException('truncated string');
    }
    final value = utf8.decode(Uint8List.sublistView(data, pos, pos + length));
    pos += length;
    return value;
  }

  /// An array count, refused if it is negative or cannot possibly fit in
  /// what is left (every element takes at least one byte).
  int count() {
    final n = int32();
    if (n < 0 || n > remaining) throw ProtocolException('bad array count $n');
    return n;
  }

  List<String> stringArray() {
    final n = count();
    return [for (var i = 0; i < n; i++) string()];
  }

  Uint8List rest() {
    final value = Uint8List.sublistView(data, pos);
    pos = data.length;
    return value;
  }
}

/// A writer already carrying the schema version.
Writer bodyWriter() => Writer()..string(schemaVersion);

/// A reader positioned past the schema version, which is verified.
Reader bodyReader(Uint8List data) {
  final reader = Reader(data);
  final version = reader.string();
  if (version != schemaVersion) {
    throw ProtocolException('schema version mismatch: broker speaks $version, '
        'this client speaks $schemaVersion');
  }
  return reader;
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// One complete frame, length prefix included.
Uint8List encodeFrame(
    int apiKey, int correlationId, String? clientId, List<int> body) {
  final client = clientId == null ? null : utf8.encode(clientId);
  final clientLen = client?.length ?? 0;
  final payloadLen = 10 + clientLen + body.length;
  final out = Uint8List(4 + payloadLen);
  final view = ByteData.sublistView(out);
  view.setInt32(0, payloadLen);
  view.setInt16(4, apiKey);
  view.setInt16(6, apiVersion);
  view.setInt32(8, correlationId);
  view.setInt16(12, client == null ? -1 : clientLen);
  if (client != null) out.setRange(14, 14 + clientLen, client);
  out.setRange(14 + clientLen, out.length, body);
  return out;
}

/// Split a frame payload (after the length prefix) into correlation id and body.
(int, Uint8List) decodeFramePayload(Uint8List payload) {
  if (payload.length < 10) {
    throw ProtocolException('frame payload shorter than its header');
  }
  final view = ByteData.sublistView(payload);
  final correlationId = view.getInt32(4);
  final clientLen = view.getInt16(8);
  var offset = 10;
  if (clientLen >= 0) offset += clientLen;
  if (offset > payload.length) {
    throw ProtocolException('frame client id runs past the payload');
  }
  return (correlationId, Uint8List.sublistView(payload, offset));
}

// ---------------------------------------------------------------------------
// CRC32C (Castagnoli) — not the zlib CRC32.
// ---------------------------------------------------------------------------

final Uint32List _crcTable = () {
  const poly = 0x82f63b78;
  final table = Uint32List(256);
  for (var i = 0; i < 256; i++) {
    var crc = i;
    for (var bit = 0; bit < 8; bit++) {
      crc = (crc & 1) != 0 ? (crc >>> 1) ^ poly : crc >>> 1;
    }
    table[i] = crc;
  }
  return table;
}();

int crc32c(List<int> data, [int start = 0, int? end]) {
  var crc = 0xffffffff;
  final stop = end ?? data.length;
  for (var i = start; i < stop; i++) {
    crc = _crcTable[(crc ^ data[i]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) & 0xffffffff;
}

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

/// Compression codec ids as they appear in a batch's attributes.
abstract final class Compression {
  static const int none = 0;
  static const int lz4 = 1;
  static const int zstd = 2;
  static const int snappy = 3;
  static const int gzip = 4;

  static const Map<String, int> byName = {
    'none': none,
    'lz4': lz4,
    'zstd': zstd,
    'snappy': snappy,
    'gzip': gzip,
  };

  static int parse(String name) {
    final codec = byName[name];
    if (codec == null) {
      throw BrahmaputraException(
          'unknown compression $name (${byName.keys.join(', ')})');
    }
    return codec;
  }

  static String nameOf(int codec) => byName.entries
      .firstWhere((e) => e.value == codec,
          orElse: () => MapEntry('unknown($codec)', codec))
      .key;
}

/// A pluggable codec: compress and decompress one batch payload.
class Codec {
  const Codec(this.compress, this.decompress);
  final List<int> Function(List<int> payload) compress;
  final List<int> Function(List<int> payload) decompress;
}

final Map<int, Codec> _externalCodecs = {};

/// Register a codec for lz4, zstd or snappy (only none and gzip are built in).
///
/// The lz4 payload the broker expects is a little-endian uint32 of the
/// uncompressed length followed by a raw LZ4 *block*, not the LZ4 frame
/// format.
void registerCodec(int codec, Codec implementation) {
  _externalCodecs[codec] = implementation;
}

Uint8List _asBytes(List<int> data) =>
    data is Uint8List ? data : Uint8List.fromList(data);

Uint8List compressPayload(int codec, Uint8List payload) {
  switch (codec) {
    case Compression.none:
      return payload;
    case Compression.gzip:
      return _asBytes(GZipCodec().encode(payload));
  }
  final external = _externalCodecs[codec];
  if (external != null) return _asBytes(external.compress(payload));
  throw BrahmaputraException(
      '${Compression.nameOf(codec)} compression is not available; '
      'register it with registerCodec() or use none/gzip');
}

class _CappedSink implements Sink<List<int>> {
  final BytesBuilder out = BytesBuilder(copy: true);
  @override
  void add(List<int> data) {
    out.add(data);
    if (out.length > maxDecompressedBytes) {
      throw ProtocolException(
          'decompressed batch exceeds $maxDecompressedBytes bytes');
    }
  }

  @override
  void close() {}
}

Uint8List decompressPayload(int codec, Uint8List payload) {
  switch (codec) {
    case Compression.none:
      return payload;
    case Compression.gzip:
      final sink = _CappedSink();
      try {
        final input = GZipCodec().decoder.startChunkedConversion(sink);
        const chunk = 64 * 1024;
        for (var i = 0; i < payload.length; i += chunk) {
          final end =
              i + chunk < payload.length ? i + chunk : payload.length;
          input.add(Uint8List.sublistView(payload, i, end));
        }
        input.close();
      } on ProtocolException {
        rethrow;
      } catch (e) {
        throw ProtocolException('gzip decode failed: $e');
      }
      return sink.out.toBytes();
  }
  final external = _externalCodecs[codec];
  if (external != null) return _asBytes(external.decompress(payload));
  throw BrahmaputraException(
      '${Compression.nameOf(codec)} decompression is not available; '
      'register it with registerCodec()');
}

// ---------------------------------------------------------------------------
// Record batches
// ---------------------------------------------------------------------------

/// An ordered, possibly repeating annotation on a record. A null value is
/// distinct from an empty one.
class RecordHeader {
  const RecordHeader(this.key, [this.value]);
  final String key;
  final List<int>? value;
  @override
  String toString() => 'RecordHeader($key, $value)';
}

/// One record inside a batch, as encoded on the wire.
class Record {
  Record({this.key, this.value, this.timestampDelta = 0, List<RecordHeader>? headers})
      : headers = headers ?? const [];
  final List<int>? key;

  /// Null is a tombstone; an empty list is an ordinary empty value.
  final List<int>? value;
  int timestampDelta;
  final List<RecordHeader> headers;
}

/// A decoded batch.
class RecordBatch {
  RecordBatch(this.baseOffset, this.maxTimestamp, this.records);
  final int baseOffset;
  final int maxTimestamp;
  final List<Record> records;
}

/// Encode one batch exactly as the broker stores it.
Uint8List encodeRecordBatch(List<Record> records, int maxTimestamp,
    [int codec = Compression.none]) {
  final hasHeaders = records.any((r) => r.headers.isNotEmpty);
  final hasNullValues = records.any((r) => r.value == null);

  final payload = BytesBuilder(copy: false);
  for (final record in records) {
    final body = BytesBuilder(copy: false);
    final key = record.key;
    if (key == null) {
      _putUvarint(body, 0);
    } else {
      _putUvarint(body, key.length + 1);
      body.add(key);
    }
    final value = record.value;
    if (hasNullValues) {
      if (value == null) {
        _putUvarint(body, 0);
      } else {
        _putUvarint(body, value.length + 1);
        body.add(value);
      }
    } else {
      _putUvarint(body, value!.length);
      body.add(value);
    }
    _putUvarint(body, _zigzag64(record.timestampDelta));
    if (hasHeaders) {
      _putUvarint(body, record.headers.length);
      for (final header in record.headers) {
        final headerKey = utf8.encode(header.key);
        _putUvarint(body, headerKey.length);
        body.add(headerKey);
        final headerValue = header.value;
        if (headerValue == null) {
          _putUvarint(body, 0);
        } else {
          _putUvarint(body, headerValue.length + 1);
          body.add(headerValue);
        }
      }
    }
    _putUvarint(payload, body.length);
    payload.add(body.takeBytes());
  }

  final compressed = compressPayload(codec, payload.toBytes());
  var attributes = codec & _compressionMask;
  if (hasHeaders) attributes |= _headersBit;
  if (hasNullValues) attributes |= _nullValueBit;

  final batchLength = _minBatchLength + compressed.length;
  final out = Uint8List(_batchHeaderLen + batchLength);
  final view = ByteData.sublistView(out);
  view.setInt64(0, 0); // base_offset, stamped by the broker
  view.setInt32(8, batchLength);
  view.setInt32(12, 0); // leader_epoch, likewise
  view.setUint8(16, _magicV1);
  view.setUint32(17, 0); // crc placeholder
  view.setUint16(21, attributes);
  view.setInt32(23, records.isEmpty ? 0 : records.length - 1);
  view.setInt64(27, maxTimestamp);
  out.setRange(_batchHeaderLen + _minBatchLength, out.length, compressed);
  view.setUint32(17, crc32c(out, 21));
  return out;
}

/// Decode one batch at [offset]; returns it and the offset after it.
(RecordBatch, int) decodeRecordBatch(Uint8List data, int offset) {
  if (data.length - offset < _batchHeaderLen) {
    throw ProtocolException('truncated batch header');
  }
  final view = ByteData.sublistView(data);
  final baseOffset = view.getInt64(offset);
  final batchLength = view.getInt32(offset + 8);
  if (batchLength < _minBatchLength) {
    throw ProtocolException('batch_length $batchLength too small');
  }
  final bodyAt = offset + _batchHeaderLen;
  final end = bodyAt + batchLength;
  if (end > data.length) throw ProtocolException('truncated batch body');

  final magic = view.getUint8(bodyAt + 4);
  if (magic != _magicV1 && magic != _magicV2) {
    throw ProtocolException('unsupported magic $magic');
  }
  final crcAt = bodyAt + 5;
  final stored = view.getUint32(crcAt);
  final computed = crc32c(data, crcAt + 4, end);
  if (stored != computed) {
    throw ProtocolException('crc mismatch: stored 0x${stored.toRadixString(16)}, '
        'computed 0x${computed.toRadixString(16)}');
  }
  var cursor = crcAt + 4;
  final attributes = view.getUint16(cursor);
  final maxTimestamp = view.getInt64(cursor + 6);
  cursor += 14;
  if (magic == _magicV2) cursor += _producerExtensionLen;
  if (cursor > end) throw ProtocolException('truncated batch header');

  final payload = decompressPayload(
      attributes & _compressionMask, Uint8List.sublistView(data, cursor, end));
  final records = _decodeRecords(payload, (attributes & _headersBit) != 0,
      (attributes & _nullValueBit) != 0);
  return (RecordBatch(baseOffset, maxTimestamp, records), end);
}

class _Cursor {
  _Cursor(this.data);
  final Uint8List data;
  int pos = 0;

  int uvarint(int limit) {
    var result = 0;
    var shift = 0;
    while (true) {
      if (pos >= limit) throw ProtocolException('truncated varint in record');
      final byte = data[pos++];
      result |= (byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) return result;
      shift += 7;
      if (shift > 63) throw ProtocolException('varint overflows 64 bits');
    }
  }

  /// A length that must fit before [limit].
  int length(int limit) {
    final n = uvarint(limit);
    if (n < 0 || n > limit - pos) {
      throw ProtocolException('record field length $n runs past its record');
    }
    return n;
  }

  Uint8List take(int n) {
    final out = Uint8List.fromList(Uint8List.sublistView(data, pos, pos + n));
    pos += n;
    return out;
  }
}

List<Record> _decodeRecords(
    Uint8List payload, bool hasHeaders, bool hasNullValues) {
  final records = <Record>[];
  final c = _Cursor(payload);
  while (c.pos < payload.length) {
    final size = c.length(payload.length);
    final end = c.pos + size;

    Uint8List? key;
    final keyLenPlusOne = c.uvarint(end);
    if (keyLenPlusOne != 0) {
      final keyLen = keyLenPlusOne - 1;
      if (keyLen < 0 || keyLen > end - c.pos) {
        throw ProtocolException('record key runs past its record');
      }
      key = c.take(keyLen);
    }

    Uint8List? value;
    final rawValueLen = c.uvarint(end);
    if (hasNullValues && rawValueLen == 0) {
      value = null; // a tombstone
    } else {
      final valueLen = hasNullValues ? rawValueLen - 1 : rawValueLen;
      if (valueLen < 0 || valueLen > end - c.pos) {
        throw ProtocolException('record value runs past its record');
      }
      value = c.take(valueLen);
    }

    final timestampDelta = _unzigzag(c.uvarint(end));

    final headers = <RecordHeader>[];
    if (hasHeaders) {
      final count = c.uvarint(end);
      if (count < 0 || count > end - c.pos) {
        throw ProtocolException('record header count exceeds record');
      }
      for (var i = 0; i < count; i++) {
        final keyLen = c.length(end);
        final headerKey =
            utf8.decode(Uint8List.sublistView(payload, c.pos, c.pos + keyLen));
        c.pos += keyLen;
        final valuePlusOne = c.uvarint(end);
        Uint8List? headerValue;
        if (valuePlusOne != 0) {
          final headerLen = valuePlusOne - 1;
          if (headerLen < 0 || headerLen > end - c.pos) {
            throw ProtocolException('header value runs past its record');
          }
          headerValue = c.take(headerLen);
        }
        headers.add(RecordHeader(headerKey, headerValue));
      }
    }
    if (c.pos != end) throw ProtocolException('trailing bytes in record');
    records.add(Record(
        key: key,
        value: value,
        timestampDelta: timestampDelta,
        headers: headers));
  }
  return records;
}

// ---------------------------------------------------------------------------
// Partitioning
// ---------------------------------------------------------------------------

/// Kafka's 32-bit murmur2 (signed result, as Kafka's `Utils.murmur2`).
/// `murmur2([]) == 275646681`.
int murmur2(List<int> data) {
  const seed = 0x9747b28c;
  const m = 0x5bd1e995;
  const r = 24;
  const mask = 0xffffffff;
  final length = data.length;
  var h = (seed ^ length) & mask;
  final chunks = length ~/ 4;
  for (var i = 0; i < chunks; i++) {
    final o = i * 4;
    var k = (data[o] & 0xff) |
        ((data[o + 1] & 0xff) << 8) |
        ((data[o + 2] & 0xff) << 16) |
        ((data[o + 3] & 0xff) << 24);
    k = (k * m) & mask;
    k ^= k >>> r;
    k = (k * m) & mask;
    h = (h * m) & mask;
    h ^= k;
  }
  final tail = chunks * 4;
  final left = length - tail;
  if (left == 3) h ^= (data[tail + 2] & 0xff) << 16;
  if (left >= 2) h ^= (data[tail + 1] & 0xff) << 8;
  if (left >= 1) {
    h ^= data[tail] & 0xff;
    h = (h * m) & mask;
  }
  h ^= h >>> 13;
  h = (h * m) & mask;
  h ^= h >>> 15;
  return h.toSigned(32);
}

/// murmur2(key) % partitions, matching Kafka's default partitioner.
int partitionForKey(List<int> key, List<int> partitions) =>
    partitions[(murmur2(key) & 0x7fffffff) % partitions.length];
