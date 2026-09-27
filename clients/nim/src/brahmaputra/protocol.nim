## Wire encodings for Brahmaputra.
##
## Three encodings share one connection and they do not agree with each
## other, so keeping them straight is most of the work:
##
## - The frame header is fixed big-endian: an int32 length prefix, then
##   apiKey/apiVersion/correlationId and an int16-prefixed client id.
## - A request body is BitPacker: every integer is a zigzag varint, every
##   string and array is a varint count followed by its contents, and the
##   whole body is prefixed with the schema version string.
## - A record batch is neither. Fixed big-endian header fields and plain
##   (non-zigzag) varints inside each record, because the broker stamps
##   offsets into it in place and validates its CRC without decoding it.
##
## Bytes are carried in Nim `string`s (Nim strings are byte arrays); a value
## that may be null is an `Option[string]`, so an empty value and a null one
## stay distinct all the way through.

import std/[options, strutils]

const brahmaputraZlib* {.booldefine.} = true
  ## Build the gzip codec on the system zlib (`libz.so.1`). Pass
  ## `-d:brahmaputraZlib=false` to leave zlib out; gzip must then be
  ## registered with `registerCodec`, like the other codecs.

when brahmaputraZlib:
  import ./zlib

const
  SchemaVersion* = "1.0.0"
    ## The BitPacker schema version every body carries first.
  ApiVersion*: int16 = 4
    ## The wire version this client speaks. The broker requires an exact match.

  ReadUncommitted*: int32 = 0
  ReadCommitted*: int32 = 1

  batchHeaderLen = 12
  minBatchLength = 4 + 1 + 4 + 2 + 4 + 8
  producerExtensionLen = 8 + 2 + 4
  magicV1 = 1'u8
  magicV2 = 2'u8
  compressionMask = 0x0007'u16
  headersBit = 0x0008'u16
  nullValueBit = 0x0040'u16
    ## Some record in this batch has a null value (a tombstone). Set only
    ## when one is present, so a batch without one encodes as it always did.

  MaxDecompressedBytes* = 256 * 1024 * 1024
  MaxFrameBytes* = 1024 * 1024 * 1024

# API keys, in wire order.
const
  ApiProduce*: int16 = 0
  ApiFetch*: int16 = 1
  ApiListOffsets*: int16 = 2
  ApiMetadata*: int16 = 3
  ApiReplicaFetch*: int16 = 4
  ApiOffsetsForLeaderEpoch*: int16 = 5
  ApiInitProducerId*: int16 = 6
  ApiJoinGroup*: int16 = 7
  ApiSyncGroup*: int16 = 8
  ApiHeartbeat*: int16 = 9
  ApiOffsetCommit*: int16 = 10
  ApiOffsetFetch*: int16 = 11
  ApiListGroups*: int16 = 12
  ApiDescribeGroup*: int16 = 13
  ApiApiVersions*: int16 = 14
  ApiProduceMulti*: int16 = 15
  ApiFetchMulti*: int16 = 16
  ApiAuthenticate*: int16 = 17
  ApiLeaveGroup*: int16 = 18

# Error codes the broker returns in a response's error_code field.
const
  ErrNone*: int32 = 0
  ErrUnknownTopicOrPartition*: int32 = 1
  ErrOffsetOutOfRange*: int32 = 2
  ErrInvalidRequest*: int32 = 3
  ErrUnsupportedVersion*: int32 = 4
  ErrInternal*: int32 = 5
  ErrNotLeaderOrFollower*: int32 = 6
  ErrFencedBrokerEpoch*: int32 = 7
  ErrFencedLeaderEpoch*: int32 = 8
  ErrUnknownLeaderEpoch*: int32 = 9
  ErrNotEnoughReplicas*: int32 = 10
  ErrFencedProducerEpoch*: int32 = 11
  ErrOutOfOrderSequence*: int32 = 12
  ErrUnknownMemberId*: int32 = 13
  ErrRebalanceInProgress*: int32 = 14
  ErrNotCoordinator*: int32 = 15
  ErrIllegalGeneration*: int32 = 16
  ErrCoordinatorLoadInProgress*: int32 = 17
  ErrSaslAuthenticationFailed*: int32 = 18
  ErrAuthorizationFailed*: int32 = 19

const errorNames = [
  "NONE", "UNKNOWN_TOPIC_OR_PARTITION", "OFFSET_OUT_OF_RANGE", "INVALID_REQUEST",
  "UNSUPPORTED_VERSION", "INTERNAL", "NOT_LEADER_OR_FOLLOWER", "FENCED_BROKER_EPOCH",
  "FENCED_LEADER_EPOCH", "UNKNOWN_LEADER_EPOCH", "NOT_ENOUGH_REPLICAS",
  "FENCED_PRODUCER_EPOCH", "OUT_OF_ORDER_SEQUENCE", "UNKNOWN_MEMBER_ID",
  "REBALANCE_IN_PROGRESS", "NOT_COORDINATOR", "ILLEGAL_GENERATION",
  "COORDINATOR_LOAD_IN_PROGRESS", "SASL_AUTHENTICATION_FAILED", "AUTHORIZATION_FAILED"]

type
  BrahmaputraError* = object of CatchableError
    ## Base of every error this client raises.
  ServerError* = object of BrahmaputraError
    ## A non-zero error code from the broker.
    code*: int32
  DecodeError* = object of BrahmaputraError
    ## A response or record batch that does not parse.
  ConnectionError* = object of BrahmaputraError
    ## A dial, I/O or framing failure. The connection involved is broken.
  RequestTimeoutError* = object of ConnectionError
    ## A round trip that exceeded its timeout. The connection is broken.
  BufferFullError* = object of BrahmaputraError
    ## `buffer.memory` stayed full for longer than `max.block.ms`.
  NoOffsetForPartitionError* = object of BrahmaputraError
    ## `auto.offset.reset=none` and there is no position to resume from.
  CodecError* = object of BrahmaputraError
    ## A compression codec that is unknown, unregistered or failed.

proc errorName*(code: int32): string =
  if code >= 0 and code < errorNames.len.int32: errorNames[code] else: "UNKNOWN"

proc newServerError*(code: int32, context: string): ref ServerError =
  var msg = "broker returned " & errorName(code) & "[" & $code & "]"
  if context.len > 0:
    msg.add " (" & context & ")"
  result = newException(ServerError, msg)
  result.code = code

proc retriable*(code: int32): bool =
  ## Whether a code means "this send did not happen": every one here is
  ## returned strictly before the broker appends, so a retry cannot duplicate.
  code in [ErrNotLeaderOrFollower, ErrFencedLeaderEpoch, ErrUnknownLeaderEpoch,
           ErrNotEnoughReplicas, ErrCoordinatorLoadInProgress, ErrInternal]

proc decodeError(msg: string) {.noreturn.} =
  raise newException(DecodeError, msg)

# ---------------------------------------------------------------------------
# Zigzag and varints
# ---------------------------------------------------------------------------

proc zigzag32*(v: int32): uint32 {.inline.} =
  (cast[uint32](v) shl 1) xor cast[uint32](ashr(v, 31))

proc zigzag64*(v: int64): uint64 {.inline.} =
  (cast[uint64](v) shl 1) xor cast[uint64](ashr(v, 63))

proc unzigzag32*(v: uint64): int32 {.inline.} =
  let u = cast[uint32](v and 0xFFFF_FFFF'u64)
  cast[int32]((u shr 1) xor (0'u32 - (u and 1'u32)))

proc unzigzag64*(v: uint64): int64 {.inline.} =
  cast[int64]((v shr 1) xor (0'u64 - (v and 1'u64)))

proc addUvarint*(buf: var string, value: uint64) =
  var v = value
  while v >= 0x80'u64:
    buf.add char((v and 0x7F'u64) or 0x80'u64)
    v = v shr 7
  buf.add char(v)

proc getUvarint(data: string, pos: var int, limit: int): uint64 =
  var shift = 0
  while true:
    if pos >= limit:
      decodeError("truncated varint")
    let b = uint64(uint8(data[pos]))
    inc pos
    result = result or ((b and 0x7F'u64) shl shift)
    if (b and 0x80'u64) == 0:
      return
    shift += 7
    if shift > 63:
      decodeError("varint overflows 64 bits")

# ---------------------------------------------------------------------------
# Big-endian helpers
# ---------------------------------------------------------------------------

proc addU16BE*(buf: var string, v: uint16) =
  buf.add char((v shr 8) and 0xFF)
  buf.add char(v and 0xFF)

proc addU32BE*(buf: var string, v: uint32) =
  for shift in [24, 16, 8, 0]:
    buf.add char((v shr shift) and 0xFF)

proc addU64BE*(buf: var string, v: uint64) =
  for shift in [56, 48, 40, 32, 24, 16, 8, 0]:
    buf.add char((v shr shift) and 0xFF)

proc putU32BE(buf: var string, at: int, v: uint32) =
  buf[at] = char((v shr 24) and 0xFF)
  buf[at + 1] = char((v shr 16) and 0xFF)
  buf[at + 2] = char((v shr 8) and 0xFF)
  buf[at + 3] = char(v and 0xFF)

proc readU16BE*(data: string, at: int): uint16 =
  (uint16(uint8(data[at])) shl 8) or uint16(uint8(data[at + 1]))

proc readU32BE*(data: string, at: int): uint32 =
  (uint32(uint8(data[at])) shl 24) or (uint32(uint8(data[at + 1])) shl 16) or
    (uint32(uint8(data[at + 2])) shl 8) or uint32(uint8(data[at + 3]))

proc readU64BE*(data: string, at: int): uint64 =
  (uint64(readU32BE(data, at)) shl 32) or uint64(readU32BE(data, at + 4))

# ---------------------------------------------------------------------------
# BitPacker bodies
# ---------------------------------------------------------------------------

type
  BodyWriter* = object
    ## Builds a BitPacker body: every integer is a zigzag varint.
    buf*: string

  BodyReader* = object
    ## Reads a BitPacker body. Every read is bounds-checked and raises
    ## `DecodeError` on truncation.
    data: string
    pos: int

proc initBodyWriter*(): BodyWriter =
  ## A writer already carrying the schema version every body starts with.
  result.buf = newStringOfCap(256)
  result.buf.addUvarint(uint64(zigzag32(int32(SchemaVersion.len))))
  result.buf.add SchemaVersion

proc writeUvarint*(w: var BodyWriter, v: uint64) = w.buf.addUvarint(v)
proc writeInt32*(w: var BodyWriter, v: int32) = w.buf.addUvarint(uint64(zigzag32(v)))
proc writeInt64*(w: var BodyWriter, v: int64) = w.buf.addUvarint(zigzag64(v))
proc writeBool*(w: var BodyWriter, v: bool) = w.buf.add(if v: '\1' else: '\0')
proc writeString*(w: var BodyWriter, v: string) =
  w.writeInt32(int32(v.len))
  w.buf.add v
proc writeStringArray*(w: var BodyWriter, values: openArray[string]) =
  w.writeInt32(int32(values.len))
  for v in values: w.writeString(v)
proc writeRaw*(w: var BodyWriter, data: string) = w.buf.add data

proc readUvarint*(r: var BodyReader): uint64 = getUvarint(r.data, r.pos, r.data.len)
proc readInt32*(r: var BodyReader): int32 = unzigzag32(r.readUvarint())
proc readInt64*(r: var BodyReader): int64 = unzigzag64(r.readUvarint())
proc readBool*(r: var BodyReader): bool =
  if r.pos >= r.data.len: decodeError("truncated bool")
  result = r.data[r.pos] != '\0'
  inc r.pos

proc readString*(r: var BodyReader): string =
  let length = r.readInt32()
  if length < 0 or int(length) > r.data.len - r.pos:
    decodeError("truncated string")
  result = r.data[r.pos ..< r.pos + int(length)]
  r.pos += int(length)

proc readCount*(r: var BodyReader): int =
  ## An array count. Every element takes at least one byte, so a count
  ## larger than what is left is corrupt rather than a reason to allocate.
  let count = r.readInt32()
  if count < 0: return 0
  if int(count) > r.data.len - r.pos:
    decodeError("array count exceeds the body")
  int(count)

proc readStringArray*(r: var BodyReader): seq[string] =
  for _ in 0 ..< r.readCount(): result.add r.readString()

proc rest*(r: var BodyReader): string =
  result = r.data[r.pos .. ^1]
  r.pos = r.data.len

proc initBodyReader*(data: sink string): BodyReader =
  ## A reader positioned past the schema version, which it verifies.
  result = BodyReader(data: data, pos: 0)
  let version = result.readString()
  if version != SchemaVersion:
    decodeError("schema version mismatch: broker speaks \"" & version &
                "\", this client speaks \"" & SchemaVersion & "\"")

proc peekErrorCode*(body: string): int32 =
  ## A response's leading error code, without consuming anything. Every
  ## group response starts with one.
  try:
    var r = initBodyReader(body)
    r.readInt32()
  except DecodeError:
    ErrNone

# ---------------------------------------------------------------------------
# Frames
# ---------------------------------------------------------------------------

proc encodeFrame*(apiKey: int16, correlationId: int32, clientId: string,
                  body: string): string =
  ## One complete frame, length prefix included. The header is fixed
  ## big-endian while the body is BitPacker.
  let payloadLen = 8 + 2 + clientId.len + body.len
  result = newStringOfCap(4 + payloadLen)
  result.addU32BE(uint32(payloadLen))
  result.addU16BE(cast[uint16](apiKey))
  result.addU16BE(cast[uint16](ApiVersion))
  result.addU32BE(cast[uint32](correlationId))
  result.addU16BE(uint16(clientId.len))
  result.add clientId
  result.add body

proc decodeFramePayload*(payload: string): tuple[correlationId: int32, body: string] =
  ## Splits a response frame payload into its correlation id and body.
  if payload.len < 10:
    decodeError("frame payload shorter than its header")
  result.correlationId = cast[int32](readU32BE(payload, 4))
  let clientLen = cast[int16](readU16BE(payload, 8))
  var offset = 10
  if clientLen >= 0: offset += int(clientLen)
  if offset > payload.len:
    decodeError("frame client id runs past the payload")
  result.body = payload[offset .. ^1]

# ---------------------------------------------------------------------------
# CRC32C (Castagnoli) — not the zlib CRC32.
# ---------------------------------------------------------------------------

proc makeCrcTable(): array[256, uint32] =
  for i in 0 ..< 256:
    var crc = uint32(i)
    for _ in 0 ..< 8:
      crc = if (crc and 1) != 0: (crc shr 1) xor 0x82F63B78'u32 else: crc shr 1
    result[i] = crc

const crcTable = makeCrcTable()

proc crc32c*(data: openArray[char]): uint32 =
  var crc = 0xFFFF_FFFF'u32
  for c in data:
    crc = crcTable[(crc xor uint32(uint8(c))) and 0xFF] xor (crc shr 8)
  crc xor 0xFFFF_FFFF'u32

proc crc32c*(data: string): uint32 = crc32c(data.toOpenArray(0, data.len - 1))

# ---------------------------------------------------------------------------
# Kafka murmur2
# ---------------------------------------------------------------------------

proc murmur2*(data: openArray[char]): uint32 =
  ## Kafka's 32-bit murmur2, transcribed so a key lands on the same
  ## partition here as with every other client. `murmur2("") == 275646681`.
  const seed = 0x9747b28c'u32
  const m = 0x5bd1e995'u32
  let length = data.len
  var h = seed xor uint32(length)
  let chunks = length div 4
  for i in 0 ..< chunks:
    let o = i * 4
    var k = uint32(uint8(data[o])) or (uint32(uint8(data[o + 1])) shl 8) or
            (uint32(uint8(data[o + 2])) shl 16) or (uint32(uint8(data[o + 3])) shl 24)
    k = k * m
    k = k xor (k shr 24)
    k = k * m
    h = h * m
    h = h xor k
  let tail = chunks * 4
  case length - tail
  of 3:
    h = h xor (uint32(uint8(data[tail + 2])) shl 16)
    h = h xor (uint32(uint8(data[tail + 1])) shl 8)
    h = h xor uint32(uint8(data[tail]))
    h = h * m
  of 2:
    h = h xor (uint32(uint8(data[tail + 1])) shl 8)
    h = h xor uint32(uint8(data[tail]))
    h = h * m
  of 1:
    h = h xor uint32(uint8(data[tail]))
    h = h * m
  else: discard
  h = h xor (h shr 13)
  h = h * m
  h = h xor (h shr 15)
  h

proc murmur2*(data: string): uint32 = murmur2(data.toOpenArray(0, data.len - 1))

proc partitionForKey*(key: string, partitions: openArray[int32]): int32 =
  ## `murmur2(key) & 0x7fffffff % partitions`, Kafka's default partitioner.
  partitions[int(murmur2(key) and 0x7fffffff'u32) mod partitions.len]

# ---------------------------------------------------------------------------
# Compression
# ---------------------------------------------------------------------------

type
  Compression* = enum
    ## Codecs, matching the broker's attribute values.
    compressionNone = 0, compressionLz4 = 1, compressionZstd = 2,
    compressionSnappy = 3, compressionGzip = 4

  CodecProc* = proc (data: string): string {.nimcall, gcsafe.}
    ## A codec function. Raise on failure.

proc `$`*(c: Compression): string =
  case c
  of compressionNone: "none"
  of compressionLz4: "lz4"
  of compressionZstd: "zstd"
  of compressionSnappy: "snappy"
  of compressionGzip: "gzip"

proc parseCompression*(name: string): Compression =
  ## Maps Kafka's `compression.type` spelling onto a codec.
  case name.toLowerAscii
  of "none", "": compressionNone
  of "lz4": compressionLz4
  of "zstd": compressionZstd
  of "snappy": compressionSnappy
  of "gzip": compressionGzip
  else:
    raise newException(CodecError, "unknown compression \"" & name &
                       "\" (none, lz4, zstd, snappy, gzip)")

when brahmaputraZlib:
  proc builtinGzipCompress(data: string): string {.nimcall, gcsafe.} =
    gzipCompress(data)
  proc builtinGzipDecompress(data: string): string {.nimcall, gcsafe.} =
    gzipDecompress(data, MaxDecompressedBytes)

var codecRegistry: array[Compression, tuple[compress, decompress: CodecProc]]
when brahmaputraZlib:
  codecRegistry[compressionGzip] = (builtinGzipCompress, builtinGzipDecompress)

proc registerCodec*(codec: Compression, compress, decompress: CodecProc) =
  ## Plugs in a codec this package does not carry itself (or replaces the
  ## built-in gzip). Register before creating producers or consumers.
  ##
  ## The lz4 payload the broker expects is a little-endian uint32 of the
  ## uncompressed length followed by a raw LZ4 *block* — not the frame format.
  if codec == compressionNone:
    raise newException(CodecError, "none cannot be replaced")
  codecRegistry[codec] = (compress, decompress)

proc codecAvailable*(codec: Compression): bool =
  codec == compressionNone or codecRegistry[codec].compress != nil

proc compress*(codec: Compression, payload: string): string =
  if codec == compressionNone: return payload
  let fn = codecRegistry[codec].compress
  if fn == nil:
    raise newException(CodecError, $codec &
      " compression is not registered; call registerCodec or use none/gzip")
  try:
    fn(payload)
  except CodecError as e:
    raise e
  except CatchableError as e:
    raise newException(CodecError, $codec & " compression failed: " & e.msg)

proc decompress*(codec: Compression, payload: string): string =
  if codec == compressionNone: return payload
  let fn = codecRegistry[codec].decompress
  if fn == nil:
    raise newException(CodecError, $codec &
      " decompression is not registered; call registerCodec")
  try:
    fn(payload)
  except CodecError as e:
    raise e
  except CatchableError as e:
    raise newException(CodecError, $codec & " decompression failed: " & e.msg)

# ---------------------------------------------------------------------------
# Record batches
# ---------------------------------------------------------------------------

type
  RecordHeader* = object
    ## An ordered, possibly repeating annotation on a record. A header value
    ## may be null, which is distinct from empty.
    key*: string
    value*: Option[string]

  Record* = object
    ## One record inside a batch. `none` key/value is null; `some("")` is empty.
    key*: Option[string]
    value*: Option[string]
    timestampDelta*: int64
      ## Milliseconds relative to the batch's max timestamp (normally <= 0).
    headers*: seq[RecordHeader]

  DecodedBatch* = object
    baseOffset*: int64
    maxTimestamp*: int64
    records*: seq[Record]

proc header*(key: string, value: string): RecordHeader =
  RecordHeader(key: key, value: some(value))

proc nullHeader*(key: string): RecordHeader =
  RecordHeader(key: key, value: none(string))

proc addNullable(buf: var string, v: Option[string]) =
  if v.isNone:
    buf.addUvarint(0)
  else:
    buf.addUvarint(uint64(v.get.len) + 1)
    buf.add v.get

proc encodeRecordBatch*(records: openArray[Record], maxTimestamp: int64,
                        codec: Compression): string =
  ## Encodes one batch exactly as the broker stores it. The broker never
  ## re-encodes this: it stamps base_offset and leader_epoch in place (both
  ## before the CRC) and writes these bytes to disk.
  var hasHeaders = false
  var hasNullValues = false
  for r in records:
    if r.headers.len > 0: hasHeaders = true
    if r.value.isNone: hasNullValues = true

  var payload = ""
  var rec = ""
  for r in records:
    rec.setLen(0)
    rec.addNullable(r.key)
    if hasNullValues:
      rec.addNullable(r.value)
    else:
      let v = r.value.get
      rec.addUvarint(uint64(v.len))
      rec.add v
    rec.addUvarint(zigzag64(r.timestampDelta))
    if hasHeaders:
      rec.addUvarint(uint64(r.headers.len))
      for h in r.headers:
        rec.addUvarint(uint64(h.key.len))
        rec.add h.key
        rec.addNullable(h.value)
    payload.addUvarint(uint64(rec.len))
    payload.add rec

  let compressed = compress(codec, payload)
  var attributes = uint16(ord(codec)) and compressionMask
  if hasHeaders: attributes = attributes or headersBit
  if hasNullValues: attributes = attributes or nullValueBit
  let batchLength = minBatchLength + compressed.len

  result = newStringOfCap(batchHeaderLen + batchLength)
  result.addU64BE(0)                       # base_offset, stamped by the broker
  result.addU32BE(uint32(batchLength))
  result.addU32BE(0)                       # leader_epoch, likewise
  result.add char(magicV1)
  let crcAt = result.len
  result.addU32BE(0)
  result.addU16BE(attributes)
  result.addU32BE(uint32(max(records.len - 1, 0)))
  result.addU64BE(cast[uint64](maxTimestamp))
  result.add compressed
  result.putU32BE(crcAt, crc32c(result.toOpenArray(crcAt + 4, result.len - 1)))

proc readNullable(payload: string, pos: var int, finish: int, what: string): Option[string] =
  let lenPlusOne = getUvarint(payload, pos, finish)
  if lenPlusOne == 0:
    return none(string)
  if lenPlusOne - 1 > uint64(finish - pos):
    decodeError("truncated record " & what)
  let size = int(lenPlusOne - 1)
  result = some(payload[pos ..< pos + size])
  pos += size

proc decodeRecords(payload: string, hasHeaders, hasNullValues: bool): seq[Record] =
  var pos = 0
  while pos < payload.len:
    let length = getUvarint(payload, pos, payload.len)
    if length > uint64(payload.len - pos):
      decodeError("truncated record")
    let finish = pos + int(length)
    var record: Record
    record.key = readNullable(payload, pos, finish, "key")
    if hasNullValues:
      record.value = readNullable(payload, pos, finish, "value")
    else:
      let valueLen = getUvarint(payload, pos, finish)
      if valueLen > uint64(finish - pos):
        decodeError("truncated record value")
      record.value = some(payload[pos ..< pos + int(valueLen)])
      pos += int(valueLen)
    record.timestampDelta = unzigzag64(getUvarint(payload, pos, finish))
    if hasHeaders:
      let count = getUvarint(payload, pos, finish)
      if count > uint64(finish - pos):
        decodeError("record header count exceeds record")
      for _ in 0'u64 ..< count:
        let keyLen = getUvarint(payload, pos, finish)
        if keyLen > uint64(finish - pos):
          decodeError("truncated record header key")
        var h = RecordHeader(key: payload[pos ..< pos + int(keyLen)])
        pos += int(keyLen)
        h.value = readNullable(payload, pos, finish, "header value")
        record.headers.add h
    if pos != finish:
      decodeError("trailing bytes in record")
    result.add record

proc decodeRecordBatch*(data: string, offset: int): tuple[batch: DecodedBatch, next: int] =
  ## Decodes one batch starting at `offset`; returns it and the offset past it.
  if offset < 0 or data.len - offset < batchHeaderLen:
    decodeError("truncated batch header")
  let baseOffset = cast[int64](readU64BE(data, offset))
  let batchLength = cast[int32](readU32BE(data, offset + 8))
  if batchLength < minBatchLength:
    # Also rejects a negative length, which would otherwise walk backwards.
    decodeError("batch_length " & $batchLength & " is invalid")
  let bodyAt = offset + batchHeaderLen
  if int(batchLength) > data.len - bodyAt:
    decodeError("truncated batch body")
  let finish = bodyAt + int(batchLength)
  let magic = uint8(data[bodyAt + 4])
  if magic != magicV1 and magic != magicV2:
    decodeError("unsupported magic " & $magic)
  let crcAt = bodyAt + 5
  let stored = readU32BE(data, crcAt)
  let computed = crc32c(data.toOpenArray(crcAt + 4, finish - 1))
  if stored != computed:
    decodeError("crc mismatch: stored 0x" & toHex(int64(stored), 8) & ", computed 0x" & toHex(int64(computed), 8))
  var cursor = crcAt + 4
  let attributes = readU16BE(data, cursor)
  let maxTimestamp = cast[int64](readU64BE(data, cursor + 6))
  cursor += 14
  if magic == magicV2:
    cursor += producerExtensionLen
    if cursor > finish: decodeError("truncated producer extension")
  let codecValue = int(attributes and compressionMask)
  if codecValue > ord(high(Compression)):
    decodeError("unknown compression " & $codecValue)
  let payload = decompress(Compression(codecValue), data[cursor ..< finish])
  let records = decodeRecords(payload, (attributes and headersBit) != 0,
                              (attributes and nullValueBit) != 0)
  (DecodedBatch(baseOffset: baseOffset, maxTimestamp: maxTimestamp, records: records), finish)
