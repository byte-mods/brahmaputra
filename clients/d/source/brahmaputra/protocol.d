/**
 * Brahmaputra wire protocol: frames, BitPacker bodies, record batches.
 *
 * Three encodings share one connection and they do not agree with each
 * other, so keeping them straight is most of the work:
 *
 * $(UL
 *   $(LI The frame header is fixed big-endian: an int32 length prefix, then
 *        apiKey/apiVersion/correlationId and an int16-prefixed client id.)
 *   $(LI A request/response body is BitPacker: every integer is a zigzag
 *        varint, every string and array is a varint count followed by its
 *        contents, and the whole body starts with the schema version string.)
 *   $(LI A record batch is neither: fixed big-endian header fields and plain
 *        (non-zigzag) varints inside each record, because the broker stamps
 *        offsets into it in place and validates its CRC without decoding it.)
 * )
 *
 * Null versus empty: a key, value or header value is $(I null) when its
 * slice `is null`, and empty-but-present otherwise. Decoding always yields
 * a non-null slice for a present-but-empty field. Beware that in D the
 * literal `[]` and `.dup` of an empty array are both null; use
 * `emptyBytes()` (or `toBytes("")`) for an explicit empty value.
 */
module brahmaputra.protocol;

import core.sync.mutex : Mutex;
import std.datetime.systime : Clock;
import std.format : format;

/// Schema version string every BitPacker body starts with.
enum string SCHEMA_VERSION = "1.0.0";

/// Wire version this client speaks. The broker requires an exact match.
enum short API_VERSION = 4;

/// Isolation levels for a fetch.
enum int READ_UNCOMMITTED = 0;
/// ditto
enum int READ_COMMITTED = 1;

/// Offset sentinels for `Consumer.listOffsets`.
enum long EARLIEST = -2;
/// ditto
enum long LATEST = -1;

/// API keys, in wire order.
enum ApiKey : short
{
    produce = 0,
    fetch = 1,
    listOffsets = 2,
    metadata = 3,
    replicaFetch = 4,
    offsetsForLeaderEpoch = 5,
    initProducerId = 6,
    joinGroup = 7,
    syncGroup = 8,
    heartbeat = 9,
    offsetCommit = 10,
    offsetFetch = 11,
    listGroups = 12,
    describeGroup = 13,
    apiVersions = 14,
    produceMulti = 15,
    fetchMulti = 16,
    authenticate = 17,
    leaveGroup = 18,
}

/// Error codes the broker returns in a response's error_code field.
enum ErrorCode : int
{
    none = 0,
    unknownTopicOrPartition = 1,
    offsetOutOfRange = 2,
    invalidRequest = 3,
    unsupportedVersion = 4,
    internal = 5,
    notLeaderOrFollower = 6,
    fencedBrokerEpoch = 7,
    fencedLeaderEpoch = 8,
    unknownLeaderEpoch = 9,
    notEnoughReplicas = 10,
    fencedProducerEpoch = 11,
    outOfOrderSequence = 12,
    unknownMemberId = 13,
    rebalanceInProgress = 14,
    notCoordinator = 15,
    illegalGeneration = 16,
    coordinatorLoadInProgress = 17,
    saslAuthenticationFailed = 18,
    authorizationFailed = 19,
}

private immutable string[] ERROR_NAMES = [
    "NONE", "UNKNOWN_TOPIC_OR_PARTITION", "OFFSET_OUT_OF_RANGE", "INVALID_REQUEST",
    "UNSUPPORTED_VERSION", "INTERNAL", "NOT_LEADER_OR_FOLLOWER", "FENCED_BROKER_EPOCH",
    "FENCED_LEADER_EPOCH", "UNKNOWN_LEADER_EPOCH", "NOT_ENOUGH_REPLICAS",
    "FENCED_PRODUCER_EPOCH", "OUT_OF_ORDER_SEQUENCE", "UNKNOWN_MEMBER_ID",
    "REBALANCE_IN_PROGRESS", "NOT_COORDINATOR", "ILLEGAL_GENERATION",
    "COORDINATOR_LOAD_IN_PROGRESS", "SASL_AUTHENTICATION_FAILED", "AUTHORIZATION_FAILED",
];

/// The broker's name for an error code.
string errorName(int code) pure nothrow @safe
{
    if (code >= 0 && code < cast(int) ERROR_NAMES.length)
        return ERROR_NAMES[code];
    return "UNKNOWN";
}

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------

/// Base of every exception this driver throws.
class BrahmaputraException : Exception
{
    this(string msg, Throwable next = null, string file = __FILE__, size_t line = __LINE__) @safe pure nothrow
    {
        super(msg, file, line, next);
    }
}

/// Malformed bytes on the wire: a frame, body or batch that does not decode.
class ProtocolException : BrahmaputraException
{
    this(string msg, string file = __FILE__, size_t line = __LINE__) @safe pure nothrow
    {
        super(msg, null, file, line);
    }
}

/// A socket failure, timeout or desynchronised stream. The connection it
/// happened on is marked broken and is never reused.
class ConnectionException : BrahmaputraException
{
    this(string msg, Throwable next = null, string file = __FILE__, size_t line = __LINE__) @safe pure nothrow
    {
        super(msg, next, file, line);
    }
}

/// A non-zero error code from the broker.
class ServerException : BrahmaputraException
{
    /// The broker's error code.
    immutable int code;
    /// What was being attempted.
    immutable string context;

    this(int code, string context, string file = __FILE__, size_t line = __LINE__) @safe
    {
        this.code = code;
        this.context = context;
        super(context.length
                ? format("broker returned %s[%d] (%s)", errorName(code), code, context)
                : format("broker returned %s[%d]", errorName(code), code),
            null, file, line);
    }
}

/// `buffer.memory` stayed full for longer than `max.block.ms`.
class BufferFullException : BrahmaputraException
{
    this(string msg, string file = __FILE__, size_t line = __LINE__) @safe pure nothrow
    {
        super(msg, null, file, line);
    }
}

/// `auto.offset.reset=none` and there is no position to resume from.
class NoOffsetForPartitionException : BrahmaputraException
{
    this(string msg = "no committed offset for partition", string file = __FILE__,
        size_t line = __LINE__) @safe pure nothrow
    {
        super(msg, null, file, line);
    }
}

/// Whether a code means "this send did not happen", so a retry cannot
/// duplicate a record. Every such code is one the broker returns strictly
/// before it appends.
bool isRetriable(int code) pure nothrow @safe
{
    switch (code)
    {
    case ErrorCode.notLeaderOrFollower, ErrorCode.fencedLeaderEpoch,
        ErrorCode.unknownLeaderEpoch, ErrorCode.notEnoughReplicas,
        ErrorCode.coordinatorLoadInProgress, ErrorCode.internal:
        return true;
    default:
        return false;
    }
}

// ---------------------------------------------------------------------------
// Small shared helpers
// ---------------------------------------------------------------------------

/// Wall-clock milliseconds since the unix epoch.
long nowMillis() @safe
{
    enum long unixEpochHnsecs = 621_355_968_000_000_000L;
    return (Clock.currStdTime - unixEpochHnsecs) / 10_000;
}

private immutable string EMPTY_STORE = "\0";

/// A zero-length, non-null slice: an empty value that is not a null value.
const(ubyte)[] emptyBytes() pure nothrow @trusted @nogc
{
    return (cast(immutable(ubyte)[]) EMPTY_STORE)[0 .. 0];
}

/// The bytes of a string, always non-null (an empty string gives an empty,
/// non-null value). Pass `null` itself where you mean null.
const(ubyte)[] toBytes(const(char)[] text) pure nothrow @trusted
{
    if (text.length == 0)
        return emptyBytes();
    return cast(const(ubyte)[]) text;
}

/// One partition of one topic.
struct TopicPartition
{
    string topic;
    int partition;

    int opCmp(const TopicPartition other) const pure nothrow @safe
    {
        if (topic != other.topic)
            return topic < other.topic ? -1 : 1;
        if (partition != other.partition)
            return partition < other.partition ? -1 : 1;
        return 0;
    }
}

// ---------------------------------------------------------------------------
// BitPacker
// ---------------------------------------------------------------------------

/// Builds a BitPacker body. Every integer goes out zigzag-varint encoded,
/// which is why this shares no code with the record-batch encoder.
struct BodyWriter
{
    private ubyte[] buf;

    /// A writer already carrying the schema version every body starts with.
    static BodyWriter start() pure nothrow @safe
    {
        BodyWriter w;
        w.buf.reserve(256);
        w.str(SCHEMA_VERSION);
        return w;
    }

    void uvarint(ulong value) pure nothrow @safe
    {
        while (value >= 0x80)
        {
            buf ~= cast(ubyte)(value | 0x80);
            value >>= 7;
        }
        buf ~= cast(ubyte) value;
    }

    void i32(int value) pure nothrow @safe
    {
        uvarint(cast(uint)((value << 1) ^ (value >> 31)));
    }

    void i64(long value) pure nothrow @safe
    {
        uvarint(cast(ulong)((value << 1) ^ (value >> 63)));
    }

    void boolean(bool value) pure nothrow @safe
    {
        buf ~= value ? 1 : 0;
    }

    void str(const(char)[] value) pure nothrow @trusted
    {
        i32(cast(int) value.length);
        buf ~= cast(const(ubyte)[]) value;
    }

    void strArray(const(string)[] values) pure nothrow @safe
    {
        i32(cast(int) values.length);
        foreach (value; values)
            str(value);
    }

    void raw(const(ubyte)[] data) pure nothrow @safe
    {
        buf ~= data;
    }

    const(ubyte)[] data() const pure nothrow @safe
    {
        return buf;
    }
}

/// Reads a BitPacker body. Every read is bounds-checked and throws
/// `ProtocolException` rather than decoding garbage.
struct BodyReader
{
    private const(ubyte)[] buf;
    private size_t pos;

    /// A reader positioned past the schema version, which it verifies: a
    /// mismatch means broker and client disagree about the message shapes.
    this(const(ubyte)[] data) @safe
    {
        buf = data;
        const version_ = str();
        if (version_ != SCHEMA_VERSION)
            throw new ProtocolException(format(
                    "schema version mismatch: broker speaks %s, this client speaks %s",
                    version_, SCHEMA_VERSION));
    }

    ulong uvarint() @safe
    {
        ulong result;
        uint shift;
        while (true)
        {
            if (pos >= buf.length)
                throw new ProtocolException("truncated varint");
            const b = buf[pos++];
            result |= cast(ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
            shift += 7;
            if (shift > 63)
                throw new ProtocolException("varint overflows 64 bits");
        }
    }

    int i32() @safe
    {
        const v = uvarint();
        return cast(int)(v >> 1) ^ -cast(int)(v & 1);
    }

    long i64() @safe
    {
        const v = uvarint();
        return cast(long)(v >> 1) ^ -cast(long)(v & 1);
    }

    bool boolean() @safe
    {
        if (pos >= buf.length)
            throw new ProtocolException("truncated bool");
        return buf[pos++] != 0;
    }

    string str() @trusted
    {
        const length = i32();
        if (length < 0 || cast(size_t) length > buf.length - pos)
            throw new ProtocolException("truncated string");
        auto value = cast(string) buf[pos .. pos + length].idup;
        pos += length;
        return value;
    }

    string[] strArray() @safe
    {
        const n = count();
        string[] out_;
        out_.reserve(n);
        foreach (_; 0 .. n)
            out_ ~= str();
        return out_;
    }

    /// An array count, rejected if it could not possibly fit in what is
    /// left (every element takes at least one byte).
    size_t count() @safe
    {
        const n = i32();
        if (n < 0)
            return 0;
        if (cast(size_t) n > buf.length - pos)
            throw new ProtocolException("array count exceeds the body");
        return cast(size_t) n;
    }

    /// Everything not yet read.
    const(ubyte)[] rest() @safe
    {
        auto value = buf[pos .. $];
        pos = buf.length;
        return value;
    }
}

/// Reads a response's leading error code without consuming it. Every group
/// response starts with one; an undecodable body reports none.
int peekErrorCode(const(ubyte)[] body_) @safe
{
    try
    {
        auto r = BodyReader(body_);
        return r.i32();
    }
    catch (ProtocolException)
        return ErrorCode.none;
}

// ---------------------------------------------------------------------------
// Big-endian helpers
// ---------------------------------------------------------------------------

package void putBE(T)(ref ubyte[] buf, T value) pure nothrow @safe
{
    foreach_reverse (i; 0 .. T.sizeof)
        buf ~= cast(ubyte)(cast(ulong) value >> (i * 8));
}

package T readBE(T)(const(ubyte)[] data, size_t at) pure nothrow @safe
{
    ulong v;
    foreach (i; 0 .. T.sizeof)
        v = (v << 8) | data[at + i];
    return cast(T) v;
}

package void writeBE(T)(ubyte[] data, size_t at, T value) pure nothrow @safe
{
    foreach (i; 0 .. T.sizeof)
        data[at + i] = cast(ubyte)(cast(ulong) value >> ((T.sizeof - 1 - i) * 8));
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// One complete frame, length prefix included. The header is fixed
/// big-endian because the broker must read it before it knows which body
/// decoder to use.
ubyte[] encodeFrame(short apiKey, int correlationId, string clientId, const(ubyte)[] body_) pure nothrow @trusted
{
    const payloadLen = 8 + 2 + clientId.length + body_.length;
    ubyte[] out_;
    out_.reserve(4 + payloadLen);
    putBE!uint(out_, cast(uint) payloadLen);
    putBE!short(out_, apiKey);
    putBE!short(out_, API_VERSION);
    putBE!int(out_, correlationId);
    putBE!short(out_, cast(short) clientId.length);
    out_ ~= cast(const(ubyte)[]) clientId;
    out_ ~= body_;
    return out_;
}

/// Splits a frame payload (after the length prefix) into its correlation id
/// and body.
const(ubyte)[] decodeFramePayload(const(ubyte)[] payload, out int correlationId) @safe
{
    if (payload.length < 10)
        throw new ProtocolException("frame payload shorter than its header");
    correlationId = readBE!int(payload, 4);
    const clientLen = readBE!short(payload, 8);
    size_t offset = 10;
    if (clientLen > 0)
        offset += clientLen;
    if (offset > payload.length)
        throw new ProtocolException("frame client id runs past the payload");
    return payload[offset .. $];
}

// ---------------------------------------------------------------------------
// CRC32C
// ---------------------------------------------------------------------------

private immutable uint[256] CRC32C_TABLE = () {
    uint[256] table;
    foreach (i; 0 .. 256)
    {
        uint c = cast(uint) i;
        foreach (_; 0 .. 8)
            c = (c & 1) ? (c >> 1) ^ 0x82F63B78 : c >> 1;
        table[i] = c;
    }
    return table;
}();

/// The Castagnoli CRC record batches carry — not the zlib CRC32.
uint crc32c(const(ubyte)[] data) pure nothrow @safe @nogc
{
    uint c = 0xFFFF_FFFF;
    foreach (b; data)
        c = CRC32C_TABLE[(c ^ b) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFF_FFFF;
}

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

/// Compression codecs, matching the broker's attribute values.
enum Compression : int
{
    none = 0,
    lz4 = 1,
    zstd = 2,
    snappy = 3,
    gzip = 4,
}

/// Maps Kafka's `compression.type` spelling onto a codec.
Compression parseCompression(string name) @safe
{
    switch (name)
    {
    case "none":
        return Compression.none;
    case "lz4":
        return Compression.lz4;
    case "zstd":
        return Compression.zstd;
    case "snappy":
        return Compression.snappy;
    case "gzip":
        return Compression.gzip;
    default:
        throw new BrahmaputraException(
            "unknown compression " ~ name ~ " (none, lz4, zstd, snappy, gzip)");
    }
}

/// A codec function: takes a payload and returns it transformed.
alias CodecFn = const(ubyte)[] delegate(const(ubyte)[] payload);

private __gshared CodecFn[int] externalCompressors;
private __gshared CodecFn[int] externalDecompressors;
private __gshared Mutex codecLock;

shared static this()
{
    codecLock = new Mutex;
}

/**
 * Plugs in a codec this package does not carry (lz4, zstd, snappy), so an
 * application that wants one pays for that dependency and one that does
 * not, does not. `none` and `gzip` are built in and cannot be replaced.
 *
 * The broker's lz4 payload is a little-endian uint32 of the uncompressed
 * length followed by a raw LZ4 $(I block) — not the LZ4 frame format.
 */
void registerCodec(Compression codec, CodecFn compressFn, CodecFn decompressFn)
{
    if (codec == Compression.none || codec == Compression.gzip)
        throw new BrahmaputraException("none and gzip are built in");
    codecLock.lock();
    scope (exit)
        codecLock.unlock();
    externalCompressors[codec] = compressFn;
    externalDecompressors[codec] = decompressFn;
}

/// ditto
void registerCodec(Compression codec,
    const(ubyte)[] function(const(ubyte)[]) compressFn,
    const(ubyte)[] function(const(ubyte)[]) decompressFn)
{
    import std.functional : toDelegate;

    registerCodec(codec, toDelegate(compressFn), toDelegate(decompressFn));
}

private CodecFn lookupCodec(ref CodecFn[int] table, Compression codec)
{
    codecLock.lock();
    scope (exit)
        codecLock.unlock();
    if (auto fn = cast(int) codec in table)
        return *fn;
    return null;
}

/// Caps decompressed output so a corrupt or hostile batch cannot make this
/// process allocate gigabytes before rejecting it.
enum size_t MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024;

/// Applies a codec to a records payload.
const(ubyte)[] compress(Compression codec, const(ubyte)[] payload)
{
    import std.zlib : Compress, HeaderFormat;

    final switch (codec)
    {
    case Compression.none:
        return payload;
    case Compression.gzip:
        auto c = new Compress(6, HeaderFormat.gzip);
        const(ubyte)[] out_ = cast(const(ubyte)[]) c.compress(payload);
        out_ ~= cast(const(ubyte)[]) c.flush();
        return out_;
    case Compression.lz4, Compression.zstd, Compression.snappy:
        if (auto fn = lookupCodec(externalCompressors, codec))
            return fn(payload);
        throw new BrahmaputraException(format(
                "%s compression is not registered; call registerCodec or use none/gzip", codec));
    }
}

/// Reverses `compress`.
const(ubyte)[] decompress(Compression codec, const(ubyte)[] payload)
{
    import std.zlib : UnCompress, HeaderFormat, ZlibException;

    switch (codec)
    {
    case Compression.none:
        return payload;
    case Compression.gzip:
        try
        {
            auto u = new UnCompress(HeaderFormat.gzip);
            const(ubyte)[] out_;
            // Fed in slices so the output cap is enforced as it grows.
            enum chunk = 64 * 1024;
            for (size_t at = 0; at < payload.length; at += chunk)
            {
                const end = at + chunk < payload.length ? at + chunk : payload.length;
                out_ ~= cast(const(ubyte)[]) u.uncompress(payload[at .. end]);
                if (out_.length > MAX_DECOMPRESSED_BYTES)
                    throw new ProtocolException("gzip payload exceeds the decompression cap");
            }
            out_ ~= cast(const(ubyte)[]) u.flush();
            if (out_.length > MAX_DECOMPRESSED_BYTES)
                throw new ProtocolException("gzip payload exceeds the decompression cap");
            return out_;
        }
        catch (ZlibException e)
            throw new ProtocolException("corrupt gzip payload: " ~ e.msg);
    case Compression.lz4, Compression.zstd, Compression.snappy:
        if (auto fn = lookupCodec(externalDecompressors, codec))
            return fn(payload);
        throw new BrahmaputraException(format(
                "%s decompression is not registered; call registerCodec", codec));
    default:
        throw new ProtocolException(format("unsupported compression %d", cast(int) codec));
    }
}

// ---------------------------------------------------------------------------
// Record batches
// ---------------------------------------------------------------------------

private enum size_t BATCH_HEADER_LEN = 12;
private enum int MIN_BATCH_LENGTH = 4 + 1 + 4 + 2 + 4 + 8;
private enum size_t PRODUCER_EXTENSION_LEN = 8 + 2 + 4;
private enum ubyte MAGIC_V1 = 1;
private enum ubyte MAGIC_V2 = 2;
private enum ushort COMPRESSION_MASK = 0x0007;
private enum ushort HEADERS_BIT = 0x0008;
/// Some record in the batch has a null value (a tombstone). Set only when
/// one is present, so a batch without one encodes exactly as before.
private enum ushort NULL_VALUE_BIT = 0x0040;

/// An ordered, possibly repeating annotation on a record. A null `value`
/// is distinct from an empty one.
struct RecordHeader
{
    string key;
    const(ubyte)[] value;
}

/// One record inside a batch.
struct Record
{
    const(ubyte)[] key;
    /// Null is a tombstone; empty-but-non-null is an ordinary empty value.
    const(ubyte)[] value;
    /// Milliseconds relative to the batch's max timestamp (zero or negative).
    long timestampDelta;
    const(RecordHeader)[] headers;
}

private void appendUvarint(ref ubyte[] buf, ulong value) pure nothrow @safe
{
    while (value >= 0x80)
    {
        buf ~= cast(ubyte)(value | 0x80);
        value >>= 7;
    }
    buf ~= cast(ubyte) value;
}

private ulong getUvarint(const(ubyte)[] data, ref size_t pos) @safe
{
    ulong result;
    uint shift;
    while (true)
    {
        if (pos >= data.length)
            throw new ProtocolException("truncated varint in record");
        const b = data[pos++];
        result |= cast(ulong)(b & 0x7F) << shift;
        if ((b & 0x80) == 0)
            return result;
        shift += 7;
        if (shift > 63)
            throw new ProtocolException("varint overflows 64 bits");
    }
}

/**
 * Encodes one batch exactly as the broker stores it. The broker never
 * re-encodes it: it stamps baseOffset and leaderEpoch in place (both sit
 * before the CRC) and writes these bytes to disk, so getting this wrong
 * corrupts the log rather than merely failing a request.
 */
ubyte[] encodeRecordBatch(const(Record)[] records, long maxTimestamp, Compression codec)
{
    bool hasHeaders = false;
    bool hasNullValues = false;
    foreach (ref record; records)
    {
        if (record.headers.length > 0)
            hasHeaders = true;
        if (record.value is null)
            hasNullValues = true;
    }

    ubyte[] payload;
    ubyte[] rec;
    foreach (ref record; records)
    {
        rec.length = 0;
        rec.assumeSafeAppend();
        if (record.key is null)
            appendUvarint(rec, 0);
        else
        {
            appendUvarint(rec, record.key.length + 1);
            rec ~= record.key;
        }
        if (hasNullValues)
        {
            if (record.value is null)
                appendUvarint(rec, 0);
            else
            {
                appendUvarint(rec, record.value.length + 1);
                rec ~= record.value;
            }
        }
        else
        {
            appendUvarint(rec, record.value.length);
            rec ~= record.value;
        }
        const delta = record.timestampDelta;
        appendUvarint(rec, cast(ulong)((delta << 1) ^ (delta >> 63)));
        if (hasHeaders)
        {
            appendUvarint(rec, record.headers.length);
            foreach (ref header; record.headers)
            {
                appendUvarint(rec, header.key.length);
                rec ~= cast(const(ubyte)[]) header.key;
                if (header.value is null)
                    appendUvarint(rec, 0);
                else
                {
                    appendUvarint(rec, header.value.length + 1);
                    rec ~= header.value;
                }
            }
        }
        appendUvarint(payload, rec.length);
        payload ~= rec;
    }

    const compressed = compress(codec, payload);

    ushort attributes = cast(ushort)(cast(ushort) codec & COMPRESSION_MASK);
    if (hasHeaders)
        attributes |= HEADERS_BIT;
    if (hasNullValues)
        attributes |= NULL_VALUE_BIT;
    const batchLength = MIN_BATCH_LENGTH + compressed.length;

    ubyte[] out_;
    out_.reserve(BATCH_HEADER_LEN + batchLength);
    putBE!long(out_, 0); // base_offset, stamped by the broker
    putBE!uint(out_, cast(uint) batchLength);
    putBE!int(out_, 0); // leader_epoch, likewise
    out_ ~= MAGIC_V1;
    const crcAt = out_.length;
    putBE!uint(out_, 0);
    putBE!ushort(out_, attributes);
    const lastDelta = records.length > 0 ? cast(int)(records.length - 1) : 0;
    putBE!int(out_, lastDelta);
    putBE!long(out_, maxTimestamp);
    out_ ~= compressed;
    writeBE!uint(out_, crcAt, crc32c(out_[crcAt + 4 .. $]));
    return out_;
}

/// One batch read back off the wire.
struct DecodedBatch
{
    long baseOffset;
    long maxTimestamp;
    Record[] records;
}

/// Decodes the batch starting at `offset`, advancing `offset` past it.
DecodedBatch decodeRecordBatch(const(ubyte)[] data, ref size_t offset)
{
    if (offset > data.length || data.length - offset < BATCH_HEADER_LEN)
        throw new ProtocolException("truncated batch header");
    const baseOffset = readBE!long(data, offset);
    const batchLength = readBE!int(data, offset + 8);
    // Also rejects a negative length, which would otherwise wrap.
    if (batchLength < MIN_BATCH_LENGTH)
        throw new ProtocolException(format("batch_length %d too small", batchLength));
    const bodyAt = offset + BATCH_HEADER_LEN;
    if (cast(size_t) batchLength > data.length - bodyAt)
        throw new ProtocolException("truncated batch body");
    const end = bodyAt + batchLength;

    const magic = data[bodyAt + 4];
    if (magic != MAGIC_V1 && magic != MAGIC_V2)
        throw new ProtocolException(format("unsupported magic %d", magic));
    const crcAt = bodyAt + 5;
    const stored = readBE!uint(data, crcAt);
    const computed = crc32c(data[crcAt + 4 .. end]);
    if (stored != computed)
        throw new ProtocolException(format("crc mismatch: stored %#08x, computed %#08x",
                stored, computed));

    size_t cursor = crcAt + 4;
    const attributes = readBE!ushort(data, cursor);
    const maxTimestamp = readBE!long(data, cursor + 6);
    cursor += 14;
    if (magic == MAGIC_V2)
    {
        if (cursor + PRODUCER_EXTENSION_LEN > end)
            throw new ProtocolException("truncated producer extension");
        cursor += PRODUCER_EXTENSION_LEN;
    }

    const payload = decompress(cast(Compression)(attributes & COMPRESSION_MASK),
        data[cursor .. end]);
    auto records = decodeRecords(payload, (attributes & HEADERS_BIT) != 0,
        (attributes & NULL_VALUE_BIT) != 0);
    offset = end;
    return DecodedBatch(baseOffset, maxTimestamp, records);
}

private Record[] decodeRecords(const(ubyte)[] payload, bool hasHeaders, bool hasNullValues)
{
    Record[] records;
    size_t pos = 0;
    // Every slice taken below points into `payload`, whose pointer is
    // non-null whenever it holds a record, so an empty field decodes as
    // empty-but-present and only an explicit null marker yields null.
    while (pos < payload.length)
    {
        const length = getUvarint(payload, pos);
        if (length > payload.length - pos)
            throw new ProtocolException("truncated record");
        const end = pos + cast(size_t) length;

        Record record;
        const keyLenPlusOne = getUvarint(payload, pos);
        if (keyLenPlusOne > 0)
        {
            if (pos > end || keyLenPlusOne - 1 > end - pos)
                throw new ProtocolException("truncated record key");
            const size = cast(size_t)(keyLenPlusOne - 1);
            record.key = payload[pos .. pos + size];
            pos += size;
        }

        ulong rawValueLen = getUvarint(payload, pos);
        if (hasNullValues && rawValueLen == 0)
            record.value = null; // a tombstone
        else
        {
            if (hasNullValues)
                rawValueLen--;
            if (pos > end || rawValueLen > end - pos)
                throw new ProtocolException("truncated record value");
            const size = cast(size_t) rawValueLen;
            record.value = payload[pos .. pos + size];
            pos += size;
        }

        const rawDelta = getUvarint(payload, pos);
        record.timestampDelta = cast(long)(rawDelta >> 1) ^ -cast(long)(rawDelta & 1);

        if (hasHeaders)
        {
            const count = getUvarint(payload, pos);
            // A count is a promise about bytes that follow; allocating on a
            // corrupt one would let a two-byte record ask for gigabytes.
            if (pos > end || count > end - pos)
                throw new ProtocolException("record header count exceeds record");
            RecordHeader[] headers;
            headers.reserve(cast(size_t) count);
            foreach (_; 0 .. cast(size_t) count)
            {
                const keyLen = getUvarint(payload, pos);
                if (pos > end || keyLen > end - pos)
                    throw new ProtocolException("truncated record header key");
                RecordHeader header;
                header.key = cast(string) payload[pos .. pos + cast(size_t) keyLen].idup;
                pos += cast(size_t) keyLen;
                const valuePlusOne = getUvarint(payload, pos);
                if (valuePlusOne > 0)
                {
                    if (pos > end || valuePlusOne - 1 > end - pos)
                        throw new ProtocolException("truncated record header value");
                    const size = cast(size_t)(valuePlusOne - 1);
                    header.value = payload[pos .. pos + size];
                    pos += size;
                }
                headers ~= header;
            }
            record.headers = headers;
        }

        if (pos != end)
            throw new ProtocolException("trailing bytes in record");
        records ~= record;
    }
    return records;
}

// ---------------------------------------------------------------------------
// Partitioning
// ---------------------------------------------------------------------------

/**
 * Kafka's 32-bit murmur2, transcribed so a key lands on the same partition
 * here as from any other client. `murmur2("") == 275646681`.
 */
uint murmur2(const(ubyte)[] data) pure nothrow @safe @nogc
{
    enum uint seed = 0x9747b28c;
    enum uint m = 0x5bd1e995;
    enum r = 24;

    const length = data.length;
    uint h = seed ^ cast(uint) length;
    const chunks = length / 4;

    foreach (i; 0 .. chunks)
    {
        const o = i * 4;
        uint k = data[o] | (cast(uint) data[o + 1] << 8) | (cast(uint) data[o + 2] << 16)
            | (cast(uint) data[o + 3] << 24);
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
    }

    const tail = chunks * 4;
    switch (length - tail)
    {
    case 3:
        h ^= cast(uint) data[tail + 2] << 16;
        goto case 2;
    case 2:
        h ^= cast(uint) data[tail + 1] << 8;
        goto case 1;
    case 1:
        h ^= data[tail];
        h *= m;
        break;
    default:
        break;
    }

    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

/// murmur2(key) % partitions — Kafka's default partitioner.
int partitionForKey(const(ubyte)[] key, const(int)[] partitions) pure nothrow @safe @nogc
{
    return partitions[(murmur2(key) & 0x7fff_ffff) % partitions.length];
}

unittest
{
    assert(murmur2(null) == 275646681);
    assert(crc32c(cast(const(ubyte)[]) "123456789") == 0xE3069283);
    auto w = BodyWriter.start();
    w.i32(-3);
    w.i64(long.min);
    w.str("ok");
    auto r = BodyReader(w.data);
    assert(r.i32() == -3 && r.i64() == long.min && r.str() == "ok");
    assert(emptyBytes() !is null && emptyBytes().length == 0);
}

unittest
{
    // Batch round trip through gzip and a registered (identity) codec, with
    // null/empty distinctions intact.
    registerCodec(Compression.snappy, (const(ubyte)[] p) => p, (const(ubyte)[] p) => p);
    foreach (codec; [Compression.none, Compression.gzip, Compression.snappy])
    {
        const records = [
            Record(emptyBytes(), toBytes("v"), 0, [RecordHeader("h", emptyBytes())]),
            Record(null, null, -3, [RecordHeader("n", null)]),
        ];
        auto encoded = encodeRecordBatch(records, 1000, codec);
        size_t pos = 0;
        auto batch = decodeRecordBatch(encoded, pos);
        assert(pos == encoded.length && batch.records.length == 2);
        assert(batch.records[0].key !is null && batch.records[0].key.length == 0);
        assert(batch.records[0].headers[0].value !is null);
        assert(batch.records[1].key is null && batch.records[1].value is null);
        assert(batch.records[1].headers[0].value is null);
        assert(batch.records[1].timestampDelta == -3);
    }
    // A negative batch length is a decode error, not a wrapped size.
    auto bad = encodeRecordBatch([Record(null, toBytes("x"), 0, null)], 0, Compression.none);
    writeBE!int(bad, 8, -1);
    size_t at = 0;
    bool threw = false;
    try
        decodeRecordBatch(bad, at);
    catch (ProtocolException)
        threw = true;
    assert(threw);
}
