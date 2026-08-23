package io.brahmaputra;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Brahmaputra wire protocol: framing, BitPacker bodies, record batches.
 *
 * <p>Three encodings live in one connection and they do not agree with each other, so keeping
 * them straight is most of the work:
 *
 * <ul>
 *   <li>The <b>frame header</b> is fixed big-endian — an int32 length prefix, then
 *       apiKey/apiVersion/correlationId and a length-prefixed client id.
 *   <li>A <b>request body</b> is BitPacker: every integer is a zigzag varint, every string and
 *       array is a varint count followed by its contents, and the whole body is prefixed with
 *       the schema version string.
 *   <li>A <b>record batch</b> is neither. Fixed big-endian header fields and <i>plain</i>
 *       (non-zigzag) varints inside each record, because the broker stamps offsets into it in
 *       place and validates its CRC without decoding it.
 * </ul>
 *
 * <p>Mixing those up produces a frame the broker rejects with no useful error, so each encoder
 * here is deliberately explicit about which one it is.
 */
public final class Protocol {

    private Protocol() {}

    /** The BitPacker schema version every body carries as its first field. */
    public static final String SCHEMA_VERSION = "1.0.0";

    /** Wire version this client speaks. The broker requires an exact match. */
    // Version 3 added transactions: Fetch carries an isolation_level, and
    // MetadataResponse carries a request-level error code so an authorization
    // denial is no longer reported as an unknown topic.
    public static final short API_VERSION = 4;

    /** Isolation levels for a fetch. READ_UNCOMMITTED is the default. */
    public static final int READ_UNCOMMITTED = 0;
    public static final int READ_COMMITTED = 1;

    static final int BATCH_HEADER_LEN = 12;
    static final int MIN_BATCH_LENGTH = 4 + 1 + 4 + 2 + 4 + 8;
    static final int PRODUCER_EXTENSION_LEN = 8 + 2 + 4;

    static final byte MAGIC_V1 = 1;
    static final byte MAGIC_V2 = 2;

    static final int COMPRESSION_MASK = 0x0007;
    static final int HEADERS_BIT = 0x0008;
    /// Some record in this batch has a null value: a tombstone, which
    /// deletes its key on a compacted topic. Set only when one is present,
    /// so a batch without one encodes exactly as it always did.
    static final int NULL_VALUE_BIT = 0x0040;

    static final int MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024;

    /** API keys, in wire order. */
    public static final class ApiKey {
        private ApiKey() {}

        public static final short PRODUCE = 0;
        public static final short FETCH = 1;
        public static final short LIST_OFFSETS = 2;
        public static final short METADATA = 3;
        public static final short REPLICA_FETCH = 4;
        public static final short OFFSETS_FOR_LEADER_EPOCH = 5;
        public static final short INIT_PRODUCER_ID = 6;
        public static final short JOIN_GROUP = 7;
        public static final short SYNC_GROUP = 8;
        public static final short HEARTBEAT = 9;
        public static final short OFFSET_COMMIT = 10;
        public static final short OFFSET_FETCH = 11;
        public static final short LIST_GROUPS = 12;
        public static final short DESCRIBE_GROUP = 13;
        public static final short API_VERSIONS = 14;
        public static final short PRODUCE_MULTI = 15;
        public static final short FETCH_MULTI = 16;
        public static final short AUTHENTICATE = 17;
        public static final short LEAVE_GROUP = 18;
    }

    /** Error codes the broker returns in a response's {@code error_code} field. */
    public static final class ErrorCode {
        private ErrorCode() {}

        public static final int NONE = 0;
        public static final int UNKNOWN_TOPIC_OR_PARTITION = 1;
        public static final int OFFSET_OUT_OF_RANGE = 2;
        public static final int INVALID_REQUEST = 3;
        public static final int UNSUPPORTED_VERSION = 4;
        public static final int INTERNAL = 5;
        public static final int NOT_LEADER_OR_FOLLOWER = 6;
        public static final int FENCED_BROKER_EPOCH = 7;
        public static final int FENCED_LEADER_EPOCH = 8;
        public static final int UNKNOWN_LEADER_EPOCH = 9;
        public static final int NOT_ENOUGH_REPLICAS = 10;
        public static final int FENCED_PRODUCER_EPOCH = 11;
        public static final int OUT_OF_ORDER_SEQUENCE = 12;
        public static final int UNKNOWN_MEMBER_ID = 13;
        public static final int REBALANCE_IN_PROGRESS = 14;
        public static final int NOT_COORDINATOR = 15;
        public static final int ILLEGAL_GENERATION = 16;
        public static final int COORDINATOR_LOAD_IN_PROGRESS = 17;
        public static final int SASL_AUTHENTICATION_FAILED = 18;
        public static final int AUTHORIZATION_FAILED = 19;

        static String name(int code) {
            switch (code) {
                case NONE: return "NONE";
                case UNKNOWN_TOPIC_OR_PARTITION: return "UNKNOWN_TOPIC_OR_PARTITION";
                case OFFSET_OUT_OF_RANGE: return "OFFSET_OUT_OF_RANGE";
                case INVALID_REQUEST: return "INVALID_REQUEST";
                case UNSUPPORTED_VERSION: return "UNSUPPORTED_VERSION";
                case INTERNAL: return "INTERNAL";
                case NOT_LEADER_OR_FOLLOWER: return "NOT_LEADER_OR_FOLLOWER";
                case FENCED_BROKER_EPOCH: return "FENCED_BROKER_EPOCH";
                case FENCED_LEADER_EPOCH: return "FENCED_LEADER_EPOCH";
                case UNKNOWN_LEADER_EPOCH: return "UNKNOWN_LEADER_EPOCH";
                case NOT_ENOUGH_REPLICAS: return "NOT_ENOUGH_REPLICAS";
                case FENCED_PRODUCER_EPOCH: return "FENCED_PRODUCER_EPOCH";
                case OUT_OF_ORDER_SEQUENCE: return "OUT_OF_ORDER_SEQUENCE";
                case UNKNOWN_MEMBER_ID: return "UNKNOWN_MEMBER_ID";
                case REBALANCE_IN_PROGRESS: return "REBALANCE_IN_PROGRESS";
                case NOT_COORDINATOR: return "NOT_COORDINATOR";
                case ILLEGAL_GENERATION: return "ILLEGAL_GENERATION";
                case COORDINATOR_LOAD_IN_PROGRESS: return "COORDINATOR_LOAD_IN_PROGRESS";
                case SASL_AUTHENTICATION_FAILED: return "SASL_AUTHENTICATION_FAILED";
                case AUTHORIZATION_FAILED: return "AUTHORIZATION_FAILED";
                default: return "UNKNOWN";
            }
        }
    }

    /**
     * Whether a broker error code means "this send did not happen, try again".
     *
     * <p>Every code here is one the broker returns strictly before it appends anything, so a
     * retry cannot duplicate a record. Anything else is returned to the caller as-is: a
     * malformed request or a failed authorization fails identically however often it is sent,
     * and the idempotence codes mean the producer's sequence state is already broken.
     */
    public static boolean isRetriable(int code) {
        switch (code) {
            case ErrorCode.NOT_LEADER_OR_FOLLOWER:
            case ErrorCode.FENCED_LEADER_EPOCH:
            case ErrorCode.UNKNOWN_LEADER_EPOCH:
            case ErrorCode.NOT_ENOUGH_REPLICAS:
            case ErrorCode.COORDINATOR_LOAD_IN_PROGRESS:
            case ErrorCode.INTERNAL:
                return true;
            default:
                return false;
        }
    }

    // -----------------------------------------------------------------------
    // Exceptions
    // -----------------------------------------------------------------------

    /** Base class for every error this client raises. */
    public static class BrahmaputraException extends RuntimeException {
        public BrahmaputraException(String message) { super(message); }
        public BrahmaputraException(String message, Throwable cause) { super(message, cause); }
    }

    /** The bytes on the wire were not what the protocol allows. */
    public static class ProtocolException extends BrahmaputraException {
        public ProtocolException(String message) { super(message); }
    }

    /** The broker answered with a non-zero error code. */
    public static class ServerException extends BrahmaputraException {
        public final int code;

        public ServerException(int code, String context) {
            super("broker returned " + ErrorCode.name(code) + "[" + code + "]"
                    + (context.isEmpty() ? "" : " (" + context + ")"));
            this.code = code;
        }
    }

    /** {@code auto.offset.reset=none} and there is no position to resume from. */
    public static class NoOffsetForPartitionException extends BrahmaputraException {
        public NoOffsetForPartitionException(String message) { super(message); }
    }

    // -----------------------------------------------------------------------
    // BitPacker primitives
    // -----------------------------------------------------------------------

    /**
     * Builds a BitPacker body. Every integer goes out zigzag-varint encoded, which is why this
     * cannot share code with the record-batch encoder below.
     */
    public static final class Writer {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream(256);

        /** A writer already carrying the schema version every body starts with. */
        public static Writer body() {
            Writer writer = new Writer();
            writer.string(SCHEMA_VERSION);
            return writer;
        }

        public Writer uvarint(long value) {
            while ((value & ~0x7FL) != 0) {
                out.write((int) ((value & 0x7F) | 0x80));
                value >>>= 7;
            }
            out.write((int) value);
            return this;
        }

        public Writer int32(int value) {
            return uvarint(Integer.toUnsignedLong((value << 1) ^ (value >> 31)));
        }

        public Writer int64(long value) {
            return uvarint((value << 1) ^ (value >> 63));
        }

        public Writer bool(boolean value) {
            out.write(value ? 1 : 0);
            return this;
        }

        public Writer string(String value) {
            byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
            int32(encoded.length);
            out.write(encoded, 0, encoded.length);
            return this;
        }

        public Writer stringArray(List<String> values) {
            int32(values.size());
            for (String value : values) {
                string(value);
            }
            return this;
        }

        public Writer raw(byte[] data) {
            out.write(data, 0, data.length);
            return this;
        }

        public byte[] bytes() {
            return out.toByteArray();
        }
    }

    /** Reads a BitPacker body. */
    public static final class Reader {
        private final byte[] data;
        private int pos;

        public Reader(byte[] data) {
            this.data = data;
        }

        /**
         * A reader positioned past the schema version, which is verified.
         *
         * <p>A mismatch means the broker and this client disagree about the message shapes
         * themselves, so failing loudly beats decoding garbage into plausible-looking fields.
         */
        public static Reader body(byte[] data) {
            Reader reader = new Reader(data);
            String version = reader.string();
            if (!SCHEMA_VERSION.equals(version)) {
                throw new ProtocolException("schema version mismatch: broker speaks " + version
                        + ", this client speaks " + SCHEMA_VERSION);
            }
            return reader;
        }

        public int remaining() {
            return data.length - pos;
        }

        public long uvarint() {
            long result = 0;
            int shift = 0;
            while (true) {
                if (pos >= data.length) {
                    throw new ProtocolException("truncated varint");
                }
                int b = data[pos++] & 0xFF;
                result |= ((long) (b & 0x7F)) << shift;
                if ((b & 0x80) == 0) {
                    return result;
                }
                shift += 7;
                if (shift > 63) {
                    throw new ProtocolException("varint overflows 64 bits");
                }
            }
        }

        public int int32() {
            long raw = uvarint();
            return (int) ((raw >>> 1) ^ -(raw & 1));
        }

        public long int64() {
            long raw = uvarint();
            return (raw >>> 1) ^ -(raw & 1);
        }

        public boolean bool() {
            if (pos >= data.length) {
                throw new ProtocolException("truncated bool");
            }
            return data[pos++] != 0;
        }

        public String string() {
            int length = int32();
            if (length < 0 || pos + length > data.length) {
                throw new ProtocolException("truncated string");
            }
            String value = new String(data, pos, length, StandardCharsets.UTF_8);
            pos += length;
            return value;
        }

        public List<String> stringArray() {
            int count = int32();
            List<String> out = new ArrayList<>(Math.max(count, 0));
            for (int index = 0; index < count; index++) {
                out.add(string());
            }
            return out;
        }

        public byte[] rest() {
            byte[] value = new byte[data.length - pos];
            System.arraycopy(data, pos, value, 0, value.length);
            pos = data.length;
            return value;
        }

        /**
         * Read and discard a field.
         *
         * <p>Named rather than ignoring a return value, because the encoding is positional: a
         * skipped field must still be <i>read</i> or everything after it misaligns.
         */
        public void skipString() { string(); }

        public void skipInt32() { int32(); }

        public void skipInt64() { int64(); }
    }

    // -----------------------------------------------------------------------
    // Frames
    // -----------------------------------------------------------------------

    /**
     * One complete frame, length prefix included.
     *
     * <p>The header is fixed big-endian while the body is BitPacker: the broker has to read the
     * header before it knows which body decoder to use, so the header cannot depend on the
     * schema.
     */
    public static byte[] encodeFrame(short apiKey, int correlationId, String clientId, byte[] body) {
        byte[] client = clientId == null
                ? null
                : clientId.getBytes(StandardCharsets.UTF_8);
        int clientLen = client == null ? 0 : client.length;
        int payloadLen = 10 + clientLen + body.length;

        ByteBuffer buffer = ByteBuffer.allocate(4 + payloadLen);
        buffer.putInt(payloadLen);
        buffer.putShort(apiKey);
        buffer.putShort(API_VERSION);
        buffer.putInt(correlationId);
        buffer.putShort(client == null ? (short) -1 : (short) clientLen);
        if (client != null) {
            buffer.put(client);
        }
        buffer.put(body);
        return buffer.array();
    }

    /** A frame payload split into its correlation id and body. */
    public static final class FramePayload {
        public final int correlationId;
        public final byte[] body;

        FramePayload(int correlationId, byte[] body) {
            this.correlationId = correlationId;
            this.body = body;
        }
    }

    public static FramePayload decodeFramePayload(byte[] payload) {
        if (payload.length < 10) {
            throw new ProtocolException("frame payload shorter than its header");
        }
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        buffer.getShort(); // api_key
        buffer.getShort(); // api_version
        int correlationId = buffer.getInt();
        short clientLen = buffer.getShort();
        int offset = 10;
        if (clientLen >= 0) {
            offset += clientLen;
        }
        if (offset > payload.length) {
            throw new ProtocolException("frame client id runs past the payload");
        }
        byte[] body = new byte[payload.length - offset];
        System.arraycopy(payload, offset, body, 0, body.length);
        return new FramePayload(correlationId, body);
    }

    // -----------------------------------------------------------------------
    // CRC32C
    // -----------------------------------------------------------------------

    private static final int[] CRC32C_TABLE = buildCrc32cTable();

    private static int[] buildCrc32cTable() {
        // Castagnoli polynomial, reflected. Record batches use CRC32C rather than the
        // java.util.zip.CRC32, so the JDK's CRC32 is no help here. (JDK 9+ does ship
        // CRC32C, but building the table keeps this driver buildable on 8.)
        int poly = 0x82F63B78;
        int[] table = new int[256];
        for (int index = 0; index < 256; index++) {
            int crc = index;
            for (int bit = 0; bit < 8; bit++) {
                crc = (crc & 1) != 0 ? (crc >>> 1) ^ poly : crc >>> 1;
            }
            table[index] = crc;
        }
        return table;
    }

    public static int crc32c(byte[] data, int from, int to) {
        int crc = 0xFFFFFFFF;
        for (int index = from; index < to; index++) {
            crc = CRC32C_TABLE[(crc ^ data[index]) & 0xFF] ^ (crc >>> 8);
        }
        return crc ^ 0xFFFFFFFF;
    }

    public static int crc32c(byte[] data) {
        return crc32c(data, 0, data.length);
    }

    // -----------------------------------------------------------------------
    // Compression
    // -----------------------------------------------------------------------

    /** Compression codecs, matching the broker's attribute values. */
    public enum Compression {
        NONE(0, "none"),
        LZ4(1, "lz4"),
        ZSTD(2, "zstd"),
        SNAPPY(3, "snappy"),
        GZIP(4, "gzip");

        public final int value;
        public final String label;

        Compression(int value, String label) {
            this.value = value;
            this.label = label;
        }

        /** Parse the {@code compression.type} spelling Kafka uses. */
        public static Compression parse(String name) {
            for (Compression codec : values()) {
                if (codec.label.equals(name)) {
                    return codec;
                }
            }
            throw new BrahmaputraException(
                    "unknown compression " + name + " (none, lz4, zstd, snappy, gzip)");
        }

        static Compression fromValue(int value) {
            for (Compression codec : values()) {
                if (codec.value == value) {
                    return codec;
                }
            }
            throw new ProtocolException("unsupported compression " + value);
        }
    }

    /**
     * A codec this driver does not carry itself.
     *
     * <p>Only {@code none} and {@code gzip} are built in, so an application that does not want
     * an lz4 or zstd dependency does not acquire one by using this client. The lz4 payload the
     * broker expects is a little-endian uint32 of the uncompressed length followed by a raw LZ4
     * block — <i>not</i> the LZ4 frame format — so a frame-format library will not interoperate.
     */
    public interface Codec {
        byte[] compress(byte[] payload);

        byte[] decompress(byte[] payload);
    }

    private static final java.util.Map<Compression, Codec> EXTERNAL_CODECS =
            new java.util.EnumMap<>(Compression.class);

    public static void registerCodec(Compression codec, Codec implementation) {
        EXTERNAL_CODECS.put(codec, implementation);
    }

    static byte[] compress(Compression codec, byte[] payload) {
        if (codec == Compression.NONE) {
            return payload;
        }
        if (codec == Compression.GZIP) {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream(payload.length / 2 + 32);
                try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
                    gzip.write(payload);
                }
                return out.toByteArray();
            } catch (IOException error) {
                throw new BrahmaputraException("gzip compression failed", error);
            }
        }
        Codec external = EXTERNAL_CODECS.get(codec);
        if (external != null) {
            return external.compress(payload);
        }
        throw new BrahmaputraException(codec.label
                + " compression is not registered; call registerCodec or use none/gzip");
    }

    static byte[] decompress(Compression codec, byte[] payload) {
        if (codec == Compression.NONE) {
            return payload;
        }
        if (codec == Compression.GZIP) {
            try (GZIPInputStream gzip = new GZIPInputStream(
                    new java.io.ByteArrayInputStream(payload))) {
                ByteArrayOutputStream out = new ByteArrayOutputStream(payload.length * 2);
                byte[] chunk = new byte[8192];
                int total = 0;
                int read;
                while ((read = gzip.read(chunk)) > 0) {
                    total += read;
                    // Capped so a corrupt or hostile batch cannot name gigabytes of output
                    // this process allocates before it can reject it.
                    if (total > MAX_DECOMPRESSED_BYTES) {
                        throw new ProtocolException("decompressed payload too large");
                    }
                    out.write(chunk, 0, read);
                }
                return out.toByteArray();
            } catch (IOException error) {
                throw new BrahmaputraException("gzip decompression failed", error);
            }
        }
        Codec external = EXTERNAL_CODECS.get(codec);
        if (external != null) {
            return external.decompress(payload);
        }
        throw new BrahmaputraException(codec.label + " decompression is not registered");
    }

    // -----------------------------------------------------------------------
    // Record batches
    // -----------------------------------------------------------------------

    /** An ordered, possibly repeating annotation on a record. A null value differs from empty. */
    public static final class RecordHeader {
        public final String key;
        public final byte[] value;

        public RecordHeader(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    /** One record inside a batch. */
    public static final class Record {
        public final byte[] key;
        public final byte[] value;
        /** Milliseconds relative to the batch's max timestamp, so normally &le; 0. */
        public long timestampDelta;
        public final List<RecordHeader> headers;

        public Record(byte[] key, byte[] value, List<RecordHeader> headers) {
            this.key = key;
            this.value = value;
            this.headers = headers == null ? new ArrayList<>() : headers;
        }

        public long timestamp(long maxTimestamp) {
            return maxTimestamp + timestampDelta;
        }

        public byte[] header(String name) {
            for (RecordHeader header : headers) {
                if (header.key.equals(name)) {
                    return header.value;
                }
            }
            return null;
        }
    }

    /** One batch read back off the wire. */
    public static final class DecodedBatch {
        public final long baseOffset;
        public final long maxTimestamp;
        public final List<Record> records;

        DecodedBatch(long baseOffset, long maxTimestamp, List<Record> records) {
            this.baseOffset = baseOffset;
            this.maxTimestamp = maxTimestamp;
            this.records = records;
        }
    }

    /**
     * Encode one record batch exactly as the broker expects it.
     *
     * <p>The broker never re-encodes this: it validates the header, stamps {@code base_offset}
     * and {@code leader_epoch} in place (both sit before the CRC, so it stays valid), and writes
     * these bytes to disk. Getting this wrong corrupts the log rather than merely failing a
     * request.
     */
    public static byte[] encodeRecordBatch(
            List<Record> records, long maxTimestamp, Compression codec) {
        boolean hasHeaders = false;
        for (Record record : records) {
            if (!record.headers.isEmpty()) {
                hasHeaders = true;
                break;
            }
        }
        // A null value is a tombstone and needs the widened length encoding;
        // a zero-length value is an ordinary record and must not trigger it.
        boolean hasNullValues = false;
        for (Record record : records) {
            if (record.value == null) {
                hasNullValues = true;
                break;
            }
        }

        ByteArrayOutputStream payload = new ByteArrayOutputStream(1024);
        for (Record record : records) {
            ByteArrayOutputStream rec = new ByteArrayOutputStream(64);
            if (record.key == null) {
                putUvarint(rec, 0);
            } else {
                putUvarint(rec, record.key.length + 1L);
                rec.write(record.key, 0, record.key.length);
            }
            if (hasNullValues) {
                if (record.value == null) {
                    putUvarint(rec, 0);
                } else {
                    putUvarint(rec, record.value.length + 1L);
                    rec.write(record.value, 0, record.value.length);
                }
            } else {
                putUvarint(rec, record.value.length);
                rec.write(record.value, 0, record.value.length);
            }
            putUvarint(rec, (record.timestampDelta << 1) ^ (record.timestampDelta >> 63));
            if (hasHeaders) {
                putUvarint(rec, record.headers.size());
                for (RecordHeader header : record.headers) {
                    byte[] key = header.key.getBytes(StandardCharsets.UTF_8);
                    putUvarint(rec, key.length);
                    rec.write(key, 0, key.length);
                    if (header.value == null) {
                        putUvarint(rec, 0);
                    } else {
                        putUvarint(rec, header.value.length + 1L);
                        rec.write(header.value, 0, header.value.length);
                    }
                }
            }
            byte[] encoded = rec.toByteArray();
            putUvarint(payload, encoded.length);
            payload.write(encoded, 0, encoded.length);
        }

        byte[] compressed = compress(codec, payload.toByteArray());
        int attributes = codec.value & COMPRESSION_MASK;
        if (hasHeaders) {
            attributes |= HEADERS_BIT;
        }
        if (hasNullValues) {
            attributes |= NULL_VALUE_BIT;
        }
        int batchLength = MIN_BATCH_LENGTH + compressed.length;

        ByteBuffer buffer = ByteBuffer.allocate(BATCH_HEADER_LEN + batchLength);
        buffer.putLong(0L);            // base_offset, stamped by the broker
        buffer.putInt(batchLength);
        buffer.putInt(0);              // leader_epoch, likewise
        buffer.put(MAGIC_V1);
        int crcAt = buffer.position();
        buffer.putInt(0);              // crc placeholder
        buffer.putShort((short) attributes);
        buffer.putInt(Math.max(records.size() - 1, 0));
        buffer.putLong(maxTimestamp);
        buffer.put(compressed);

        byte[] out = buffer.array();
        int crc = crc32c(out, crcAt + 4, out.length);
        ByteBuffer.wrap(out).putInt(crcAt, crc);
        return out;
    }

    /** Decoded batch plus the offset just past it. */
    public static final class BatchAt {
        public final DecodedBatch batch;
        public final int next;

        BatchAt(DecodedBatch batch, int next) {
            this.batch = batch;
            this.next = next;
        }
    }

    public static BatchAt decodeRecordBatch(byte[] data, int offset) {
        if (data.length - offset < BATCH_HEADER_LEN) {
            throw new ProtocolException("truncated batch header");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long baseOffset = buffer.getLong(offset);
        int batchLength = buffer.getInt(offset + 8);
        if (batchLength < MIN_BATCH_LENGTH) {
            throw new ProtocolException("batch_length too small");
        }
        int bodyAt = offset + BATCH_HEADER_LEN;
        int end = bodyAt + batchLength;
        if (end > data.length) {
            throw new ProtocolException("truncated batch body");
        }

        byte magic = data[bodyAt + 4];
        if (magic != MAGIC_V1 && magic != MAGIC_V2) {
            throw new ProtocolException("unsupported magic " + magic);
        }
        int crcAt = bodyAt + 5;
        int stored = buffer.getInt(crcAt);
        int computed = crc32c(data, crcAt + 4, end);
        if (stored != computed) {
            throw new ProtocolException(String.format(
                    "crc mismatch: stored %#010x, computed %#010x", stored, computed));
        }

        int cursor = crcAt + 4;
        int attributes = buffer.getShort(cursor) & 0xFFFF;
        long maxTimestamp = buffer.getLong(cursor + 6);
        cursor += 14;
        if (magic == MAGIC_V2) {
            cursor += PRODUCER_EXTENSION_LEN;
        }

        byte[] region = new byte[end - cursor];
        System.arraycopy(data, cursor, region, 0, region.length);
        byte[] decompressed =
                decompress(Compression.fromValue(attributes & COMPRESSION_MASK), region);
        List<Record> records =
                decodeRecords(
                        decompressed,
                        (attributes & HEADERS_BIT) != 0,
                        (attributes & NULL_VALUE_BIT) != 0);
        return new BatchAt(new DecodedBatch(baseOffset, maxTimestamp, records), end);
    }

    private static List<Record> decodeRecords(
            byte[] payload, boolean hasHeaders, boolean hasNullValues) {
        List<Record> records = new ArrayList<>();
        int pos = 0;
        while (pos < payload.length) {
            long[] read = getUvarint(payload, pos);
            int length = (int) read[0];
            pos = (int) read[1];
            if (pos + length > payload.length) {
                throw new ProtocolException("truncated record");
            }
            int end = pos + length;

            read = getUvarint(payload, pos);
            long keyLenPlusOne = read[0];
            pos = (int) read[1];
            byte[] key = null;
            if (keyLenPlusOne > 0) {
                int size = (int) keyLenPlusOne - 1;
                key = new byte[size];
                System.arraycopy(payload, pos, key, 0, size);
                pos += size;
            }

            read = getUvarint(payload, pos);
            long rawValueLen = read[0];
            pos = (int) read[1];
            byte[] value;
            if (hasNullValues && rawValueLen == 0) {
                // A tombstone. Null rather than an empty array, which is
                // what distinguishes a deletion from an empty value.
                value = null;
            } else {
                int valueLen = (int) (hasNullValues ? rawValueLen - 1 : rawValueLen);
                value = new byte[valueLen];
                System.arraycopy(payload, pos, value, 0, valueLen);
                pos += valueLen;
            }

            read = getUvarint(payload, pos);
            long rawDelta = read[0];
            pos = (int) read[1];

            List<RecordHeader> headers = new ArrayList<>();
            if (hasHeaders) {
                read = getUvarint(payload, pos);
                long count = read[0];
                pos = (int) read[1];
                // A count is a promise about bytes that follow; if it exceeds what is left it
                // is corrupt, and allocating on it would let a two-byte record ask for
                // gigabytes.
                if (count > end - pos) {
                    throw new ProtocolException("record header count exceeds record");
                }
                for (long index = 0; index < count; index++) {
                    read = getUvarint(payload, pos);
                    int keyLen = (int) read[0];
                    pos = (int) read[1];
                    String headerKey = new String(payload, pos, keyLen, StandardCharsets.UTF_8);
                    pos += keyLen;
                    read = getUvarint(payload, pos);
                    long valuePlusOne = read[0];
                    pos = (int) read[1];
                    byte[] headerValue = null;
                    if (valuePlusOne > 0) {
                        int size = (int) valuePlusOne - 1;
                        headerValue = new byte[size];
                        System.arraycopy(payload, pos, headerValue, 0, size);
                        pos += size;
                    }
                    headers.add(new RecordHeader(headerKey, headerValue));
                }
            }

            if (pos != end) {
                throw new ProtocolException("trailing bytes in record");
            }
            Record record = new Record(key, value, headers);
            record.timestampDelta = (rawDelta >>> 1) ^ -(rawDelta & 1);
            records.add(record);
        }
        return records;
    }

    private static void putUvarint(ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }

    /** Returns {@code {value, nextPosition}}. */
    private static long[] getUvarint(byte[] data, int pos) {
        long result = 0;
        int shift = 0;
        while (true) {
            if (pos >= data.length) {
                throw new ProtocolException("truncated varint in record");
            }
            int b = data[pos++] & 0xFF;
            result |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0) {
                return new long[] {result, pos};
            }
            shift += 7;
            if (shift > 63) {
                throw new ProtocolException("varint overflows 64 bits");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Partitioning
    // -----------------------------------------------------------------------

    /**
     * Kafka's 32-bit murmur2, so a key lands on the same partition here as it would there.
     *
     * <p>Reproduced rather than imported because the whole point is that a Java producer and a
     * Rust producer writing the same key must agree, and "some murmur2" is not good enough — it
     * has to be this one.
     */
    public static int murmur2(byte[] data) {
        final int seed = 0x9747b28c;
        final int m = 0x5bd1e995;
        final int r = 24;

        int length = data.length;
        int h = seed ^ length;
        int chunks = length / 4;

        for (int index = 0; index < chunks; index++) {
            int offset = index * 4;
            int k = (data[offset] & 0xFF)
                    | ((data[offset + 1] & 0xFF) << 8)
                    | ((data[offset + 2] & 0xFF) << 16)
                    | ((data[offset + 3] & 0xFF) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        int tail = chunks * 4;
        switch (length - tail) {
            case 3:
                h ^= (data[tail + 2] & 0xFF) << 16;
                h ^= (data[tail + 1] & 0xFF) << 8;
                h ^= data[tail] & 0xFF;
                h *= m;
                break;
            case 2:
                h ^= (data[tail + 1] & 0xFF) << 8;
                h ^= data[tail] & 0xFF;
                h *= m;
                break;
            case 1:
                h ^= data[tail] & 0xFF;
                h *= m;
                break;
            default:
                break;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    /** {@code murmur2(key) % partitions}, matching Kafka's default partitioner. */
    public static int partitionForKey(byte[] key, List<Integer> partitions) {
        return partitions.get(Math.floorMod(murmur2(key) & 0x7fffffff, partitions.size()));
    }
}
