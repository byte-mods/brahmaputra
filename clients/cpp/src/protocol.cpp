#include "brahmaputra/protocol.hpp"

#include <array>
#include <cstdio>

namespace brahmaputra {

namespace {

constexpr std::size_t kBatchHeaderLen = 12;
constexpr std::size_t kMinBatchLength = 4 + 1 + 4 + 2 + 4 + 8;
constexpr std::size_t kProducerExtensionLen = 8 + 2 + 4;
constexpr std::uint8_t kMagicV1 = 1;
constexpr std::uint8_t kMagicV2 = 2;
constexpr std::uint16_t kCompressionMask = 0x0007;
constexpr std::uint16_t kHeadersBit = 0x0008;
// Some record in this batch has a null value (a tombstone). Set only when
// one is present, so a batch without one encodes exactly as it always did.
constexpr std::uint16_t kNullValueBit = 0x0040;

void putU16(Bytes& out, std::uint16_t v) {
    out.push_back(static_cast<std::uint8_t>(v >> 8));
    out.push_back(static_cast<std::uint8_t>(v));
}
void putU32(Bytes& out, std::uint32_t v) {
    for (int shift = 24; shift >= 0; shift -= 8) out.push_back(static_cast<std::uint8_t>(v >> shift));
}
void putU64(Bytes& out, std::uint64_t v) {
    for (int shift = 56; shift >= 0; shift -= 8) out.push_back(static_cast<std::uint8_t>(v >> shift));
}
std::uint16_t getU16(const std::uint8_t* p) {
    return static_cast<std::uint16_t>((p[0] << 8) | p[1]);
}
std::uint32_t getU32(const std::uint8_t* p) {
    return (std::uint32_t(p[0]) << 24) | (std::uint32_t(p[1]) << 16) | (std::uint32_t(p[2]) << 8) |
           std::uint32_t(p[3]);
}
std::uint64_t getU64(const std::uint8_t* p) {
    return (std::uint64_t(getU32(p)) << 32) | getU32(p + 4);
}

void appendUvarint(Bytes& buf, std::uint64_t value) {
    while (value >= 0x80) {
        buf.push_back(static_cast<std::uint8_t>(value | 0x80));
        value >>= 7;
    }
    buf.push_back(static_cast<std::uint8_t>(value));
}

std::uint64_t readUvarint(const Bytes& data, std::size_t& pos, std::size_t end, const char* what) {
    std::uint64_t result = 0;
    unsigned shift = 0;
    for (;;) {
        if (pos >= end) throw Error(std::string("truncated varint in ") + what);
        std::uint8_t b = data[pos++];
        result |= std::uint64_t(b & 0x7F) << shift;
        if ((b & 0x80) == 0) return result;
        shift += 7;
        if (shift > 63) throw Error("varint overflows 64 bits");
    }
}

std::uint64_t zigzag64(std::int64_t v) {
    return (static_cast<std::uint64_t>(v) << 1) ^ static_cast<std::uint64_t>(v >> 63);
}
std::int64_t unzigzag64(std::uint64_t v) {
    return static_cast<std::int64_t>((v >> 1) ^ (~(v & 1) + 1));
}

}  // namespace

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

const char* errorName(std::int32_t code) {
    static const char* const names[] = {
        "NONE", "UNKNOWN_TOPIC_OR_PARTITION", "OFFSET_OUT_OF_RANGE", "INVALID_REQUEST",
        "UNSUPPORTED_VERSION", "INTERNAL", "NOT_LEADER_OR_FOLLOWER", "FENCED_BROKER_EPOCH",
        "FENCED_LEADER_EPOCH", "UNKNOWN_LEADER_EPOCH", "NOT_ENOUGH_REPLICAS",
        "FENCED_PRODUCER_EPOCH", "OUT_OF_ORDER_SEQUENCE", "UNKNOWN_MEMBER_ID",
        "REBALANCE_IN_PROGRESS", "NOT_COORDINATOR", "ILLEGAL_GENERATION",
        "COORDINATOR_LOAD_IN_PROGRESS", "SASL_AUTHENTICATION_FAILED", "AUTHORIZATION_FAILED",
    };
    if (code < 0 || code >= static_cast<std::int32_t>(sizeof(names) / sizeof(names[0]))) {
        return "UNKNOWN";
    }
    return names[code];
}

static std::string serverErrorMessage(std::int32_t code, const std::string& context) {
    std::string msg = std::string("broker returned ") + errorName(code) + "[" +
                      std::to_string(code) + "]";
    if (!context.empty()) msg += " (" + context + ")";
    return msg;
}

ServerError::ServerError(std::int32_t code, const std::string& context)
    : Error(serverErrorMessage(code, context)), code_(code) {}

bool isRetriable(std::int32_t code) {
    switch (code) {
        case errc::NotLeaderOrFollower:
        case errc::FencedLeaderEpoch:
        case errc::UnknownLeaderEpoch:
        case errc::NotEnoughReplicas:
        case errc::CoordinatorLoadInProgress:
        case errc::Internal:
            return true;
        default:
            return false;
    }
}

// ---------------------------------------------------------------------------
// BitPacker
// ---------------------------------------------------------------------------

BodyWriter::BodyWriter() {
    buf_.reserve(256);
    string(kSchemaVersion);
}

void BodyWriter::uvarint(std::uint64_t value) { appendUvarint(buf_, value); }

void BodyWriter::int32(std::int32_t value) {
    uvarint((static_cast<std::uint32_t>(value) << 1) ^ static_cast<std::uint32_t>(value >> 31));
}

void BodyWriter::int64(std::int64_t value) { uvarint(zigzag64(value)); }

void BodyWriter::boolean(bool value) { buf_.push_back(value ? 1 : 0); }

void BodyWriter::string(const std::string& value) {
    int32(static_cast<std::int32_t>(value.size()));
    buf_.insert(buf_.end(), value.begin(), value.end());
}

void BodyWriter::stringArray(const std::vector<std::string>& values) {
    int32(static_cast<std::int32_t>(values.size()));
    for (const auto& v : values) string(v);
}

void BodyWriter::raw(const Bytes& data) { buf_.insert(buf_.end(), data.begin(), data.end()); }

BodyReader::BodyReader(const Bytes& data) : data_(data) {
    std::string version = string();
    if (version != kSchemaVersion) {
        throw Error("schema version mismatch: broker speaks \"" + version +
                    "\", this client speaks \"" + kSchemaVersion + "\"");
    }
}

std::uint64_t BodyReader::uvarint() { return readUvarint(data_, pos_, data_.size(), "body"); }

std::int32_t BodyReader::int32() {
    std::uint64_t v = uvarint();
    std::uint32_t u = static_cast<std::uint32_t>(v);
    return static_cast<std::int32_t>((u >> 1) ^ (~(u & 1) + 1));
}

std::int64_t BodyReader::int64() { return unzigzag64(uvarint()); }

bool BodyReader::boolean() {
    if (pos_ >= data_.size()) throw Error("truncated bool");
    return data_[pos_++] != 0;
}

std::string BodyReader::string() {
    std::int32_t length = int32();
    if (length < 0 || pos_ + static_cast<std::size_t>(length) > data_.size()) {
        throw Error("truncated string");
    }
    std::string value(reinterpret_cast<const char*>(data_.data() + pos_),
                      static_cast<std::size_t>(length));
    pos_ += static_cast<std::size_t>(length);
    return value;
}

std::vector<std::string> BodyReader::stringArray() {
    std::int32_t count = int32();
    std::vector<std::string> out;
    if (count <= 0) return out;
    if (static_cast<std::size_t>(count) > data_.size() - pos_) throw Error("truncated array");
    out.reserve(static_cast<std::size_t>(count));
    for (std::int32_t i = 0; i < count; ++i) out.push_back(string());
    return out;
}

Bytes BodyReader::rest() {
    Bytes out(data_.begin() + static_cast<std::ptrdiff_t>(pos_), data_.end());
    pos_ = data_.size();
    return out;
}

std::int32_t peekErrorCode(const Bytes& body) {
    try {
        BodyReader r(body);
        return r.int32();
    } catch (const Error&) {
        return errc::None;
    }
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

Bytes encodeFrame(std::int16_t apiKey, std::int32_t correlationId, const std::string& clientId,
                  const Bytes& body) {
    std::size_t payloadLen = 8 + 2 + clientId.size() + body.size();
    Bytes out;
    out.reserve(4 + payloadLen);
    putU32(out, static_cast<std::uint32_t>(payloadLen));
    putU16(out, static_cast<std::uint16_t>(apiKey));
    putU16(out, static_cast<std::uint16_t>(kApiVersion));
    putU32(out, static_cast<std::uint32_t>(correlationId));
    putU16(out, static_cast<std::uint16_t>(clientId.size()));
    out.insert(out.end(), clientId.begin(), clientId.end());
    out.insert(out.end(), body.begin(), body.end());
    return out;
}

std::pair<std::int32_t, Bytes> decodeFramePayload(const Bytes& payload) {
    if (payload.size() < 10) throw Error("frame payload shorter than its header");
    auto correlationId = static_cast<std::int32_t>(getU32(payload.data() + 4));
    auto clientLen = static_cast<std::int16_t>(getU16(payload.data() + 8));
    std::size_t offset = 10;
    if (clientLen > 0) offset += static_cast<std::size_t>(clientLen);
    if (offset > payload.size()) throw Error("frame client id runs past the payload");
    return {correlationId, Bytes(payload.begin() + static_cast<std::ptrdiff_t>(offset), payload.end())};
}

// ---------------------------------------------------------------------------
// CRC32C
// ---------------------------------------------------------------------------

std::uint32_t crc32c(const std::uint8_t* data, std::size_t len) {
    static const std::array<std::uint32_t, 256> table = [] {
        std::array<std::uint32_t, 256> t{};
        for (std::uint32_t i = 0; i < 256; ++i) {
            std::uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
            t[i] = c;
        }
        return t;
    }();
    std::uint32_t crc = 0xFFFFFFFFu;
    for (std::size_t i = 0; i < len; ++i) crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

// ---------------------------------------------------------------------------
// murmur2
// ---------------------------------------------------------------------------

std::uint32_t murmur2(const std::uint8_t* data, std::size_t len) {
    const std::uint32_t seed = 0x9747b28c;
    const std::uint32_t m = 0x5bd1e995;
    const int r = 24;

    std::uint32_t h = seed ^ static_cast<std::uint32_t>(len);
    std::size_t chunks = len / 4;
    for (std::size_t i = 0; i < chunks; ++i) {
        const std::uint8_t* p = data + i * 4;
        std::uint32_t k = std::uint32_t(p[0]) | (std::uint32_t(p[1]) << 8) |
                          (std::uint32_t(p[2]) << 16) | (std::uint32_t(p[3]) << 24);
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
    }
    std::size_t tail = chunks * 4;
    switch (len - tail) {
        case 3:
            h ^= std::uint32_t(data[tail + 2]) << 16;
            [[fallthrough]];
        case 2:
            h ^= std::uint32_t(data[tail + 1]) << 8;
            [[fallthrough]];
        case 1:
            h ^= std::uint32_t(data[tail]);
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

std::int32_t partitionForKey(const Bytes& key, const std::vector<std::int32_t>& partitions) {
    if (partitions.empty()) throw Error("topic has no partitions");
    return partitions[(murmur2(key) & 0x7fffffffu) % partitions.size()];
}

// ---------------------------------------------------------------------------
// Record batches
// ---------------------------------------------------------------------------

Bytes encodeRecordBatch(const std::vector<Record>& records, std::int64_t maxTimestamp,
                        Compression codec) {
    bool hasHeaders = false;
    bool hasNullValues = false;
    for (const auto& record : records) {
        if (!record.headers.empty()) hasHeaders = true;
        // A null value is a tombstone and needs the widened length encoding;
        // an empty non-null value is an ordinary record and must not trigger it.
        if (!record.value) hasNullValues = true;
    }

    Bytes payload;
    Bytes rec;
    for (const auto& record : records) {
        rec.clear();
        if (!record.key) {
            appendUvarint(rec, 0);
        } else {
            appendUvarint(rec, record.key->size() + 1);
            rec.insert(rec.end(), record.key->begin(), record.key->end());
        }
        if (hasNullValues) {
            if (!record.value) {
                appendUvarint(rec, 0);
            } else {
                appendUvarint(rec, record.value->size() + 1);
                rec.insert(rec.end(), record.value->begin(), record.value->end());
            }
        } else {
            appendUvarint(rec, record.value->size());
            rec.insert(rec.end(), record.value->begin(), record.value->end());
        }
        appendUvarint(rec, zigzag64(record.timestampDelta));
        if (hasHeaders) {
            appendUvarint(rec, record.headers.size());
            for (const auto& header : record.headers) {
                appendUvarint(rec, header.key.size());
                rec.insert(rec.end(), header.key.begin(), header.key.end());
                if (!header.value) {
                    appendUvarint(rec, 0);
                } else {
                    appendUvarint(rec, header.value->size() + 1);
                    rec.insert(rec.end(), header.value->begin(), header.value->end());
                }
            }
        }
        appendUvarint(payload, rec.size());
        payload.insert(payload.end(), rec.begin(), rec.end());
    }

    Bytes compressed = compress(codec, payload);

    std::uint16_t attributes = static_cast<std::uint16_t>(codec) & kCompressionMask;
    if (hasHeaders) attributes |= kHeadersBit;
    if (hasNullValues) attributes |= kNullValueBit;
    std::size_t batchLength = kMinBatchLength + compressed.size();

    Bytes out;
    out.reserve(kBatchHeaderLen + batchLength);
    putU64(out, 0);  // base_offset, stamped by the broker
    putU32(out, static_cast<std::uint32_t>(batchLength));
    putU32(out, 0);  // leader_epoch, likewise
    out.push_back(kMagicV1);
    std::size_t crcAt = out.size();
    putU32(out, 0);
    putU16(out, attributes);
    std::int32_t lastDelta = records.empty() ? 0 : static_cast<std::int32_t>(records.size() - 1);
    putU32(out, static_cast<std::uint32_t>(lastDelta));
    putU64(out, static_cast<std::uint64_t>(maxTimestamp));
    out.insert(out.end(), compressed.begin(), compressed.end());

    std::uint32_t crc = crc32c(out.data() + crcAt + 4, out.size() - crcAt - 4);
    out[crcAt] = static_cast<std::uint8_t>(crc >> 24);
    out[crcAt + 1] = static_cast<std::uint8_t>(crc >> 16);
    out[crcAt + 2] = static_cast<std::uint8_t>(crc >> 8);
    out[crcAt + 3] = static_cast<std::uint8_t>(crc);
    return out;
}

namespace {

NullableBytes takeBytes(const Bytes& payload, std::size_t& pos, std::size_t end, std::uint64_t size,
                        const char* what) {
    if (size > end - pos) throw Error(std::string("truncated record ") + what);
    Bytes out(payload.begin() + static_cast<std::ptrdiff_t>(pos),
              payload.begin() + static_cast<std::ptrdiff_t>(pos + size));
    pos += static_cast<std::size_t>(size);
    return out;
}

std::vector<Record> decodeRecords(const Bytes& payload, bool hasHeaders, bool hasNullValues) {
    std::vector<Record> records;
    std::size_t pos = 0;
    while (pos < payload.size()) {
        std::uint64_t length = readUvarint(payload, pos, payload.size(), "record");
        if (length > payload.size() - pos) throw Error("truncated record");
        std::size_t end = pos + static_cast<std::size_t>(length);

        Record record;
        std::uint64_t keyPlusOne = readUvarint(payload, pos, end, "record");
        if (keyPlusOne > 0) record.key = takeBytes(payload, pos, end, keyPlusOne - 1, "key");

        std::uint64_t rawValueLen = readUvarint(payload, pos, end, "record");
        if (hasNullValues && rawValueLen == 0) {
            record.value = std::nullopt;  // a tombstone
        } else {
            std::uint64_t valueLen = hasNullValues ? rawValueLen - 1 : rawValueLen;
            record.value = takeBytes(payload, pos, end, valueLen, "value");
        }

        record.timestampDelta = unzigzag64(readUvarint(payload, pos, end, "record"));

        if (hasHeaders) {
            std::uint64_t count = readUvarint(payload, pos, end, "record");
            // A count is a promise about bytes that follow; allocating on a
            // corrupt one would let a two-byte record ask for gigabytes.
            if (count > end - pos) throw Error("record header count exceeds record");
            for (std::uint64_t i = 0; i < count; ++i) {
                RecordHeader header;
                std::uint64_t keyLen = readUvarint(payload, pos, end, "header");
                NullableBytes key = takeBytes(payload, pos, end, keyLen, "header key");
                header.key.assign(key->begin(), key->end());
                std::uint64_t valuePlusOne = readUvarint(payload, pos, end, "header");
                if (valuePlusOne > 0) {
                    header.value = takeBytes(payload, pos, end, valuePlusOne - 1, "header value");
                }
                record.headers.push_back(std::move(header));
            }
        }
        if (pos != end) throw Error("trailing bytes in record");
        records.push_back(std::move(record));
    }
    return records;
}

}  // namespace

DecodedBatch decodeRecordBatch(const Bytes& data, std::size_t& offset) {
    if (offset > data.size() || data.size() - offset < kBatchHeaderLen) {
        throw Error("truncated batch header");
    }
    DecodedBatch batch;
    batch.baseOffset = static_cast<std::int64_t>(getU64(data.data() + offset));
    auto batchLength = static_cast<std::int32_t>(getU32(data.data() + offset + 8));
    if (batchLength < static_cast<std::int32_t>(kMinBatchLength)) throw Error("batch_length too small");
    std::size_t bodyAt = offset + kBatchHeaderLen;
    std::size_t end = bodyAt + static_cast<std::size_t>(batchLength);
    if (end > data.size()) throw Error("truncated batch body");

    std::uint8_t magic = data[bodyAt + 4];
    if (magic != kMagicV1 && magic != kMagicV2) {
        throw Error("unsupported magic " + std::to_string(magic));
    }
    std::size_t crcAt = bodyAt + 5;
    std::uint32_t stored = getU32(data.data() + crcAt);
    std::uint32_t computed = crc32c(data.data() + crcAt + 4, end - crcAt - 4);
    if (stored != computed) {
        char buf[96];
        std::snprintf(buf, sizeof buf, "crc mismatch: stored %#010x, computed %#010x", stored,
                      computed);
        throw Error(buf);
    }

    std::size_t cursor = crcAt + 4;
    std::uint16_t attributes = getU16(data.data() + cursor);
    batch.maxTimestamp = static_cast<std::int64_t>(getU64(data.data() + cursor + 6));
    cursor += 14;
    if (magic == kMagicV2) cursor += kProducerExtensionLen;
    if (cursor > end) throw Error("truncated batch body");

    Bytes compressed(data.begin() + static_cast<std::ptrdiff_t>(cursor),
                     data.begin() + static_cast<std::ptrdiff_t>(end));
    Bytes payload = decompress(static_cast<Compression>(attributes & kCompressionMask), compressed);
    batch.records = decodeRecords(payload, (attributes & kHeadersBit) != 0,
                                  (attributes & kNullValueBit) != 0);
    offset = end;
    return batch;
}

}  // namespace brahmaputra
