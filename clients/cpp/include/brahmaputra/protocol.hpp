// Wire-level encodings for the Brahmaputra protocol.
//
// Three encodings share one connection and they do not agree with each
// other, so keeping them straight is most of the work:
//
//   - The frame header is fixed big-endian: an int32 length prefix, then
//     apiKey/apiVersion/correlationId and an int16-prefixed client id.
//   - A request body is BitPacker: every integer is a zigzag varint, every
//     string and array is a varint count followed by its contents, and the
//     whole body is prefixed with the schema version string.
//   - A record batch is neither. Fixed big-endian header fields and plain
//     (non-zigzag) varints inside each record, because the broker stamps
//     offsets into it in place and validates its CRC without decoding it.
#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace brahmaputra {

using Bytes = std::vector<std::uint8_t>;
/// A byte string that may be null. Null is distinct from empty everywhere
/// this is used: a null value is a tombstone, a null header value is not
/// the same as an empty one.
using NullableBytes = std::optional<Bytes>;

/// Convenience: bytes from a string literal / std::string.
inline Bytes toBytes(const std::string& s) { return Bytes(s.begin(), s.end()); }
inline std::string toString(const Bytes& b) { return std::string(b.begin(), b.end()); }

/// The BitPacker schema version every body carries first.
inline constexpr const char* kSchemaVersion = "1.0.0";

/// The wire version this client speaks. The broker requires an exact match.
inline constexpr std::int16_t kApiVersion = 4;

inline constexpr std::int32_t kReadUncommitted = 0;
inline constexpr std::int32_t kReadCommitted = 1;

/// API keys, in wire order.
namespace api {
inline constexpr std::int16_t Produce = 0;
inline constexpr std::int16_t Fetch = 1;
inline constexpr std::int16_t ListOffsets = 2;
inline constexpr std::int16_t Metadata = 3;
inline constexpr std::int16_t ReplicaFetch = 4;
inline constexpr std::int16_t OffsetsForLeaderEpoch = 5;
inline constexpr std::int16_t InitProducerId = 6;
inline constexpr std::int16_t JoinGroup = 7;
inline constexpr std::int16_t SyncGroup = 8;
inline constexpr std::int16_t Heartbeat = 9;
inline constexpr std::int16_t OffsetCommit = 10;
inline constexpr std::int16_t OffsetFetch = 11;
inline constexpr std::int16_t ListGroups = 12;
inline constexpr std::int16_t DescribeGroup = 13;
inline constexpr std::int16_t ApiVersions = 14;
inline constexpr std::int16_t ProduceMulti = 15;
inline constexpr std::int16_t FetchMulti = 16;
inline constexpr std::int16_t Authenticate = 17;
inline constexpr std::int16_t LeaveGroup = 18;
}  // namespace api

/// Error codes the broker returns in a response's error_code field.
namespace errc {
inline constexpr std::int32_t None = 0;
inline constexpr std::int32_t UnknownTopicOrPartition = 1;
inline constexpr std::int32_t OffsetOutOfRange = 2;
inline constexpr std::int32_t InvalidRequest = 3;
inline constexpr std::int32_t UnsupportedVersion = 4;
inline constexpr std::int32_t Internal = 5;
inline constexpr std::int32_t NotLeaderOrFollower = 6;
inline constexpr std::int32_t FencedBrokerEpoch = 7;
inline constexpr std::int32_t FencedLeaderEpoch = 8;
inline constexpr std::int32_t UnknownLeaderEpoch = 9;
inline constexpr std::int32_t NotEnoughReplicas = 10;
inline constexpr std::int32_t FencedProducerEpoch = 11;
inline constexpr std::int32_t OutOfOrderSequence = 12;
inline constexpr std::int32_t UnknownMemberId = 13;
inline constexpr std::int32_t RebalanceInProgress = 14;
inline constexpr std::int32_t NotCoordinator = 15;
inline constexpr std::int32_t IllegalGeneration = 16;
inline constexpr std::int32_t CoordinatorLoadInProgress = 17;
inline constexpr std::int32_t SaslAuthenticationFailed = 18;
inline constexpr std::int32_t AuthorizationFailed = 19;
}  // namespace errc

/// Human-readable name for an error code ("UNKNOWN" if unrecognised).
const char* errorName(std::int32_t code);

/// Every error this library throws derives from this.
class Error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/// A non-zero error code from the broker.
class ServerError : public Error {
public:
    ServerError(std::int32_t code, const std::string& context);
    std::int32_t code() const noexcept { return code_; }

private:
    std::int32_t code_;
};

/// Thrown when auto.offset.reset=none is in force and there is no position
/// to resume from.
class NoOffsetForPartition : public Error {
public:
    NoOffsetForPartition() : Error("no committed offset for partition") {}
};

/// Thrown when buffer.memory is exhausted for longer than max.block.ms.
class BufferFullError : public Error {
public:
    using Error::Error;
};

/// A socket-level failure: connect, read, write, or a timeout.
class NetworkError : public Error {
public:
    using Error::Error;
};

/// Whether a code means "this send did not happen" and may be retried
/// without risk of duplication. Every code here is one the broker returns
/// strictly before it appends.
bool isRetriable(std::int32_t code);

// ---------------------------------------------------------------------------
// BitPacker
// ---------------------------------------------------------------------------

/// Builds a BitPacker body. Every integer goes out zigzag-varint encoded.
class BodyWriter {
public:
    /// Starts with the schema version already written, as every body must.
    BodyWriter();

    void uvarint(std::uint64_t value);
    void int32(std::int32_t value);
    void int64(std::int64_t value);
    void boolean(bool value);
    void string(const std::string& value);
    void stringArray(const std::vector<std::string>& values);
    void raw(const Bytes& data);

    const Bytes& bytes() const { return buf_; }
    Bytes take() { return std::move(buf_); }

private:
    Bytes buf_;
};

/// Reads a BitPacker body. Throws Error on truncation or a schema-version
/// mismatch: a mismatch means broker and client disagree about the message
/// shapes themselves, so failing loudly beats decoding garbage.
class BodyReader {
public:
    explicit BodyReader(const Bytes& data);

    std::uint64_t uvarint();
    std::int32_t int32();
    std::int64_t int64();
    bool boolean();
    std::string string();
    std::vector<std::string> stringArray();
    /// Everything not yet read.
    Bytes rest();

    // Fields must still be *read* in order even when ignored, because the
    // encoding is positional.
    void skipString() { (void)string(); }
    void skipInt32() { (void)int32(); }
    void skipInt64() { (void)int64(); }

private:
    const Bytes& data_;
    std::size_t pos_ = 0;
};

/// Reads a response's leading error code without disturbing anything.
std::int32_t peekErrorCode(const Bytes& body);

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// Builds one complete frame, length prefix included.
Bytes encodeFrame(std::int16_t apiKey, std::int32_t correlationId,
                  const std::string& clientId, const Bytes& body);

/// Splits a frame payload (after the length prefix) into its correlation id
/// and body.
std::pair<std::int32_t, Bytes> decodeFramePayload(const Bytes& payload);

// ---------------------------------------------------------------------------
// CRC32C and murmur2
// ---------------------------------------------------------------------------

/// Castagnoli CRC, which record batches carry — not the zlib CRC32.
std::uint32_t crc32c(const std::uint8_t* data, std::size_t len);
inline std::uint32_t crc32c(const Bytes& data) { return crc32c(data.data(), data.size()); }
inline std::uint32_t crc32c(const std::string& data) {
    return crc32c(reinterpret_cast<const std::uint8_t*>(data.data()), data.size());
}

/// Kafka's 32-bit murmur2, so a key lands on the same partition as it would
/// from any other Brahmaputra or Kafka client. murmur2("") == 275646681.
std::uint32_t murmur2(const std::uint8_t* data, std::size_t len);
inline std::uint32_t murmur2(const Bytes& data) { return murmur2(data.data(), data.size()); }
inline std::uint32_t murmur2(const std::string& data) {
    return murmur2(reinterpret_cast<const std::uint8_t*>(data.data()), data.size());
}

/// murmur2(key) % partitions — Kafka's default partitioner.
std::int32_t partitionForKey(const Bytes& key, const std::vector<std::int32_t>& partitions);

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

/// Codecs, numbered as the broker's batch attributes number them.
enum class Compression : std::uint16_t {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
    Snappy = 3,
    Gzip = 4,
};

/// Maps Kafka's compression.type spelling onto a codec. Throws on unknown.
Compression parseCompression(const std::string& name);
std::string compressionName(Compression codec);

using CodecFn = std::function<Bytes(const Bytes&)>;

/// Plugs in a compression codec this library does not carry itself, so an
/// application that wants lz4 or zstd pays for that dependency and one that
/// does not, does not. Also replaces a built-in codec if called for one.
///
/// The lz4 payload the broker expects is a little-endian uint32 of the
/// uncompressed length followed by a raw LZ4 block — not the LZ4 frame
/// format — so a frame-format library will not interoperate.
void registerCodec(Compression codec, CodecFn compress, CodecFn decompress);

/// Whether a codec can currently be used (none always; gzip when built with
/// zlib; others once registered).
bool codecAvailable(Compression codec);

Bytes compress(Compression codec, const Bytes& payload);
Bytes decompress(Compression codec, const Bytes& payload);

// ---------------------------------------------------------------------------
// Record batches
// ---------------------------------------------------------------------------

/// An ordered, possibly repeating annotation on a record. value may be null,
/// which is distinct from empty.
struct RecordHeader {
    std::string key;
    NullableBytes value;
};

/// One record inside a batch.
struct Record {
    NullableBytes key;
    /// std::nullopt is a tombstone; an empty vector is an ordinary record.
    NullableBytes value;
    /// Milliseconds relative to the batch's max timestamp (normally <= 0).
    std::int64_t timestampDelta = 0;
    std::vector<RecordHeader> headers;
};

/// Encodes one batch exactly as the broker stores it. The broker never
/// re-encodes this: getting it wrong corrupts the log.
Bytes encodeRecordBatch(const std::vector<Record>& records, std::int64_t maxTimestamp,
                        Compression codec);

struct DecodedBatch {
    std::int64_t baseOffset = 0;
    std::int64_t maxTimestamp = 0;
    std::vector<Record> records;
};

/// Decodes one batch starting at `offset`; returns it and advances `offset`
/// just past it. Verifies the CRC.
DecodedBatch decodeRecordBatch(const Bytes& data, std::size_t& offset);

}  // namespace brahmaputra
