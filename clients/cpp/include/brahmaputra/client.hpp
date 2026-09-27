// Connection, metadata routing, producer and single-partition consumer.
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "brahmaputra/protocol.hpp"

namespace brahmaputra {

/// Offset sentinels for listOffsets.
inline constexpr std::int64_t kEarliest = -2;
inline constexpr std::int64_t kLatest = -1;

/// Wall-clock unix milliseconds.
std::int64_t nowMillis();

/// A `key=value` property map, for configuring with Kafka's dotted names
/// ("linger.ms", "acks", ...). Each config struct has a fromProperties().
using Properties = std::map<std::string, std::string>;

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

struct ApiVersionRange {
    std::int32_t apiKey;
    std::int32_t minVersion;
    std::int32_t maxVersion;
};

/// One TCP connection to one broker.
///
/// A mutex serialises request/response pairs, and responses are checked
/// against their correlation id. A socket error or timeout closes the
/// socket (the stream may be desynchronised) and the next request dials
/// again, so a transient broker restart does not poison the client.
class Connection {
public:
    Connection(std::string host, std::uint16_t port, std::string clientId,
               std::chrono::milliseconds connectTimeout, std::chrono::milliseconds ioTimeout);
    ~Connection();
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    /// Sends one request and returns the matching response body.
    Bytes request(std::int16_t apiKey, const Bytes& body);
    /// Sends without awaiting a response (acks=0).
    void sendOneway(std::int16_t apiKey, const Bytes& body);

    /// Asks the broker which API versions it speaks, and its version string.
    std::pair<std::vector<ApiVersionRange>, std::string> apiVersions();

    void close();
    const std::string& host() const { return host_; }
    std::uint16_t port() const { return port_; }

private:
    void ensureOpenLocked();
    void closeLocked();
    void writeAllLocked(const Bytes& data);
    Bytes readFrameLocked();

    std::string host_;
    std::uint16_t port_;
    std::string clientId_;
    std::chrono::milliseconds connectTimeout_;
    std::chrono::milliseconds ioTimeout_;
    std::mutex mu_;
    int fd_ = -1;
    std::int32_t next_ = 0;
};

// ---------------------------------------------------------------------------
// Metadata and routing
// ---------------------------------------------------------------------------

struct BrokerInfo {
    std::int32_t nodeId = -1;
    std::string host;
    std::int32_t port = 0;
    /// Failure domain, empty when the broker was started without --rack.
    std::string rack;
};

struct PartitionInfo {
    std::int32_t partition = 0;
    std::int32_t leader = -1;
    std::vector<std::int32_t> replicas;
    std::vector<std::int32_t> isr;
    std::int32_t leaderEpoch = 0;
};

struct TopicInfo {
    std::string name;
    std::int32_t errorCode = 0;
    std::vector<PartitionInfo> partitions;
};

struct ClusterMetadata {
    std::vector<BrokerInfo> brokers;
    std::int32_t controllerId = -1;
    std::vector<TopicInfo> topics;

    /// A topic's partition ids, ascending; empty if unknown.
    std::vector<std::int32_t> partitionsOf(const std::string& topic) const;
    /// The broker id leading a partition, or -1.
    std::int32_t leaderOf(const std::string& topic, std::int32_t partition) const;
};

/// Keeps connections to every broker and routes by partition leader.
///
/// Metadata is cached and refreshed only when a request comes back saying
/// the route was stale, because refreshing per request would put the
/// control plane on the data path.
class Router {
public:
    /// `bootstrap` is "host:port", or a comma-separated list of them
    /// (bootstrap.servers); the first that accepts a connection is the seed.
    Router(const std::string& bootstrap, std::string clientId,
           std::chrono::milliseconds connectTimeout, std::chrono::milliseconds ioTimeout);
    ~Router();

    std::shared_ptr<Connection> seed() const { return seed_; }

    /// Metadata for `topics` (all topics if empty); cached unless `refresh`.
    ClusterMetadata metadata(const std::vector<std::string>& topics, bool refresh);
    ClusterMetadata refresh(const std::string& topic);
    /// A topic's partitions. A topic the broker auto-creates on first
    /// reference is created by this call.
    std::vector<std::int32_t> partitions(const std::string& topic);
    /// The connection to a partition's leader.
    std::shared_ptr<Connection> connFor(const std::string& topic, std::int32_t partition);

    void close();

private:
    std::string clientId_;
    std::chrono::milliseconds connectTimeout_;
    std::chrono::milliseconds ioTimeout_;
    std::shared_ptr<Connection> seed_;
    std::mutex mu_;
    std::map<std::int32_t, std::shared_ptr<Connection>> conns_;
    std::optional<ClusterMetadata> metadata_;
};

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

/// Named as Kafka names its producer settings. Where a default differs from
/// Kafka's it is called out.
struct ProducerConfig {
    std::string clientId = "brahmaputra-cpp";
    /// acks: 0 fire-and-forget, 1 leader append, -1 ("all") every ISR.
    std::int32_t acks = 1;
    /// batch.size: flush a partition buffer once it holds this many bytes.
    std::size_t batchSize = 16 * 1024;
    /// linger.ms: flush every non-empty buffer at least this often. 0 sends
    /// each record immediately. Kafka defaults to 0; this defaults to 5.
    int lingerMs = 5;
    /// compression.type: none, gzip, or a codec registered with registerCodec.
    std::string compressionType = "none";
    /// request.timeout.ms: the broker-side wait for acknowledgements.
    std::int32_t requestTimeoutMs = 30'000;
    /// retries of a send refused with a retriable error — one the broker
    /// returns before appending, so a retry cannot duplicate.
    int retries = 5;
    /// retry.backoff.ms between retries.
    int retryBackoffMs = 100;
    /// delivery.timeout.ms caps a record's whole send, buffering included.
    int deliveryTimeoutMs = 120'000;
    /// buffer.memory: cap on unacknowledged record bytes held client-side.
    std::size_t bufferMemory = 32 * 1024 * 1024;
    /// max.block.ms: how long send() may block on a full buffer.
    int maxBlockMs = 60'000;
    /// socket.connection.setup.timeout.ms
    int connectTimeoutMs = 30'000;

    /// Builds a config from Kafka property names; unknown keys throw.
    static ProducerConfig fromProperties(const Properties& props);
};

/// A record to send. `partition` unset means: keyed records go to
/// murmur2(key) % partitions, unkeyed ones round-robin. `timestampMs` unset
/// means now.
struct ProducerRecord {
    std::string topic;
    std::optional<std::int32_t> partition;
    NullableBytes key;
    /// std::nullopt is a tombstone; an empty vector is an empty value.
    NullableBytes value;
    std::vector<RecordHeader> headers;
    std::optional<std::int64_t> timestampMs;
};

/// Batches records per partition and sends each batch as one Produce
/// request. Thread-safe: share one across threads rather than creating one
/// per message — the batching is the point.
///
/// A background linger thread flushes buffers every linger.ms. An error in
/// a background flush is remembered and thrown from the next flush().
class Producer {
public:
    Producer(const std::string& bootstrap, ProducerConfig config = {});
    ~Producer();
    Producer(const Producer&) = delete;
    Producer& operator=(const Producer&) = delete;

    /// Buffers one record. Returns without an offset: with batching it is
    /// not known until the batch goes out. Blocks up to max.block.ms when
    /// buffer.memory is exhausted, then throws BufferFullError.
    void send(const ProducerRecord& record);

    /// Convenience: keyed (or round-robin when key is nullopt).
    void send(const std::string& topic, NullableBytes value, NullableBytes key = std::nullopt,
              std::vector<RecordHeader> headers = {});
    /// Convenience: an explicit partition, bypassing the partitioner.
    void sendTo(const std::string& topic, std::int32_t partition, NullableBytes value,
                NullableBytes key = std::nullopt, std::vector<RecordHeader> headers = {});

    /// Sends one record on its own and returns its offset. A full round trip
    /// per record — correct, and slow. Returns -1 for acks=0.
    std::int64_t sendSync(const ProducerRecord& record);

    /// Sends every buffered record and waits for acknowledgement.
    void flush();
    /// Flushes, stops the linger thread and releases connections.
    void close();

    Router& router() { return *router_; }
    const ProducerConfig& config() const { return config_; }

private:
    struct Buffered {
        Record record;
        std::int64_t createdMs;   // the record's timestamp
        std::int64_t enqueuedMs;  // when send() buffered it, for linger.ms
        std::size_t size;
    };
    using Slot = std::pair<std::string, std::int32_t>;

    std::int32_t choosePartition(const std::string& topic, const NullableBytes& key);
    void reserve(std::size_t size);
    void release(std::size_t size);
    void flushSlot(const Slot& slot);
    std::int64_t produce(const std::string& topic, std::int32_t partition,
                         std::vector<Buffered>& batch);
    void lingerLoop();
    void rethrowBackgroundError();

    ProducerConfig config_;
    Compression codec_;
    std::unique_ptr<Router> router_;

    std::mutex mu_;
    std::condition_variable spaceFreed_;
    std::condition_variable lingerWake_;
    std::map<Slot, std::vector<Buffered>> buffers_;
    std::map<Slot, std::size_t> sizes_;
    std::size_t bufferedBytes_ = 0;
    std::size_t roundRobin_ = 0;
    bool closed_ = false;
    std::exception_ptr backgroundError_;

    // Held across take-and-send so two flushers cannot reorder batches of
    // one partition.
    std::mutex sendMu_;
    std::thread linger_;
};

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

/// One record delivered to the application.
struct ConsumedRecord {
    std::string topic;
    std::int32_t partition = 0;
    std::int64_t offset = 0;
    NullableBytes key;
    /// std::nullopt for a tombstone.
    NullableBytes value;
    /// Absolute unix milliseconds.
    std::int64_t timestamp = 0;
    std::vector<RecordHeader> headers;

    /// The first header stored under `key`, or nullptr.
    const RecordHeader* header(const std::string& key) const;
};

/// Named as Kafka names its consumer settings.
struct ConsumerConfig {
    std::string clientId = "brahmaputra-cpp";
    /// fetch.max.bytes caps a response.
    std::int32_t fetchMaxBytes = 8 * 1024 * 1024;
    /// fetch.min.bytes returns early once this many bytes are ready.
    std::int32_t fetchMinBytes = 1;
    /// fetch.max.wait.ms is the long-poll ceiling when caught up.
    std::int32_t fetchMaxWaitMs = 500;
    /// client.rack; empty for none.
    std::string clientRack;
    /// isolation.level: kReadUncommitted or kReadCommitted.
    std::int32_t isolationLevel = kReadUncommitted;
    /// max.poll.records (used by GroupConsumer::poll).
    int maxPollRecords = 500;
    /// request.timeout.ms: client-side socket timeout on top of the long poll.
    int requestTimeoutMs = 30'000;
    int connectTimeoutMs = 30'000;

    static ConsumerConfig fromProperties(const Properties& props);
};

struct FetchResult {
    std::vector<ConsumedRecord> records;
    std::int64_t highWatermark = 0;
};

/// Reads one partition at a time, with no group coordination.
class Consumer {
public:
    Consumer(const std::string& bootstrap, ConsumerConfig config = {});
    ~Consumer();

    std::vector<std::int32_t> partitions(const std::string& topic);
    /// Resolves kEarliest, kLatest, or a unix-ms timestamp to an offset.
    std::int64_t listOffsets(const std::string& topic, std::int32_t partition,
                             std::int64_t timestamp);
    /// Reads from one partition starting at `offset`, waiting up to
    /// min(maxWaitMs, fetch.max.wait.ms) when caught up.
    std::vector<ConsumedRecord> fetch(const std::string& topic, std::int32_t partition,
                                      std::int64_t offset, std::int32_t maxWaitMs);
    /// As fetch(), also returning the partition's high watermark.
    FetchResult fetchWithWatermark(const std::string& topic, std::int32_t partition,
                                   std::int64_t offset, std::int32_t maxWaitMs);

    void close();
    Router& router() { return *router_; }
    const ConsumerConfig& config() const { return config_; }

private:
    ConsumerConfig config_;
    std::unique_ptr<Router> router_;
};

}  // namespace brahmaputra
