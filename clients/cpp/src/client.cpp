// Producer and single-partition consumer.
#include <algorithm>

#include "brahmaputra/client.hpp"

namespace brahmaputra {

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

namespace detail {

long long parseInteger(const std::string& key, const std::string& value) {
    try {
        std::size_t used = 0;
        long long parsed = std::stoll(value, &used);
        if (used != value.size()) throw std::invalid_argument(value);
        return parsed;
    } catch (const std::exception&) {
        throw Error("config " + key + ": \"" + value + "\" is not an integer");
    }
}

bool parseBoolean(const std::string& key, const std::string& value) {
    if (value == "true") return true;
    if (value == "false") return false;
    throw Error("config " + key + ": \"" + value + "\" is not true/false");
}

[[noreturn]] void unknownProperty(const std::string& key) {
    throw Error("unknown config property \"" + key + "\"");
}

}  // namespace detail

using detail::parseInteger;

ProducerConfig ProducerConfig::fromProperties(const Properties& props) {
    ProducerConfig c;
    for (const auto& [key, value] : props) {
        if (key == "bootstrap.servers") continue;  // a constructor argument
        if (key == "client.id") c.clientId = value;
        else if (key == "acks") c.acks = value == "all" ? -1 : static_cast<std::int32_t>(parseInteger(key, value));
        else if (key == "batch.size") c.batchSize = static_cast<std::size_t>(parseInteger(key, value));
        else if (key == "linger.ms") c.lingerMs = static_cast<int>(parseInteger(key, value));
        else if (key == "compression.type") c.compressionType = value;
        else if (key == "request.timeout.ms") c.requestTimeoutMs = static_cast<std::int32_t>(parseInteger(key, value));
        else if (key == "retries") c.retries = static_cast<int>(parseInteger(key, value));
        else if (key == "retry.backoff.ms") c.retryBackoffMs = static_cast<int>(parseInteger(key, value));
        else if (key == "delivery.timeout.ms") c.deliveryTimeoutMs = static_cast<int>(parseInteger(key, value));
        else if (key == "buffer.memory") c.bufferMemory = static_cast<std::size_t>(parseInteger(key, value));
        else if (key == "max.block.ms") c.maxBlockMs = static_cast<int>(parseInteger(key, value));
        else if (key == "socket.connection.setup.timeout.ms") c.connectTimeoutMs = static_cast<int>(parseInteger(key, value));
        else detail::unknownProperty(key);
    }
    return c;
}

ConsumerConfig ConsumerConfig::fromProperties(const Properties& props) {
    ConsumerConfig c;
    for (const auto& [key, value] : props) {
        if (key == "bootstrap.servers") continue;
        if (key == "client.id") c.clientId = value;
        else if (key == "fetch.max.bytes") c.fetchMaxBytes = static_cast<std::int32_t>(parseInteger(key, value));
        else if (key == "fetch.min.bytes") c.fetchMinBytes = static_cast<std::int32_t>(parseInteger(key, value));
        else if (key == "fetch.max.wait.ms") c.fetchMaxWaitMs = static_cast<std::int32_t>(parseInteger(key, value));
        else if (key == "client.rack") c.clientRack = value;
        else if (key == "isolation.level") {
            if (value == "read_uncommitted") c.isolationLevel = kReadUncommitted;
            else if (value == "read_committed") c.isolationLevel = kReadCommitted;
            else throw Error("config isolation.level: \"" + value + "\"");
        }
        else if (key == "max.poll.records") c.maxPollRecords = static_cast<int>(parseInteger(key, value));
        else if (key == "request.timeout.ms") c.requestTimeoutMs = static_cast<int>(parseInteger(key, value));
        else if (key == "socket.connection.setup.timeout.ms") c.connectTimeoutMs = static_cast<int>(parseInteger(key, value));
        else detail::unknownProperty(key);
    }
    return c;
}

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

Producer::Producer(const std::string& bootstrap, ProducerConfig config)
    : config_(std::move(config)), codec_(parseCompression(config_.compressionType)) {
    if (!codecAvailable(codec_)) {
        // Fail at construction rather than on the first background flush.
        (void)compress(codec_, Bytes{});
    }
    router_ = std::make_unique<Router>(
        bootstrap, config_.clientId, std::chrono::milliseconds(config_.connectTimeoutMs),
        std::chrono::milliseconds(static_cast<long long>(config_.requestTimeoutMs) + 5'000));
    if (config_.lingerMs > 0) linger_ = std::thread([this] { lingerLoop(); });
}

Producer::~Producer() {
    try {
        close();
    } catch (...) {
        // A destructor cannot report; call close() explicitly to see errors.
    }
}

void Producer::close() {
    std::exception_ptr flushError;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (closed_) return;
    }
    try {
        flush();
    } catch (...) {
        flushError = std::current_exception();
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        closed_ = true;
    }
    lingerWake_.notify_all();
    spaceFreed_.notify_all();
    if (linger_.joinable()) linger_.join();
    if (router_) router_->close();
    if (flushError) std::rethrow_exception(flushError);
}

void Producer::send(const std::string& topic, NullableBytes value, NullableBytes key,
                    std::vector<RecordHeader> headers) {
    ProducerRecord record;
    record.topic = topic;
    record.value = std::move(value);
    record.key = std::move(key);
    record.headers = std::move(headers);
    send(record);
}

void Producer::sendTo(const std::string& topic, std::int32_t partition, NullableBytes value,
                      NullableBytes key, std::vector<RecordHeader> headers) {
    ProducerRecord record;
    record.topic = topic;
    record.partition = partition;
    record.value = std::move(value);
    record.key = std::move(key);
    record.headers = std::move(headers);
    send(record);
}

void Producer::send(const ProducerRecord& input) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (closed_) throw Error("producer is closed");
    }
    std::int32_t partition = input.partition ? *input.partition
                                             : choosePartition(input.topic, input.key);
    std::size_t size = (input.value ? input.value->size() : 0) +
                       (input.key ? input.key->size() : 0) + 16;
    for (const auto& header : input.headers) {
        size += header.key.size() + (header.value ? header.value->size() : 0) + 4;
    }
    reserve(size);

    Slot slot{input.topic, partition};
    Buffered item{Record{input.key, input.value, 0, input.headers},
                  input.timestampMs ? *input.timestampMs : nowMillis(), nowMillis(), size};
    bool full;
    {
        std::lock_guard<std::mutex> lock(mu_);
        buffers_[slot].push_back(std::move(item));
        full = (sizes_[slot] += size) >= config_.batchSize;
    }
    if (config_.lingerMs <= 0 || full) flushSlot(slot);
}

std::int64_t Producer::sendSync(const ProducerRecord& input) {
    std::int32_t partition = input.partition ? *input.partition
                                             : choosePartition(input.topic, input.key);
    Slot slot{input.topic, partition};
    // Anything already buffered for this partition goes first, so a sync
    // send never overtakes an earlier async one.
    flushSlot(slot);
    std::vector<Buffered> batch;
    batch.push_back(Buffered{Record{input.key, input.value, 0, input.headers},
                             input.timestampMs ? *input.timestampMs : nowMillis(), nowMillis(), 0});
    std::lock_guard<std::mutex> sendLock(sendMu_);
    return produce(input.topic, partition, batch);
}

void Producer::flush() {
    std::vector<Slot> slots;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& [slot, records] : buffers_) {
            if (!records.empty()) slots.push_back(slot);
        }
    }
    for (const auto& slot : slots) flushSlot(slot);
    rethrowBackgroundError();
}

void Producer::rethrowBackgroundError() {
    std::exception_ptr error;
    {
        std::lock_guard<std::mutex> lock(mu_);
        std::swap(error, backgroundError_);
    }
    if (error) std::rethrow_exception(error);
}

std::int32_t Producer::choosePartition(const std::string& topic, const NullableBytes& key) {
    auto partitions = router_->partitions(topic);
    if (key) return partitionForKey(*key, partitions);
    std::lock_guard<std::mutex> lock(mu_);
    return partitions[roundRobin_++ % partitions.size()];
}

// This is what makes buffer.memory real: a producer faster than its broker
// is slowed down here rather than allowed to grow without limit and die
// holding records nobody has acknowledged.
void Producer::reserve(std::size_t size) {
    std::unique_lock<std::mutex> lock(mu_);
    std::size_t limit = config_.bufferMemory;
    if (limit == 0 || size >= limit) {
        // A record larger than the whole budget is admitted rather than
        // waiting forever on a condition that can never hold; refusing
        // oversized records is the broker's job.
        bufferedBytes_ += size;
        return;
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.maxBlockMs);
    while (bufferedBytes_ + size > limit) {
        if (closed_) throw Error("producer is closed");
        if (spaceFreed_.wait_until(lock, deadline) == std::cv_status::timeout &&
            bufferedBytes_ + size > limit) {
            throw BufferFullError("producer buffer full: " + std::to_string(bufferedBytes_) +
                                  " of " + std::to_string(limit) +
                                  " bytes unacknowledged after max.block.ms=" +
                                  std::to_string(config_.maxBlockMs));
        }
    }
    bufferedBytes_ += size;
}

void Producer::release(std::size_t size) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        bufferedBytes_ = size > bufferedBytes_ ? 0 : bufferedBytes_ - size;
    }
    spaceFreed_.notify_all();
}

void Producer::lingerLoop() {
    const auto tick = std::chrono::milliseconds(std::max(1, config_.lingerMs));
    std::unique_lock<std::mutex> lock(mu_);
    while (!closed_) {
        lingerWake_.wait_for(lock, tick);
        if (closed_) break;
        // A batch goes once its oldest record has lingered linger.ms.
        std::int64_t now = nowMillis();
        std::vector<Slot> due;
        for (const auto& [slot, records] : buffers_) {
            if (!records.empty() && now - records.front().enqueuedMs >= config_.lingerMs) {
                due.push_back(slot);
            }
        }
        lock.unlock();
        for (const auto& slot : due) {
            try {
                flushSlot(slot);
            } catch (...) {
                // A background flush that fails must not kill the thread; the
                // next flush() surfaces it to a caller who can act on it.
                std::lock_guard<std::mutex> errLock(mu_);
                if (!backgroundError_) backgroundError_ = std::current_exception();
            }
        }
        lock.lock();
    }
}

void Producer::flushSlot(const Slot& slot) {
    std::lock_guard<std::mutex> sendLock(sendMu_);
    std::vector<Buffered> batch;
    std::size_t size = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = buffers_.find(slot);
        if (it == buffers_.end() || it->second.empty()) return;
        batch.swap(it->second);
        size = sizes_[slot];
        sizes_.erase(slot);
    }
    try {
        produce(slot.first, slot.second, batch);
    } catch (...) {
        release(size);
        throw;
    }
    release(size);
}

std::int64_t Producer::produce(const std::string& topic, std::int32_t partition,
                               std::vector<Buffered>& batch) {
    if (batch.empty()) return -1;
    // The batch stores one base timestamp and a delta per record;
    // maxTimestamp is the newest record's time.
    std::int64_t maxTimestamp = batch.front().createdMs;
    for (const auto& item : batch) maxTimestamp = std::max(maxTimestamp, item.createdMs);
    std::vector<Record> records;
    records.reserve(batch.size());
    for (auto& item : batch) {
        item.record.timestampDelta = item.createdMs - maxTimestamp;
        records.push_back(std::move(item.record));
    }

    Bytes encoded = encodeRecordBatch(records, maxTimestamp, codec_);
    BodyWriter w;
    w.string(topic);
    w.int32(partition);
    w.int32(config_.acks);
    w.int32(config_.requestTimeoutMs);
    w.int64(static_cast<std::int64_t>(encoded.size()));
    w.raw(encoded);
    const Bytes& body = w.bytes();

    if (config_.acks == 0) {
        router_->connFor(topic, partition)->sendOneway(api::Produce, body);
        return -1;
    }

    // Retries alone do not bound latency: N retries that each take
    // request.timeout.ms is an unbounded wait. delivery.timeout.ms does.
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(config_.deliveryTimeoutMs);
    int attemptsLeft = config_.retries;
    for (;;) {
        auto conn = router_->connFor(topic, partition);
        Bytes response = conn->request(api::Produce, body);
        BodyReader r(response);
        r.skipString();  // topic
        r.skipInt32();   // partition
        std::int32_t code = r.int32();
        std::int64_t baseOffset = r.int64();
        r.skipInt64();  // log_append_time_ms
        if (code == errc::None) return baseOffset;
        if (!isRetriable(code) || attemptsLeft <= 0 ||
            std::chrono::steady_clock::now() >= deadline) {
            throw ServerError(code, "produce to " + topic + "-" + std::to_string(partition));
        }
        --attemptsLeft;
        if (code == errc::NotLeaderOrFollower || code == errc::FencedLeaderEpoch ||
            code == errc::UnknownLeaderEpoch) {
            // A stale route is the most common retriable cause, and resending
            // to the same broker would just repeat it.
            router_->refresh(topic);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.retryBackoffMs));
    }
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

const RecordHeader* ConsumedRecord::header(const std::string& name) const {
    for (const auto& h : headers) {
        if (h.key == name) return &h;
    }
    return nullptr;
}

Consumer::Consumer(const std::string& bootstrap, ConsumerConfig config)
    : config_(std::move(config)) {
    router_ = std::make_unique<Router>(
        bootstrap, config_.clientId, std::chrono::milliseconds(config_.connectTimeoutMs),
        std::chrono::milliseconds(static_cast<long long>(config_.requestTimeoutMs) +
                                  config_.fetchMaxWaitMs));
}

Consumer::~Consumer() { close(); }

void Consumer::close() {
    if (router_) router_->close();
}

std::vector<std::int32_t> Consumer::partitions(const std::string& topic) {
    return router_->partitions(topic);
}

std::int64_t Consumer::listOffsets(const std::string& topic, std::int32_t partition,
                                   std::int64_t timestamp) {
    BodyWriter w;
    w.string(topic);
    w.int32(partition);
    w.int64(timestamp);
    Bytes response = router_->connFor(topic, partition)->request(api::ListOffsets, w.bytes());
    BodyReader r(response);
    r.skipString();  // topic
    r.skipInt32();   // partition
    std::int32_t code = r.int32();
    std::int64_t offset = r.int64();
    r.skipInt64();  // timestamp
    if (code != errc::None) {
        throw ServerError(code, "list_offsets " + topic + "-" + std::to_string(partition));
    }
    return offset;
}

std::vector<ConsumedRecord> Consumer::fetch(const std::string& topic, std::int32_t partition,
                                            std::int64_t offset, std::int32_t maxWaitMs) {
    return fetchWithWatermark(topic, partition, offset, maxWaitMs).records;
}

namespace {

struct FetchOnce {
    std::int32_t code = 0;
    std::int64_t highWatermark = 0;
    std::vector<DecodedBatch> batches;
};

FetchOnce fetchOnce(Connection& conn, const Bytes& body) {
    Bytes response = conn.request(api::Fetch, body);
    BodyReader r(response);
    FetchOnce out;
    r.skipString();  // topic
    r.skipInt32();   // partition
    out.code = r.int32();
    out.highWatermark = r.int64();
    r.skipInt64();  // last_stable_offset
    std::int64_t batchesLength = r.int64();
    // Read even though unused: the batches trail the whole struct, so
    // skipping a field would take them from the wrong offset.
    r.skipInt32();  // preferred_read_replica
    Bytes trailing = r.rest();
    if (batchesLength < 0 || batchesLength > static_cast<std::int64_t>(trailing.size())) {
        throw Error("fetch response claims more batch bytes than it carries");
    }
    trailing.resize(static_cast<std::size_t>(batchesLength));
    std::size_t pos = 0;
    while (pos < trailing.size()) out.batches.push_back(decodeRecordBatch(trailing, pos));
    return out;
}

}  // namespace

FetchResult Consumer::fetchWithWatermark(const std::string& topic, std::int32_t partition,
                                         std::int64_t offset, std::int32_t maxWaitMs) {
    maxWaitMs = std::min(maxWaitMs, config_.fetchMaxWaitMs);
    if (maxWaitMs < 0) maxWaitMs = 0;
    BodyWriter w;
    w.string(topic);
    w.int32(partition);
    w.int64(offset);
    w.int32(config_.fetchMaxBytes);
    w.int32(maxWaitMs);
    w.int32(config_.fetchMinBytes);
    w.int32(config_.isolationLevel);
    // client.rack: with it set the leader may name an in-sync replica in the
    // same rack for this client to read from instead.
    w.string(config_.clientRack);
    const Bytes& body = w.bytes();

    FetchOnce result = fetchOnce(*router_->connFor(topic, partition), body);
    if (result.code == errc::NotLeaderOrFollower) {
        router_->refresh(topic);
        result = fetchOnce(*router_->connFor(topic, partition), body);
    }
    if (result.code != errc::None) {
        throw ServerError(result.code, "fetch " + topic + "-" + std::to_string(partition));
    }

    FetchResult out;
    out.highWatermark = result.highWatermark;
    for (auto& batch : result.batches) {
        for (std::size_t index = 0; index < batch.records.size(); ++index) {
            std::int64_t recordOffset = batch.baseOffset + static_cast<std::int64_t>(index);
            // A batch can start before the requested offset; skip what the
            // caller has already seen.
            if (recordOffset < offset) continue;
            Record& record = batch.records[index];
            ConsumedRecord consumed;
            consumed.topic = topic;
            consumed.partition = partition;
            consumed.offset = recordOffset;
            consumed.key = std::move(record.key);
            consumed.value = std::move(record.value);
            consumed.timestamp = batch.maxTimestamp + record.timestampDelta;
            consumed.headers = std::move(record.headers);
            out.records.push_back(std::move(consumed));
        }
    }
    return out;
}

}  // namespace brahmaputra
