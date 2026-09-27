// End-to-end suite for the C++ driver against a live broker.
//
//   brahmaputra-server --data-dir ./data --default-partitions 4
//   ./build/manual_test 127.0.0.1 9092
//
// A port of clients/go/cmd/manualtest: the same sections and the same
// checks. Every check asserts a property of the system, not that a function
// ran: records come back byte-identical, keys pin partitions, headers
// survive, offsets are contiguous. Exits 1 if any check fails, 2 on a fatal
// error.
#include <brahmaputra/brahmaputra.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <cstdio>
#include <iostream>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace bp = brahmaputra;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

namespace {

int passed = 0;
int failed = 0;

void check(const std::string& name, bool ok, const std::string& detail = "") {
    if (ok) {
        ++passed;
        std::printf("  ok   %s\n", name.c_str());
        return;
    }
    ++failed;
    if (!detail.empty()) {
        std::printf("  FAIL %s: %s\n", name.c_str(), detail.c_str());
    } else {
        std::printf("  FAIL %s\n", name.c_str());
    }
}

void section(const std::string& title) { std::printf("\n%s\n", title.c_str()); }

std::string unique(const std::string& prefix) {
    auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
    return prefix + "-" + std::to_string(nanos % 1'000'000'000);
}

bp::Bytes B(const std::string& s) { return bp::toBytes(s); }

bp::ProducerConfig immediateProducer() {
    bp::ProducerConfig config;
    config.lingerMs = 0;
    return config;
}

bool startsWith(const bp::Bytes& data, const bp::Bytes& prefix) {
    return data.size() >= prefix.size() && std::equal(prefix.begin(), prefix.end(), data.begin());
}

// A listening socket on 127.0.0.1 with an ephemeral port. Returns fd, sets port.
int listenLocal(std::uint16_t& port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw bp::Error("socket failed");
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0 || ::listen(fd, 64) != 0) {
        ::close(fd);
        throw bp::Error("bind/listen failed");
    }
    socklen_t len = sizeof addr;
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    port = ntohs(addr.sin_port);
    return fd;
}

// Forwards TCP to the broker and can sever every live connection, which is
// how a broker restart or an idle timeout looks to a client.
class Proxy {
public:
    explicit Proxy(const std::string& target) {
        auto colon = target.rfind(':');
        targetHost_ = target.substr(0, colon);
        targetPort_ = static_cast<std::uint16_t>(std::stoi(target.substr(colon + 1)));
        std::uint16_t port = 0;
        listenFd_ = listenLocal(port);
        address = "127.0.0.1:" + std::to_string(port);
        acceptor_ = std::thread([this] { acceptLoop(); });
    }
    ~Proxy() { close(); }

    void dropAll() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (int fd : live_) ::shutdown(fd, SHUT_RDWR);
            live_.clear();
        }
        std::this_thread::sleep_for(50ms);
    }

    void close() {
        if (listenFd_ < 0) return;
        ::shutdown(listenFd_, SHUT_RDWR);
        if (acceptor_.joinable()) acceptor_.join();
        ::close(listenFd_);
        listenFd_ = -1;
        dropAll();
        for (auto& t : pumps_) t.join();
        for (int fd : all_) ::close(fd);
    }

    std::string address;

private:
    void acceptLoop() {
        for (;;) {
            int client = ::accept(listenFd_, nullptr, nullptr);
            if (client < 0) return;
            int upstream = ::socket(AF_INET, SOCK_STREAM, 0);
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(targetPort_);
            ::inet_pton(AF_INET, targetHost_ == "localhost" ? "127.0.0.1" : targetHost_.c_str(),
                        &addr.sin_addr);
            if (::connect(upstream, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0) {
                ::close(upstream);
                ::close(client);
                continue;
            }
            std::lock_guard<std::mutex> lock(mu_);
            live_.push_back(client);
            live_.push_back(upstream);
            all_.push_back(client);
            all_.push_back(upstream);
            pumps_.emplace_back([client, upstream] { pump(client, upstream); });
            pumps_.emplace_back([client, upstream] { pump(upstream, client); });
        }
    }

    static void pump(int from, int to) {
        char buf[64 * 1024];
        for (;;) {
            ssize_t n = ::recv(from, buf, sizeof buf, 0);
            if (n <= 0) break;
            ssize_t off = 0;
            while (off < n) {
                ssize_t w = ::send(to, buf + off, static_cast<std::size_t>(n - off), MSG_NOSIGNAL);
                if (w <= 0) {
                    off = -1;
                    break;
                }
                off += w;
            }
            if (off < 0) break;
        }
        ::shutdown(to, SHUT_RDWR);
        ::shutdown(from, SHUT_RDWR);
    }

    std::string targetHost_;
    std::uint16_t targetPort_ = 0;
    int listenFd_ = -1;
    std::thread acceptor_;
    std::mutex mu_;
    std::vector<int> live_;
    std::vector<int> all_;
    std::vector<std::thread> pumps_;
};

std::vector<bp::ConsumedRecord> fetchAll(bp::Consumer& consumer, const std::string& topic,
                                         std::size_t want) {
    std::vector<bp::ConsumedRecord> got;
    std::int64_t offset = 0;
    while (got.size() < want) {
        std::vector<bp::ConsumedRecord> batch;
        try {
            batch = consumer.fetch(topic, 0, offset, 500);
        } catch (const bp::Error&) {
            break;
        }
        if (batch.empty()) break;
        offset = batch.back().offset + 1;
        for (auto& r : batch) got.push_back(std::move(r));
    }
    return got;
}

void run(const std::string& address) {
    section("connection and metadata");
    {
        bp::Consumer consumer(address);
        bool answered = false;
        std::string brokerVersion, err;
        std::size_t ranges = 0;
        try {
            auto [versions, version] = consumer.router().seed()->apiVersions();
            ranges = versions.size();
            brokerVersion = version;
            answered = true;
        } catch (const std::exception& e) {
            err = e.what();
        }
        check("ApiVersions answers", answered && ranges > 0, err);
        check("broker reports a version", !brokerVersion.empty(), brokerVersion);
        auto metadata = consumer.router().metadata({}, true);
        check("metadata lists brokers", metadata.brokers.size() >= 1,
              std::to_string(metadata.brokers.size()) + " brokers");
    }

    section("produce and consume round trip");
    std::string topic = unique("cpp-roundtrip");
    std::vector<bp::Bytes> payloads;
    for (int i = 0; i < 50; ++i) payloads.push_back(B("record-" + std::to_string(i)));
    {
        bp::Producer producer(address, immediateProducer());
        for (const auto& payload : payloads) producer.sendTo(topic, 0, payload);
        producer.flush();
        producer.close();
    }
    {
        bp::Consumer consumer(address);
        auto got = consumer.fetch(topic, 0, 0, 500);
        check("every record comes back", got.size() == payloads.size(),
              "got " + std::to_string(got.size()));
        bool identical = got.size() == payloads.size();
        for (std::size_t i = 0; identical && i < got.size(); ++i) {
            if (!got[i].value || *got[i].value != payloads[i] ||
                got[i].offset != static_cast<std::int64_t>(i)) {
                identical = false;
            }
        }
        check("values byte-identical and offsets contiguous", identical);
    }

    section("compression codecs");
    // Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
    // registerCodec so applications that do not want those dependencies do
    // not carry them.
    for (const std::string codec : {"none", "gzip"}) {
        if (!bp::codecAvailable(bp::parseCompression(codec))) {
            check(codec + ": round trips", false,
                  "not available (built with BRAHMAPUTRA_WITH_GZIP=OFF?)");
            continue;
        }
        std::string codecTopic = unique("cpp-" + codec);
        bp::Bytes body;
        for (int i = 0; i < 40; ++i) {
            auto line = B("the same line over and over. ");
            body.insert(body.end(), line.begin(), line.end());
        }
        auto config = immediateProducer();
        config.compressionType = codec;
        bp::Producer producer(address, config);
        for (int i = 0; i < 20; ++i) {
            bp::Bytes value = body;
            value.push_back(static_cast<std::uint8_t>('0' + i % 10));
            producer.sendTo(codecTopic, 0, value);
        }
        producer.flush();
        producer.close();

        bp::Consumer consumer(address);
        auto got = consumer.fetch(codecTopic, 0, 0, 500);
        check(codec + ": round trips",
              got.size() == 20 && got[0].value && startsWith(*got[0].value, body),
              "got " + std::to_string(got.size()) + " records");
    }

    section("keys, partitioning and ordering");
    {
        std::string keyTopic = unique("cpp-keys");
        bp::Producer producer(address, immediateProducer());
        auto partitions = producer.router().partitions(keyTopic);
        for (int i = 0; i < 30; ++i) {
            producer.send(keyTopic, B("v" + std::to_string(i)), B("user-7"));
        }
        producer.flush();
        producer.close();

        std::int32_t target = bp::partitionForKey(B("user-7"), partitions);
        bp::Consumer consumer(address);
        auto onTarget = consumer.fetch(keyTopic, target, 0, 500);
        check("a key pins every record to one partition", onTarget.size() == 30,
              "partition " + std::to_string(target) + " holds " +
                  std::to_string(onTarget.size()) + " of 30");

        bool ordered = onTarget.size() == 30;
        for (std::size_t i = 0; ordered && i < onTarget.size(); ++i) {
            if (!onTarget[i].value || bp::toString(*onTarget[i].value) != "v" + std::to_string(i)) {
                ordered = false;
            }
        }
        check("per-key order is preserved", ordered);

        std::size_t strays = 0;
        for (std::int32_t partition : partitions) {
            if (partition == target) continue;
            strays += consumer.fetch(keyTopic, partition, 0, 200).size();
        }
        check("no keyed record landed elsewhere", strays == 0, std::to_string(strays) + " strays");
    }

    section("murmur2 agrees with the broker's partitioner");
    check("murmur2(\"\") is stable", bp::murmur2(bp::Bytes{}) == 275646681u,
          std::to_string(bp::murmur2(bp::Bytes{})));
    check("murmur2 is deterministic", bp::murmur2(B("user-7")) == bp::murmur2(B("user-7")));
    check("different keys hash differently", bp::murmur2(B("user-7")) != bp::murmur2(B("user-8")));

    section("record headers and timestamps");
    {
        std::string headerTopic = unique("cpp-headers");
        std::int64_t before = bp::nowMillis() - 1000;
        bp::Producer producer(address, immediateProducer());
        producer.sendTo(headerTopic, 0, B("annotated"), std::nullopt,
                        {
                            {"trace-id", B("abc-123")},
                            {"content-type", B("application/json")},
                            {"tombstone-reason", std::nullopt},
                        });
        producer.sendTo(headerTopic, 0, B("plain"));
        producer.flush();
        producer.close();
        std::int64_t after = bp::nowMillis() + 1000;

        bp::Consumer consumer(address);
        auto got = consumer.fetch(headerTopic, 0, 0, 500);
        check("both records arrive", got.size() == 2, "got " + std::to_string(got.size()));
        if (got.size() == 2) {
            const auto& annotated = got[0];
            const auto& plain = got[1];
            check("headers survive the round trip", annotated.headers.size() == 3,
                  std::to_string(annotated.headers.size()) + " headers");
            const auto* trace = annotated.header("trace-id");
            check("header values are exact", trace && trace->value && *trace->value == B("abc-123"));
            check("a null header value stays null",
                  annotated.headers.size() == 3 && !annotated.headers[2].value);
            check("a record with no headers gains none from its batch", plain.headers.empty(),
                  std::to_string(plain.headers.size()) + " headers");
            bool inWindow = true;
            for (const auto& record : got) {
                if (record.timestamp < before || record.timestamp > after) inWindow = false;
            }
            check("timestamps are real wall-clock values", inWindow,
                  std::to_string(got[0].timestamp) + "," + std::to_string(got[1].timestamp) +
                      " outside " + std::to_string(before) + ".." + std::to_string(after));
        }
    }

    section("tombstones");
    {
        std::string tombTopic = unique("cpp-tombstones");
        bp::Producer producer(address, immediateProducer());
        producer.sendTo(tombTopic, 0, B("set"), B("k1"));
        producer.sendTo(tombTopic, 0, bp::Bytes{}, B("k2"));
        // A null value is a deletion, and must stay distinguishable from the
        // empty value above all the way through the round trip.
        producer.sendTo(tombTopic, 0, std::nullopt, B("k3"));
        producer.flush();
        producer.close();

        bp::Consumer consumer(address);
        auto got = consumer.fetch(tombTopic, 0, 0, 500);
        check("all three records arrive", got.size() == 3, "got " + std::to_string(got.size()));
        if (got.size() == 3) {
            check("an ordinary value round-trips", got[0].value && *got[0].value == B("set"));
            check("an empty value is empty, not null", got[1].value && got[1].value->empty(),
                  got[1].value ? std::to_string(got[1].value->size()) + " bytes" : "null");
            check("a tombstone arrives as a null value", !got[2].value,
                  got[2].value ? std::to_string(got[2].value->size()) + " bytes" : "");
        }
    }

    section("offsets");
    {
        bp::Consumer consumer(address);
        auto earliest = consumer.listOffsets(topic, 0, bp::kEarliest);
        auto latest = consumer.listOffsets(topic, 0, bp::kLatest);
        check("earliest is 0 on a fresh topic", earliest == 0, std::to_string(earliest));
        check("latest equals the record count", latest == 50, std::to_string(latest));
    }

    section("acks");
    for (std::int32_t acks : {0, 1, -1}) {
        std::string acksTopic = unique("cpp-acks" + std::to_string(acks));
        auto config = immediateProducer();
        config.acks = acks;
        bp::Producer producer(address, config);
        producer.sendTo(acksTopic, 0, B("durable"));
        producer.flush();
        producer.close();
        std::this_thread::sleep_for(400ms);

        bp::Consumer consumer(address);
        auto got = consumer.fetch(acksTopic, 0, 0, 500);
        check("acks=" + std::to_string(acks) + " stores the record", got.size() == 1,
              "got " + std::to_string(got.size()));
    }

    section("consumer group: assignment, commit, resume");
    {
        std::string groupTopic = unique("cpp-group");
        std::string groupId = unique("cpp-billing");
        bp::Producer producer(address, immediateProducer());
        for (int i = 0; i < 40; ++i) producer.send(groupTopic, B("g" + std::to_string(i)));
        producer.flush();
        producer.close();

        bp::GroupConfig groupConfig;
        groupConfig.autoCommitIntervalMs = 0;
        bp::GroupConsumer consumer(address, groupId, groupConfig);
        consumer.subscribe({groupTopic});

        std::vector<bp::ConsumedRecord> seen;
        auto deadline = Clock::now() + 30s;
        while (seen.size() < 40 && Clock::now() < deadline) {
            for (auto& record : consumer.poll(500ms)) seen.push_back(std::move(record));
        }
        check("the group consumes every record", seen.size() == 40,
              "got " + std::to_string(seen.size()));

        std::set<std::pair<std::int32_t, std::int64_t>> distinct;
        for (const auto& record : seen) distinct.insert({record.partition, record.offset});
        check("no record is delivered twice", distinct.size() == seen.size());

        consumer.commit();
        auto committed = consumer.committed();
        std::int64_t total = 0;
        for (const auto& [slot, offset] : committed) total += offset;
        check("commit records a position", total == 40, std::to_string(total));
        consumer.close();

        // A second consumer in the same group must resume, not replay.
        bp::GroupConsumer rejoined(address, groupId, groupConfig);
        rejoined.subscribe({groupTopic});
        std::size_t replayed = 0;
        auto until = Clock::now() + 5s;
        while (Clock::now() < until) {
            try {
                replayed += rejoined.poll(300ms).size();
            } catch (const bp::Error&) {
            }
        }
        check("a rejoining group resumes from its commit", replayed == 0,
              "replayed " + std::to_string(replayed) + " records it had already committed");
        rejoined.close();
    }

    section("auto.offset.reset");
    {
        std::string resetTopic = unique("cpp-reset");
        bp::Producer producer(address, immediateProducer());
        for (int i = 0; i < 10; ++i) producer.send(resetTopic, B("r" + std::to_string(i)));
        producer.flush();
        producer.close();

        bp::GroupConfig latestConfig;
        latestConfig.autoCommitIntervalMs = 0;
        latestConfig.autoOffsetReset = bp::offset_reset::Latest;
        bp::GroupConsumer consumer(address, unique("cpp-latest"), latestConfig);
        consumer.subscribe({resetTopic});
        std::size_t skipped = 0;
        auto until = Clock::now() + 4s;
        while (Clock::now() < until) {
            try {
                skipped += consumer.poll(300ms).size();
            } catch (const bp::Error&) {
            }
        }
        check("latest skips records produced before the group existed", skipped == 0,
              "saw " + std::to_string(skipped));
        consumer.close();

        bp::GroupConfig noneConfig;
        noneConfig.autoCommitIntervalMs = 0;
        noneConfig.autoOffsetReset = bp::offset_reset::None;
        bp::GroupConsumer strict(address, unique("cpp-none"), noneConfig);
        strict.subscribe({resetTopic});
        bool raised = false;
        until = Clock::now() + 5s;
        while (Clock::now() < until && !raised) {
            try {
                strict.poll(300ms);
            } catch (const bp::NoOffsetForPartition&) {
                raised = true;
            } catch (const bp::Error&) {
            }
        }
        check("none refuses to guess a position", raised);
        strict.close();
    }

    section("assignors");
    for (const std::string assignor :
         {bp::assignor::Range, bp::assignor::RoundRobin, bp::assignor::Sticky}) {
        std::string assignorTopic = unique("cpp-" + assignor);
        bp::Producer producer(address, immediateProducer());
        for (int i = 0; i < 20; ++i) producer.send(assignorTopic, B("a" + std::to_string(i)));
        producer.flush();
        producer.close();

        bp::GroupConfig groupConfig;
        groupConfig.autoCommitIntervalMs = 0;
        groupConfig.partitionAssignmentStrategy = assignor;
        bp::GroupConsumer consumer(address, unique("cpp-grp-" + assignor), groupConfig);
        consumer.subscribe({assignorTopic});
        std::size_t collected = 0;
        auto deadline = Clock::now() + 20s;
        while (collected < 20 && Clock::now() < deadline) {
            try {
                collected += consumer.poll(500ms).size();
            } catch (const bp::Error&) {
            }
        }
        check(assignor + ": consumes every record", collected == 20,
              "got " + std::to_string(collected));
        consumer.close();
    }

    section("bounded client buffer");
    {
        std::string bufferTopic = unique("cpp-buffer");
        bp::ProducerConfig config;
        config.lingerMs = 10'000;  // never flush on time during this check
        config.bufferMemory = 2048;
        config.maxBlockMs = 300;
        bp::Producer producer(address, config);
        bool blocked = false;
        for (int i = 0; i < 500 && !blocked; ++i) {
            try {
                producer.sendTo(bufferTopic, 0, bp::Bytes(256, 'x'));
            } catch (const bp::BufferFullError& e) {
                blocked = std::string(e.what()).find("buffer full") != std::string::npos;
            }
        }
        check("a full buffer blocks and then reports", blocked);
    }
    section("wire edge cases");
    {
        std::string edgeTopic = unique("cpp-edge");
        bp::Producer producer(address, immediateProducer());
        bp::Bytes large(1 << 20);
        for (std::size_t i = 0; i < large.size(); ++i) large[i] = static_cast<std::uint8_t>(i * 7);
        bp::Bytes unicodeKey = B("ключ-✓-🔑");
        bp::Bytes unicodeValue = B("значение — 数据 — 🚀");
        producer.sendTo(edgeTopic, 0, large);
        producer.sendTo(edgeTopic, 0, unicodeValue, unicodeKey, {{"ünïcødé-🏷", B("✓")}});
        // An empty key and an empty header value are values, not nulls.
        producer.sendTo(edgeTopic, 0, B("empty-key"), bp::Bytes{},
                        {{"empty", bp::Bytes{}}, {"null", std::nullopt}});
        producer.sendTo(edgeTopic, 0, B("null-key"), std::nullopt);
        producer.close();

        bp::Consumer consumer(address);
        auto got = fetchAll(consumer, edgeTopic, 4);
        check("edge records all arrive", got.size() == 4, "got " + std::to_string(got.size()));
        if (got.size() == 4) {
            check("a 1 MiB value round-trips byte-identical", got[0].value && *got[0].value == large,
                  std::to_string(got[0].value ? got[0].value->size() : 0) + " bytes");
            check("unicode key, value and header key round-trip",
                  got[1].key && *got[1].key == unicodeKey && got[1].value &&
                      *got[1].value == unicodeValue && got[1].headers.size() == 1 &&
                      got[1].headers[0].key == "ünïcødé-🏷");
            check("an empty key stays empty, not null", got[2].key && got[2].key->empty(),
                  got[2].key ? std::to_string(got[2].key->size()) + " bytes" : "null");
            check("an empty header value stays empty, not null",
                  got[2].headers.size() == 2 && got[2].headers[0].value &&
                      got[2].headers[0].value->empty() && !got[2].headers[1].value,
                  std::to_string(got[2].headers.size()) + " headers");
            check("a null key stays null", !got[3].key);
        }
    }

    section("ordering under linger flushes");
    {
        std::string orderTopic = unique("cpp-order");
        bp::ProducerConfig config;
        config.lingerMs = 1;
        config.batchSize = 256;
        bp::Producer producer(address, config);
        const int total = 5000;
        for (int i = 0; i < total; ++i) producer.sendTo(orderTopic, 0, B(std::to_string(i)));
        producer.close();

        bp::Consumer consumer(address);
        auto got = fetchAll(consumer, orderTopic, total);
        std::vector<int> values;
        for (const auto& r : got) values.push_back(r.value ? std::stoi(bp::toString(*r.value)) : -1);
        int inversions = 0;
        for (std::size_t i = 1; i < values.size(); ++i) {
            if (values[i] < values[i - 1]) ++inversions;
        }
        check("every record of a partition arrives", values.size() == static_cast<std::size_t>(total),
              "got " + std::to_string(values.size()));
        check("a partition's records keep send order", inversions == 0,
              std::to_string(inversions) + " inversions");
    }

    section("background flush failures are reported");
    {
        bp::ProducerConfig config;
        config.lingerMs = 20;
        auto producer = std::make_shared<bp::Producer>(address, config);
        // Partition 999 does not exist, so the linger thread's flush fails.
        std::string sendErr, flushErr;
        try {
            producer->sendTo(unique("cpp-bgfail"), 999, B("lost"));
        } catch (const std::exception& e) {
            sendErr = e.what();
        }
        std::this_thread::sleep_for(300ms);
        try {
            producer->flush();
        } catch (const std::exception& e) {
            flushErr = e.what();
        }
        check("a failed linger flush surfaces on the next Flush",
              sendErr.empty() && !flushErr.empty(), "send=" + sendErr + " flush=" + flushErr);
        auto closed = std::async(std::launch::async, [producer] {
            try {
                producer->close();
            } catch (const std::exception&) {
            }
        });
        check("Close returns after a failed flush",
              closed.wait_for(5s) == std::future_status::ready, "hung");
    }

    section("connection failures");
    {
        // A broker that accepts and never answers must cost an error, not a
        // thread blocked forever. The kernel completes the handshake from the
        // listen backlog; nothing ever reads or replies.
        std::uint16_t silentPort = 0;
        int silent = listenLocal(silentPort);
        {
            bp::Connection conn("127.0.0.1", silentPort, "cpp-test", 1000ms, 30000ms);
            conn.setRequestTimeout(300ms);
            auto started = Clock::now();
            std::string requestErr;
            try {
                conn.apiVersions();
            } catch (const std::exception& e) {
                requestErr = e.what();
            }
            check("a request to an unresponsive broker times out",
                  !requestErr.empty() && Clock::now() - started < 3s, requestErr);
            check("a timed-out connection is not reused", conn.broken());
        }
        ::close(silent);

        // A connection the broker drops is redialled, not kept forever.
        Proxy proxy(address);
        std::string dropTopic = unique("cpp-drop");
        {
            bp::Producer producer(proxy.address, immediateProducer());
            producer.sendTo(dropTopic, 0, B("before"));
            proxy.dropAll();
            std::string recovered = "not attempted";
            for (int attempt = 0; attempt < 3 && !recovered.empty(); ++attempt) {
                try {
                    producer.sendTo(dropTopic, 0, B("after"));
                    recovered.clear();
                } catch (const std::exception& e) {
                    recovered = e.what();
                }
            }
            check("a producer recovers after its connection drops", recovered.empty(), recovered);
            try {
                producer.close();
            } catch (const std::exception&) {
            }
        }
        {
            bp::Consumer consumer(proxy.address);
            consumer.fetch(dropTopic, 0, 0, 100);
            proxy.dropAll();
            std::string fetchErr = "not attempted";
            std::size_t fetched = 0;
            for (int attempt = 0; attempt < 3 && !fetchErr.empty(); ++attempt) {
                try {
                    fetched = consumer.fetch(dropTopic, 0, 0, 100).size();
                    fetchErr.clear();
                } catch (const std::exception& e) {
                    fetchErr = e.what();
                }
            }
            check("a consumer recovers after its connection drops",
                  fetchErr.empty() && fetched >= 1, fetchErr);
        }
        proxy.close();
    }

    section("consumer group: max.poll.interval and rejoin");
    {
        std::string slowTopic = unique("cpp-slow");
        bp::Producer producer(address, immediateProducer());
        for (int i = 0; i < 10; ++i) producer.send(slowTopic, B("s" + std::to_string(i)));
        bp::GroupConfig groupConfig;
        groupConfig.autoCommitIntervalMs = 0;
        groupConfig.maxPollIntervalMs = 1500;
        bp::GroupConsumer consumer(address, unique("cpp-slow-grp"), groupConfig);
        consumer.subscribe({slowTopic});
        std::size_t first = 0;
        auto deadline = Clock::now() + 15s;
        try {
            while (first < 10 && Clock::now() < deadline) first += consumer.poll(300ms).size();
        } catch (const bp::Error&) {
        }
        consumer.commit();
        // Stall past max.poll.interval.ms: the member leaves the group.
        std::this_thread::sleep_for(2500ms);
        for (int i = 10; i < 20; ++i) producer.send(slowTopic, B("s" + std::to_string(i)));
        producer.close();
        std::size_t second = 0;
        std::string pollErr;
        deadline = Clock::now() + 15s;
        try {
            while (second < 10 && Clock::now() < deadline) second += consumer.poll(300ms).size();
        } catch (const std::exception& e) {
            pollErr = e.what();
        }
        check("a member that stalled rejoins on its next poll",
              first == 10 && second == 10 && pollErr.empty(),
              "first=" + std::to_string(first) + " second=" + std::to_string(second) +
                  " err=" + pollErr);
        consumer.close();
    }

    section("consumer group: time inside poll does not count against max.poll.interval");
    {
        std::string joinTopic = unique("cpp-inpoll");
        bp::Producer producer(address, immediateProducer());
        producer.router().partitions(joinTopic);
        bp::GroupConfig groupConfig;
        groupConfig.autoCommitIntervalMs = 0;
        // Far shorter than the first poll below, which spends ~1s joining
        // (the broker's initial rebalance delay) and then waits for data.
        groupConfig.maxPollIntervalMs = 600;
        bp::GroupConsumer consumer(address, unique("cpp-inpoll-grp"), groupConfig);
        consumer.subscribe({joinTopic});
        std::thread late([&producer, joinTopic] {
            std::this_thread::sleep_for(2s);
            for (int i = 0; i < 10; ++i) {
                try {
                    producer.send(joinTopic, B("j" + std::to_string(i)));
                } catch (const bp::Error&) {
                }
            }
        });
        // One long poll: it joins, then waits for the records above.
        std::size_t got = 0;
        std::string pollErr, commitErr;
        try {
            got = consumer.poll(4s).size();
        } catch (const std::exception& e) {
            pollErr = e.what();
        }
        // Committed straight away, before another poll could quietly rejoin:
        // this fails if the member left the group mid-poll.
        try {
            consumer.commit();
        } catch (const std::exception& e) {
            commitErr = e.what();
        }
        late.join();
        check("a member is still in its group after a long poll",
              pollErr.empty() && got > 0 && commitErr.empty(),
              "got=" + std::to_string(got) + " poll=" + pollErr + " commit=" + commitErr);
        consumer.close();
        producer.close();
    }
}

}  // namespace

int main(int argc, char** argv) {
    std::string address = "127.0.0.1:9092";
    if (argc > 2) {
        address = std::string(argv[1]) + ":" + argv[2];
    } else if (argc > 1) {
        address = argv[1];
    }
    try {
        run(address);
    } catch (const std::exception& e) {
        std::printf("  FATAL %s\n", e.what());
        std::printf("\n%d passed, %d failed\n", passed, failed + 1);
        return 2;
    }
    std::printf("\n%d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
