// POSIX socket connection and leader routing.
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "brahmaputra/client.hpp"

namespace brahmaputra {

std::int64_t nowMillis() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

namespace {

std::string errnoText(const std::string& what) {
    return what + ": " + std::strerror(errno);
}

int dialTcp(const std::string& host, std::uint16_t port, std::chrono::milliseconds timeout) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* result = nullptr;
    std::string service = std::to_string(port);
    int rc = ::getaddrinfo(host.c_str(), service.c_str(), &hints, &result);
    if (rc != 0) {
        throw NetworkError("resolve " + host + ": " + ::gai_strerror(rc));
    }
    std::string lastError = "no addresses for " + host;
    for (addrinfo* ai = result; ai != nullptr; ai = ai->ai_next) {
        int fd = ::socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol);
        if (fd < 0) {
            lastError = errnoText("socket");
            continue;
        }
        int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        rc = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (rc != 0 && errno == EINPROGRESS) {
            pollfd pfd{fd, POLLOUT, 0};
            int ready = ::poll(&pfd, 1, static_cast<int>(timeout.count()));
            if (ready == 1) {
                int soError = 0;
                socklen_t len = sizeof soError;
                ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soError, &len);
                if (soError == 0) {
                    rc = 0;
                } else {
                    errno = soError;
                }
            } else if (ready == 0) {
                errno = ETIMEDOUT;
            }
        }
        if (rc != 0) {
            lastError = errnoText("connect " + host + ":" + service);
            ::close(fd);
            continue;
        }
        ::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
        // Responses are small and latency matters more than packet count;
        // without this every request pays Nagle plus the peer's delayed ACK.
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        ::freeaddrinfo(result);
        return fd;
    }
    ::freeaddrinfo(result);
    throw NetworkError(lastError);
}

void setIoTimeout(int fd, std::chrono::milliseconds timeout) {
    timeval tv{};
    tv.tv_sec = static_cast<time_t>(timeout.count() / 1000);
    tv.tv_usec = static_cast<suseconds_t>((timeout.count() % 1000) * 1000);
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
}

std::pair<std::string, std::uint16_t> parseHostPort(const std::string& address) {
    auto colon = address.rfind(':');
    if (colon == std::string::npos) return {address, 9092};
    std::string host = address.substr(0, colon);
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
    }
    int port = std::stoi(address.substr(colon + 1));
    if (port <= 0 || port > 65535) throw Error("invalid port in \"" + address + "\"");
    return {host, static_cast<std::uint16_t>(port)};
}

}  // namespace

Connection::Connection(std::string host, std::uint16_t port, std::string clientId,
                       std::chrono::milliseconds connectTimeout,
                       std::chrono::milliseconds ioTimeout)
    : host_(std::move(host)),
      port_(port),
      clientId_(std::move(clientId)),
      connectTimeout_(connectTimeout),
      ioTimeout_(ioTimeout) {
    std::lock_guard<std::mutex> lock(mu_);
    ensureOpenLocked();
}

Connection::~Connection() { close(); }

void Connection::close() {
    std::lock_guard<std::mutex> lock(mu_);
    closeLocked();
}

void Connection::closeLocked() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void Connection::failLocked() {
    closeLocked();
    broken_.store(true);
}

void Connection::ensureOpenLocked() {
    if (fd_ >= 0) return;
    fd_ = dialTcp(host_, port_, connectTimeout_);
    setIoTimeout(fd_, ioTimeout_);
    broken_.store(false);
}

void Connection::setRequestTimeout(std::chrono::milliseconds timeout) {
    std::lock_guard<std::mutex> lock(mu_);
    ioTimeout_ = timeout;
    if (fd_ >= 0) setIoTimeout(fd_, ioTimeout_);
}

void Connection::writeAllLocked(const Bytes& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = ::send(fd_, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::string msg = (errno == EAGAIN || errno == EWOULDBLOCK)
                                  ? std::string("write timed out")
                                  : errnoText("write");
            failLocked();
            throw NetworkError(msg + " (" + host_ + ":" + std::to_string(port_) + ")");
        }
        sent += static_cast<std::size_t>(n);
    }
}

Bytes Connection::readFrameLocked() {
    auto readExact = [this](std::uint8_t* out, std::size_t len) {
        std::size_t got = 0;
        while (got < len) {
            ssize_t n = ::recv(fd_, out + got, len - got, 0);
            if (n == 0) {
                failLocked();
                throw NetworkError("connection closed by broker (" + host_ + ":" +
                                   std::to_string(port_) + ")");
            }
            if (n < 0) {
                if (errno == EINTR) continue;
                std::string msg = (errno == EAGAIN || errno == EWOULDBLOCK)
                                      ? std::string("request timed out")
                                      : errnoText("read");
                // A half-read response leaves the stream desynchronised, so
                // the socket goes; the next request dials again.
                failLocked();
                throw NetworkError(msg + " (" + host_ + ":" + std::to_string(port_) + ")");
            }
            got += static_cast<std::size_t>(n);
        }
    };
    std::uint8_t header[4];
    readExact(header, 4);
    auto length = static_cast<std::int32_t>((std::uint32_t(header[0]) << 24) |
                                            (std::uint32_t(header[1]) << 16) |
                                            (std::uint32_t(header[2]) << 8) | header[3]);
    if (length < 0) {
        failLocked();
        throw Error("negative frame length " + std::to_string(length));
    }
    Bytes payload(static_cast<std::size_t>(length));
    if (length > 0) readExact(payload.data(), payload.size());
    return payload;
}

Bytes Connection::request(std::int16_t apiKey, const Bytes& body) {
    std::lock_guard<std::mutex> lock(mu_);
    ensureOpenLocked();
    std::int32_t correlationId = ++next_;
    writeAllLocked(encodeFrame(apiKey, correlationId, clientId_, body));
    Bytes payload = readFrameLocked();
    auto [got, responseBody] = decodeFramePayload(payload);
    if (got != correlationId) {
        // The stream has desynchronised; continuing would pair every later
        // response with the wrong request.
        failLocked();
        throw Error("correlation id mismatch: expected " + std::to_string(correlationId) +
                    ", got " + std::to_string(got));
    }
    return std::move(responseBody);
}

void Connection::sendOneway(std::int16_t apiKey, const Bytes& body) {
    std::lock_guard<std::mutex> lock(mu_);
    ensureOpenLocked();
    writeAllLocked(encodeFrame(apiKey, ++next_, clientId_, body));
}

std::pair<std::vector<ApiVersionRange>, std::string> Connection::apiVersions() {
    BodyWriter w;
    w.string("brahmaputra-cpp");
    w.string("0.1.0");
    Bytes response = request(api::ApiVersions, w.bytes());
    BodyReader r(response);
    std::int32_t code = r.int32();
    if (code != errc::None) throw ServerError(code, "api_versions");
    std::int32_t count = r.int32();
    std::vector<ApiVersionRange> ranges;
    for (std::int32_t i = 0; i < count; ++i) {
        ApiVersionRange range{};
        range.apiKey = r.int32();
        range.minVersion = r.int32();
        range.maxVersion = r.int32();
        ranges.push_back(range);
    }
    std::string brokerVersion = r.string();
    return {ranges, brokerVersion};
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

std::vector<std::int32_t> ClusterMetadata::partitionsOf(const std::string& topic) const {
    std::vector<std::int32_t> out;
    for (const auto& info : topics) {
        if (info.name != topic) continue;
        for (const auto& p : info.partitions) out.push_back(p.partition);
        std::sort(out.begin(), out.end());
        break;
    }
    return out;
}

std::int32_t ClusterMetadata::leaderOf(const std::string& topic, std::int32_t partition) const {
    for (const auto& info : topics) {
        if (info.name != topic) continue;
        for (const auto& p : info.partitions) {
            if (p.partition == partition) return p.leader;
        }
    }
    return -1;
}

namespace {

ClusterMetadata decodeMetadata(BodyReader& r) {
    // Field order is exactly the schema's: error_code, brokers,
    // controller_id, topics. The leading code is request-level (an
    // authorization denial, say); the per-topic one is "no such topic".
    ClusterMetadata metadata;
    std::int32_t code = r.int32();
    if (code != errc::None) throw ServerError(code, "metadata");
    for (std::int32_t count = r.int32(); count > 0; --count) {
        BrokerInfo broker;
        broker.nodeId = r.int32();
        broker.host = r.string();
        broker.port = r.int32();
        broker.rack = r.string();
        metadata.brokers.push_back(std::move(broker));
    }
    metadata.controllerId = r.int32();
    for (std::int32_t count = r.int32(); count > 0; --count) {
        TopicInfo topic;
        topic.name = r.string();
        topic.errorCode = r.int32();
        for (std::int32_t pcount = r.int32(); pcount > 0; --pcount) {
            PartitionInfo info;
            info.partition = r.int32();
            info.leader = r.int32();
            for (std::int32_t n = r.int32(); n > 0; --n) info.replicas.push_back(r.int32());
            for (std::int32_t n = r.int32(); n > 0; --n) info.isr.push_back(r.int32());
            info.leaderEpoch = r.int32();
            topic.partitions.push_back(std::move(info));
        }
        if (topic.errorCode != errc::None && topic.errorCode != errc::UnknownTopicOrPartition) {
            throw ServerError(topic.errorCode, "metadata for " + topic.name);
        }
        metadata.topics.push_back(std::move(topic));
    }
    return metadata;
}

}  // namespace

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

Router::Router(const std::string& bootstrap, std::string clientId,
               std::chrono::milliseconds connectTimeout, std::chrono::milliseconds ioTimeout)
    : clientId_(std::move(clientId)), connectTimeout_(connectTimeout), ioTimeout_(ioTimeout) {
    std::string lastError = "empty bootstrap address";
    std::size_t start = 0;
    while (start <= bootstrap.size()) {
        std::size_t comma = bootstrap.find(',', start);
        std::string address = bootstrap.substr(
            start, comma == std::string::npos ? std::string::npos : comma - start);
        start = comma == std::string::npos ? bootstrap.size() + 1 : comma + 1;
        address.erase(0, address.find_first_not_of(" \t"));
        address.erase(address.find_last_not_of(" \t") + 1);
        if (address.empty()) continue;
        try {
            auto [host, port] = parseHostPort(address);
            seed_ = std::make_shared<Connection>(host, port, clientId_, connectTimeout_, ioTimeout_);
            return;
        } catch (const Error& e) {
            lastError = e.what();
        }
    }
    throw NetworkError("no bootstrap server reachable: " + lastError);
}

Router::~Router() { close(); }

void Router::close() {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& [id, conn] : conns_) {
        if (conn != seed_) conn->close();
    }
    conns_.clear();
    if (seed_) seed_->close();
}

ClusterMetadata Router::metadata(const std::vector<std::string>& topics, bool refresh) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!refresh && metadata_) return *metadata_;
    BodyWriter w;
    w.stringArray(topics);
    Bytes response = seed_->request(api::Metadata, w.bytes());
    BodyReader r(response);
    metadata_ = decodeMetadata(r);
    return *metadata_;
}

ClusterMetadata Router::refresh(const std::string& topic) { return metadata({topic}, true); }

std::vector<std::int32_t> Router::partitions(const std::string& topic) {
    ClusterMetadata md = metadata({topic}, false);
    auto partitions = md.partitionsOf(topic);
    if (partitions.empty()) {
        // A topic auto-created on first reference is not in the cached image
        // yet; one refresh distinguishes "new" from "absent".
        partitions = refresh(topic).partitionsOf(topic);
    }
    if (partitions.empty()) throw Error("topic \"" + topic + "\" has no partitions");
    return partitions;
}

std::shared_ptr<Connection> Router::connFor(const std::string& topic, std::int32_t partition) {
    ClusterMetadata md = metadata({topic}, false);
    std::int32_t leader = md.leaderOf(topic, partition);
    if (leader < 0) {
        md = refresh(topic);
        leader = md.leaderOf(topic, partition);
    }
    if (leader < 0) {
        throw Error("no leader for " + topic + "-" + std::to_string(partition));
    }

    std::lock_guard<std::mutex> lock(mu_);
    auto it = conns_.find(leader);
    if (it != conns_.end()) return it->second;
    for (const auto& broker : md.brokers) {
        if (broker.nodeId != leader) continue;
        // A single-broker cluster advertises the address it was configured
        // with, which may not be the one we dialled; reuse the seed rather
        // than opening a second connection to ourselves.
        if (md.brokers.size() == 1) {
            conns_[leader] = seed_;
            return seed_;
        }
        auto conn = std::make_shared<Connection>(broker.host,
                                                 static_cast<std::uint16_t>(broker.port),
                                                 clientId_, connectTimeout_, ioTimeout_);
        conns_[leader] = conn;
        return conn;
    }
    throw Error("broker " + std::to_string(leader) + " is not in the metadata");
}

}  // namespace brahmaputra
