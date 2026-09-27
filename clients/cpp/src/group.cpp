// Consumer-group membership, offsets and assignors.
#include "brahmaputra/group.hpp"

#include <algorithm>
#include <set>

namespace brahmaputra {

namespace detail {
long long parseInteger(const std::string& key, const std::string& value);
bool parseBoolean(const std::string& key, const std::string& value);
[[noreturn]] void unknownProperty(const std::string& key);
}  // namespace detail

namespace {
// Bounds retries after a coordinator move or load.
constexpr int kCoordinatorAttempts = 4;
// Bounds join+sync rounds for a group that will not settle.
constexpr int kJoinAttempts = 4;
}  // namespace

GroupConfig GroupConfig::fromProperties(const Properties& props) {
    using detail::parseInteger;
    GroupConfig c;
    for (const auto& [key, value] : props) {
        if (key == "bootstrap.servers" || key == "group.id") continue;  // constructor arguments
        if (key == "client.id") c.clientId = value;
        else if (key == "session.timeout.ms") c.sessionTimeoutMs = static_cast<std::int32_t>(parseInteger(key, value));
        else if (key == "rebalance.timeout.ms") c.rebalanceTimeoutMs = static_cast<std::int32_t>(parseInteger(key, value));
        else if (key == "max.poll.interval.ms") c.maxPollIntervalMs = static_cast<int>(parseInteger(key, value));
        else if (key == "enable.auto.commit") c.enableAutoCommit = detail::parseBoolean(key, value);
        else if (key == "auto.commit.interval.ms") c.autoCommitIntervalMs = static_cast<int>(parseInteger(key, value));
        else if (key == "auto.offset.reset") c.autoOffsetReset = value;
        else if (key == "partition.assignment.strategy") c.partitionAssignmentStrategy = value;
        else if (key == "group.instance.id") c.groupInstanceId = value;
        else if (key == "max.poll.records") c.maxPollRecords = static_cast<int>(parseInteger(key, value));
        else if (key == "fetch.max.bytes") c.fetchMaxBytes = static_cast<std::int32_t>(parseInteger(key, value));
        else if (key == "fetch.min.bytes") c.fetchMinBytes = static_cast<std::int32_t>(parseInteger(key, value));
        else if (key == "fetch.max.wait.ms") c.fetchMaxWaitMs = static_cast<std::int32_t>(parseInteger(key, value));
        else if (key == "request.timeout.ms") c.requestTimeoutMs = static_cast<int>(parseInteger(key, value));
        else if (key == "socket.connection.setup.timeout.ms") c.connectTimeoutMs = static_cast<int>(parseInteger(key, value));
        else detail::unknownProperty(key);
    }
    return c;
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

GroupConsumer::GroupConsumer(const std::string& bootstrap, std::string groupId, GroupConfig config)
    : groupId_(std::move(groupId)), config_(std::move(config)) {
    const auto& a = config_.partitionAssignmentStrategy;
    if (a != assignor::Range && a != assignor::RoundRobin && a != assignor::Sticky) {
        throw Error("unknown partition.assignment.strategy \"" + a + "\"");
    }
    const auto& r = config_.autoOffsetReset;
    if (r != offset_reset::Earliest && r != offset_reset::Latest && r != offset_reset::None) {
        throw Error("unknown auto.offset.reset \"" + r + "\"");
    }
    ConsumerConfig cc;
    cc.clientId = config_.clientId;
    cc.fetchMaxBytes = config_.fetchMaxBytes;
    cc.fetchMinBytes = config_.fetchMinBytes;
    cc.fetchMaxWaitMs = config_.fetchMaxWaitMs;
    cc.maxPollRecords = config_.maxPollRecords;
    cc.requestTimeoutMs = config_.requestTimeoutMs;
    cc.connectTimeoutMs = config_.connectTimeoutMs;
    consumer_ = std::make_unique<Consumer>(bootstrap, cc);
    lastCommitMs_ = nowMillis();
    lastPollMs_ = nowMillis();
    heartbeat_ = std::thread([this] { heartbeatLoop(); });
}

GroupConsumer::~GroupConsumer() {
    try {
        close();
    } catch (...) {
    }
}

std::string GroupConsumer::memberId() const {
    std::lock_guard<std::mutex> lock(mu_);
    return memberId_;
}

std::int32_t GroupConsumer::generation() const {
    std::lock_guard<std::mutex> lock(mu_);
    return generation_;
}

bool GroupConsumer::needsRejoin() const {
    std::lock_guard<std::mutex> lock(mu_);
    return !joined_;
}

void GroupConsumer::markRejoin() {
    std::lock_guard<std::mutex> lock(mu_);
    joined_ = false;
}

void GroupConsumer::subscribe(const std::vector<std::string>& topics) {
    subscribed_ = topics;
    markRejoin();
}

void GroupConsumer::close() {
    bool wasJoined;
    std::string member;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (closed_) return;
        closed_ = true;
        wasJoined = joined_;
        member = memberId_;
    }
    wake_.notify_all();
    if (heartbeat_.joinable()) heartbeat_.join();

    if (wasJoined) {
        // Re-committing positions an explicit-commit user already committed
        // changes nothing; for auto-commit it records the last poll's records.
        try {
            commit();
        } catch (const Error&) {
        }
    }
    if (!member.empty()) {
        // Leaving is what separates a clean shutdown from a crash. Without it
        // the coordinator must wait out session.timeout.ms before
        // reassigning. Best effort: failing costs only that wait.
        try {
            leave();
        } catch (const Error&) {
        }
    }
    consumer_->close();
}

// ---------------------------------------------------------------------------
// Poll and offsets
// ---------------------------------------------------------------------------

std::vector<ConsumedRecord> GroupConsumer::poll(std::chrono::milliseconds timeout) {
    if (subscribed_.empty()) throw Error("subscribe to at least one topic before polling");
    {
        // Stamped on entry: the interval bounds how long the application may
        // go without asking for records, and a poll that blocks for its full
        // timeout is the consumer working normally.
        std::lock_guard<std::mutex> lock(mu_);
        if (closed_) throw Error("group consumer is closed");
        lastPollMs_ = nowMillis();
        inPoll_ = true;
    }
    // Time spent inside poll() — a slow join, a long fetch — is the consumer
    // working, not the application stalling, so it never counts toward
    // max.poll.interval.ms. The interval restarts when poll() returns.
    struct PollGuard {
        GroupConsumer& self;
        ~PollGuard() {
            std::lock_guard<std::mutex> lock(self.mu_);
            self.inPoll_ = false;
            self.lastPollMs_ = nowMillis();
        }
    } guard{*this};

    auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        if (needsRejoin()) join();
        if (!buffered_.empty()) return takeBuffered();
        if (assignment_.empty()) {
            if (std::chrono::steady_clock::now() >= deadline) return {};
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
        }

        bool gotAny = false;
        for (const auto& slot : assignment_) {
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
            auto waitMs = static_cast<std::int32_t>(
                std::clamp<long long>(remaining.count(), 0, 500));
            std::vector<ConsumedRecord> records;
            try {
                records = consumer_->fetch(slot.topic, slot.partition, fetchPositions_[slot], waitMs);
            } catch (const ServerError& e) {
                if (e.code() == errc::OffsetOutOfRange) {
                    // The position fell off the log; restart where the policy says.
                    std::int64_t reset = resetOffset(slot);
                    fetchPositions_[slot] = reset;
                    positions_[slot] = reset;
                    continue;
                }
                if (e.code() == errc::NotLeaderOrFollower) {
                    consumer_->router().refresh(slot.topic);
                    continue;
                }
                throw;
            }
            if (!records.empty()) {
                gotAny = true;
                fetchPositions_[slot] = records.back().offset + 1;
                for (auto& record : records) buffered_.push_back(std::move(record));
            }
        }

        maybeAutoCommit();
        if (!buffered_.empty()) return takeBuffered();
        if (!gotAny && std::chrono::steady_clock::now() >= deadline) return {};
    }
}

std::vector<ConsumedRecord> GroupConsumer::takeBuffered() {
    std::size_t limit = buffered_.size();
    if (config_.maxPollRecords > 0 && static_cast<std::size_t>(config_.maxPollRecords) < limit) {
        limit = static_cast<std::size_t>(config_.maxPollRecords);
    }
    std::vector<ConsumedRecord> delivered(std::make_move_iterator(buffered_.begin()),
                                          std::make_move_iterator(buffered_.begin() +
                                                                  static_cast<std::ptrdiff_t>(limit)));
    buffered_.erase(buffered_.begin(), buffered_.begin() + static_cast<std::ptrdiff_t>(limit));
    for (const auto& record : delivered) {
        // The consumed position advances only over records actually handed
        // to the caller; committing what was merely fetched would silently
        // skip records nobody processed.
        positions_[TopicPartition{record.topic, record.partition}] = record.offset + 1;
    }
    return delivered;
}

void GroupConsumer::commit() {
    if (positions_.empty()) return;
    std::string member;
    std::int32_t generation;
    {
        std::lock_guard<std::mutex> lock(mu_);
        member = memberId_;
        generation = generation_;
    }
    BodyWriter w;
    w.string(groupId_);
    w.int32(generation);
    w.string(member);
    w.int32(static_cast<std::int32_t>(positions_.size()));
    for (const auto& [slot, offset] : positions_) {  // std::map: already sorted
        w.string(slot.topic);
        w.int32(slot.partition);
        w.int64(offset);
    }
    Bytes response = coordinatorRequest(api::OffsetCommit, w.bytes());
    BodyReader r(response);
    std::int32_t code = r.int32();
    if (code != errc::None) {
        if (code == errc::IllegalGeneration || code == errc::UnknownMemberId ||
            code == errc::RebalanceInProgress) {
            // Fenced: the group moved on without this member's generation.
            markRejoin();
        }
        throw ServerError(code, "offset_commit");
    }
    lastCommitMs_ = nowMillis();
}

std::map<TopicPartition, std::int64_t> GroupConsumer::committed(
    const std::vector<TopicPartition>& partitions) {
    BodyWriter w;
    w.string(groupId_);
    w.int32(static_cast<std::int32_t>(partitions.size()));
    for (const auto& slot : partitions) {
        w.string(slot.topic);
        w.int32(slot.partition);
    }
    Bytes response = coordinatorRequest(api::OffsetFetch, w.bytes());
    BodyReader r(response);
    std::int32_t code = r.int32();
    if (code != errc::None) throw ServerError(code, "offset_fetch");
    std::map<TopicPartition, std::int64_t> out;
    for (std::int32_t count = r.int32(); count > 0; --count) {
        TopicPartition slot;
        slot.topic = r.string();
        slot.partition = r.int32();
        out[slot] = r.int64();
    }
    return out;
}

void GroupConsumer::maybeAutoCommit() {
    if (!config_.enableAutoCommit || config_.autoCommitIntervalMs <= 0 || positions_.empty()) return;
    if (nowMillis() - lastCommitMs_ < config_.autoCommitIntervalMs) return;
    try {
        commit();
    } catch (const Error&) {
        // Retried on the next poll; an explicit commit() is what a caller
        // relies on.
    }
}

std::int64_t GroupConsumer::resetOffset(const TopicPartition& slot) {
    const auto& policy = config_.autoOffsetReset;
    if (policy == offset_reset::Earliest) return consumer_->listOffsets(slot.topic, slot.partition, kEarliest);
    if (policy == offset_reset::Latest) return consumer_->listOffsets(slot.topic, slot.partition, kLatest);
    throw NoOffsetForPartition();
}

// ---------------------------------------------------------------------------
// Membership
// ---------------------------------------------------------------------------

void GroupConsumer::join() {
    // Before giving up the current assignment, record what was delivered so
    // the next owner does not replay it (Kafka's commit-on-revoke).
    if (config_.enableAutoCommit && config_.autoCommitIntervalMs > 0 && !positions_.empty()) {
        try {
            commit();
        } catch (const Error&) {
        }
    }

    for (int attempt = 0; attempt < kJoinAttempts; ++attempt) {
        std::string member;
        {
            std::lock_guard<std::mutex> lock(mu_);
            member = memberId_;
        }
        BodyWriter w;
        w.string(groupId_);
        w.int32(config_.sessionTimeoutMs);
        w.int32(config_.rebalanceTimeoutMs);
        w.string(member);
        w.stringArray(subscribed_);
        w.string(config_.groupInstanceId);

        Bytes response = coordinatorRequest(api::JoinGroup, w.bytes());
        BodyReader r(response);
        std::int32_t code = r.int32();
        if (code == errc::RebalanceInProgress) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }
        if (code == errc::UnknownMemberId && !member.empty()) {
            // The coordinator forgot this member (it left, or was evicted);
            // join afresh.
            std::lock_guard<std::mutex> lock(mu_);
            memberId_.clear();
            continue;
        }
        if (code != errc::None) throw ServerError(code, "join_group");

        std::int32_t generation = r.int32();
        std::string memberId = r.string();
        std::string leaderId = r.string();

        std::vector<AssignorMember> members;
        Assignment previous;
        for (std::int32_t count = r.int32(); count > 0; --count) {
            AssignorMember info;
            info.id = r.string();
            info.topics = r.stringArray();
            std::vector<TopicPartition> held;
            for (std::int32_t n = r.int32(); n > 0; --n) {
                TopicPartition slot;
                slot.topic = r.string();
                slot.partition = r.int32();
                held.push_back(std::move(slot));
            }
            previous[info.id] = std::move(held);
            members.push_back(std::move(info));
        }

        {
            std::lock_guard<std::mutex> lock(mu_);
            memberId_ = memberId;
            generation_ = generation;
        }

        std::vector<MemberAssignment> assignments;
        if (memberId == leaderId) {
            TopicPartitions topicPartitions;
            for (const auto& m : members) {
                for (const auto& topic : m.topics) {
                    if (topicPartitions.count(topic)) continue;
                    topicPartitions[topic] = consumer_->partitions(topic);
                }
            }
            Assignment computed;
            const auto& strategy = config_.partitionAssignmentStrategy;
            if (strategy == assignor::Range) computed = rangeAssign(members, topicPartitions);
            else if (strategy == assignor::RoundRobin) computed = roundRobinAssign(members, topicPartitions);
            else computed = stickyAssign(members, topicPartitions, previous);
            for (auto& [id, partitions] : computed) {  // std::map: sorted by member id
                assignments.push_back(MemberAssignment{id, std::move(partitions)});
            }
        }

        if (sync(assignments)) {
            std::lock_guard<std::mutex> lock(mu_);
            joined_ = true;
            return;
        }
    }
    throw Error("consumer group failed to stabilise after " + std::to_string(kJoinAttempts) +
                " join attempts");
}

bool GroupConsumer::sync(const std::vector<MemberAssignment>& assignments) {
    std::string member;
    std::int32_t generation;
    {
        std::lock_guard<std::mutex> lock(mu_);
        member = memberId_;
        generation = generation_;
    }
    BodyWriter w;
    w.string(groupId_);
    w.int32(generation);
    w.string(member);
    w.int32(static_cast<std::int32_t>(assignments.size()));
    for (const auto& a : assignments) {
        w.string(a.memberId);
        w.int32(static_cast<std::int32_t>(a.partitions.size()));
        for (const auto& slot : a.partitions) {
            w.string(slot.topic);
            w.int32(slot.partition);
        }
    }
    Bytes response = coordinatorRequest(api::SyncGroup, w.bytes());
    BodyReader r(response);
    std::int32_t code = r.int32();
    if (code == errc::RebalanceInProgress || code == errc::IllegalGeneration) return false;
    if (code == errc::UnknownMemberId) {
        // Evicted between join and sync: forget the id and join afresh.
        std::lock_guard<std::mutex> lock(mu_);
        memberId_.clear();
        return false;
    }
    if (code != errc::None) throw ServerError(code, "sync_group");
    std::vector<TopicPartition> assignment;
    for (std::int32_t count = r.int32(); count > 0; --count) {
        TopicPartition slot;
        slot.topic = r.string();
        slot.partition = r.int32();
        assignment.push_back(std::move(slot));
    }
    applyAssignment(assignment);
    return true;
}

void GroupConsumer::applyAssignment(const std::vector<TopicPartition>& assignment) {
    assignment_ = assignment;
    std::set<TopicPartition> owned(assignment.begin(), assignment.end());
    for (auto it = positions_.begin(); it != positions_.end();) {
        it = owned.count(it->first) ? std::next(it) : positions_.erase(it);
    }
    // Buffered records sit ahead of the consumed position and were never
    // delivered, so a new assignment simply drops them.
    buffered_.clear();

    std::vector<TopicPartition> needed;
    for (const auto& slot : assignment) {
        if (!positions_.count(slot)) needed.push_back(slot);
    }
    if (!needed.empty()) {
        auto committedOffsets = committed(needed);
        for (const auto& slot : needed) {
            auto it = committedOffsets.find(slot);
            positions_[slot] = (it == committedOffsets.end() || it->second < 0) ? resetOffset(slot)
                                                                               : it->second;
        }
    }
    fetchPositions_ = positions_;
}

void GroupConsumer::leave() {
    std::string member;
    {
        std::lock_guard<std::mutex> lock(mu_);
        member = memberId_;
    }
    if (member.empty()) return;
    BodyWriter w;
    w.string(groupId_);
    w.string(member);
    Bytes response = coordinatorRequest(api::LeaveGroup, w.bytes());
    BodyReader r(response);
    std::int32_t code = r.int32();
    {
        std::lock_guard<std::mutex> lock(mu_);
        joined_ = false;
        memberId_.clear();
        generation_ = -1;
    }
    if (code != errc::None) throw ServerError(code, "leave_group");
}

void GroupConsumer::heartbeatLoop() {
    // This loop enforces two independent deadlines, so it wakes often
    // enough for the shorter of them.
    int interval = std::max(1, std::min(static_cast<int>(config_.sessionTimeoutMs) / 3,
                                        config_.maxPollIntervalMs / 3));
    bool leftForSlowPoll = false;
    std::unique_lock<std::mutex> lock(mu_);
    for (;;) {
        wake_.wait_for(lock, std::chrono::milliseconds(interval));
        if (closed_) return;
        if (!joined_ || memberId_.empty()) continue;

        std::int64_t idleMs = inPoll_ ? 0 : nowMillis() - lastPollMs_;
        if (idleMs >= config_.maxPollIntervalMs) {
            // The application has stopped consuming though the process is
            // alive. Heartbeating on would assert a liveness this member no
            // longer has, holding its partitions from one that could progress.
            if (!leftForSlowPoll) {
                leftForSlowPoll = true;
                lock.unlock();
                try {
                    leave();
                } catch (const Error&) {
                }
                lock.lock();
                joined_ = false;
            }
            continue;
        }
        leftForSlowPoll = false;

        BodyWriter w;
        w.string(groupId_);
        w.int32(generation_);
        w.string(memberId_);
        lock.unlock();
        std::int32_t code = errc::None;
        try {
            code = peekErrorCode(coordinatorRequest(api::Heartbeat, w.bytes()));
        } catch (const Error&) {
            // transient: retry next tick
        }
        lock.lock();
        if (code == errc::RebalanceInProgress || code == errc::UnknownMemberId ||
            code == errc::IllegalGeneration) {
            joined_ = false;
        }
    }
}

// ---------------------------------------------------------------------------
// Coordinator routing
// ---------------------------------------------------------------------------

std::int32_t GroupConsumer::coordinatorPartition() {
    auto partitions = consumer_->partitions(kOffsetsTopic);
    return static_cast<std::int32_t>(crc32c(groupId_) % partitions.size());
}

Bytes GroupConsumer::coordinatorRequest(std::int16_t apiKey, const Bytes& body) {
    for (int attempt = 0; attempt < kCoordinatorAttempts; ++attempt) {
        auto conn = consumer_->router().connFor(kOffsetsTopic, coordinatorPartition());
        Bytes response = conn->request(apiKey, body);
        std::int32_t code = peekErrorCode(response);
        if (code == errc::CoordinatorLoadInProgress) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }
        if (code == errc::NotCoordinator || code == errc::NotLeaderOrFollower) {
            consumer_->router().refresh(kOffsetsTopic);
            continue;
        }
        return response;
    }
    throw Error("group coordinator unavailable after " + std::to_string(kCoordinatorAttempts) +
                " attempts");
}

// ---------------------------------------------------------------------------
// Assignors
// ---------------------------------------------------------------------------

namespace {

bool subscribes(const AssignorMember& m, const std::string& topic) {
    return std::find(m.topics.begin(), m.topics.end(), topic) != m.topics.end();
}

Assignment emptyAssignment(const std::vector<AssignorMember>& members) {
    Assignment out;
    for (const auto& m : members) out[m.id];
    return out;
}

}  // namespace

Assignment rangeAssign(const std::vector<AssignorMember>& members, const TopicPartitions& topics) {
    Assignment assignment = emptyAssignment(members);
    for (const auto& [topic, partitions] : topics) {  // sorted by topic
        std::vector<std::string> subscribers;
        for (const auto& m : members) {
            if (subscribes(m, topic)) subscribers.push_back(m.id);
        }
        std::sort(subscribers.begin(), subscribers.end());
        if (subscribers.empty()) continue;
        std::size_t base = partitions.size() / subscribers.size();
        std::size_t extra = partitions.size() % subscribers.size();
        std::size_t cursor = 0;
        for (std::size_t index = 0; index < subscribers.size(); ++index) {
            std::size_t count = base + (index < extra ? 1 : 0);
            for (std::size_t i = cursor; i < cursor + count; ++i) {
                assignment[subscribers[index]].push_back(TopicPartition{topic, partitions[i]});
            }
            cursor += count;
        }
    }
    return assignment;
}

Assignment roundRobinAssign(const std::vector<AssignorMember>& members,
                            const TopicPartitions& topics) {
    Assignment assignment = emptyAssignment(members);
    std::vector<AssignorMember> circle = members;
    std::sort(circle.begin(), circle.end(),
              [](const AssignorMember& a, const AssignorMember& b) { return a.id < b.id; });
    if (circle.empty()) return assignment;
    std::size_t cursor = 0;
    for (const auto& [topic, partitions] : topics) {
        for (std::int32_t partition : partitions) {
            std::size_t start = cursor;
            for (;;) {
                const auto& member = circle[cursor % circle.size()];
                ++cursor;
                if (subscribes(member, topic)) {
                    assignment[member.id].push_back(TopicPartition{topic, partition});
                    break;
                }
                if (cursor - start >= circle.size()) break;  // nobody subscribes
            }
        }
    }
    return assignment;
}

Assignment stickyAssign(const std::vector<AssignorMember>& members, const TopicPartitions& topics,
                        const Assignment& previous) {
    Assignment assignment = emptyAssignment(members);
    if (members.empty()) return assignment;

    auto memberSubscribes = [&](const std::string& id, const std::string& topic) {
        for (const auto& m : members) {
            if (m.id == id) return subscribes(m, topic);
        }
        return false;
    };

    std::vector<TopicPartition> unassigned;
    std::map<TopicPartition, std::string> claimed;
    for (const auto& [topic, partitions] : topics) {
        for (std::int32_t partition : partitions) {
            TopicPartition slot{topic, partition};
            std::string holder;
            for (const auto& [id, held] : previous) {  // sorted by member id
                if (std::find(held.begin(), held.end(), slot) != held.end() &&
                    memberSubscribes(id, topic)) {
                    holder = id;
                    break;
                }
            }
            if (holder.empty()) unassigned.push_back(slot);
            else claimed[slot] = holder;
        }
    }

    std::vector<std::string> eligible;
    for (const auto& m : members) {
        for (const auto& topic : m.topics) {
            if (topics.count(topic)) {
                eligible.push_back(m.id);
                break;
            }
        }
    }
    std::sort(eligible.begin(), eligible.end());
    if (eligible.empty()) return assignment;

    std::size_t total = 0;
    for (const auto& [topic, partitions] : topics) total += partitions.size();
    std::size_t base = total / eligible.size();
    std::size_t extra = total % eligible.size();
    std::map<std::string, std::size_t> quota;
    for (std::size_t i = 0; i < eligible.size(); ++i) quota[eligible[i]] = base + (i < extra ? 1 : 0);

    Assignment kept;
    for (const auto& [slot, id] : claimed) {  // sorted by slot
        if (kept[id].size() < quota[id]) kept[id].push_back(slot);
        else unassigned.push_back(slot);
    }
    for (auto& [id, held] : kept) {
        if (assignment.count(id)) assignment[id] = held;
    }

    std::sort(unassigned.begin(), unassigned.end());
    for (const auto& slot : unassigned) {
        std::string taker;
        for (const auto& id : eligible) {
            if (memberSubscribes(id, slot.topic) && assignment[id].size() < quota[id]) {
                taker = id;
                break;
            }
        }
        if (taker.empty()) {
            // Quotas exhausted (possible with uneven subscriptions): an
            // unassigned partition is a stalled one, so fall back to any
            // subscribed member rather than dropping it.
            for (const auto& id : eligible) {
                if (memberSubscribes(id, slot.topic)) {
                    taker = id;
                    break;
                }
            }
        }
        if (!taker.empty()) assignment[taker].push_back(slot);
    }
    for (auto& [id, slots] : assignment) std::sort(slots.begin(), slots.end());
    return assignment;
}

}  // namespace brahmaputra
