// Consumer groups: join/sync/heartbeat, assignment, offsets.
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "brahmaputra/client.hpp"

namespace brahmaputra {

/// The internal topic whose partition leaders coordinate consumer groups.
inline constexpr const char* kOffsetsTopic = "__consumer_offsets";

/// auto.offset.reset values.
namespace offset_reset {
/// Start from the oldest retained record. Reprocesses; never skips.
inline constexpr const char* Earliest = "earliest";
/// Start from the end. Skips what was missed; never reprocesses.
inline constexpr const char* Latest = "latest";
/// Refuse to guess: poll() throws NoOffsetForPartition.
inline constexpr const char* None = "none";
}  // namespace offset_reset

/// partition.assignment.strategy values.
namespace assignor {
inline constexpr const char* Range = "range";
inline constexpr const char* RoundRobin = "roundrobin";
/// Keeps members on the partitions they already hold; prefer it when
/// consumers carry per-partition state.
inline constexpr const char* Sticky = "sticky";
}  // namespace assignor

struct TopicPartition {
    std::string topic;
    std::int32_t partition = 0;

    bool operator==(const TopicPartition& o) const {
        return partition == o.partition && topic == o.topic;
    }
    bool operator!=(const TopicPartition& o) const { return !(*this == o); }
    bool operator<(const TopicPartition& o) const {
        return topic != o.topic ? topic < o.topic : partition < o.partition;
    }
};

/// Named as Kafka names its consumer-group settings.
struct GroupConfig {
    std::string clientId = "brahmaputra-cpp";
    /// session.timeout.ms: the coordinator evicts a member that stops
    /// heartbeating for this long. Kafka defaults to 45s; this to 10s.
    std::int32_t sessionTimeoutMs = 10'000;
    /// heartbeat.interval.ms: how often the background thread heartbeats.
    /// <= 0 means session.timeout.ms / 3. Must be below session.timeout.ms.
    int heartbeatIntervalMs = 0;
    /// rebalance.timeout.ms: how long the coordinator waits for rejoins.
    std::int32_t rebalanceTimeoutMs = 3'000;
    /// max.poll.interval.ms: the longest gap between poll() calls before this
    /// member is presumed stuck and leaves the group. Heartbeats prove the
    /// process is alive; this proves the application is still consuming.
    int maxPollIntervalMs = 300'000;
    /// enable.auto.commit
    bool enableAutoCommit = true;
    /// auto.commit.interval.ms; <= 0 also disables auto-commit.
    int autoCommitIntervalMs = 5'000;
    /// auto.offset.reset: earliest, latest or none.
    std::string autoOffsetReset = offset_reset::Earliest;
    /// partition.assignment.strategy: range, roundrobin or sticky.
    std::string partitionAssignmentStrategy = assignor::Range;
    /// group.instance.id: a stable identity across restarts (KIP-345), so a
    /// rolling restart does not rebalance twice per instance. Empty means a
    /// dynamic member.
    std::string groupInstanceId;
    /// max.poll.records
    int maxPollRecords = 500;
    std::int32_t fetchMaxBytes = 8 * 1024 * 1024;
    std::int32_t fetchMinBytes = 1;
    std::int32_t fetchMaxWaitMs = 500;
    int requestTimeoutMs = 30'000;
    int connectTimeoutMs = 30'000;

    static GroupConfig fromProperties(const Properties& props);
};

/// Shares a set of topics' partitions with the rest of its group.
///
/// Like Kafka's consumer, poll()/commit()/close() are meant to be called
/// from one thread; a background thread only heartbeats and enforces
/// max.poll.interval.ms.
class GroupConsumer {
public:
    GroupConsumer(const std::string& bootstrap, std::string groupId, GroupConfig config = {});
    ~GroupConsumer();
    GroupConsumer(const GroupConsumer&) = delete;
    GroupConsumer& operator=(const GroupConsumer&) = delete;

    /// Sets the topics this member wants a share of; takes effect on the
    /// next poll (which rejoins).
    void subscribe(const std::vector<std::string>& topics);

    /// Returns up to max.poll.records records, joining the group if needed.
    /// Returns an empty vector if nothing arrives within `timeout`.
    std::vector<ConsumedRecord> poll(std::chrono::milliseconds timeout);

    /// Commits the delivered positions. At-least-once: call it after
    /// processing, not before. Throws ServerError(ILLEGAL_GENERATION) if the
    /// group has moved on (generation fencing) — the next poll rejoins.
    void commit();

    /// The group's committed offsets; an empty list asks for every
    /// partition the group has committed.
    std::map<TopicPartition, std::int64_t> committed(const std::vector<TopicPartition>& partitions = {});

    /// The partitions this member currently holds.
    std::vector<TopicPartition> assignment() const { return assignment_; }
    std::string memberId() const;
    std::int32_t generation() const;

    /// Commits, sends LeaveGroup so partitions move at once rather than after
    /// session.timeout.ms, and stops the heartbeat thread.
    void close();

    Consumer& consumer() { return *consumer_; }

private:
    struct MemberAssignment {
        std::string memberId;
        std::vector<TopicPartition> partitions;
    };

    void join();
    bool sync(const std::vector<MemberAssignment>& assignments);
    void applyAssignment(const std::vector<TopicPartition>& assignment);
    void leave();
    std::int64_t resetOffset(const TopicPartition& slot);
    void maybeAutoCommit();
    std::vector<ConsumedRecord> takeBuffered();
    std::int32_t coordinatorPartition();
    Bytes coordinatorRequest(std::int16_t apiKey, const Bytes& body);
    void heartbeatLoop();
    bool needsRejoin() const;
    void markRejoin();

    std::string groupId_;
    GroupConfig config_;
    std::unique_ptr<Consumer> consumer_;

    // Owned by the polling thread.
    std::vector<std::string> subscribed_;
    std::vector<TopicPartition> assignment_;
    // positions_: next offset to *deliver* — what gets committed. It only
    // advances over records handed to the caller.
    std::map<TopicPartition, std::int64_t> positions_;
    // fetchPositions_: next offset to *fetch*; runs ahead of positions_ by
    // exactly the records sitting in buffered_.
    std::map<TopicPartition, std::int64_t> fetchPositions_;
    std::vector<ConsumedRecord> buffered_;
    std::int64_t lastCommitMs_;

    // Shared with the heartbeat thread.
    mutable std::mutex mu_;
    std::condition_variable wake_;
    std::string memberId_;
    std::int32_t generation_ = -1;
    bool joined_ = false;
    bool closed_ = false;
    std::int64_t lastPollMs_;
    bool inPoll_ = false;
    std::thread heartbeat_;
};

// ---------------------------------------------------------------------------
// Assignors (exposed so they can be tested and reused)
// ---------------------------------------------------------------------------

struct AssignorMember {
    std::string id;
    std::vector<std::string> topics;
};

using Assignment = std::map<std::string, std::vector<TopicPartition>>;
using TopicPartitions = std::map<std::string, std::vector<std::int32_t>>;

/// Each subscribed member gets a contiguous range per topic; the first
/// (partitions % members) members take one extra.
Assignment rangeAssign(const std::vector<AssignorMember>& members, const TopicPartitions& topics);
/// Deals every partition around the circle of members sorted by id.
Assignment roundRobinAssign(const std::vector<AssignorMember>& members,
                            const TopicPartitions& topics);
/// Keeps members on what they hold and moves only what balance requires.
/// Mirrors the Rust implementation exactly, because a leader running a
/// different algorithm from its predecessor would reshuffle the group.
Assignment stickyAssign(const std::vector<AssignorMember>& members, const TopicPartitions& topics,
                        const Assignment& previous);

}  // namespace brahmaputra
