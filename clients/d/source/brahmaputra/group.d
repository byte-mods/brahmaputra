/// Consumer groups: join/sync/heartbeat, commits, rebalancing.
module brahmaputra.group;

import brahmaputra.assignor;
import brahmaputra.connection;
import brahmaputra.consumer;
import brahmaputra.protocol;

import core.sync.condition : Condition;
import core.sync.mutex : Mutex;
import core.thread : Thread;
import core.time : Duration, MonoTime, msecs;
import std.algorithm.sorting : sort;
import std.format : format;

/// The internal topic whose partition leaders coordinate consumer groups.
enum string OFFSETS_TOPIC = "__consumer_offsets";

private enum int COORDINATOR_ATTEMPTS = 4;
private enum int JOIN_ATTEMPTS = 4;

/// `auto.offset.reset`: start from the oldest retained record.
enum string AUTO_OFFSET_RESET_EARLIEST = "earliest";
/// `auto.offset.reset`: start from the end, skipping what was missed.
enum string AUTO_OFFSET_RESET_LATEST = "latest";
/// `auto.offset.reset`: refuse to guess; poll throws
/// `NoOffsetForPartitionException`.
enum string AUTO_OFFSET_RESET_NONE = "none";

/// Consumer-group settings, named as Kafka names them.
struct GroupConfig
{
    string clientId = "brahmaputra-d";
    /// `session.timeout.ms`: the coordinator evicts a member silent this long.
    int sessionTimeoutMs = 10_000;
    /// `rebalance.timeout.ms`: how long the coordinator waits for rejoins.
    int rebalanceTimeoutMs = 3_000;
    /// `max.poll.interval.ms`: the longest gap between polls before this
    /// member is presumed stuck and leaves. Time spent inside `poll` does not
    /// count against it.
    int maxPollIntervalMs = 300_000;
    /// `auto.commit.interval.ms`; 0 disables auto-commit.
    int autoCommitIntervalMs = 5_000;
    /// `auto.offset.reset`: earliest, latest or none.
    string autoOffsetReset = AUTO_OFFSET_RESET_EARLIEST;
    /// `partition.assignment.strategy`: range, roundrobin or sticky.
    string assignor = ASSIGNOR_RANGE;
    /// `group.instance.id`: static membership (KIP-345); empty for dynamic.
    string groupInstanceId;
    /// `max.poll.records`.
    int maxPollRecords = 500;
    /// `fetch.max.bytes`.
    int fetchMaxBytes = 8 * 1024 * 1024;
    /// Time allowed to open a TCP connection.
    Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    /// Socket round-trip bound.
    Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;
}

/**
 * Shares a topic's partitions with the rest of its group.
 *
 * Single-threaded by design, as Kafka's consumer is: use one per thread.
 * A background thread heartbeats and enforces `max.poll.interval.ms`; the
 * state it shares with the caller's thread is guarded by one mutex.
 */
final class GroupConsumer
{
    private string groupId;
    private GroupConfig config;
    private Consumer consumer_;
    private string[] subscribed;

    // Shared with the heartbeat thread; every access holds mu.
    private Mutex mu;
    private Condition wake;
    private string memberId;
    private int generation = -1;
    private bool joined;
    private long lastPollMs;
    private bool inPoll;
    private bool closed;
    private bool heartbeatDone;

    // Caller-thread state only.
    private TopicPartition[] assignment;
    // Next offset to deliver: what gets committed.
    private long[TopicPartition] positions;
    // Next offset to fetch; runs ahead of positions by `buffered`.
    private long[TopicPartition] fetchPositions;
    private ConsumedRecord[] buffered;
    private long lastCommitMs;
    private Thread heartbeatThread;

    this(string address, string groupId, GroupConfig config = GroupConfig.init)
    {
        ConsumerConfig consumerConfig;
        consumerConfig.clientId = config.clientId;
        consumerConfig.fetchMaxBytes = config.fetchMaxBytes;
        consumerConfig.maxPollRecords = config.maxPollRecords;
        consumerConfig.connectTimeout = config.connectTimeout;
        consumerConfig.requestTimeout = config.requestTimeout;
        this.consumer_ = new Consumer(address, consumerConfig);
        this.groupId = groupId;
        this.config = config;
        this.mu = new Mutex;
        this.wake = new Condition(mu);
        this.lastPollMs = nowMillis();
        this.lastCommitMs = nowMillis();
        heartbeatThread = new Thread(&heartbeatLoop);
        heartbeatThread.isDaemon = true;
        heartbeatThread.start();
    }

    /// The underlying partition consumer.
    @property Consumer consumer()
    {
        return consumer_;
    }

    /// The partitions this member currently owns.
    @property const(TopicPartition)[] assigned() const
    {
        return assignment;
    }

    /// Sets the topics this member wants a share of.
    void subscribe(const(string)[] topics)
    {
        subscribed = topics.dup;
        setJoined(false);
    }

    private struct Membership
    {
        string memberId;
        int generation;
        bool joined;
    }

    private Membership membership()
    {
        mu.lock();
        scope (exit)
            mu.unlock();
        return Membership(memberId, generation, joined);
    }

    private void setJoined(bool value)
    {
        mu.lock();
        scope (exit)
            mu.unlock();
        joined = value;
    }

    private void clearMemberId()
    {
        mu.lock();
        scope (exit)
            mu.unlock();
        memberId = null;
    }

    /**
     * Commits, leaves the group, then stops. Leaving lets the coordinator
     * reassign at once instead of waiting out the session timeout.
     */
    void close()
    {
        {
            mu.lock();
            scope (exit)
                mu.unlock();
            closed = true;
            wake.notifyAll();
        }
        const m = membership();
        if (m.joined)
        {
            try
                commit();
            catch (Exception)
            {
            }
        }
        if (m.memberId.length > 0)
        {
            // Best effort: failing costs only the session timeout.
            try
                leave();
            catch (Exception)
            {
            }
        }
        const until = MonoTime.currTime + 2000.msecs;
        while (MonoTime.currTime < until)
        {
            mu.lock();
            const done = heartbeatDone;
            mu.unlock();
            if (done)
                break;
            Thread.sleep(5.msecs);
        }
        consumer_.close();
    }

    /// Returns up to `max.poll.records` records, joining the group if needed.
    ConsumedRecord[] poll(Duration timeout)
    {
        if (subscribed.length == 0)
            throw new BrahmaputraException("subscribe to at least one topic before polling");
        // Stamped on entry and on return and not enforced in between: the
        // interval bounds how long the application goes without asking for
        // records, and a poll that blocks is the consumer working normally.
        {
            mu.lock();
            scope (exit)
                mu.unlock();
            lastPollMs = nowMillis();
            inPoll = true;
        }
        scope (exit)
        {
            mu.lock();
            lastPollMs = nowMillis();
            inPoll = false;
            mu.unlock();
        }

        const deadline = MonoTime.currTime + timeout;
        while (true)
        {
            // Checked every sweep: a rebalance the heartbeat learns of
            // mid-poll must stop this member fetching partitions it may no
            // longer own.
            if (!membership().joined)
                join();
            if (buffered.length > 0)
                return takeBuffered();
            if (assignment.length == 0)
            {
                if (MonoTime.currTime > deadline)
                    return null;
                Thread.sleep(50.msecs);
                continue;
            }

            bool gotAny = false;
            foreach (slot; assignment)
            {
                auto remaining = deadline - MonoTime.currTime;
                long waitMs = remaining > Duration.zero ? remaining.total!"msecs" : 0;
                if (waitMs > 500)
                    waitMs = 500;
                ConsumedRecord[] records;
                try
                    records = consumer_.fetch(slot.topic, slot.partition,
                        fetchPositions.get(slot, 0), cast(int) waitMs);
                catch (ServerException e)
                {
                    if (e.code == ErrorCode.offsetOutOfRange)
                    {
                        // The committed offset fell off the log.
                        const reset = resetOffset(slot.topic, slot.partition);
                        fetchPositions[slot] = reset;
                        positions[slot] = reset;
                        continue;
                    }
                    if (e.code == ErrorCode.notLeaderOrFollower)
                    {
                        try
                            consumer_.router.refresh(slot.topic);
                        catch (Exception)
                        {
                        }
                        continue;
                    }
                    throw e;
                }
                if (records.length > 0)
                {
                    gotAny = true;
                    fetchPositions[slot] = records[$ - 1].offset + 1;
                    buffered ~= records;
                }
            }

            maybeAutoCommit();
            if (buffered.length > 0)
                return takeBuffered();
            if (!gotAny && MonoTime.currTime > deadline)
                return null;
        }
    }

    private ConsumedRecord[] takeBuffered()
    {
        size_t limit = config.maxPollRecords;
        if (limit == 0 || limit > buffered.length)
            limit = buffered.length;
        auto delivered = buffered[0 .. limit];
        buffered = buffered[limit .. $];
        // The consumed position advances only over records handed to the
        // caller; committing what was merely fetched would skip records
        // nobody processed.
        foreach (ref record; delivered)
            positions[TopicPartition(record.topic, record.partition)] = record.offset + 1;
        return delivered;
    }

    /// Commits the delivered positions. At-least-once: call it after
    /// processing, not before.
    void commit()
    {
        if (positions.length == 0)
            return;
        auto slots = positions.keys;
        slots.sort();
        const m = membership();
        auto w = BodyWriter.start();
        w.str(groupId);
        w.i32(m.generation);
        w.str(m.memberId);
        w.i32(cast(int) slots.length);
        foreach (slot; slots)
        {
            w.str(slot.topic);
            w.i32(slot.partition);
            w.i64(positions[slot]);
        }
        auto r = BodyReader(coordinatorRequest(ApiKey.offsetCommit, w.data));
        const code = r.i32();
        if (code != ErrorCode.none)
            throw new ServerException(code, "offset_commit");
        lastCommitMs = nowMillis();
    }

    /// The group's committed offsets. An empty list asks for every
    /// partition the group holds.
    long[TopicPartition] committed(const(TopicPartition)[] partitions = null)
    {
        auto w = BodyWriter.start();
        w.str(groupId);
        w.i32(cast(int) partitions.length);
        foreach (ref slot; partitions)
        {
            w.str(slot.topic);
            w.i32(slot.partition);
        }
        auto r = BodyReader(coordinatorRequest(ApiKey.offsetFetch, w.data));
        const code = r.i32();
        if (code != ErrorCode.none)
            throw new ServerException(code, "offset_fetch");
        long[TopicPartition] out_;
        foreach (_; 0 .. r.count())
        {
            const topic = r.str();
            const partition = r.i32();
            out_[TopicPartition(topic, partition)] = r.i64();
        }
        return out_;
    }

    private void maybeAutoCommit()
    {
        const interval = config.autoCommitIntervalMs;
        if (interval <= 0 || positions.length == 0)
            return;
        if (nowMillis() - lastCommitMs < interval)
            return;
        // A failed auto-commit is retried on the next poll; the explicit
        // commit is what a caller relies on.
        try
            commit();
        catch (Exception)
        {
        }
    }

    private long resetOffset(string topic, int partition)
    {
        switch (config.autoOffsetReset)
        {
        case AUTO_OFFSET_RESET_EARLIEST:
            return consumer_.listOffsets(topic, partition, EARLIEST);
        case AUTO_OFFSET_RESET_LATEST:
            return consumer_.listOffsets(topic, partition, LATEST);
        case AUTO_OFFSET_RESET_NONE:
            throw new NoOffsetForPartitionException(format(
                    "no committed offset for partition %s-%d", topic, partition));
        default:
            throw new BrahmaputraException("unknown auto.offset.reset " ~ config.autoOffsetReset);
        }
    }

    // -----------------------------------------------------------------------
    // Membership
    // -----------------------------------------------------------------------

    private void join()
    {
        foreach (attempt; 0 .. JOIN_ATTEMPTS)
        {
            auto w = BodyWriter.start();
            w.str(groupId);
            w.i32(config.sessionTimeoutMs);
            w.i32(config.rebalanceTimeoutMs);
            w.str(membership().memberId);
            w.strArray(subscribed);
            w.str(config.groupInstanceId);

            auto r = BodyReader(coordinatorRequest(ApiKey.joinGroup, w.data));
            const code = r.i32();
            if (code == ErrorCode.rebalanceInProgress)
            {
                Thread.sleep(100.msecs);
                continue;
            }
            if (code == ErrorCode.unknownMemberId)
            {
                // Dropped by the coordinator: join again as a new member.
                clearMemberId();
                continue;
            }
            if (code != ErrorCode.none)
                throw new ServerException(code, "join_group");

            const newGeneration = r.i32();
            const newMemberId = r.str();
            const leaderId = r.str();
            AssignorMember[] members;
            TopicPartition[][string] previous;
            foreach (_; 0 .. r.count())
            {
                AssignorMember member;
                member.id = r.str();
                member.topics = r.strArray();
                TopicPartition[] held;
                foreach (__; 0 .. r.count())
                {
                    const topic = r.str();
                    const partition = r.i32();
                    held ~= TopicPartition(topic, partition);
                }
                previous[member.id] = held;
                members ~= member;
            }

            {
                mu.lock();
                scope (exit)
                    mu.unlock();
                memberId = newMemberId;
                generation = newGeneration;
            }

            MemberAssignment[] assignments;
            if (newMemberId == leaderId)
            {
                int[][string] topicPartitions;
                foreach (ref member; members)
                    foreach (topic; member.topics)
                        if (topic !in topicPartitions)
                            topicPartitions[topic] = consumer_.partitions(topic);
                assignments = computeAssignment(config.assignor, members,
                    topicPartitions, previous);
            }

            if (sync(assignments))
            {
                setJoined(true);
                return;
            }
        }
        throw new BrahmaputraException(format(
                "consumer group failed to stabilise after %d join attempts", JOIN_ATTEMPTS));
    }

    private bool sync(MemberAssignment[] assignments)
    {
        const m = membership();
        auto w = BodyWriter.start();
        w.str(groupId);
        w.i32(m.generation);
        w.str(m.memberId);
        w.i32(cast(int) assignments.length);
        foreach (ref a; assignments)
        {
            w.str(a.memberId);
            w.i32(cast(int) a.partitions.length);
            foreach (ref slot; a.partitions)
            {
                w.str(slot.topic);
                w.i32(slot.partition);
            }
        }
        auto r = BodyReader(coordinatorRequest(ApiKey.syncGroup, w.data));
        const code = r.i32();
        if (code == ErrorCode.rebalanceInProgress || code == ErrorCode.illegalGeneration)
            return false;
        if (code == ErrorCode.unknownMemberId)
        {
            clearMemberId();
            return false;
        }
        if (code != ErrorCode.none)
            throw new ServerException(code, "sync_group");
        TopicPartition[] assigned_;
        foreach (_; 0 .. r.count())
        {
            const topic = r.str();
            const partition = r.i32();
            assigned_ ~= TopicPartition(topic, partition);
        }
        applyAssignment(assigned_);
        return true;
    }

    private void applyAssignment(TopicPartition[] newAssignment)
    {
        assignment = newAssignment;
        bool[TopicPartition] owned;
        foreach (slot; newAssignment)
            owned[slot] = true;
        foreach (slot; positions.keys)
            if (slot !in owned)
                positions.remove(slot);
        // Buffered records sit ahead of the consumed position and were never
        // delivered, so a new assignment simply drops them.
        buffered = null;

        TopicPartition[] needed;
        foreach (slot; newAssignment)
            if (slot !in positions)
                needed ~= slot;
        if (needed.length > 0)
        {
            auto committedOffsets = committed(needed);
            foreach (slot; needed)
            {
                auto found = slot in committedOffsets;
                long offset = found ? *found : -1;
                if (offset < 0)
                    offset = resetOffset(slot.topic, slot.partition);
                positions[slot] = offset;
            }
        }
        fetchPositions = null;
        foreach (slot, offset; positions)
            fetchPositions[slot] = offset;
    }

    private void leave()
    {
        auto w = BodyWriter.start();
        w.str(groupId);
        w.str(membership().memberId);
        auto r = BodyReader(coordinatorRequest(ApiKey.leaveGroup, w.data));
        const code = r.i32();
        if (code != ErrorCode.none)
            throw new ServerException(code, "leave_group");
        setJoined(false);
    }

    private void heartbeatLoop()
    {
        scope (exit)
        {
            mu.lock();
            heartbeatDone = true;
            mu.unlock();
        }
        // Two independent deadlines, so wake often enough for the shorter.
        int heartbeatEvery = config.sessionTimeoutMs / 3;
        if (heartbeatEvery < 1)
            heartbeatEvery = 1;
        int pollCheckEvery = config.maxPollIntervalMs / 3;
        if (pollCheckEvery < 1)
            pollCheckEvery = 1;
        const interval = (heartbeatEvery < pollCheckEvery ? heartbeatEvery : pollCheckEvery).msecs;
        bool leftForSlowPoll = false;

        while (true)
        {
            long idleMs;
            bool polling;
            Membership m;
            {
                mu.lock();
                scope (exit)
                    mu.unlock();
                const until = MonoTime.currTime + interval;
                while (!closed)
                {
                    const remaining = until - MonoTime.currTime;
                    if (remaining <= Duration.zero)
                        break;
                    wake.wait(remaining);
                }
                if (closed)
                    return;
                idleMs = nowMillis() - lastPollMs;
                polling = inPoll;
                m = Membership(memberId, generation, joined);
            }
            if (!m.joined || m.memberId.length == 0)
                continue;

            try
            {
                if (!polling && idleMs >= config.maxPollIntervalMs)
                {
                    // The application stopped consuming though the process
                    // lives; heartbeating on would hold its partitions away
                    // from a consumer that could make progress.
                    if (!leftForSlowPoll)
                    {
                        leftForSlowPoll = true;
                        setJoined(false);
                        leave();
                    }
                    continue;
                }
                leftForSlowPoll = false;

                auto w = BodyWriter.start();
                w.str(groupId);
                w.i32(m.generation);
                w.str(m.memberId);
                auto r = BodyReader(coordinatorRequest(ApiKey.heartbeat, w.data));
                const code = r.i32();
                if (code == ErrorCode.rebalanceInProgress || code == ErrorCode.unknownMemberId
                        || code == ErrorCode.illegalGeneration)
                {
                    // Only if nothing changed since the snapshot: a reply for
                    // an old generation must not send a rejoined member round
                    // again.
                    mu.lock();
                    scope (exit)
                        mu.unlock();
                    if (generation == m.generation && memberId == m.memberId)
                        joined = false;
                }
            }
            catch (Exception)
            {
                // Transient: retry next tick.
            }
        }
    }

    // -----------------------------------------------------------------------
    // Coordinator routing
    // -----------------------------------------------------------------------

    private int coordinatorPartition()
    {
        auto partitions = consumer_.partitions(OFFSETS_TOPIC);
        return cast(int)(crc32c(cast(const(ubyte)[]) groupId) % cast(uint) partitions.length);
    }

    // Sends to the group's coordinator, following moves and waiting out loads.
    private const(ubyte)[] coordinatorRequest(short apiKey, const(ubyte)[] body_)
    {
        foreach (attempt; 0 .. COORDINATOR_ATTEMPTS)
        {
            auto conn = consumer_.router.connFor(OFFSETS_TOPIC, coordinatorPartition());
            auto response = conn.request(apiKey, body_);
            const code = peekErrorCode(response);
            if (code == ErrorCode.coordinatorLoadInProgress)
            {
                Thread.sleep(100.msecs);
                continue;
            }
            if (code == ErrorCode.notCoordinator || code == ErrorCode.notLeaderOrFollower)
            {
                consumer_.router.refresh(OFFSETS_TOPIC);
                continue;
            }
            return response;
        }
        throw new BrahmaputraException(format(
                "group coordinator unavailable after %d attempts", COORDINATOR_ATTEMPTS));
    }
}
