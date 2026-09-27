package io.brahmaputra;

import static io.brahmaputra.Protocol.ApiKey;
import static io.brahmaputra.Protocol.BrahmaputraException;
import static io.brahmaputra.Protocol.ErrorCode;
import static io.brahmaputra.Protocol.NoOffsetForPartitionException;
import static io.brahmaputra.Protocol.Reader;
import static io.brahmaputra.Protocol.ServerException;
import static io.brahmaputra.Protocol.Writer;

import io.brahmaputra.Client.ConsumedRecord;
import io.brahmaputra.Client.Consumer;
import io.brahmaputra.Client.ConsumerConfig;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * A consumer that shares a topic's partitions with the rest of its group.
 *
 * <p>The coordinator for a group is the leader of {@code __consumer_offsets} partition
 * {@code crc32c(group_id) % partitions}, so every group request goes to that broker and nowhere
 * else.
 *
 * <p>Single-threaded by design, matching Kafka's consumer: use one per thread and give each its
 * own client id.
 */
public final class GroupConsumer implements AutoCloseable {

    /** Internal topic whose partition leaders coordinate consumer groups. */
    public static final String OFFSETS_TOPIC = "__consumer_offsets";

    /** Retries per coordinator request after a coordinator move or load. */
    private static final int COORDINATOR_ATTEMPTS = 4;
    /** Join+sync rounds before giving up on a group that will not settle. */
    private static final int JOIN_ATTEMPTS = 4;

    /**
     * Where to start when a partition has no valid position — either the group never committed
     * one, or the committed one has fallen off the front of the log because retention deleted it.
     * Both are the same situation to a consumer, so they take one policy.
     */
    public enum AutoOffsetReset {
        /** Oldest record still retained. Reprocesses history; never silently skips. */
        EARLIEST,
        /** The end. Skips whatever was missed; never reprocesses. */
        LATEST,
        /**
         * Refuse to guess and throw {@link NoOffsetForPartitionException} — the honest answer
         * when neither reprocessing nor skipping is safe.
         */
        NONE
    }

    /** Partition assignment strategies. */
    public enum Assignor {
        RANGE,
        ROUNDROBIN,
        /**
         * Keeps members on the partitions they already hold. Prefer this when consumers carry
         * per-partition state, because every partition that moves throws that state away.
         */
        STICKY
    }

    /** Group settings, named as Kafka names them. */
    public static final class GroupConfig {
        public String clientId = "brahmaputra-java";
        /**
         * The coordinator evicts a member that stops heartbeating for this long. Kafka defaults
         * to 45s; this defaults to 10s as the Rust client does.
         */
        public int sessionTimeoutMs = 10_000;
        public int rebalanceTimeoutMs = 3_000;
        /**
         * Longest gap between polls before this member is presumed stuck and leaves. Separate
         * from the session timeout on purpose: heartbeats prove the process is alive, this
         * proves the application is still consuming.
         */
        public int maxPollIntervalMs = 300_000;
        /** 0 disables auto-commit. */
        public int autoCommitIntervalMs = 5_000;
        public AutoOffsetReset autoOffsetReset = AutoOffsetReset.EARLIEST;
        public Assignor assignor = Assignor.RANGE;
        /**
         * Stable identity across restarts (KIP-345), so a rolling restart does not rebalance
         * twice per instance. Empty means a dynamic member.
         */
        public String groupInstanceId = "";
        public int maxPollRecords = 500;
        public int fetchMaxBytes = 8 * 1024 * 1024;
        public int dialTimeoutMs = 30_000;
    }

    /** A topic-partition pair. */
    public static final class TopicPartition implements Comparable<TopicPartition> {
        public final String topic;
        public final int partition;

        public TopicPartition(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof TopicPartition)) {
                return false;
            }
            TopicPartition slot = (TopicPartition) other;
            return partition == slot.partition && topic.equals(slot.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition);
        }

        @Override
        public int compareTo(TopicPartition other) {
            int byTopic = topic.compareTo(other.topic);
            return byTopic != 0 ? byTopic : Integer.compare(partition, other.partition);
        }

        @Override
        public String toString() {
            return topic + "-" + partition;
        }
    }

    private final String groupId;
    private final GroupConfig config;
    private final Consumer consumer;

    private List<String> subscribed = new ArrayList<>();
    // Read by the heartbeat thread, written by the polling one.
    private volatile String memberId = "";
    private volatile int generation = -1;
    private volatile boolean joined;
    private List<TopicPartition> assignment = new ArrayList<>();

    /** Next offset to <i>deliver</i> — what gets committed. */
    private final Map<TopicPartition, Long> positions = new TreeMap<>();
    /** Next offset to <i>fetch</i>; runs ahead of {@link #positions} by the buffer. */
    private Map<TopicPartition, Long> fetchPositions = new HashMap<>();
    private final List<ConsumedRecord> buffered = new ArrayList<>();

    private volatile long lastPollMs = Client.nowMs();
    /**
     * True while {@link #poll} runs. {@code maxPollIntervalMs} bounds the gap <i>between</i>
     * polls — time the application spends processing — so a poll that is itself busy joining a
     * slow rebalance must not count against it.
     */
    private volatile boolean inPoll;
    /** Guards compare-and-clear of {@link #joined} against a concurrent (re)join. */
    private final Object membership = new Object();
    private long lastCommitMs = Client.nowMs();
    private volatile boolean closed;
    private final Thread heartbeat;

    public GroupConsumer(String host, int port, String groupId, GroupConfig config) {
        this.groupId = groupId;
        this.config = config;

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.clientId = config.clientId;
        consumerConfig.fetchMaxBytes = config.fetchMaxBytes;
        consumerConfig.maxPollRecords = config.maxPollRecords;
        consumerConfig.dialTimeoutMs = config.dialTimeoutMs;
        this.consumer = new Consumer(host, port, consumerConfig);

        this.heartbeat = new Thread(this::heartbeatLoop, "brahmaputra-heartbeat");
        this.heartbeat.setDaemon(true);
        this.heartbeat.start();
    }

    public void subscribe(List<String> topics) {
        this.subscribed = new ArrayList<>(topics);
        this.joined = false;
    }

    /**
     * Commit, leave the group, then stop.
     *
     * <p>Leaving is what separates a clean shutdown from a crash. Without it the coordinator
     * cannot tell the difference and must wait out {@code sessionTimeoutMs} before reassigning,
     * so a rolling restart of N instances costs N session timeouts of stalled partitions.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // Wakes the heartbeat thread from its sleep so it sees `closed` now rather than a
        // third of a session timeout from now.
        heartbeat.interrupt();
        try {
            if (joined) {
                commit();
            }
        } catch (BrahmaputraException ignored) {
            // A failed final commit shows up as the next consumer resuming from an older
            // position, not as a crash on the shutdown path.
        }
        try {
            if (!memberId.isEmpty()) {
                leave();
            }
        } catch (BrahmaputraException ignored) {
            // Best effort: failing here costs only the session timeout it was avoiding.
        }
        try {
            heartbeat.join(2000);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
        consumer.close();
    }

    /** The partitions this member currently owns; empty before the first poll joins. */
    public List<TopicPartition> assignment() {
        return Collections.unmodifiableList(new ArrayList<>(assignment));
    }

    /** This member's id as the coordinator assigned it; empty before the first join. */
    public String memberId() {
        return memberId;
    }

    /** The group generation this member last joined. */
    public int generation() {
        return generation;
    }

    /** Returns up to {@code maxPollRecords} records, joining the group if needed. */
    public List<ConsumedRecord> poll(long timeoutMs) {
        if (closed) {
            throw new BrahmaputraException("consumer is closed");
        }
        if (subscribed.isEmpty()) {
            throw new BrahmaputraException("subscribe to at least one topic before polling");
        }
        // Stamped on entry and again on return, and not enforced in between: the interval
        // bounds how long the *application* may go without asking for records, and a poll that
        // blocks — for its timeout, or on a slow rebalance — is the consumer working normally.
        lastPollMs = Client.nowMs();
        inPoll = true;
        try {
            return pollInside(timeoutMs);
        } finally {
            lastPollMs = Client.nowMs();
            inPoll = false;
        }
    }

    private List<ConsumedRecord> pollInside(long timeoutMs) {
        long deadline = Client.nowMs() + timeoutMs;
        while (true) {
            // Checked every sweep, not only on entry: a rebalance the heartbeat learns of
            // mid-poll must stop this member fetching partitions it may no longer own, rather
            // than carrying on until the timeout.
            if (!joined) {
                join();
            }
            if (!buffered.isEmpty()) {
                return takeBuffered();
            }
            if (assignment.isEmpty()) {
                if (Client.nowMs() >= deadline) {
                    return Collections.emptyList();
                }
                sleep(50);
                continue;
            }

            boolean gotAny = false;
            for (TopicPartition slot : new ArrayList<>(assignment)) {
                long remaining = Math.max(0, deadline - Client.nowMs());
                long offset = fetchPositions.getOrDefault(slot, 0L);
                List<ConsumedRecord> records;
                try {
                    records = consumer.fetch(slot.topic, slot.partition, offset,
                            (int) Math.min(remaining, 500));
                } catch (ServerException error) {
                    if (error.code == ErrorCode.OFFSET_OUT_OF_RANGE) {
                        // The committed offset fell off the log; restart where the policy says.
                        long reset = resetOffset(slot.topic, slot.partition);
                        fetchPositions.put(slot, reset);
                        positions.put(slot, reset);
                        buffered.removeIf(record -> record.topic.equals(slot.topic)
                                && record.partition == slot.partition);
                        continue;
                    }
                    if (error.code == ErrorCode.NOT_LEADER_OR_FOLLOWER) {
                        consumer.router().refresh(slot.topic);
                        continue;
                    }
                    throw error;
                }
                if (!records.isEmpty()) {
                    gotAny = true;
                    fetchPositions.put(slot, records.get(records.size() - 1).offset + 1);
                    buffered.addAll(records);
                }
            }

            maybeAutoCommit();
            if (!buffered.isEmpty()) {
                return takeBuffered();
            }
            if (!gotAny && Client.nowMs() >= deadline) {
                return Collections.emptyList();
            }
        }
    }

    private List<ConsumedRecord> takeBuffered() {
        int limit = config.maxPollRecords <= 0
                ? buffered.size()
                : Math.min(config.maxPollRecords, buffered.size());
        List<ConsumedRecord> delivered = new ArrayList<>(buffered.subList(0, limit));
        buffered.subList(0, limit).clear();
        for (ConsumedRecord record : delivered) {
            // The consumed position advances only over records actually handed to the caller;
            // committing what was merely fetched would silently skip records nobody processed.
            positions.put(new TopicPartition(record.topic, record.partition), record.offset + 1);
        }
        return delivered;
    }

    /** Commit delivered positions. At-least-once: call after processing, not before. */
    public void commit() {
        if (positions.isEmpty()) {
            return;
        }
        Writer writer = Writer.body()
                .string(groupId)
                .int32(generation)
                .string(memberId)
                .int32(positions.size());
        for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
            writer.string(entry.getKey().topic)
                    .int32(entry.getKey().partition)
                    .int64(entry.getValue());
        }
        Reader reader = Reader.body(coordinatorRequest(ApiKey.OFFSET_COMMIT, writer.bytes()));
        int code = reader.int32();
        if (code != ErrorCode.NONE) {
            throw new ServerException(code, "offset_commit");
        }
        lastCommitMs = Client.nowMs();
    }

    /** Read committed offsets. An empty list asks for every partition the group holds. */
    public Map<TopicPartition, Long> committed(List<TopicPartition> partitions) {
        Writer writer = Writer.body().string(groupId).int32(partitions.size());
        for (TopicPartition slot : partitions) {
            writer.string(slot.topic).int32(slot.partition);
        }
        Reader reader = Reader.body(coordinatorRequest(ApiKey.OFFSET_FETCH, writer.bytes()));
        int code = reader.int32();
        if (code != ErrorCode.NONE) {
            throw new ServerException(code, "offset_fetch");
        }
        Map<TopicPartition, Long> out = new LinkedHashMap<>();
        for (int count = reader.int32(); count > 0; count--) {
            String topic = reader.string();
            int partition = reader.int32();
            out.put(new TopicPartition(topic, partition), reader.int64());
        }
        return out;
    }

    private void maybeAutoCommit() {
        if (config.autoCommitIntervalMs <= 0 || positions.isEmpty()) {
            return;
        }
        if (Client.nowMs() - lastCommitMs < config.autoCommitIntervalMs) {
            return;
        }
        try {
            commit();
        } catch (BrahmaputraException ignored) {
            // An auto-commit that fails is retried on the next poll; the explicit commit is
            // what a caller relies on.
        }
    }

    private long resetOffset(String topic, int partition) {
        switch (config.autoOffsetReset) {
            case EARLIEST:
                return consumer.listOffsets(topic, partition, Client.EARLIEST);
            case LATEST:
                return consumer.listOffsets(topic, partition, Client.LATEST);
            case NONE:
            default:
                throw new NoOffsetForPartitionException(
                        "no committed offset for " + topic + "-" + partition);
        }
    }

    // -----------------------------------------------------------------------
    // Membership
    // -----------------------------------------------------------------------

    private static final class MemberInfo {
        final String id;
        final List<String> topics;
        final List<TopicPartition> held;

        MemberInfo(String id, List<String> topics, List<TopicPartition> held) {
            this.id = id;
            this.topics = topics;
            this.held = held;
        }
    }

    private void join() {
        for (int attempt = 0; attempt < JOIN_ATTEMPTS; attempt++) {
            Writer writer = Writer.body()
                    .string(groupId)
                    .int32(config.sessionTimeoutMs)
                    .int32(config.rebalanceTimeoutMs)
                    .string(memberId)
                    .stringArray(subscribed)
                    .string(config.groupInstanceId);

            Reader reader = Reader.body(coordinatorRequest(ApiKey.JOIN_GROUP, writer.bytes()));
            int code = reader.int32();
            if (code == ErrorCode.REBALANCE_IN_PROGRESS) {
                sleep(100);
                continue;
            }
            if (code == ErrorCode.UNKNOWN_MEMBER_ID) {
                // The coordinator dropped this member (session expiry, or removed while it
                // waited): join again as a new one.
                memberId = "";
                continue;
            }
            if (code != ErrorCode.NONE) {
                throw new ServerException(code, "join_group");
            }

            int newGeneration = reader.int32();
            String newMemberId = reader.string();
            String leaderId = reader.string();
            List<MemberInfo> members = new ArrayList<>();
            for (int count = reader.int32(); count > 0; count--) {
                String id = reader.string();
                List<String> topics = reader.stringArray();
                List<TopicPartition> held = new ArrayList<>();
                for (int hcount = reader.int32(); hcount > 0; hcount--) {
                    held.add(new TopicPartition(reader.string(), reader.int32()));
                }
                members.add(new MemberInfo(id, topics, held));
            }

            synchronized (membership) {
                memberId = newMemberId;
                generation = newGeneration;
            }

            Map<String, List<TopicPartition>> assignments = memberId.equals(leaderId)
                    ? computeAssignment(members)
                    : Collections.emptyMap();
            if (sync(assignments)) {
                joined = true;
                return;
            }
        }
        throw new BrahmaputraException(
                "consumer group failed to stabilise after " + JOIN_ATTEMPTS + " join attempts");
    }

    private boolean sync(Map<String, List<TopicPartition>> assignments) {
        Writer writer = Writer.body()
                .string(groupId)
                .int32(generation)
                .string(memberId)
                .int32(assignments.size());
        for (Map.Entry<String, List<TopicPartition>> entry : assignments.entrySet()) {
            writer.string(entry.getKey()).int32(entry.getValue().size());
            for (TopicPartition slot : entry.getValue()) {
                writer.string(slot.topic).int32(slot.partition);
            }
        }

        Reader reader = Reader.body(coordinatorRequest(ApiKey.SYNC_GROUP, writer.bytes()));
        int code = reader.int32();
        if (code == ErrorCode.REBALANCE_IN_PROGRESS || code == ErrorCode.ILLEGAL_GENERATION) {
            return false;
        }
        if (code == ErrorCode.UNKNOWN_MEMBER_ID) {
            memberId = "";
            return false;
        }
        if (code != ErrorCode.NONE) {
            throw new ServerException(code, "sync_group");
        }
        List<TopicPartition> mine = new ArrayList<>();
        for (int count = reader.int32(); count > 0; count--) {
            mine.add(new TopicPartition(reader.string(), reader.int32()));
        }
        applyAssignment(mine);
        return true;
    }

    private void applyAssignment(List<TopicPartition> mine) {
        assignment = mine;
        Set<TopicPartition> owned = new HashSet<>(mine);
        positions.keySet().retainAll(owned);
        // Buffered records sit ahead of the consumed position and were never delivered, so a
        // new assignment simply drops them.
        buffered.clear();

        List<TopicPartition> needed = new ArrayList<>();
        for (TopicPartition slot : mine) {
            if (!positions.containsKey(slot)) {
                needed.add(slot);
            }
        }
        if (!needed.isEmpty()) {
            Map<TopicPartition, Long> known = committed(needed);
            for (TopicPartition slot : needed) {
                Long offset = known.get(slot);
                if (offset == null || offset < 0) {
                    offset = resetOffset(slot.topic, slot.partition);
                }
                positions.put(slot, offset);
            }
        }
        fetchPositions = new HashMap<>(positions);
    }

    private void leave() {
        Writer writer = Writer.body().string(groupId).string(memberId);
        Reader reader = Reader.body(coordinatorRequest(ApiKey.LEAVE_GROUP, writer.bytes()));
        int code = reader.int32();
        if (code != ErrorCode.NONE) {
            throw new ServerException(code, "leave_group");
        }
        joined = false;
    }

    private void heartbeatLoop() {
        // This loop enforces two independent deadlines, so it has to wake often enough for the
        // shorter of them. Deriving the tick from the session timeout alone would leave a long
        // session with a short poll interval unchecked until long after it stalled.
        int heartbeatEvery = Math.max(config.sessionTimeoutMs / 3, 1);
        int pollCheckEvery = Math.max(config.maxPollIntervalMs / 3, 1);
        int interval = Math.min(heartbeatEvery, pollCheckEvery);
        boolean leftForSlowPoll = false;

        while (!closed) {
            sleep(interval);
            if (closed) {
                return;
            }
            String currentMember;
            int currentGeneration;
            synchronized (membership) {
                currentMember = memberId;
                currentGeneration = generation;
            }
            if (!joined || currentMember.isEmpty()) {
                continue;
            }

            long idleMs = Client.nowMs() - lastPollMs;
            if (!inPoll && idleMs >= config.maxPollIntervalMs) {
                // The application has stopped consuming even though the process is alive.
                // Continuing to heartbeat would assert a liveness this member no longer has,
                // holding its partitions away from a consumer that could make progress.
                if (!leftForSlowPoll) {
                    try {
                        leave();
                    } catch (BrahmaputraException ignored) {
                        // The session timeout is the fallback.
                    }
                    leftForSlowPoll = true;
                    joined = false;
                }
                continue;
            }
            leftForSlowPoll = false;

            try {
                Writer writer = Writer.body()
                        .string(groupId)
                        .int32(currentGeneration)
                        .string(currentMember);
                Reader reader = Reader.body(coordinatorRequest(ApiKey.HEARTBEAT, writer.bytes()));
                int code = reader.int32();
                if (code == ErrorCode.REBALANCE_IN_PROGRESS
                        || code == ErrorCode.UNKNOWN_MEMBER_ID
                        || code == ErrorCode.ILLEGAL_GENERATION) {
                    // Only if nothing has changed since the snapshot: a heartbeat for an old
                    // generation answering after the member already rejoined must not send it
                    // round again.
                    synchronized (membership) {
                        if (generation == currentGeneration && memberId.equals(currentMember)) {
                            joined = false;
                        }
                    }
                }
            } catch (BrahmaputraException ignored) {
                // Transient: retry on the next tick.
            }
        }
    }

    // -----------------------------------------------------------------------
    // Coordinator routing
    // -----------------------------------------------------------------------

    private int coordinatorPartition() {
        List<Integer> partitions = consumer.partitions(OFFSETS_TOPIC);
        int hash = Protocol.crc32c(groupId.getBytes(StandardCharsets.UTF_8));
        return (int) (Integer.toUnsignedLong(hash) % partitions.size());
    }

    /** Send to the group's coordinator, following moves and waiting out loads. */
    private byte[] coordinatorRequest(short apiKey, byte[] body) {
        for (int attempt = 0; attempt < COORDINATOR_ATTEMPTS; attempt++) {
            int partition = coordinatorPartition();
            byte[] response =
                    consumer.router().connectionFor(OFFSETS_TOPIC, partition).request(apiKey, body);
            int code = peekErrorCode(response);
            if (code == ErrorCode.COORDINATOR_LOAD_IN_PROGRESS) {
                sleep(100);
                continue;
            }
            if (code == ErrorCode.NOT_COORDINATOR || code == ErrorCode.NOT_LEADER_OR_FOLLOWER) {
                consumer.router().refresh(OFFSETS_TOPIC);
                continue;
            }
            return response;
        }
        throw new BrahmaputraException(
                "group coordinator unavailable after " + COORDINATOR_ATTEMPTS + " attempts");
    }

    /**
     * Read a response's leading error code without consuming the body. Every group response
     * starts with one, which is what makes a generic coordinator-retry wrapper possible at all.
     */
    private static int peekErrorCode(byte[] body) {
        try {
            return Reader.body(body).int32();
        } catch (BrahmaputraException error) {
            return ErrorCode.NONE;
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    // -----------------------------------------------------------------------
    // Assignors
    // -----------------------------------------------------------------------

    private Map<String, List<TopicPartition>> computeAssignment(List<MemberInfo> members) {
        Map<String, List<Integer>> topicPartitions = new TreeMap<>();
        for (MemberInfo member : members) {
            for (String topic : member.topics) {
                topicPartitions.computeIfAbsent(topic, consumer::partitions);
            }
        }
        Map<String, List<TopicPartition>> previous = new TreeMap<>();
        for (MemberInfo member : members) {
            previous.put(member.id, member.held);
        }

        switch (config.assignor) {
            case RANGE:
                return rangeAssign(members, topicPartitions);
            case ROUNDROBIN:
                return roundRobinAssign(members, topicPartitions);
            case STICKY:
                return stickyAssign(members, topicPartitions, previous);
            default:
                throw new BrahmaputraException("unknown assignor " + config.assignor);
        }
    }

    private static Map<String, List<TopicPartition>> empty(List<MemberInfo> members) {
        Map<String, List<TopicPartition>> out = new TreeMap<>();
        for (MemberInfo member : members) {
            out.put(member.id, new ArrayList<>());
        }
        return out;
    }

    /** Contiguous ranges per topic; the first (n % members) take one extra. */
    static Map<String, List<TopicPartition>> rangeAssign(
            List<MemberInfo> members, Map<String, List<Integer>> topicPartitions) {
        Map<String, List<TopicPartition>> assignment = empty(members);
        for (Map.Entry<String, List<Integer>> entry : topicPartitions.entrySet()) {
            String topic = entry.getKey();
            List<Integer> partitions = entry.getValue();
            List<String> subscribers = new ArrayList<>();
            for (MemberInfo member : members) {
                if (member.topics.contains(topic)) {
                    subscribers.add(member.id);
                }
            }
            Collections.sort(subscribers);
            if (subscribers.isEmpty()) {
                continue;
            }
            int base = partitions.size() / subscribers.size();
            int extra = partitions.size() % subscribers.size();
            int cursor = 0;
            for (int index = 0; index < subscribers.size(); index++) {
                int count = base + (index < extra ? 1 : 0);
                for (int partition : partitions.subList(cursor, cursor + count)) {
                    assignment.get(subscribers.get(index))
                            .add(new TopicPartition(topic, partition));
                }
                cursor += count;
            }
        }
        return assignment;
    }

    /** Deal every partition around the circle of members sorted by id. */
    static Map<String, List<TopicPartition>> roundRobinAssign(
            List<MemberInfo> members, Map<String, List<Integer>> topicPartitions) {
        Map<String, List<TopicPartition>> assignment = empty(members);
        List<MemberInfo> circle = new ArrayList<>(members);
        circle.sort((a, b) -> a.id.compareTo(b.id));
        if (circle.isEmpty()) {
            return assignment;
        }
        int cursor = 0;
        for (Map.Entry<String, List<Integer>> entry : topicPartitions.entrySet()) {
            for (int partition : entry.getValue()) {
                int start = cursor;
                while (true) {
                    MemberInfo member = circle.get(cursor % circle.size());
                    cursor++;
                    if (member.topics.contains(entry.getKey())) {
                        assignment.get(member.id)
                                .add(new TopicPartition(entry.getKey(), partition));
                        break;
                    }
                    if (cursor - start >= circle.size()) {
                        break; // nobody subscribes to this topic
                    }
                }
            }
        }
        return assignment;
    }

    /**
     * Keep members on what they hold; move only what balance requires.
     *
     * <p>Mirrors the Rust implementation exactly, because members computing the assignment
     * independently must agree — a leader running a different algorithm from its predecessor
     * would reshuffle the whole group.
     */
    static Map<String, List<TopicPartition>> stickyAssign(
            List<MemberInfo> members,
            Map<String, List<Integer>> topicPartitions,
            Map<String, List<TopicPartition>> previous) {
        Map<String, List<TopicPartition>> assignment = empty(members);
        if (members.isEmpty()) {
            return assignment;
        }

        Map<String, Set<String>> subscriptions = new HashMap<>();
        for (MemberInfo member : members) {
            subscriptions.put(member.id, new HashSet<>(member.topics));
        }

        List<TopicPartition> unassigned = new ArrayList<>();
        Map<TopicPartition, String> claimed = new TreeMap<>();
        for (Map.Entry<String, List<Integer>> entry : topicPartitions.entrySet()) {
            for (int partition : entry.getValue()) {
                TopicPartition slot = new TopicPartition(entry.getKey(), partition);
                String holder = null;
                for (Map.Entry<String, List<TopicPartition>> held : previous.entrySet()) {
                    Set<String> topics = subscriptions.get(held.getKey());
                    if (held.getValue().contains(slot)
                            && topics != null
                            && topics.contains(entry.getKey())) {
                        holder = held.getKey();
                        break;
                    }
                }
                if (holder == null) {
                    unassigned.add(slot);
                } else {
                    claimed.put(slot, holder);
                }
            }
        }

        List<String> eligible = new ArrayList<>();
        for (MemberInfo member : members) {
            for (String topic : member.topics) {
                if (topicPartitions.containsKey(topic)) {
                    eligible.add(member.id);
                    break;
                }
            }
        }
        Collections.sort(eligible);
        if (eligible.isEmpty()) {
            return assignment;
        }

        int total = 0;
        for (List<Integer> partitions : topicPartitions.values()) {
            total += partitions.size();
        }
        int base = total / eligible.size();
        int extra = total % eligible.size();
        Map<String, Integer> quota = new HashMap<>();
        for (int index = 0; index < eligible.size(); index++) {
            quota.put(eligible.get(index), base + (index < extra ? 1 : 0));
        }

        Map<String, List<TopicPartition>> kept = new HashMap<>();
        for (Map.Entry<TopicPartition, String> entry : claimed.entrySet()) {
            List<TopicPartition> held =
                    kept.computeIfAbsent(entry.getValue(), unused -> new ArrayList<>());
            if (held.size() < quota.getOrDefault(entry.getValue(), 0)) {
                held.add(entry.getKey());
            } else {
                unassigned.add(entry.getKey());
            }
        }
        for (Map.Entry<String, List<TopicPartition>> entry : kept.entrySet()) {
            if (assignment.containsKey(entry.getKey())) {
                assignment.put(entry.getKey(), entry.getValue());
            }
        }

        Collections.sort(unassigned);
        for (TopicPartition slot : unassigned) {
            String taker = null;
            for (String memberId : eligible) {
                Set<String> topics = subscriptions.get(memberId);
                if (topics != null
                        && topics.contains(slot.topic)
                        && assignment.get(memberId).size() < quota.getOrDefault(memberId, 0)) {
                    taker = memberId;
                    break;
                }
            }
            if (taker == null) {
                // Quotas exhausted (possible with uneven subscriptions): an unassigned
                // partition is a stalled partition, so fall back to any subscribed member
                // rather than dropping it.
                for (String memberId : eligible) {
                    Set<String> topics = subscriptions.get(memberId);
                    if (topics != null && topics.contains(slot.topic)) {
                        taker = memberId;
                        break;
                    }
                }
            }
            if (taker != null) {
                assignment.get(taker).add(slot);
            }
        }

        for (List<TopicPartition> held : assignment.values()) {
            Collections.sort(held);
        }
        return assignment;
    }
}
