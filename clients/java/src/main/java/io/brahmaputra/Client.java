package io.brahmaputra;

import static io.brahmaputra.Protocol.ApiKey;
import static io.brahmaputra.Protocol.BrahmaputraException;
import static io.brahmaputra.Protocol.Compression;
import static io.brahmaputra.Protocol.DecodedBatch;
import static io.brahmaputra.Protocol.ErrorCode;
import static io.brahmaputra.Protocol.ProtocolException;
import static io.brahmaputra.Protocol.Reader;
import static io.brahmaputra.Protocol.Record;
import static io.brahmaputra.Protocol.RecordHeader;
import static io.brahmaputra.Protocol.ServerException;
import static io.brahmaputra.Protocol.Writer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** Connection, routing, producer and simple consumer. */
public final class Client {

    private Client() {}

    /** Offset sentinel: the oldest record still retained. */
    public static final long EARLIEST = -2L;
    /** Offset sentinel: the log end. */
    public static final long LATEST = -1L;

    static long nowMs() {
        return System.currentTimeMillis();
    }

    // -----------------------------------------------------------------------
    // Connection
    // -----------------------------------------------------------------------

    /**
     * One TCP connection to one broker, multiplexed by correlation id.
     *
     * <p>The broker may answer out of order, so responses are matched by correlation id. Requests
     * are serialised on this object; that is enough for a producer that batches, and matches how
     * the Rust client behaves.
     */
    public static final class Connection implements AutoCloseable {
        private final Socket socket;
        private final DataInputStream input;
        private final OutputStream output;
        private final String clientId;
        private final AtomicInteger correlation = new AtomicInteger();

        private Connection(Socket socket, String clientId) throws IOException {
            this.socket = socket;
            this.input = new DataInputStream(socket.getInputStream());
            this.output = socket.getOutputStream();
            this.clientId = clientId;
        }

        public static Connection connect(String host, int port, String clientId, int timeoutMs) {
            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(host, port), timeoutMs);
                // Responses are small and latency matters more than packet count; without this
                // every request pays Nagle plus the peer's delayed ACK.
                socket.setTcpNoDelay(true);
                return new Connection(socket, clientId);
            } catch (IOException error) {
                throw new BrahmaputraException("connect to " + host + ":" + port + " failed", error);
            }
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Closing a socket that is already gone is not a failure worth reporting.
            }
        }

        public synchronized byte[] request(short apiKey, byte[] body) {
            int correlationId = correlation.incrementAndGet();
            try {
                output.write(Protocol.encodeFrame(apiKey, correlationId, clientId, body));
                output.flush();
                byte[] payload = readFrame();
                Protocol.FramePayload frame = Protocol.decodeFramePayload(payload);
                if (frame.correlationId != correlationId) {
                    // A response for a request we are not waiting on can only mean the stream
                    // has desynchronised; continuing would pair every later response with the
                    // wrong request.
                    throw new ProtocolException("correlation id mismatch: expected "
                            + correlationId + ", got " + frame.correlationId);
                }
                return frame.body;
            } catch (IOException error) {
                throw new BrahmaputraException("request failed", error);
            }
        }

        /** Send without awaiting a response (acks=0). */
        public synchronized void sendOneway(short apiKey, byte[] body) {
            try {
                output.write(
                        Protocol.encodeFrame(apiKey, correlation.incrementAndGet(), clientId, body));
                output.flush();
            } catch (IOException error) {
                throw new BrahmaputraException("send failed", error);
            }
        }

        private byte[] readFrame() throws IOException {
            int length = input.readInt();
            if (length < 0) {
                throw new ProtocolException("negative frame length " + length);
            }
            byte[] payload = new byte[length];
            input.readFully(payload);
            return payload;
        }

        /**
         * Bind a principal to this connection.
         *
         * <p>The password crosses the wire in the clear exactly as SASL/PLAIN does, so the broker
         * refuses this on a plaintext listener. Use TLS.
         */
        public String[] authenticate(String username, String password) {
            Writer writer = Writer.body().string(username).string(password);
            Reader reader = Reader.body(request(ApiKey.AUTHENTICATE, writer.bytes()));
            int code = reader.int32();
            String principal = reader.string();
            String role = reader.string();
            if (code != ErrorCode.NONE) {
                throw new ServerException(code, "authenticate");
            }
            return new String[] {principal, role};
        }

        /** What the broker speaks — the one call that works across a version mismatch. */
        public Map<Integer, int[]> apiVersions() {
            Writer writer = Writer.body().string("brahmaputra-java").string("0.1.0");
            Reader reader = Reader.body(request(ApiKey.API_VERSIONS, writer.bytes()));
            int code = reader.int32();
            if (code != ErrorCode.NONE) {
                throw new ServerException(code, "api_versions");
            }
            int count = reader.int32();
            Map<Integer, int[]> versions = new LinkedHashMap<>();
            for (int index = 0; index < count; index++) {
                int apiKey = reader.int32();
                versions.put(apiKey, new int[] {reader.int32(), reader.int32()});
            }
            reader.skipString(); // broker_version
            return versions;
        }
    }

    // -----------------------------------------------------------------------
    // Metadata and routing
    // -----------------------------------------------------------------------

    public static final class BrokerInfo {
        public final int nodeId;
        public final String host;
        public final int port;

        BrokerInfo(int nodeId, String host, int port) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
        }
    }

    public static final class PartitionInfo {
        public final int partition;
        public final int leader;
        public final List<Integer> replicas;
        public final List<Integer> isr;
        public final int leaderEpoch;

        PartitionInfo(int partition, int leader, List<Integer> replicas, List<Integer> isr,
                int leaderEpoch) {
            this.partition = partition;
            this.leader = leader;
            this.replicas = replicas;
            this.isr = isr;
            this.leaderEpoch = leaderEpoch;
        }
    }

    public static final class TopicInfo {
        public final String name;
        public final List<PartitionInfo> partitions;

        TopicInfo(String name, List<PartitionInfo> partitions) {
            this.name = name;
            this.partitions = partitions;
        }
    }

    public static final class ClusterMetadata {
        public final List<BrokerInfo> brokers;
        public final List<TopicInfo> topics;

        ClusterMetadata(List<BrokerInfo> brokers, List<TopicInfo> topics) {
            this.brokers = brokers;
            this.topics = topics;
        }

        public List<Integer> partitionsOf(String topic) {
            for (TopicInfo info : topics) {
                if (info.name.equals(topic)) {
                    List<Integer> out = new ArrayList<>();
                    for (PartitionInfo partition : info.partitions) {
                        out.add(partition.partition);
                    }
                    Collections.sort(out);
                    return out;
                }
            }
            return Collections.emptyList();
        }

        public int leaderOf(String topic, int partition) {
            for (TopicInfo info : topics) {
                if (!info.name.equals(topic)) {
                    continue;
                }
                for (PartitionInfo entry : info.partitions) {
                    if (entry.partition == partition) {
                        return entry.leader;
                    }
                }
            }
            return -1;
        }
    }

    static ClusterMetadata decodeMetadata(Reader reader) {
        // Field order is exactly the schema's: brokers, controller_id, topics. There is no
        // leading error code — a per-topic one lives inside each topic entry instead.
        List<BrokerInfo> brokers = new ArrayList<>();
        for (int count = reader.int32(); count > 0; count--) {
            brokers.add(new BrokerInfo(reader.int32(), reader.string(), reader.int32()));
        }
        reader.skipInt32(); // controller_id
        List<TopicInfo> topics = new ArrayList<>();
        for (int count = reader.int32(); count > 0; count--) {
            String name = reader.string();
            int topicError = reader.int32();
            List<PartitionInfo> partitions = new ArrayList<>();
            for (int pcount = reader.int32(); pcount > 0; pcount--) {
                int partition = reader.int32();
                int leader = reader.int32();
                List<Integer> replicas = new ArrayList<>();
                for (int rc = reader.int32(); rc > 0; rc--) {
                    replicas.add(reader.int32());
                }
                List<Integer> isr = new ArrayList<>();
                for (int ic = reader.int32(); ic > 0; ic--) {
                    isr.add(reader.int32());
                }
                partitions.add(new PartitionInfo(partition, leader, replicas, isr, reader.int32()));
            }
            if (topicError != ErrorCode.NONE
                    && topicError != ErrorCode.UNKNOWN_TOPIC_OR_PARTITION) {
                throw new ServerException(topicError, "metadata for " + name);
            }
            topics.add(new TopicInfo(name, partitions));
        }
        return new ClusterMetadata(brokers, topics);
    }

    /**
     * Keeps connections to every broker and routes by partition leader.
     *
     * <p>Metadata is cached and refreshed only when a request says the route was stale, because
     * refreshing per request would put the control plane on the data path.
     */
    public static final class Router implements AutoCloseable {
        private final String clientId;
        private final int timeoutMs;
        private final Connection seed;
        private final Map<Integer, Connection> connections = new HashMap<>();
        private ClusterMetadata metadata;

        private Router(String clientId, int timeoutMs, Connection seed) {
            this.clientId = clientId;
            this.timeoutMs = timeoutMs;
            this.seed = seed;
        }

        public static Router connect(String host, int port, String clientId, int timeoutMs) {
            return new Router(clientId, timeoutMs,
                    Connection.connect(host, port, clientId, timeoutMs));
        }

        public Connection seed() {
            return seed;
        }

        @Override
        public void close() {
            for (Connection connection : connections.values()) {
                if (connection != seed) {
                    connection.close();
                }
            }
            connections.clear();
            seed.close();
        }

        public synchronized ClusterMetadata metadata(List<String> topics, boolean refresh) {
            if (!refresh && metadata != null) {
                return metadata;
            }
            Writer writer = Writer.body().stringArray(topics);
            metadata = decodeMetadata(Reader.body(seed.request(ApiKey.METADATA, writer.bytes())));
            return metadata;
        }

        public ClusterMetadata refresh(String topic) {
            return metadata(Collections.singletonList(topic), true);
        }

        public List<Integer> partitions(String topic) {
            ClusterMetadata image = metadata(Collections.singletonList(topic), false);
            List<Integer> partitions = image.partitionsOf(topic);
            if (partitions.isEmpty()) {
                // A topic auto-created on first produce is not in the cached image yet; one
                // refresh distinguishes "new" from "absent".
                partitions = refresh(topic).partitionsOf(topic);
            }
            if (partitions.isEmpty()) {
                throw new BrahmaputraException("topic " + topic + " has no partitions");
            }
            return partitions;
        }

        public synchronized Connection connectionFor(String topic, int partition) {
            ClusterMetadata image = metadata(Collections.singletonList(topic), false);
            int leader = image.leaderOf(topic, partition);
            if (leader < 0) {
                image = refresh(topic);
                leader = image.leaderOf(topic, partition);
            }
            if (leader < 0) {
                throw new BrahmaputraException("no leader for " + topic + "-" + partition);
            }
            Connection existing = connections.get(leader);
            if (existing != null) {
                return existing;
            }
            for (BrokerInfo broker : image.brokers) {
                if (broker.nodeId != leader) {
                    continue;
                }
                // A single-broker cluster advertises the address it was configured with, which
                // may not be the one we dialled; reuse the seed rather than opening a second
                // connection to ourselves.
                if (image.brokers.size() == 1) {
                    connections.put(leader, seed);
                    return seed;
                }
                Connection connection =
                        Connection.connect(broker.host, broker.port, clientId, timeoutMs);
                connections.put(leader, connection);
                return connection;
            }
            throw new BrahmaputraException("broker " + leader + " is not in the metadata");
        }
    }

    // -----------------------------------------------------------------------
    // Producer
    // -----------------------------------------------------------------------

    /** Producer settings, named as Kafka names them. */
    public static final class ProducerConfig {
        public String clientId = "brahmaputra-java";
        /** 0 fire-and-forget, 1 leader append, -1 every in-sync replica. */
        public int acks = 1;
        /** Flush a partition buffer once it holds this many bytes. */
        public int batchSize = 16 * 1024;
        /**
         * Flush every non-empty buffer at least this often; 0 sends each record immediately.
         * Kafka defaults to 0; this defaults to 5 because an unbatched producer is slow enough
         * to look broken.
         */
        public int lingerMs = 5;
        /** none or gzip built in; others via {@link Protocol#registerCodec}. */
        public String compressionType = "none";
        public int requestTimeoutMs = 30_000;
        /**
         * Retries of a send the broker refused with a retriable error — one it returns before
         * appending, so a retry cannot duplicate.
         */
        public int retries = 5;
        public int retryBackoffMs = 100;
        /** Caps the whole send, first attempt through last retry. */
        public int deliveryTimeoutMs = 120_000;
        /** Caps unflushed record bytes held client-side. */
        public int bufferMemory = 32 * 1024 * 1024;
        /** How long send may block on a full buffer before failing. */
        public int maxBlockMs = 60_000;
        public int dialTimeoutMs = 30_000;
    }

    private static final class Buffered {
        final Record record;
        final long createdMs;
        final String topic;
        final int partition;

        Buffered(Record record, long createdMs, String topic, int partition) {
            this.record = record;
            this.createdMs = createdMs;
            this.topic = topic;
            this.partition = partition;
        }
    }

    private static final class Slot {
        final String topic;
        final int partition;

        Slot(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Slot)) {
                return false;
            }
            Slot slot = (Slot) other;
            return partition == slot.partition && topic.equals(slot.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition);
        }
    }

    /**
     * A batching producer. Share one instance rather than creating one per message: the batching
     * is the point.
     */
    public static final class Producer implements AutoCloseable {
        private final ProducerConfig config;
        private final Compression codec;
        private final Router router;
        private final Map<Slot, List<Buffered>> buffers = new HashMap<>();
        private final Map<Slot, Integer> sizes = new HashMap<>();
        private final Object lock = new Object();
        private long bufferedBytes;
        private int roundRobin;
        private volatile boolean closed;
        private final Thread ticker;

        public Producer(String host, int port, ProducerConfig config) {
            this.config = config;
            this.codec = Compression.parse(config.compressionType);
            this.router = Router.connect(host, port, config.clientId, config.dialTimeoutMs);
            if (config.lingerMs > 0) {
                this.ticker = new Thread(this::lingerLoop, "brahmaputra-linger");
                this.ticker.setDaemon(true);
                this.ticker.start();
            } else {
                this.ticker = null;
            }
        }

        public Router router() {
            return router;
        }

        @Override
        public void close() {
            flush();
            closed = true;
            synchronized (lock) {
                lock.notifyAll();
            }
            if (ticker != null) {
                try {
                    ticker.join(2000);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            router.close();
        }

        /** Buffer one record. Call {@link #flush} to await delivery. */
        public void send(String topic, byte[] value, byte[] key, List<RecordHeader> headers) {
            List<Integer> partitions = router.partitions(topic);
            int partition;
            if (key != null) {
                partition = Protocol.partitionForKey(key, partitions);
            } else {
                synchronized (lock) {
                    partition = partitions.get(roundRobin % partitions.size());
                    roundRobin++;
                }
            }
            sendTo(topic, partition, value, key, headers);
        }

        /** Buffer one record on an explicit partition, bypassing the partitioner. */
        public void sendTo(String topic, int partition, byte[] value, byte[] key,
                List<RecordHeader> headers) {
            List<RecordHeader> effective = headers == null ? new ArrayList<>() : headers;
            Record record = new Record(key, value, effective);
            int size = value.length + (key == null ? 0 : key.length) + 16;
            for (RecordHeader header : effective) {
                size += header.key.length() + (header.value == null ? 0 : header.value.length) + 4;
            }
            reserve(size);

            Slot slot = new Slot(topic, partition);
            boolean full;
            synchronized (lock) {
                buffers.computeIfAbsent(slot, unused -> new ArrayList<>())
                        .add(new Buffered(record, nowMs(), topic, partition));
                sizes.merge(slot, size, Integer::sum);
                full = sizes.get(slot) >= config.batchSize;
            }
            if (config.lingerMs == 0 || full) {
                flushSlot(slot);
            }
        }

        /** Send one record on its own and return its offset. Slow by design. */
        public long sendSync(String topic, byte[] value, byte[] key, List<RecordHeader> headers) {
            List<Integer> partitions = router.partitions(topic);
            int partition = key != null
                    ? Protocol.partitionForKey(key, partitions)
                    : partitions.get(roundRobin++ % partitions.size());
            Record record = new Record(key, value, headers);
            return produce(topic, partition, Collections.singletonList(
                    new Buffered(record, nowMs(), topic, partition)));
        }

        public void flush() {
            List<Slot> slots;
            synchronized (lock) {
                slots = new ArrayList<>();
                for (Map.Entry<Slot, List<Buffered>> entry : buffers.entrySet()) {
                    if (!entry.getValue().isEmpty()) {
                        slots.add(entry.getKey());
                    }
                }
            }
            for (Slot slot : slots) {
                flushSlot(slot);
            }
        }

        /**
         * Wait until {@code size} more bytes may be buffered.
         *
         * <p>This is what makes {@code bufferMemory} real: a producer faster than its broker is
         * slowed down here rather than allowed to grow without limit and die holding records
         * nobody has acknowledged.
         */
        private void reserve(int size) {
            synchronized (lock) {
                int limit = config.bufferMemory;
                if (limit <= 0 || size >= limit) {
                    // A record larger than the whole budget is admitted rather than waiting
                    // forever on a condition that can never hold; refusing oversized records is
                    // the broker's job (max.message.bytes).
                    bufferedBytes += size;
                    return;
                }
                long deadline = nowMs() + config.maxBlockMs;
                while (bufferedBytes + size > limit) {
                    long remaining = deadline - nowMs();
                    if (remaining <= 0) {
                        throw new BrahmaputraException("producer buffer full: " + bufferedBytes
                                + " of " + limit + " bytes unflushed after maxBlockMs="
                                + config.maxBlockMs);
                    }
                    try {
                        lock.wait(Math.min(remaining, 20));
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        throw new BrahmaputraException("interrupted waiting for buffer space");
                    }
                }
                bufferedBytes += size;
            }
        }

        private void release(int size) {
            synchronized (lock) {
                bufferedBytes = Math.max(0, bufferedBytes - size);
                lock.notifyAll();
            }
        }

        private void lingerLoop() {
            while (!closed) {
                try {
                    Thread.sleep(config.lingerMs);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (closed) {
                    return;
                }
                try {
                    flush();
                } catch (BrahmaputraException ignored) {
                    // A background flush that fails must not kill the ticker; the next explicit
                    // flush surfaces the error to a caller who can act on it.
                }
            }
        }

        private void flushSlot(Slot slot) {
            List<Buffered> batch;
            int size;
            synchronized (lock) {
                batch = buffers.get(slot);
                if (batch == null || batch.isEmpty()) {
                    return;
                }
                buffers.put(slot, new ArrayList<>());
                size = sizes.getOrDefault(slot, 0);
                sizes.remove(slot);
            }
            release(size);
            produce(slot.topic, slot.partition, batch);
        }

        private long produce(String topic, int partition, List<Buffered> batch) {
            if (batch.isEmpty()) {
                return -1;
            }
            // The batch stores one base timestamp and a delta per record, so the rebasing
            // happens here; maxTimestamp becomes the newest record's time, which is what makes
            // it a truthful answer to "how recent is this batch".
            long maxTimestamp = Long.MIN_VALUE;
            for (Buffered item : batch) {
                maxTimestamp = Math.max(maxTimestamp, item.createdMs);
            }
            List<Record> records = new ArrayList<>(batch.size());
            for (Buffered item : batch) {
                item.record.timestampDelta = item.createdMs - maxTimestamp;
                records.add(item.record);
            }

            byte[] encoded = Protocol.encodeRecordBatch(records, maxTimestamp, codec);
            byte[] body = Writer.body()
                    .string(topic)
                    .int32(partition)
                    .int32(config.acks)
                    .int32(config.requestTimeoutMs)
                    .int64(encoded.length)
                    .raw(encoded)
                    .bytes();

            if (config.acks == 0) {
                router.connectionFor(topic, partition).sendOneway(ApiKey.PRODUCE, body);
                return -1;
            }

            long deadline = nowMs() + config.deliveryTimeoutMs;
            int attemptsLeft = config.retries;
            while (true) {
                Connection connection = router.connectionFor(topic, partition);
                Reader reader = Reader.body(connection.request(ApiKey.PRODUCE, body));
                reader.skipString(); // topic
                reader.skipInt32();  // partition
                int code = reader.int32();
                long baseOffset = reader.int64();
                reader.skipInt64();  // log_append_time_ms
                if (code == ErrorCode.NONE) {
                    return baseOffset;
                }
                if (!Protocol.isRetriable(code) || attemptsLeft <= 0 || nowMs() >= deadline) {
                    throw new ServerException(code, "produce to " + topic + "-" + partition);
                }
                attemptsLeft--;
                if (code == ErrorCode.NOT_LEADER_OR_FOLLOWER
                        || code == ErrorCode.FENCED_LEADER_EPOCH
                        || code == ErrorCode.UNKNOWN_LEADER_EPOCH) {
                    // A stale route is the most common retriable cause, and resending to the
                    // same broker would just repeat it.
                    router.refresh(topic);
                }
                try {
                    Thread.sleep(config.retryBackoffMs);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new BrahmaputraException("interrupted while retrying a send");
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Consumer
    // -----------------------------------------------------------------------

    /** One record delivered to the application. */
    public static final class ConsumedRecord {
        public final String topic;
        public final int partition;
        public final long offset;
        public final byte[] key;
        public final byte[] value;
        /** Absolute unix milliseconds, already resolved against the batch base. */
        public final long timestamp;
        public final List<RecordHeader> headers;

        ConsumedRecord(String topic, int partition, long offset, byte[] key, byte[] value,
                long timestamp, List<RecordHeader> headers) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.headers = headers;
        }

        public byte[] header(String name) {
            for (RecordHeader header : headers) {
                if (header.key.equals(name)) {
                    return header.value;
                }
            }
            return null;
        }
    }

    /** Consumer settings, named as Kafka names them. */
    public static final class ConsumerConfig {
        public String clientId = "brahmaputra-java";
        public int fetchMaxBytes = 8 * 1024 * 1024;
        public int fetchMinBytes = 1;
        public int fetchMaxWaitMs = 500;
        public int maxPollRecords = 500;
        public int dialTimeoutMs = 30_000;
    }

    /** Reads one partition at a time, with no group coordination. */
    public static final class Consumer implements AutoCloseable {
        final ConsumerConfig config;
        final Router router;

        public Consumer(String host, int port, ConsumerConfig config) {
            this.config = config;
            this.router = Router.connect(host, port, config.clientId, config.dialTimeoutMs);
        }

        public Router router() {
            return router;
        }

        @Override
        public void close() {
            router.close();
        }

        public List<Integer> partitions(String topic) {
            return router.partitions(topic);
        }

        /** Resolve {@link #EARLIEST}, {@link #LATEST} or a unix-ms timestamp. */
        public long listOffsets(String topic, int partition, long timestamp) {
            byte[] body = Writer.body()
                    .string(topic)
                    .int32(partition)
                    .int64(timestamp)
                    .bytes();
            Reader reader = Reader.body(
                    router.connectionFor(topic, partition).request(ApiKey.LIST_OFFSETS, body));
            reader.skipString(); // topic
            reader.skipInt32();  // partition
            int code = reader.int32();
            long offset = reader.int64();
            reader.skipInt64();  // timestamp
            if (code != ErrorCode.NONE) {
                throw new ServerException(code, "list_offsets " + topic + "-" + partition);
            }
            return offset;
        }

        public List<ConsumedRecord> fetch(String topic, int partition, long offset, int maxWaitMs) {
            byte[] body = Writer.body()
                    .string(topic)
                    .int32(partition)
                    .int64(offset)
                    .int32(config.fetchMaxBytes)
                    .int32(Math.min(maxWaitMs, config.fetchMaxWaitMs))
                    .int32(config.fetchMinBytes)
                    .bytes();

            Object[] result = fetchOnce(router.connectionFor(topic, partition), body);
            int code = (Integer) result[0];
            if (code == ErrorCode.NOT_LEADER_OR_FOLLOWER) {
                router.refresh(topic);
                result = fetchOnce(router.connectionFor(topic, partition), body);
                code = (Integer) result[0];
            }
            if (code != ErrorCode.NONE) {
                throw new ServerException(code, "fetch " + topic + "-" + partition);
            }

            @SuppressWarnings("unchecked")
            List<DecodedBatch> batches = (List<DecodedBatch>) result[2];
            List<ConsumedRecord> out = new ArrayList<>();
            for (DecodedBatch batch : batches) {
                for (int index = 0; index < batch.records.size(); index++) {
                    long recordOffset = batch.baseOffset + index;
                    // A batch can start before the requested offset; skip what the caller has
                    // already seen.
                    if (recordOffset < offset) {
                        continue;
                    }
                    Record record = batch.records.get(index);
                    out.add(new ConsumedRecord(topic, partition, recordOffset, record.key,
                            record.value, record.timestamp(batch.maxTimestamp), record.headers));
                }
            }
            return out;
        }

        /** Returns {@code {errorCode, highWatermark, batches}}. */
        private Object[] fetchOnce(Connection connection, byte[] body) {
            Reader reader = Reader.body(connection.request(ApiKey.FETCH, body));
            reader.skipString(); // topic
            reader.skipInt32();  // partition
            int code = reader.int32();
            long highWatermark = reader.int64();
            reader.skipInt64();  // last_stable_offset
            long batchesLength = reader.int64();
            byte[] trailing = reader.rest();
            if (batchesLength > trailing.length) {
                throw new ProtocolException(
                        "fetch response claims more batch bytes than it carries");
            }
            byte[] raw = new byte[(int) batchesLength];
            System.arraycopy(trailing, 0, raw, 0, raw.length);

            List<DecodedBatch> batches = new ArrayList<>();
            int pos = 0;
            while (pos < raw.length) {
                Protocol.BatchAt at = Protocol.decodeRecordBatch(raw, pos);
                batches.add(at.batch);
                pos = at.next;
            }
            return new Object[] {code, highWatermark, batches};
        }
    }
}
