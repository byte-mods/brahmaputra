/// Batching producer with a background linger thread.
module brahmaputra.producer;

import brahmaputra.connection;
import brahmaputra.protocol;

import core.sync.condition : Condition;
import core.sync.mutex : Mutex;
import core.thread : Thread;
import core.time : Duration, MonoTime, msecs;
import std.format : format;

/// Producer settings, named as Kafka names them. Where a default differs
/// from Kafka's it is called out.
struct ProducerConfig
{
    string clientId = "brahmaputra-d";
    /// `acks`: 0 fire-and-forget, 1 leader append, -1 ("all") every in-sync replica.
    int acks = 1;
    /// `batch.size`: flush a partition buffer once it holds this many bytes.
    size_t batchSize = 16 * 1024;
    /// `linger.ms`: flush every non-empty buffer at least this often; 0 sends
    /// each record immediately. Kafka defaults to 0; this defaults to 5.
    int lingerMs = 5;
    /// `compression.type`: none, gzip (built in), or lz4/zstd/snappy once
    /// registered with `registerCodec`.
    string compressionType = "none";
    /// `request.timeout.ms`: the broker-side wait for acknowledgements.
    int requestTimeoutMs = 30_000;
    /// `retries` of a send the broker refused before appending.
    int retries = 5;
    /// `retry.backoff.ms` between retries.
    int retryBackoffMs = 100;
    /// `delivery.timeout.ms` caps the whole send, first attempt to last retry.
    int deliveryTimeoutMs = 120_000;
    /// `buffer.memory`: unflushed record bytes held client-side.
    size_t bufferMemory = 32 * 1024 * 1024;
    /// `max.block.ms`: how long a send may block on a full buffer.
    int maxBlockMs = 60_000;
    /// Time allowed to open a TCP connection.
    Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    /// Socket round-trip bound (default 120 s); a connection that exceeds it
    /// is closed and redialled on next use.
    Duration socketTimeout = DEFAULT_REQUEST_TIMEOUT;
}

/// A background (linger) flush failed; those records are lost. Reported by
/// the next `flush` or `close`.
class DeliveryException : BrahmaputraException
{
    this(string msg, Throwable cause, string file = __FILE__, size_t line = __LINE__) @safe pure nothrow
    {
        super(msg, cause, file, line);
    }
}

private struct Buffered
{
    Record record;
    long createdMs;
}

/**
 * Batches records per partition and sends each batch as one Produce
 * request. Thread-safe: share one across threads rather than creating one
 * per message, because the batching is the point.
 */
final class Producer
{
    private ProducerConfig config;
    private Compression codec;
    private Router router_;

    private Mutex mu;
    private Condition spaceFreed; // buffer.memory released
    private Condition stopSignal; // wakes the linger thread on close
    private Buffered[][TopicPartition] buffers;
    private size_t[TopicPartition] sizes;
    private size_t bufferedBytes;
    private size_t roundRobin;
    private bool closed;
    private bool lingerDone;
    private Thread lingerThread;
    // Serialises sends per partition, held across the round trip and any
    // retries: a partition has at most one batch in flight, and batches
    // leave in the order they were taken, so a linger flush and a
    // batch-full flush can never reorder the log.
    private Mutex[TopicPartition] sendLocks;
    // The first failure of a linger-driven flush. Those records already
    // left the buffer, so this is the only trace of them.
    private Exception backgroundError;

    this(string address, ProducerConfig config = ProducerConfig.init)
    {
        this.config = config;
        this.codec = parseCompression(config.compressionType);
        this.router_ = new Router(address, config.clientId, config.connectTimeout,
            config.socketTimeout);
        this.mu = new Mutex;
        this.spaceFreed = new Condition(mu);
        this.stopSignal = new Condition(mu);
        if (config.lingerMs > 0)
        {
            lingerThread = new Thread(&lingerLoop);
            lingerThread.isDaemon = true;
            lingerThread.start();
        }
    }

    /// The routing layer, for callers that need metadata.
    @property Router router()
    {
        return router_;
    }

    /**
     * Flushes, stops the linger thread and releases connections. The thread
     * and connections are released even when the final flush fails; that
     * failure is still thrown.
     */
    void close()
    {
        Exception flushError;
        try
            flush();
        catch (Exception e)
            flushError = e;
        {
            mu.lock();
            scope (exit)
                mu.unlock();
            closed = true;
            stopSignal.notifyAll();
            spaceFreed.notifyAll();
        }
        if (lingerThread !is null)
        {
            // Bounded: a thread stuck in a round trip must not hang close.
            const until = MonoTime.currTime + Duration.zero + 2000.msecs;
            while (MonoTime.currTime < until)
            {
                mu.lock();
                const done = lingerDone;
                mu.unlock();
                if (done)
                    break;
                Thread.sleep(5.msecs);
            }
        }
        router_.close();
        if (flushError !is null)
            throw flushError;
    }

    /**
     * Buffers one record, partitioned by murmur2 of its key (round-robin when
     * the key is null). Call `flush` to await delivery. Pass a null `value`
     * for a tombstone.
     */
    void send(string topic, const(ubyte)[] value, const(ubyte)[] key = null,
        const(RecordHeader)[] headers = null)
    {
        sendTo(topic, choosePartition(topic, key), value, key, headers);
    }

    /// Buffers one record on an explicit partition, bypassing the partitioner.
    void sendTo(string topic, int partition, const(ubyte)[] value,
        const(ubyte)[] key = null, const(RecordHeader)[] headers = null)
    {
        const record = Record(key, value, 0, headers);
        size_t size = value.length + key.length + 16;
        foreach (ref h; headers)
            size += h.key.length + h.value.length + 4;
        reserve(size);

        const slot = TopicPartition(topic, partition);
        bool full;
        {
            mu.lock();
            scope (exit)
                mu.unlock();
            buffers[slot] ~= Buffered(record, nowMillis());
            sizes[slot] = sizes.get(slot, 0) + size;
            full = sizes[slot] >= config.batchSize;
        }
        if (config.lingerMs == 0 || full)
            flushPartition(slot);
    }

    /// Sends one record on its own and returns its offset. A full round
    /// trip per record — correct, and slow.
    long sendSync(string topic, const(ubyte)[] value, const(ubyte)[] key = null,
        const(RecordHeader)[] headers = null)
    {
        const partition = choosePartition(topic, key);
        return produce(topic, partition, [Buffered(Record(key, value, 0, headers), nowMillis())]);
    }

    /**
     * Sends every buffered record and waits for acknowledgement. Also throws
     * a `DeliveryException` for any background flush that failed since the
     * last call, because those records are gone and nothing else would say so.
     */
    void flush()
    {
        Exception first;
        try
            flushAll();
        catch (Exception e)
            first = e;
        Exception background;
        {
            mu.lock();
            scope (exit)
                mu.unlock();
            background = backgroundError;
            backgroundError = null;
        }
        if (first !is null)
            throw first;
        if (background !is null)
            throw new DeliveryException("background flush failed: " ~ background.msg, background);
    }

    private void flushAll()
    {
        TopicPartition[] slots;
        {
            mu.lock();
            scope (exit)
                mu.unlock();
            foreach (slot, records; buffers)
                if (records.length > 0)
                    slots ~= slot;
        }
        foreach (slot; slots)
            flushPartition(slot);
    }

    private int choosePartition(string topic, const(ubyte)[] key)
    {
        auto partitions = router_.partitions(topic);
        if (key !is null)
            return partitionForKey(key, partitions);
        mu.lock();
        scope (exit)
            mu.unlock();
        return partitions[roundRobin++ % partitions.length];
    }

    // Blocks until `size` more bytes may be buffered: what makes
    // buffer.memory real rather than letting a producer outrun its broker
    // without limit.
    private void reserve(size_t size)
    {
        const limit = config.bufferMemory;
        mu.lock();
        scope (exit)
            mu.unlock();
        if (limit == 0 || size >= limit)
        {
            // Larger than the whole budget: admitted rather than waiting on a
            // condition that can never hold.
            bufferedBytes += size;
            return;
        }
        const deadline = MonoTime.currTime + config.maxBlockMs.msecs;
        while (bufferedBytes + size > limit)
        {
            const remaining = deadline - MonoTime.currTime;
            if (remaining <= Duration.zero || closed)
                throw new BufferFullException(format(
                        "producer buffer full: %d of %d bytes unflushed after max.block.ms=%d",
                        bufferedBytes, limit, config.maxBlockMs));
            spaceFreed.wait(remaining);
        }
        bufferedBytes += size;
    }

    private void release(size_t size)
    {
        mu.lock();
        scope (exit)
            mu.unlock();
        bufferedBytes = size > bufferedBytes ? 0 : bufferedBytes - size;
        spaceFreed.notifyAll();
    }

    private void lingerLoop()
    {
        scope (exit)
        {
            mu.lock();
            lingerDone = true;
            mu.unlock();
        }
        const interval = config.lingerMs.msecs;
        auto next = MonoTime.currTime + interval;
        while (true)
        {
            {
                mu.lock();
                scope (exit)
                    mu.unlock();
                while (!closed)
                {
                    const remaining = next - MonoTime.currTime;
                    if (remaining <= Duration.zero)
                        break;
                    stopSignal.wait(remaining);
                }
                if (closed)
                    return;
            }
            // A failed background flush must not kill the thread; the next
            // explicit flush surfaces it to a caller who can act on it.
            try
                flushAll();
            catch (Exception e)
            {
                mu.lock();
                scope (exit)
                    mu.unlock();
                if (backgroundError is null)
                    backgroundError = e;
            }
            next = MonoTime.currTime + interval;
        }
    }

    private void flushPartition(TopicPartition slot)
    {
        Mutex lock;
        {
            mu.lock();
            scope (exit)
                mu.unlock();
            if (auto existing = slot in sendLocks)
                lock = *existing;
            else
            {
                lock = new Mutex;
                sendLocks[slot] = lock;
            }
        }
        lock.lock();
        scope (exit)
            lock.unlock();

        Buffered[] batch;
        size_t size;
        {
            mu.lock();
            scope (exit)
                mu.unlock();
            batch = buffers.get(slot, null);
            if (batch.length == 0)
                return;
            buffers.remove(slot);
            size = sizes.get(slot, 0);
            sizes.remove(slot);
        }
        release(size);
        produce(slot.topic, slot.partition, batch);
    }

    private long produce(string topic, int partition, Buffered[] batch)
    {
        if (batch.length == 0)
            return -1;
        // One base timestamp and a delta per record: maxTimestamp is the
        // newest record's time.
        long maxTimestamp = batch[0].createdMs;
        foreach (ref item; batch[1 .. $])
            if (item.createdMs > maxTimestamp)
                maxTimestamp = item.createdMs;
        auto records = new Record[batch.length];
        foreach (i, ref item; batch)
        {
            records[i] = item.record;
            records[i].timestampDelta = item.createdMs - maxTimestamp;
        }

        const encoded = encodeRecordBatch(records, maxTimestamp, codec);
        auto w = BodyWriter.start();
        w.str(topic);
        w.i32(partition);
        w.i32(config.acks);
        w.i32(config.requestTimeoutMs);
        w.i64(cast(long) encoded.length);
        w.raw(encoded);
        const body_ = w.data;

        if (config.acks == 0)
        {
            router_.connFor(topic, partition).sendOneway(ApiKey.produce, body_);
            return -1;
        }

        const deadline = MonoTime.currTime + config.deliveryTimeoutMs.msecs;
        int attemptsLeft = config.retries;
        while (true)
        {
            auto conn = router_.connFor(topic, partition);
            auto r = BodyReader(conn.request(ApiKey.produce, body_));
            r.str(); // topic
            r.i32(); // partition
            const code = r.i32();
            const baseOffset = r.i64();
            r.i64(); // log_append_time_ms
            if (code == ErrorCode.none)
                return baseOffset;
            if (!isRetriable(code) || attemptsLeft <= 0 || MonoTime.currTime > deadline)
                throw new ServerException(code, format("produce to %s-%d", topic, partition));
            attemptsLeft--;
            if (code == ErrorCode.notLeaderOrFollower || code == ErrorCode.fencedLeaderEpoch
                    || code == ErrorCode.unknownLeaderEpoch)
            {
                // A stale route is the usual cause; resending to the same
                // broker would repeat it.
                try
                    router_.refresh(topic);
                catch (Exception)
                {
                }
            }
            Thread.sleep(config.retryBackoffMs.msecs);
        }
    }
}
