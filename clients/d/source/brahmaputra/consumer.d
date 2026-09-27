/// Partition consumer: fetch, list offsets, high watermark.
module brahmaputra.consumer;

import brahmaputra.connection;
import brahmaputra.protocol;

import core.time : Duration, seconds;
import std.format : format;

/// One record delivered to the application.
struct ConsumedRecord
{
    string topic;
    int partition;
    long offset;
    /// Null when the record has no key; empty-but-non-null when its key is empty.
    const(ubyte)[] key;
    /// Null for a tombstone; empty-but-non-null for an empty value.
    const(ubyte)[] value;
    /// Absolute unix milliseconds, already resolved against the batch base.
    long timestamp;
    const(RecordHeader)[] headers;

    /// The first value stored under `name`, or null.
    const(ubyte)[] header(string name) const
    {
        foreach (ref h; headers)
            if (h.key == name)
                return h.value;
        return null;
    }
}

/// Consumer settings, named as Kafka names them.
struct ConsumerConfig
{
    string clientId = "brahmaputra-d";
    /// `fetch.max.bytes`: caps one response.
    int fetchMaxBytes = 8 * 1024 * 1024;
    /// `fetch.min.bytes`: return early once this many bytes are ready.
    int fetchMinBytes = 1;
    /// `fetch.max.wait.ms`: the long-poll ceiling when caught up.
    int fetchMaxWaitMs = 500;
    /// `client.rack`: this consumer's failure domain, empty for none.
    string rack;
    /// `isolation.level`: READ_UNCOMMITTED (default) or READ_COMMITTED.
    int isolationLevel = READ_UNCOMMITTED;
    /// `max.poll.records`: records a group poll returns at most.
    int maxPollRecords = 500;
    /// Time allowed to open a TCP connection.
    Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    /// `request.timeout.ms` on the socket: bound on one round trip.
    Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;
}

/// What `Consumer.fetchVerbose` returns.
struct FetchResult
{
    ConsumedRecord[] records;
    long highWatermark;
}

/// Reads one partition at a time, with no group coordination.
final class Consumer
{
    private ConsumerConfig config;
    private Router router_;

    this(string address, ConsumerConfig config = ConsumerConfig.init)
    {
        this.config = config;
        this.router_ = new Router(address, config.clientId, config.connectTimeout,
            config.requestTimeout);
    }

    void close()
    {
        router_.close();
    }

    /// The routing layer, for callers that need metadata.
    @property Router router()
    {
        return router_;
    }

    int[] partitions(string topic)
    {
        return router_.partitions(topic);
    }

    /// Resolves `EARLIEST`, `LATEST` or a unix-ms timestamp to an offset.
    long listOffsets(string topic, int partition, long timestamp)
    {
        auto w = BodyWriter.start();
        w.str(topic);
        w.i32(partition);
        w.i64(timestamp);
        auto conn = router_.connFor(topic, partition);
        auto r = BodyReader(conn.request(ApiKey.listOffsets, w.data));
        r.str(); // topic
        r.i32(); // partition
        const code = r.i32();
        const offset = r.i64();
        r.i64(); // timestamp
        if (code != ErrorCode.none)
            throw new ServerException(code, format("list_offsets %s-%d", topic, partition));
        return offset;
    }

    /// Reads from one partition starting at `offset`.
    ConsumedRecord[] fetch(string topic, int partition, long offset, int maxWaitMs)
    {
        return fetchVerbose(topic, partition, offset, maxWaitMs).records;
    }

    /// Also returns the partition's high watermark.
    FetchResult fetchVerbose(string topic, int partition, long offset, int maxWaitMs)
    {
        if (maxWaitMs > config.fetchMaxWaitMs)
            maxWaitMs = config.fetchMaxWaitMs;
        auto w = BodyWriter.start();
        w.str(topic);
        w.i32(partition);
        w.i64(offset);
        w.i32(config.fetchMaxBytes);
        w.i32(maxWaitMs);
        w.i32(config.fetchMinBytes);
        w.i32(config.isolationLevel);
        w.str(config.rack);
        const body_ = w.data;

        auto response = fetchOnce(router_.connFor(topic, partition), body_);
        if (response.code == ErrorCode.notLeaderOrFollower)
        {
            router_.refresh(topic);
            response = fetchOnce(router_.connFor(topic, partition), body_);
        }
        if (response.code != ErrorCode.none)
            throw new ServerException(response.code, format("fetch %s-%d", topic, partition));

        FetchResult result;
        result.highWatermark = response.highWatermark;
        foreach (ref batch; response.batches)
        {
            foreach (index, ref record; batch.records)
            {
                const recordOffset = batch.baseOffset + cast(long) index;
                // A batch can start before the requested offset.
                if (recordOffset < offset)
                    continue;
                result.records ~= ConsumedRecord(topic, partition, recordOffset, record.key,
                    record.value, batch.maxTimestamp + record.timestampDelta, record.headers);
            }
        }
        return result;
    }

    private static struct RawFetch
    {
        int code;
        long highWatermark;
        DecodedBatch[] batches;
    }

    private RawFetch fetchOnce(Connection conn, const(ubyte)[] body_)
    {
        auto r = BodyReader(conn.request(ApiKey.fetch, body_));
        RawFetch out_;
        r.str(); // topic
        r.i32(); // partition
        out_.code = r.i32();
        out_.highWatermark = r.i64();
        r.i64(); // last_stable_offset
        const batchesLength = r.i64();
        // Read though unused: the batches trail the whole struct.
        r.i32(); // preferred_read_replica
        const trailing = r.rest();
        if (batchesLength < 0 || batchesLength > cast(long) trailing.length)
            throw new ProtocolException("fetch response claims more batch bytes than it carries");
        const raw = trailing[0 .. cast(size_t) batchesLength];
        size_t pos = 0;
        while (pos < raw.length)
            out_.batches ~= decodeRecordBatch(raw, pos);
        return out_;
    }
}
