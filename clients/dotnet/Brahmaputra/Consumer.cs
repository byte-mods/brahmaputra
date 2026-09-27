using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Brahmaputra;

/// <summary>One record delivered to the application.</summary>
public sealed class ConsumeResult
{
    /// <summary>Topic.</summary>
    public required string Topic { get; init; }

    /// <summary>Partition.</summary>
    public int Partition { get; init; }

    /// <summary>Offset within the partition.</summary>
    public long Offset { get; init; }

    /// <summary>Key, or null.</summary>
    public byte[]? Key { get; init; }

    /// <summary>Value; null is a tombstone, distinct from an empty value.</summary>
    public byte[]? Value { get; init; }

    /// <summary>Absolute unix milliseconds, already resolved against the batch base.</summary>
    public long Timestamp { get; init; }

    /// <summary>Headers, in order.</summary>
    public IReadOnlyList<RecordHeader> Headers { get; init; } = Array.Empty<RecordHeader>();

    /// <summary>The topic and partition this record came from.</summary>
    public TopicPartition TopicPartition => new(Topic, Partition);

    /// <summary>The first value stored under a header key, or null.</summary>
    public byte[]? Header(string key)
    {
        foreach (var header in Headers)
            if (header.Key == key) return header.Value;
        return null;
    }
}

/// <summary>A fetch's records plus the partition's high watermark.</summary>
public sealed record FetchResult(IReadOnlyList<ConsumeResult> Records, long HighWatermark);

/// <summary>Consumer settings, named as Kafka names them.</summary>
public class ConsumerConfig
{
    /// <summary><c>bootstrap.servers</c>: comma-separated host:port list.</summary>
    public string BootstrapServers { get; set; } = "127.0.0.1:9092";

    /// <summary><c>client.id</c>.</summary>
    public string ClientId { get; set; } = "brahmaputra-dotnet";

    /// <summary><c>fetch.max.bytes</c>: caps a response.</summary>
    public int FetchMaxBytes { get; set; } = 8 * 1024 * 1024;

    /// <summary><c>fetch.min.bytes</c>: return early once this many bytes are ready.</summary>
    public int FetchMinBytes { get; set; } = 1;

    /// <summary><c>fetch.max.wait.ms</c>: the long-poll ceiling when caught up.</summary>
    public int FetchMaxWaitMs { get; set; } = 500;

    /// <summary>
    /// <c>max.poll.records</c>: the most records one fetch (or group poll)
    /// returns; the rest come on the next one. 0 is unlimited.
    /// </summary>
    public int MaxPollRecords { get; set; } = 500;

    /// <summary><c>client.rack</c>: this consumer's failure domain, empty for none.</summary>
    public string ClientRack { get; set; } = "";

    /// <summary><c>isolation.level</c>: <see cref="Wire.ReadUncommitted"/> or <see cref="Wire.ReadCommitted"/>.</summary>
    public int IsolationLevel { get; set; } = Wire.ReadUncommitted;

    /// <summary><c>request.timeout.ms</c>: client-side ceiling on one request.</summary>
    public int RequestTimeoutMs { get; set; } = 30_000;

    /// <summary><c>socket.connection.setup.timeout.ms</c>.</summary>
    public int ConnectTimeoutMs { get; set; } = 30_000;
}

/// <summary>Reads partitions directly, with no group coordination.</summary>
public sealed class Consumer : IDisposable
{
    private readonly ConsumerConfig _config;

    /// <summary>The routing layer, for callers that need metadata.</summary>
    public Router Router { get; }

    /// <summary>Connects to the cluster.</summary>
    public Consumer(ConsumerConfig config)
    {
        _config = config;
        Router = new Router(config.BootstrapServers, config.ClientId,
            TimeSpan.FromMilliseconds(config.ConnectTimeoutMs), TimeSpan.FromMilliseconds(config.RequestTimeoutMs));
    }

    /// <summary>Closes connections.</summary>
    public void Dispose() => Router.Dispose();

    /// <summary>A topic's partition ids.</summary>
    public Task<IReadOnlyList<int>> PartitionsAsync(string topic, CancellationToken cancellationToken = default) =>
        Router.PartitionsAsync(topic, cancellationToken);

    /// <summary>Synchronous <see cref="PartitionsAsync"/>.</summary>
    public IReadOnlyList<int> Partitions(string topic) => Router.Partitions(topic);

    /// <summary>
    /// Resolves <see cref="Wire.Earliest"/>, <see cref="Wire.Latest"/> or a
    /// unix-ms timestamp to an offset.
    /// </summary>
    public async Task<long> ListOffsetsAsync(string topic, int partition, long timestamp, CancellationToken cancellationToken = default)
    {
        var w = new BodyWriter();
        w.String(topic);
        w.Int32(partition);
        w.Int64(timestamp);
        byte[] body = w.ToArray();
        for (int attempt = 0; ; attempt++)
        {
            var conn = await Router.ConnectionForAsync(topic, partition, cancellationToken).ConfigureAwait(false);
            var r = new BodyReader(await conn.RequestAsync(ApiKey.ListOffsets, body, cancellationToken).ConfigureAwait(false));
            r.String(); // topic
            r.Int32(); // partition
            int code = r.Int32();
            long offset = r.Int64();
            r.Int64(); // timestamp
            if (code == (int)ErrorCode.NotLeaderOrFollower && attempt == 0)
            {
                await Router.RefreshAsync(topic, cancellationToken).ConfigureAwait(false);
                continue;
            }
            if (code != 0) throw new ServerException(code, $"list_offsets {topic}-{partition}");
            return offset;
        }
    }

    /// <summary>Synchronous <see cref="ListOffsetsAsync"/>.</summary>
    public long ListOffsets(string topic, int partition, long timestamp) =>
        ListOffsetsAsync(topic, partition, timestamp).GetAwaiter().GetResult();

    /// <summary>Reads from one partition starting at an offset.</summary>
    public async Task<IReadOnlyList<ConsumeResult>> FetchAsync(
        string topic, int partition, long offset, int maxWaitMs, CancellationToken cancellationToken = default) =>
        (await FetchVerboseAsync(topic, partition, offset, maxWaitMs, cancellationToken).ConfigureAwait(false)).Records;

    /// <summary>Synchronous <see cref="FetchAsync"/>.</summary>
    public IReadOnlyList<ConsumeResult> Fetch(string topic, int partition, long offset, int maxWaitMs) =>
        FetchAsync(topic, partition, offset, maxWaitMs).GetAwaiter().GetResult();

    /// <summary>Synchronous <see cref="FetchVerboseAsync"/>.</summary>
    public FetchResult FetchVerbose(string topic, int partition, long offset, int maxWaitMs) =>
        FetchVerboseAsync(topic, partition, offset, maxWaitMs).GetAwaiter().GetResult();

    /// <summary>Reads from one partition and also returns its high watermark.</summary>
    public async Task<FetchResult> FetchVerboseAsync(
        string topic, int partition, long offset, int maxWaitMs, CancellationToken cancellationToken = default)
    {
        maxWaitMs = Math.Clamp(maxWaitMs, 0, _config.FetchMaxWaitMs);
        var w = new BodyWriter();
        w.String(topic);
        w.Int32(partition);
        w.Int64(offset);
        w.Int32(_config.FetchMaxBytes);
        w.Int32(maxWaitMs);
        w.Int32(_config.FetchMinBytes);
        w.Int32(_config.IsolationLevel);
        w.String(_config.ClientRack);
        byte[] body = w.ToArray();

        var conn = await Router.ConnectionForAsync(topic, partition, cancellationToken).ConfigureAwait(false);
        var (code, highWatermark, batches) = await FetchOnceAsync(conn, body, maxWaitMs, cancellationToken).ConfigureAwait(false);
        if (code == (int)ErrorCode.NotLeaderOrFollower)
        {
            await Router.RefreshAsync(topic, cancellationToken).ConfigureAwait(false);
            conn = await Router.ConnectionForAsync(topic, partition, cancellationToken).ConfigureAwait(false);
            (code, highWatermark, batches) = await FetchOnceAsync(conn, body, maxWaitMs, cancellationToken).ConfigureAwait(false);
        }
        if (code != 0) throw new ServerException(code, $"fetch {topic}-{partition}");

        var output = new List<ConsumeResult>();
        // max.poll.records: the caller resumes from the last returned offset + 1,
        // so what is cut here is fetched again next time rather than lost.
        int cap = _config.MaxPollRecords > 0 ? _config.MaxPollRecords : int.MaxValue;
        foreach (var batch in batches)
        {
            for (int index = 0; index < batch.Records.Count && output.Count < cap; index++)
            {
                long recordOffset = batch.BaseOffset + index;
                // A batch can start before the requested offset; skip what the
                // caller has already seen.
                if (recordOffset < offset) continue;
                var record = batch.Records[index];
                output.Add(new ConsumeResult
                {
                    Topic = topic,
                    Partition = partition,
                    Offset = recordOffset,
                    Key = record.Key,
                    Value = record.Value,
                    Timestamp = batch.MaxTimestamp + record.TimestampDelta,
                    Headers = record.Headers,
                });
            }
        }
        return new FetchResult(output, highWatermark);
    }

    private static async Task<(int Code, long HighWatermark, List<DecodedBatch> Batches)> FetchOnceAsync(
        BrokerConnection conn, byte[] body, int maxWaitMs, CancellationToken cancellationToken)
    {
        byte[] response = await conn.RequestAsync(ApiKey.Fetch, body, cancellationToken, TimeSpan.FromMilliseconds(maxWaitMs))
            .ConfigureAwait(false);
        var r = new BodyReader(response);
        r.String(); // topic
        r.Int32(); // partition
        int code = r.Int32();
        long highWatermark = r.Int64();
        r.Int64(); // last_stable_offset
        long batchesLength = r.Int64();
        // Read even though unused: the batches trail the whole struct, so
        // skipping a field would take them from the wrong offset.
        r.Int32(); // preferred_read_replica
        ReadOnlyMemory<byte> trailing = r.Rest();
        if (batchesLength < 0 || batchesLength > trailing.Length)
            throw new BrahmaputraException("fetch response claims more batch bytes than it carries");
        var batches = RecordBatch.DecodeAll(trailing.Span[..(int)batchesLength]);
        return (code, highWatermark, batches);
    }
}
