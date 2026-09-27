using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Brahmaputra;

/// <summary><c>acks</c>: how much durability a send waits for.</summary>
public enum Acks
{
    /// <summary>Fire and forget; no response is awaited and no offset is known.</summary>
    None = 0,

    /// <summary>The partition leader has appended the record.</summary>
    Leader = 1,

    /// <summary>Every in-sync replica has the record.</summary>
    All = -1,
}

/// <summary>
/// Producer settings, named as Kafka names them. Where a default differs from
/// Kafka's it is called out.
/// </summary>
public class ProducerConfig
{
    /// <summary><c>bootstrap.servers</c>: comma-separated host:port list.</summary>
    public string BootstrapServers { get; set; } = "127.0.0.1:9092";

    /// <summary><c>client.id</c>.</summary>
    public string ClientId { get; set; } = "brahmaputra-dotnet";

    /// <summary><c>acks</c>.</summary>
    public Acks Acks { get; set; } = Acks.Leader;

    /// <summary><c>batch.size</c>: a partition's buffer is sent once it holds this many bytes.</summary>
    public int BatchSize { get; set; } = 16 * 1024;

    /// <summary>
    /// <c>linger.ms</c>: how long a record may wait for company before its batch
    /// is sent. 0 sends as soon as the sender is free. Kafka defaults to 0; this
    /// defaults to 5 because an unbatched producer is slow enough to look broken.
    /// </summary>
    public int LingerMs { get; set; } = 5;

    /// <summary><c>compression.type</c>. Codecs other than none and gzip must be registered with <see cref="Codecs.Register"/>.</summary>
    public CompressionType CompressionType { get; set; } = CompressionType.None;

    /// <summary><c>request.timeout.ms</c>: the broker-side wait for acknowledgements.</summary>
    public int RequestTimeoutMs { get; set; } = 30_000;

    /// <summary><c>retries</c> of a send the broker refused with a retriable error, one it returns before appending.</summary>
    public int Retries { get; set; } = 5;

    /// <summary><c>retry.backoff.ms</c>.</summary>
    public int RetryBackoffMs { get; set; } = 100;

    /// <summary><c>delivery.timeout.ms</c>: caps a record's whole life, from Send through the last retry.</summary>
    public int DeliveryTimeoutMs { get; set; } = 120_000;

    /// <summary><c>buffer.memory</c>: caps unacknowledged record bytes held client-side.</summary>
    public long BufferMemory { get; set; } = 32L * 1024 * 1024;

    /// <summary><c>max.block.ms</c>: how long Send may block on a full buffer before failing.</summary>
    public int MaxBlockMs { get; set; } = 60_000;

    /// <summary><c>socket.connection.setup.timeout.ms</c>.</summary>
    public int ConnectTimeoutMs { get; set; } = 30_000;
}

/// <summary>A record to send.</summary>
public sealed class ProducerRecord
{
    /// <summary>Creates a record.</summary>
    public ProducerRecord(string topic, byte[]? value, byte[]? key = null)
    {
        Topic = topic;
        Value = value;
        Key = key;
    }

    /// <summary>Destination topic.</summary>
    public string Topic { get; }

    /// <summary>Explicit partition; null lets the partitioner choose.</summary>
    public int? Partition { get; init; }

    /// <summary>Key, or null. A key pins every record sharing it to one partition.</summary>
    public byte[]? Key { get; }

    /// <summary>Value. Null is a tombstone, distinct from an empty value.</summary>
    public byte[]? Value { get; }

    /// <summary>Headers, in order; values may be null.</summary>
    public IReadOnlyList<RecordHeader>? Headers { get; init; }

    /// <summary>Unix-ms timestamp; null stamps the time of the Send call.</summary>
    public long? Timestamp { get; init; }
}

/// <summary>Where a record landed.</summary>
/// <param name="Topic">Topic.</param>
/// <param name="Partition">Partition.</param>
/// <param name="Offset">Offset, or -1 with acks=0.</param>
/// <param name="Timestamp">The record's timestamp (or the broker's append time, if it uses one).</param>
public sealed record RecordMetadata(string Topic, int Partition, long Offset, long Timestamp);

/// <summary>
/// Batches records per partition and sends each batch as one Produce request.
/// Thread-safe: share one across threads rather than creating one per message,
/// because the batching is the point.
/// </summary>
public sealed class Producer : IDisposable, IAsyncDisposable
{
    private sealed class Pending
    {
        public required BatchRecord Record;
        public required long TimestampMs;
        public required long CreatedMs;
        public required int Size;
        // True when the caller awaits this record's delivery itself
        // (ProduceAsync), so a failure is already reported to someone.
        public bool Observed;
        public readonly TaskCompletionSource<RecordMetadata> Completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class PartitionQueue
    {
        public readonly List<Pending> Items = new();
        public int Bytes;
        public List<Pending>? InFlight;
    }

    private readonly ProducerConfig _config;
    private readonly object _lock = new();
    private readonly Dictionary<TopicPartition, PartitionQueue> _queues = new();
    private readonly SemaphoreSlim _wake = new(0);
    private readonly Task _sender;
    private long _bufferedBytes;
    private TaskCompletionSource _spaceFreed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _flushers;
    private int _roundRobin;
    private bool _closing;
    private bool _disposed;
    // The first delivery failure of a record nobody awaited, reported by the
    // next Flush or Close so a failed background send is never silent.
    private Exception? _backgroundError;

    /// <summary>The routing layer, for callers that need metadata.</summary>
    public Router Router { get; }

    /// <summary>Connects and starts the background sender.</summary>
    public Producer(ProducerConfig config)
    {
        if (config.BatchSize <= 0) throw new ArgumentException("batch.size must be positive");
        _config = config;
        Router = new Router(config.BootstrapServers, config.ClientId,
            TimeSpan.FromMilliseconds(config.ConnectTimeoutMs), TimeSpan.FromMilliseconds(config.RequestTimeoutMs));
        _sender = Task.Run(SenderLoopAsync);
    }

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// <summary>
    /// Buffers one record and returns a task that completes when it is
    /// acknowledged. Blocks for up to max.block.ms while the buffer is full and
    /// then throws <see cref="BufferFullException"/>.
    /// </summary>
    public Task<RecordMetadata> Send(ProducerRecord record) => EnqueueAsync(record, CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>Buffers a keyed (or round-robin, if key is null) record.</summary>
    public Task<RecordMetadata> Send(string topic, byte[]? value, byte[]? key = null, params RecordHeader[] headers) =>
        Send(new ProducerRecord(topic, value, key) { Headers = headers });

    /// <summary>Buffers a record on an explicit partition, bypassing the partitioner.</summary>
    public Task<RecordMetadata> SendTo(string topic, int partition, byte[]? value, byte[]? key = null, params RecordHeader[] headers) =>
        Send(new ProducerRecord(topic, value, key) { Partition = partition, Headers = headers });

    /// <summary>
    /// Buffers one record, waiting asynchronously for buffer space, and returns
    /// the delivery task without awaiting it.
    /// </summary>
    public async Task<Task<RecordMetadata>> SendAsync(ProducerRecord record, CancellationToken cancellationToken = default) =>
        await EnqueueAsync(record, cancellationToken).ConfigureAwait(false);

    /// <summary>Sends one record and waits until it is acknowledged.</summary>
    public async Task<RecordMetadata> ProduceAsync(ProducerRecord record, CancellationToken cancellationToken = default)
    {
        Task<RecordMetadata> delivery = await EnqueueAsync(record, cancellationToken, observed: true).ConfigureAwait(false);
        return await delivery.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Synchronous <see cref="ProduceAsync"/>: one full round trip, correct and slow.</summary>
    public RecordMetadata Produce(ProducerRecord record) => ProduceAsync(record).GetAwaiter().GetResult();

    /// <summary>
    /// Sends every buffered record now and waits for all of them, throwing the
    /// first delivery failure.
    /// </summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        List<Task<RecordMetadata>> tasks;
        lock (_lock)
        {
            _flushers++;
            tasks = _queues.Values
                .SelectMany(q => (q.InFlight ?? Enumerable.Empty<Pending>()).Concat(q.Items))
                .Select(p => p.Completion.Task)
                .ToList();
        }
        _wake.Release();
        try
        {
            try
            {
                await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                // Reported below, as the first failure itself rather than an aggregate.
            }
            Exception? background;
            lock (_lock)
            {
                background = _backgroundError;
                _backgroundError = null;
            }
            var failed = tasks.FirstOrDefault(t => t.IsFaulted);
            if (failed != null) throw failed.Exception!.InnerException!;
            if (background != null) throw background;
        }
        finally
        {
            lock (_lock) _flushers--;
        }
    }

    /// <summary>Synchronous <see cref="FlushAsync"/>.</summary>
    public void Flush() => FlushAsync().GetAwaiter().GetResult();

    /// <summary>
    /// Flushes, stops the sender and closes connections, then throws the first
    /// delivery failure not yet reported, if any. Always releases resources.
    /// </summary>
    public async Task CloseAsync()
    {
        Exception? failure = null;
        try { await FlushAsync().ConfigureAwait(false); }
        catch (Exception e) { failure = e; }
        await DisposeAsync().ConfigureAwait(false);
        if (failure != null) throw failure;
    }

    /// <summary>Synchronous <see cref="CloseAsync"/>.</summary>
    public void Close() => CloseAsync().GetAwaiter().GetResult();

    /// <summary>
    /// Flushes, stops the sender and closes connections. Delivery failures are
    /// left on the records' tasks; use <see cref="CloseAsync"/> to have them thrown.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            _closing = true;
        }
        _wake.Release();
        // The sender drains every queue before it exits, so awaiting it is
        // the flush; delivery.timeout.ms bounds how long that can take.
        await _sender.ConfigureAwait(false);
        Router.Dispose();
    }

    /// <summary>Synchronous <see cref="DisposeAsync"/>.</summary>
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    // -----------------------------------------------------------------------
    // Buffering
    // -----------------------------------------------------------------------

    private async Task<Task<RecordMetadata>> EnqueueAsync(ProducerRecord record, CancellationToken cancellationToken, bool observed = false)
    {
        lock (_lock)
        {
            if (_closing) throw new ObjectDisposedException(nameof(Producer));
        }
        int partition = record.Partition ?? await ChoosePartitionAsync(record.Topic, record.Key, cancellationToken).ConfigureAwait(false);
        var headers = record.Headers ?? Array.Empty<RecordHeader>();
        int size = (record.Value?.Length ?? 0) + (record.Key?.Length ?? 0) + 16;
        foreach (var header in headers) size += Encoding.UTF8.GetByteCount(header.Key) + (header.Value?.Length ?? 0) + 4;

        await ReserveAsync(size, cancellationToken).ConfigureAwait(false);

        long now = NowMs();
        var pending = new Pending
        {
            Record = new BatchRecord { Key = record.Key, Value = record.Value, Headers = headers.ToArray() },
            TimestampMs = record.Timestamp ?? now,
            CreatedMs = now,
            Size = size,
            Observed = observed,
        };
        bool wake;
        lock (_lock)
        {
            if (_closing)
            {
                ReleaseLocked(size);
                throw new ObjectDisposedException(nameof(Producer));
            }
            var slot = new TopicPartition(record.Topic, partition);
            if (!_queues.TryGetValue(slot, out var queue)) _queues[slot] = queue = new PartitionQueue();
            queue.Items.Add(pending);
            queue.Bytes += size;
            wake = _config.LingerMs <= 0 || queue.Bytes >= _config.BatchSize || queue.Items.Count == 1;
        }
        if (wake) _wake.Release();
        return pending.Completion.Task;
    }

    private async Task<int> ChoosePartitionAsync(string topic, byte[]? key, CancellationToken cancellationToken)
    {
        var partitions = await Router.PartitionsAsync(topic, cancellationToken).ConfigureAwait(false);
        if (key != null) return Partitioner.PartitionForKey(key, partitions);
        int index = (int)((uint)Interlocked.Increment(ref _roundRobin) % (uint)partitions.Count);
        return partitions[index];
    }

    /// <summary>
    /// Waits until size more bytes may be buffered. This is what makes
    /// buffer.memory real: a producer faster than its broker is slowed down
    /// here rather than allowed to grow without limit.
    /// </summary>
    private async Task ReserveAsync(int size, CancellationToken cancellationToken)
    {
        long limit = _config.BufferMemory;
        var deadline = DateTime.UtcNow.AddMilliseconds(_config.MaxBlockMs);
        while (true)
        {
            Task freed;
            lock (_lock)
            {
                // A record larger than the whole budget is admitted rather than
                // waiting forever on a condition that can never hold.
                if (limit <= 0 || size >= limit || _bufferedBytes + size <= limit)
                {
                    _bufferedBytes += size;
                    return;
                }
                freed = _spaceFreed.Task;
            }
            TimeSpan remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                long used;
                lock (_lock) used = _bufferedBytes;
                throw new BufferFullException(
                    $"producer buffer full: {used} of {limit} bytes unflushed after max.block.ms={_config.MaxBlockMs}");
            }
            await Task.WhenAny(freed, Task.Delay(remaining, cancellationToken)).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    private void ReleaseLocked(long size)
    {
        _bufferedBytes = Math.Max(0, _bufferedBytes - size);
        var freed = _spaceFreed;
        _spaceFreed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        freed.TrySetResult();
    }

    // -----------------------------------------------------------------------
    // Sender
    // -----------------------------------------------------------------------

    private async Task SenderLoopAsync()
    {
        while (true)
        {
            var ready = new List<(TopicPartition Slot, List<Pending> Batch)>();
            int waitMs = Timeout.Infinite;
            lock (_lock)
            {
                long now = NowMs();
                bool anything = false;
                foreach (var (slot, queue) in _queues)
                {
                    if (queue.InFlight != null) { anything = true; continue; }
                    if (queue.Items.Count == 0) continue;
                    anything = true;
                    long lingerDue = queue.Items[0].CreatedMs + _config.LingerMs;
                    bool isReady = _closing || _flushers > 0 || _config.LingerMs <= 0 ||
                                   queue.Bytes >= _config.BatchSize || now >= lingerDue;
                    if (!isReady)
                    {
                        int until = (int)Math.Max(1, lingerDue - now);
                        waitMs = waitMs == Timeout.Infinite ? until : Math.Min(waitMs, until);
                        continue;
                    }
                    // One batch per partition in flight at a time, which is what
                    // keeps per-partition order across retries.
                    var batch = new List<Pending>();
                    int bytes = 0;
                    while (queue.Items.Count > 0 && (batch.Count == 0 || bytes + queue.Items[0].Size <= _config.BatchSize))
                    {
                        bytes += queue.Items[0].Size;
                        batch.Add(queue.Items[0]);
                        queue.Items.RemoveAt(0);
                    }
                    queue.Bytes -= bytes;
                    queue.InFlight = batch;
                    ready.Add((slot, batch));
                }
                if (!anything && _closing) return;
            }

            foreach (var (slot, batch) in ready)
            {
                _ = SendBatchAndCompleteAsync(slot, batch);
            }
            if (ready.Count == 0) await _wake.WaitAsync(waitMs).ConfigureAwait(false);
        }
    }

    private async Task SendBatchAndCompleteAsync(TopicPartition slot, List<Pending> batch)
    {
        try
        {
            await SendBatchAsync(slot, batch).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            foreach (var pending in batch) pending.Completion.TrySetException(e);
            if (batch.Any(p => !p.Observed))
                lock (_lock) _backgroundError ??= e;
        }
        finally
        {
            lock (_lock)
            {
                _queues[slot].InFlight = null;
                ReleaseLocked(batch.Sum(p => (long)p.Size));
            }
            _wake.Release();
        }
    }

    private async Task SendBatchAsync(TopicPartition slot, List<Pending> batch)
    {
        long deliveryDeadline = batch.Min(p => p.CreatedMs) + _config.DeliveryTimeoutMs;
        if (NowMs() > deliveryDeadline)
            throw new TimeoutException($"records for {slot} expired after delivery.timeout.ms={_config.DeliveryTimeoutMs} before they were sent");

        // The batch stores one base timestamp and a delta per record; the
        // newest record's time is the base, so deltas are zero or negative.
        long maxTimestamp = batch.Max(p => p.TimestampMs);
        var records = batch.Select(p =>
        {
            p.Record.TimestampDelta = p.TimestampMs - maxTimestamp;
            return p.Record;
        }).ToList();
        byte[] encoded = RecordBatch.Encode(records, maxTimestamp, _config.CompressionType);

        var w = new BodyWriter();
        w.String(slot.Topic);
        w.Int32(slot.Partition);
        w.Int32((int)_config.Acks);
        w.Int32(_config.RequestTimeoutMs);
        w.Int64(encoded.Length);
        w.Raw(encoded);
        byte[] body = w.ToArray();

        if (_config.Acks == Acks.None)
        {
            var conn = await Router.ConnectionForAsync(slot.Topic, slot.Partition).ConfigureAwait(false);
            await conn.SendOnewayAsync(ApiKey.Produce, body).ConfigureAwait(false);
            foreach (var pending in batch)
                pending.Completion.TrySetResult(new RecordMetadata(slot.Topic, slot.Partition, -1, pending.TimestampMs));
            return;
        }

        int attemptsLeft = _config.Retries;
        while (true)
        {
            BrokerConnection conn;
            try
            {
                conn = await Router.ConnectionForAsync(slot.Topic, slot.Partition).ConfigureAwait(false);
            }
            catch (BrahmaputraException) when (attemptsLeft > 0 && NowMs() < deliveryDeadline)
            {
                // Nothing was written, so retrying a failed connect cannot duplicate.
                attemptsLeft--;
                await Task.Delay(_config.RetryBackoffMs).ConfigureAwait(false);
                continue;
            }
            // The broker waits up to request.timeout.ms for acknowledgements, so
            // the client-side ceiling has to sit above it.
            byte[] response = await conn.RequestAsync(ApiKey.Produce, body, CancellationToken.None, TimeSpan.FromSeconds(5))
                .ConfigureAwait(false);
            var r = new BodyReader(response);
            r.String(); // topic
            r.Int32(); // partition
            int code = r.Int32();
            long baseOffset = r.Int64();
            long logAppendTime = r.Int64();
            if (code == 0)
            {
                for (int i = 0; i < batch.Count; i++)
                {
                    long timestamp = logAppendTime > 0 ? logAppendTime : batch[i].TimestampMs;
                    batch[i].Completion.TrySetResult(new RecordMetadata(slot.Topic, slot.Partition, baseOffset + i, timestamp));
                }
                return;
            }
            if (!ServerException.IsRetriable(code) || attemptsLeft <= 0 || NowMs() >= deliveryDeadline)
                throw new ServerException(code, $"produce to {slot}");
            attemptsLeft--;
            if (code is (int)ErrorCode.NotLeaderOrFollower or (int)ErrorCode.FencedLeaderEpoch or (int)ErrorCode.UnknownLeaderEpoch)
            {
                // A stale route is the most common retriable cause, and
                // resending to the same broker would just repeat it.
                try { await Router.RefreshAsync(slot.Topic).ConfigureAwait(false); }
                catch (BrahmaputraException) { /* retried below */ }
            }
            await Task.Delay(_config.RetryBackoffMs).ConfigureAwait(false);
        }
    }
}
