using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Brahmaputra;

/// <summary>
/// <c>auto.offset.reset</c>: where to start when a partition has no valid
/// position, either because the group never committed one or because the
/// committed one fell off the front of the log.
/// </summary>
public enum AutoOffsetReset
{
    /// <summary>The oldest retained record. Reprocesses; never silently skips.</summary>
    Earliest,

    /// <summary>The end of the log. Skips what was missed; never reprocesses.</summary>
    Latest,

    /// <summary>Refuse to guess: poll throws <see cref="NoOffsetForPartitionException"/>.</summary>
    None,
}

/// <summary>Consumer-group settings, named as Kafka names them.</summary>
public class GroupConsumerConfig : ConsumerConfig
{
    /// <summary><c>group.id</c>.</summary>
    public string GroupId { get; set; } = "";

    /// <summary>
    /// <c>session.timeout.ms</c>: the coordinator evicts a member that stops
    /// heartbeating for this long. Kafka defaults to 45s; this defaults to 10s
    /// as the Rust client does.
    /// </summary>
    public int SessionTimeoutMs { get; set; } = 10_000;

    /// <summary>
    /// <c>heartbeat.interval.ms</c>: how often the background task heartbeats.
    /// Keep it well under <see cref="SessionTimeoutMs"/> (Kafka's rule of thumb
    /// is a third); 0 derives it as a third of the session timeout.
    /// </summary>
    public int HeartbeatIntervalMs { get; set; } = 3_000;

    /// <summary>How long the coordinator waits for members to rejoin during a rebalance.</summary>
    public int RebalanceTimeoutMs { get; set; } = 3_000;

    /// <summary>
    /// <c>max.poll.interval.ms</c>: the longest gap between polls before this
    /// member is presumed stuck and leaves the group. Heartbeats prove the
    /// process is alive; this proves the application is still consuming.
    /// </summary>
    public int MaxPollIntervalMs { get; set; } = 300_000;

    /// <summary><c>enable.auto.commit</c>.</summary>
    public bool EnableAutoCommit { get; set; } = true;

    /// <summary><c>auto.commit.interval.ms</c>.</summary>
    public int AutoCommitIntervalMs { get; set; } = 5_000;

    /// <summary><c>auto.offset.reset</c>.</summary>
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Earliest;

    /// <summary><c>partition.assignment.strategy</c>.</summary>
    public PartitionAssignmentStrategy PartitionAssignmentStrategy { get; set; } = PartitionAssignmentStrategy.Range;

    /// <summary>
    /// <c>group.instance.id</c>: a stable identity across restarts (static
    /// membership, KIP-345), so a rolling restart does not rebalance twice per
    /// instance. Null or empty means a dynamic member.
    /// </summary>
    public string? GroupInstanceId { get; set; }
}

/// <summary>
/// Shares a topic's partitions with the rest of its group.
/// </summary>
/// <remarks>
/// Not thread-safe, matching Kafka's consumer: poll and commit from one thread
/// (or one logical async flow). Heartbeats run on a background task.
/// </remarks>
public sealed class GroupConsumer : IDisposable, IAsyncDisposable
{
    /// <summary>The internal topic whose partition leaders coordinate groups.</summary>
    public const string OffsetsTopic = "__consumer_offsets";

    private const int CoordinatorAttempts = 4;
    private const int JoinAttempts = 6;

    private readonly GroupConsumerConfig _config;
    private readonly Consumer _consumer;
    private readonly object _state = new();
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _heartbeat;

    private List<string> _subscribed = new();
    private string _memberId = "";
    private int _generation = -1;
    private volatile bool _joined;
    private List<TopicPartition> _assignment = new();

    // _positions is the next offset to *deliver*, which is what gets
    // committed; it only advances over records handed to the caller.
    // _fetchPositions runs ahead of it by exactly the buffered records.
    private Dictionary<TopicPartition, long> _positions = new();
    private Dictionary<TopicPartition, long> _fetchPositions = new();
    private readonly List<ConsumeResult> _buffered = new();

    private long _lastPollMs;
    private volatile bool _inPoll;
    private long _lastCommitMs;
    private bool _closed;

    /// <summary>Connects and starts heartbeating.</summary>
    public GroupConsumer(GroupConsumerConfig config)
    {
        if (string.IsNullOrEmpty(config.GroupId)) throw new ArgumentException("group.id is required");
        _config = config;
        _consumer = new Consumer(config);
        _lastPollMs = NowMs();
        _lastCommitMs = NowMs();
        _heartbeat = Task.Run(HeartbeatLoopAsync);
    }

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>The group id.</summary>
    public string GroupId => _config.GroupId;

    /// <summary>This member's id, empty before the first join.</summary>
    public string MemberId { get { lock (_state) return _memberId; } }

    /// <summary>The group generation this member last joined.</summary>
    public int Generation { get { lock (_state) return _generation; } }

    /// <summary>The partitions currently assigned to this member.</summary>
    public IReadOnlyList<TopicPartition> Assignment => _assignment.ToList();

    /// <summary>The underlying partition consumer.</summary>
    public Consumer Consumer => _consumer;

    /// <summary>Sets the topics this member wants a share of; takes effect on the next poll.</summary>
    public void Subscribe(IEnumerable<string> topics)
    {
        _subscribed = topics.ToList();
        _joined = false;
    }

    /// <summary>
    /// Returns up to max.poll.records records, joining the group first if
    /// needed. Returns an empty list when nothing arrives within the timeout.
    /// </summary>
    public async Task<IReadOnlyList<ConsumeResult>> PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        if (_subscribed.Count == 0) throw new InvalidOperationException("subscribe to at least one topic before polling");
        // Stamped on entry and again on return, and not enforced in between:
        // the interval bounds how long the *application* goes without asking
        // for records, and a poll that blocks (for its timeout, or on a slow
        // rebalance) is the consumer working normally.
        Interlocked.Exchange(ref _lastPollMs, NowMs());
        _inPoll = true;
        try
        {
            return await PollCoreAsync(timeout, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Exchange(ref _lastPollMs, NowMs());
            _inPoll = false;
        }
    }

    private async Task<IReadOnlyList<ConsumeResult>> PollCoreAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = Stopwatch.StartNew();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_joined) await JoinAsync(cancellationToken).ConfigureAwait(false);
            if (_buffered.Count > 0) return TakeBuffered();

            TimeSpan remaining = timeout - deadline.Elapsed;
            if (_assignment.Count == 0)
            {
                if (remaining <= TimeSpan.Zero) return Array.Empty<ConsumeResult>();
                await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(50, remaining.TotalMilliseconds)), cancellationToken)
                    .ConfigureAwait(false);
                continue;
            }

            bool gotAny = false;
            foreach (var slot in _assignment.ToList())
            {
                if (!_joined) break;
                remaining = timeout - deadline.Elapsed;
                int waitMs = (int)Math.Clamp(remaining.TotalMilliseconds, 0, 500);
                if (!_fetchPositions.TryGetValue(slot, out long offset)) continue;
                IReadOnlyList<ConsumeResult> records;
                try
                {
                    records = await _consumer.FetchAsync(slot.Topic, slot.Partition, offset, waitMs, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (ServerException e) when (e.Error == ErrorCode.OffsetOutOfRange)
                {
                    // The position fell off the log; restart where the policy says.
                    long reset = await ResetOffsetAsync(slot, cancellationToken).ConfigureAwait(false);
                    _fetchPositions[slot] = reset;
                    _positions[slot] = reset;
                    continue;
                }
                catch (ServerException e) when (e.Error == ErrorCode.NotLeaderOrFollower)
                {
                    await _consumer.Router.RefreshAsync(slot.Topic, cancellationToken).ConfigureAwait(false);
                    continue;
                }
                if (records.Count > 0)
                {
                    gotAny = true;
                    _fetchPositions[slot] = records[^1].Offset + 1;
                    _buffered.AddRange(records);
                }
            }

            await MaybeAutoCommitAsync(cancellationToken).ConfigureAwait(false);
            if (_buffered.Count > 0) return TakeBuffered();
            if (!gotAny && deadline.Elapsed >= timeout) return Array.Empty<ConsumeResult>();
        }
    }

    /// <summary>Synchronous <see cref="PollAsync"/>.</summary>
    public IReadOnlyList<ConsumeResult> Poll(TimeSpan timeout) => PollAsync(timeout).GetAwaiter().GetResult();

    private List<ConsumeResult> TakeBuffered()
    {
        int limit = _config.MaxPollRecords <= 0 ? _buffered.Count : Math.Min(_config.MaxPollRecords, _buffered.Count);
        var delivered = _buffered.GetRange(0, limit);
        _buffered.RemoveRange(0, limit);
        // The consumed position advances only over records actually handed to
        // the caller; committing what was merely fetched would skip records
        // nobody processed.
        foreach (var record in delivered) _positions[record.TopicPartition] = record.Offset + 1;
        return delivered;
    }

    /// <summary>
    /// Commits the delivered positions. At-least-once: call it after
    /// processing, not before.
    /// </summary>
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (_positions.Count == 0) return;
        var slots = _positions.Keys.OrderBy(s => s).ToList();
        string memberId;
        int generation;
        lock (_state)
        {
            memberId = _memberId;
            generation = _generation;
        }
        var w = new BodyWriter();
        w.String(_config.GroupId);
        w.Int32(generation);
        w.String(memberId);
        w.Int32(slots.Count);
        foreach (var slot in slots)
        {
            w.String(slot.Topic);
            w.Int32(slot.Partition);
            w.Int64(_positions[slot]);
        }
        var r = new BodyReader(await CoordinatorRequestAsync(ApiKey.OffsetCommit, w.ToArray(), cancellationToken).ConfigureAwait(false));
        int code = r.Int32();
        if (code != 0)
        {
            // Generation fencing: a member that was rebalanced out may not
            // commit for partitions that now belong to someone else.
            if (code is (int)ErrorCode.IllegalGeneration or (int)ErrorCode.UnknownMemberId or (int)ErrorCode.RebalanceInProgress)
            {
                // The next poll rejoins, as a new member when the coordinator
                // no longer knows this one.
                if (code == (int)ErrorCode.UnknownMemberId) lock (_state) _memberId = "";
                _joined = false;
            }
            throw new ServerException(code, "offset_commit");
        }
        _lastCommitMs = NowMs();
    }

    /// <summary>Synchronous <see cref="CommitAsync"/>.</summary>
    public void Commit() => CommitAsync().GetAwaiter().GetResult();

    /// <summary>
    /// Reads the group's committed offsets. Null or empty asks for every
    /// partition the group holds.
    /// </summary>
    public async Task<Dictionary<TopicPartition, long>> CommittedAsync(
        IReadOnlyCollection<TopicPartition>? partitions = null, CancellationToken cancellationToken = default)
    {
        partitions ??= Array.Empty<TopicPartition>();
        var w = new BodyWriter();
        w.String(_config.GroupId);
        w.Int32(partitions.Count);
        foreach (var slot in partitions)
        {
            w.String(slot.Topic);
            w.Int32(slot.Partition);
        }
        var r = new BodyReader(await CoordinatorRequestAsync(ApiKey.OffsetFetch, w.ToArray(), cancellationToken).ConfigureAwait(false));
        int code = r.Int32();
        if (code != 0) throw new ServerException(code, "offset_fetch");
        var output = new Dictionary<TopicPartition, long>();
        for (int count = r.Count(); count > 0; count--)
        {
            string topic = r.String();
            int partition = r.Int32();
            output[new TopicPartition(topic, partition)] = r.Int64();
        }
        return output;
    }

    /// <summary>Synchronous <see cref="CommittedAsync"/>.</summary>
    public Dictionary<TopicPartition, long> Committed(IReadOnlyCollection<TopicPartition>? partitions = null) =>
        CommittedAsync(partitions).GetAwaiter().GetResult();

    private async Task MaybeAutoCommitAsync(CancellationToken cancellationToken)
    {
        if (!_config.EnableAutoCommit || _config.AutoCommitIntervalMs <= 0 || _positions.Count == 0) return;
        if (NowMs() - _lastCommitMs < _config.AutoCommitIntervalMs) return;
        try
        {
            await CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (BrahmaputraException)
        {
            // Retried on the next poll; an explicit Commit is what a caller
            // relies on.
        }
    }

    private async Task<long> ResetOffsetAsync(TopicPartition slot, CancellationToken cancellationToken) =>
        _config.AutoOffsetReset switch
        {
            AutoOffsetReset.Earliest => await _consumer.ListOffsetsAsync(slot.Topic, slot.Partition, Wire.Earliest, cancellationToken).ConfigureAwait(false),
            AutoOffsetReset.Latest => await _consumer.ListOffsetsAsync(slot.Topic, slot.Partition, Wire.Latest, cancellationToken).ConfigureAwait(false),
            _ => throw new NoOffsetForPartitionException(slot),
        };

    /// <summary>
    /// Commits (when auto-commit is on), leaves the group, then stops.
    /// Leaving is what separates a clean shutdown from a crash: without it the
    /// coordinator must wait out session.timeout.ms before reassigning.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        lock (_state)
        {
            if (_closed) return;
            _closed = true;
        }
        if (_joined && _config.EnableAutoCommit)
        {
            try { await CommitAsync().ConfigureAwait(false); }
            catch (Exception) { /* best effort on shutdown */ }
        }
        if (MemberId.Length > 0)
        {
            // Best effort: failing here costs only the session timeout.
            try { await LeaveAsync(CancellationToken.None).ConfigureAwait(false); }
            catch (Exception) { /* best effort on shutdown */ }
        }
        _stop.Cancel();
        try { await _heartbeat.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false); }
        catch (Exception) { /* the loop only ever ends by cancellation */ }
        _consumer.Dispose();
    }

    /// <summary>Synchronous <see cref="DisposeAsync"/>.</summary>
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    /// <summary>Synchronous alias of <see cref="Dispose"/>, for Kafka familiarity.</summary>
    public void Close() => Dispose();

    // -----------------------------------------------------------------------
    // Membership
    // -----------------------------------------------------------------------

    private async Task JoinAsync(CancellationToken cancellationToken)
    {
        for (int attempt = 0; attempt < JoinAttempts; attempt++)
        {
            var w = new BodyWriter();
            w.String(_config.GroupId);
            w.Int32(_config.SessionTimeoutMs);
            w.Int32(_config.RebalanceTimeoutMs);
            w.String(MemberId);
            w.StringArray(_subscribed);
            w.String(_config.GroupInstanceId ?? "");

            var r = new BodyReader(await CoordinatorRequestAsync(ApiKey.JoinGroup, w.ToArray(), cancellationToken).ConfigureAwait(false));
            int code = r.Int32();
            if (code == (int)ErrorCode.RebalanceInProgress)
            {
                await Task.Delay(100, cancellationToken).ConfigureAwait(false);
                continue;
            }
            if (code == (int)ErrorCode.UnknownMemberId)
            {
                // The coordinator forgot this member (session expiry, or it
                // left): rejoin as a new one.
                lock (_state) _memberId = "";
                continue;
            }
            if (code != 0) throw new ServerException(code, "join_group");

            int generation = r.Int32();
            string memberId = r.String();
            string leaderId = r.String();
            var members = new List<GroupMemberSubscription>();
            for (int count = r.Count(); count > 0; count--)
            {
                string id = r.String();
                var topics = r.StringArray();
                var held = new List<TopicPartition>();
                for (int n = r.Count(); n > 0; n--) held.Add(new TopicPartition(r.String(), r.Int32()));
                members.Add(new GroupMemberSubscription(id, topics, held));
            }
            lock (_state)
            {
                _memberId = memberId;
                _generation = generation;
            }

            var assignments = new List<(string MemberId, List<TopicPartition> Partitions)>();
            if (memberId == leaderId)
            {
                var topicPartitions = new Dictionary<string, IReadOnlyList<int>>();
                foreach (string topic in members.SelectMany(m => m.Topics).Distinct())
                    topicPartitions[topic] = await _consumer.PartitionsAsync(topic, cancellationToken).ConfigureAwait(false);
                var computed = PartitionAssignors.Assign(_config.PartitionAssignmentStrategy, members, topicPartitions);
                assignments = computed.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => (kv.Key, kv.Value)).ToList();
            }

            if (await SyncAsync(generation, memberId, assignments, cancellationToken).ConfigureAwait(false))
            {
                _joined = true;
                return;
            }
        }
        throw new BrahmaputraException($"consumer group failed to stabilise after {JoinAttempts} join attempts");
    }

    private async Task<bool> SyncAsync(
        int generation, string memberId, List<(string MemberId, List<TopicPartition> Partitions)> assignments,
        CancellationToken cancellationToken)
    {
        var w = new BodyWriter();
        w.String(_config.GroupId);
        w.Int32(generation);
        w.String(memberId);
        w.Int32(assignments.Count);
        foreach (var (member, partitions) in assignments)
        {
            w.String(member);
            w.Int32(partitions.Count);
            foreach (var slot in partitions)
            {
                w.String(slot.Topic);
                w.Int32(slot.Partition);
            }
        }
        var r = new BodyReader(await CoordinatorRequestAsync(ApiKey.SyncGroup, w.ToArray(), cancellationToken).ConfigureAwait(false));
        int code = r.Int32();
        if (code is (int)ErrorCode.RebalanceInProgress or (int)ErrorCode.IllegalGeneration) return false;
        if (code == (int)ErrorCode.UnknownMemberId)
        {
            lock (_state) _memberId = "";
            return false;
        }
        if (code != 0) throw new ServerException(code, "sync_group");
        var assignment = new List<TopicPartition>();
        for (int count = r.Count(); count > 0; count--) assignment.Add(new TopicPartition(r.String(), r.Int32()));
        await ApplyAssignmentAsync(assignment, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private async Task ApplyAssignmentAsync(List<TopicPartition> assignment, CancellationToken cancellationToken)
    {
        var owned = assignment.ToHashSet();
        foreach (var slot in _positions.Keys.Where(s => !owned.Contains(s)).ToList()) _positions.Remove(slot);
        // Buffered records sit ahead of the consumed position and were never
        // delivered, so a new assignment simply drops them.
        _buffered.Clear();

        var needed = assignment.Where(s => !_positions.ContainsKey(s)).ToList();
        if (needed.Count > 0)
        {
            var committed = await CommittedAsync(needed, cancellationToken).ConfigureAwait(false);
            foreach (var slot in needed)
            {
                if (!committed.TryGetValue(slot, out long offset) || offset < 0)
                    offset = await ResetOffsetAsync(slot, cancellationToken).ConfigureAwait(false);
                _positions[slot] = offset;
            }
        }
        _fetchPositions = new Dictionary<TopicPartition, long>(_positions);
        _assignment = assignment;
    }

    private async Task LeaveAsync(CancellationToken cancellationToken)
    {
        var w = new BodyWriter();
        w.String(_config.GroupId);
        w.String(MemberId);
        var r = new BodyReader(await CoordinatorRequestAsync(ApiKey.LeaveGroup, w.ToArray(), cancellationToken).ConfigureAwait(false));
        int code = r.Int32();
        _joined = false;
        // A dynamic member that left is gone; its next join is a fresh one.
        if (string.IsNullOrEmpty(_config.GroupInstanceId)) lock (_state) _memberId = "";
        if (code != 0) throw new ServerException(code, "leave_group");
    }

    private async Task HeartbeatLoopAsync()
    {
        // This loop enforces two independent deadlines, so it wakes often
        // enough for the shorter of them.
        int heartbeatEvery = _config.HeartbeatIntervalMs > 0 ? _config.HeartbeatIntervalMs : _config.SessionTimeoutMs / 3;
        int interval = Math.Max(1, Math.Min(heartbeatEvery, _config.MaxPollIntervalMs / 3));
        bool leftForSlowPoll = false;
        var token = _stop.Token;
        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            if (!_joined || MemberId.Length == 0) continue;

            long idleMs = NowMs() - Interlocked.Read(ref _lastPollMs);
            if (!_inPoll && idleMs >= _config.MaxPollIntervalMs)
            {
                // The application stopped consuming though the process is
                // alive. Heartbeating on would hold its partitions away from a
                // consumer that could make progress.
                if (!leftForSlowPoll)
                {
                    try { await LeaveAsync(token).ConfigureAwait(false); }
                    catch (Exception) { _joined = false; }
                    leftForSlowPoll = true;
                }
                continue;
            }
            leftForSlowPoll = false;

            try
            {
                var w = new BodyWriter();
                string memberId;
                int generation;
                lock (_state)
                {
                    memberId = _memberId;
                    generation = _generation;
                }
                w.String(_config.GroupId);
                w.Int32(generation);
                w.String(memberId);
                var r = new BodyReader(await CoordinatorRequestAsync(ApiKey.Heartbeat, w.ToArray(), token).ConfigureAwait(false));
                var code = (ErrorCode)r.Int32();
                if (code is ErrorCode.RebalanceInProgress or ErrorCode.IllegalGeneration or ErrorCode.UnknownMemberId)
                {
                    lock (_state)
                    {
                        // Only if nothing changed since the snapshot: a late
                        // answer for an old generation must not send a member
                        // that already rejoined round again.
                        if (_generation == generation && _memberId == memberId)
                        {
                            if (code == ErrorCode.UnknownMemberId) _memberId = "";
                            _joined = false;
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                return;
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

    private async Task<int> CoordinatorPartitionAsync(CancellationToken cancellationToken)
    {
        var partitions = await _consumer.PartitionsAsync(OffsetsTopic, cancellationToken).ConfigureAwait(false);
        return (int)(Crc32C.Compute(_config.GroupId) % (uint)partitions.Count);
    }

    /// <summary>
    /// Sends to the group's coordinator (the leader of the offsets partition
    /// the group id hashes to), following moves and waiting out loads.
    /// </summary>
    private async Task<byte[]> CoordinatorRequestAsync(ApiKey apiKey, byte[] body, CancellationToken cancellationToken)
    {
        for (int attempt = 0; attempt < CoordinatorAttempts; attempt++)
        {
            int partition = await CoordinatorPartitionAsync(cancellationToken).ConfigureAwait(false);
            var conn = await _consumer.Router.ConnectionForAsync(OffsetsTopic, partition, cancellationToken).ConfigureAwait(false);
            byte[] response = await conn.RequestAsync(apiKey, body, cancellationToken).ConfigureAwait(false);
            switch ((ErrorCode)BodyReader.PeekErrorCode(response))
            {
                case ErrorCode.CoordinatorLoadInProgress:
                    await Task.Delay(100, cancellationToken).ConfigureAwait(false);
                    continue;
                case ErrorCode.NotCoordinator:
                case ErrorCode.NotLeaderOrFollower:
                    await _consumer.Router.RefreshAsync(OffsetsTopic, cancellationToken).ConfigureAwait(false);
                    continue;
            }
            return response;
        }
        throw new BrahmaputraException($"group coordinator unavailable after {CoordinatorAttempts} attempts");
    }
}
