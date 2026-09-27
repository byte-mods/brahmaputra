using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Brahmaputra;

/// <summary>One broker in the cluster.</summary>
/// <param name="NodeId">Broker id.</param>
/// <param name="Host">Advertised host.</param>
/// <param name="Port">Advertised port.</param>
/// <param name="Rack">Failure domain, empty when the broker was started without --rack.</param>
public sealed record BrokerInfo(int NodeId, string Host, int Port, string Rack);

/// <summary>One partition's replica placement.</summary>
public sealed record PartitionInfo(int Partition, int Leader, IReadOnlyList<int> Replicas, IReadOnlyList<int> Isr, int LeaderEpoch);

/// <summary>One topic's partitions.</summary>
public sealed record TopicInfo(string Name, IReadOnlyList<PartitionInfo> Partitions);

/// <summary>A snapshot of the cluster.</summary>
public sealed class ClusterMetadata
{
    /// <summary>Every broker.</summary>
    public IReadOnlyList<BrokerInfo> Brokers { get; init; } = Array.Empty<BrokerInfo>();

    /// <summary>The controller's broker id.</summary>
    public int ControllerId { get; init; }

    /// <summary>Topics by name.</summary>
    public IReadOnlyDictionary<string, TopicInfo> Topics { get; init; } = new Dictionary<string, TopicInfo>();

    /// <summary>A topic's partition ids in ascending order; empty if unknown.</summary>
    public IReadOnlyList<int> PartitionsOf(string topic) =>
        Topics.TryGetValue(topic, out var info)
            ? info.Partitions.Select(p => p.Partition).OrderBy(p => p).ToList()
            : Array.Empty<int>();

    /// <summary>The broker id leading a partition, or -1.</summary>
    public int LeaderOf(string topic, int partition)
    {
        if (!Topics.TryGetValue(topic, out var info)) return -1;
        foreach (var p in info.Partitions)
            if (p.Partition == partition) return p.Leader;
        return -1;
    }

    internal static ClusterMetadata Decode(BodyReader r)
    {
        // Field order is exactly the schema's: error_code, brokers,
        // controller_id, topics. The leading code is request-level (an
        // authorization denial, say), distinct from the per-topic one.
        int code = r.Int32();
        if (code != 0) throw new ServerException(code, "metadata");
        var brokers = new List<BrokerInfo>();
        for (int count = r.Count(); count > 0; count--)
            brokers.Add(new BrokerInfo(r.Int32(), r.String(), r.Int32(), r.String()));
        int controller = r.Int32();
        var topics = new Dictionary<string, TopicInfo>();
        for (int count = r.Count(); count > 0; count--)
        {
            string name = r.String();
            int topicError = r.Int32();
            var partitions = new List<PartitionInfo>();
            for (int pcount = r.Count(); pcount > 0; pcount--)
            {
                int partition = r.Int32();
                int leader = r.Int32();
                var replicas = new List<int>();
                for (int n = r.Count(); n > 0; n--) replicas.Add(r.Int32());
                var isr = new List<int>();
                for (int n = r.Count(); n > 0; n--) isr.Add(r.Int32());
                partitions.Add(new PartitionInfo(partition, leader, replicas, isr, r.Int32()));
            }
            if (topicError != 0 && topicError != (int)ErrorCode.UnknownTopicOrPartition)
                throw new ServerException(topicError, "metadata for " + name);
            if (topicError == 0) topics[name] = new TopicInfo(name, partitions);
        }
        return new ClusterMetadata { Brokers = brokers, ControllerId = controller, Topics = topics };
    }
}

/// <summary>
/// Keeps connections to every broker and routes requests to partition leaders.
/// Metadata is cached and refreshed only when a request says the route was
/// stale, because refreshing per request would put the control plane on the
/// data path.
/// </summary>
public sealed class Router : IDisposable
{
    private readonly IReadOnlyList<string> _bootstrap;
    private readonly string _clientId;
    private readonly TimeSpan _connectTimeout;
    private readonly TimeSpan _requestTimeout;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly Dictionary<int, BrokerConnection> _conns = new();
    private BrokerConnection? _seed;
    private List<BrokerInfo> _brokers = new();
    private int _controllerId = -1;
    private readonly Dictionary<string, TopicInfo> _topics = new();
    private bool _haveMetadata;
    private bool _disposed;

    /// <summary>Creates a router and connects to the first reachable bootstrap server.</summary>
    public Router(string bootstrapServers, string clientId, TimeSpan connectTimeout, TimeSpan requestTimeout)
    {
        _bootstrap = bootstrapServers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (_bootstrap.Count == 0) throw new ArgumentException("bootstrap.servers is empty");
        _clientId = clientId;
        _connectTimeout = connectTimeout;
        _requestTimeout = requestTimeout;
        _seed = DialSeedAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    private async Task<BrokerConnection> DialSeedAsync(CancellationToken cancellationToken)
    {
        Exception? last = null;
        foreach (string address in _bootstrap)
        {
            try
            {
                return await BrokerConnection.ConnectAsync(address, _clientId, _connectTimeout, _requestTimeout, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (BrahmaputraException e)
            {
                last = e;
            }
        }
        throw new BrahmaputraException("no bootstrap server reachable: " + last?.Message, last!);
    }

    /// <summary>The bootstrap connection, re-dialled if it broke.</summary>
    public async Task<BrokerConnection> SeedAsync(CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await SeedLockedAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>Synchronous <see cref="SeedAsync"/>.</summary>
    public BrokerConnection Seed() => SeedAsync().GetAwaiter().GetResult();

    private async Task<BrokerConnection> SeedLockedAsync(CancellationToken cancellationToken)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(Router));
        if (_seed == null || _seed.IsBroken) _seed = await DialSeedAsync(cancellationToken).ConfigureAwait(false);
        return _seed;
    }

    /// <summary>Closes every connection.</summary>
    public void Dispose()
    {
        _lock.Wait();
        try
        {
            _disposed = true;
            foreach (var conn in _conns.Values) conn.Dispose();
            _conns.Clear();
            _seed?.Dispose();
        }
        finally
        {
            _lock.Release();
        }
    }

    private ClusterMetadata Snapshot() => new()
    {
        Brokers = _brokers.ToList(),
        ControllerId = _controllerId,
        Topics = new Dictionary<string, TopicInfo>(_topics),
    };

    /// <summary>
    /// Returns cluster metadata. With <paramref name="refresh"/> false a cached
    /// image is returned when one exists and covers the requested topics. A
    /// null or empty topic list asks for every topic.
    /// </summary>
    public async Task<ClusterMetadata> MetadataAsync(
        IReadOnlyCollection<string>? topics = null, bool refresh = false, CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            bool covered = _haveMetadata && (topics == null || topics.Count == 0 || topics.All(_topics.ContainsKey));
            if (!refresh && covered) return Snapshot();
            var w = new BodyWriter();
            w.StringArray(topics ?? Array.Empty<string>());
            BrokerConnection seed = await SeedLockedAsync(cancellationToken).ConfigureAwait(false);
            var r = new BodyReader(await seed.RequestAsync(ApiKey.Metadata, w.ToArray(), cancellationToken).ConfigureAwait(false));
            ClusterMetadata fresh = ClusterMetadata.Decode(r);
            _brokers = fresh.Brokers.ToList();
            _controllerId = fresh.ControllerId;
            if (topics == null || topics.Count == 0) _topics.Clear();
            // Merged rather than replaced, so refreshing one topic does not
            // evict every other topic's routes from the cache.
            if (topics != null) foreach (string topic in topics) _topics.Remove(topic);
            foreach (var (name, info) in fresh.Topics) _topics[name] = info;
            _haveMetadata = true;
            // A broker that left the cluster must not keep a pooled connection.
            foreach (int stale in _conns.Keys.Where(id => _brokers.All(b => b.NodeId != id)).ToList())
            {
                if (_conns[stale] != _seed) _conns[stale].Dispose();
                _conns.Remove(stale);
            }
            return Snapshot();
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>Synchronous <see cref="MetadataAsync"/>.</summary>
    public ClusterMetadata Metadata(IReadOnlyCollection<string>? topics = null, bool refresh = false) =>
        MetadataAsync(topics, refresh).GetAwaiter().GetResult();

    /// <summary>Forces a metadata refresh for one topic.</summary>
    public Task<ClusterMetadata> RefreshAsync(string topic, CancellationToken cancellationToken = default) =>
        MetadataAsync(new[] { topic }, true, cancellationToken);

    /// <summary>
    /// A topic's partition ids in ascending order. The broker auto-creates a
    /// topic on first reference, so this also creates one that does not exist.
    /// </summary>
    public async Task<IReadOnlyList<int>> PartitionsAsync(string topic, CancellationToken cancellationToken = default)
    {
        var metadata = await MetadataAsync(new[] { topic }, false, cancellationToken).ConfigureAwait(false);
        var partitions = metadata.PartitionsOf(topic);
        if (partitions.Count == 0)
        {
            // A topic auto-created on first reference may not be in the image
            // yet; one refresh distinguishes "new" from "absent".
            metadata = await RefreshAsync(topic, cancellationToken).ConfigureAwait(false);
            partitions = metadata.PartitionsOf(topic);
        }
        if (partitions.Count == 0) throw new BrahmaputraException($"topic \"{topic}\" has no partitions");
        return partitions;
    }

    /// <summary>Synchronous <see cref="PartitionsAsync"/>.</summary>
    public IReadOnlyList<int> Partitions(string topic) => PartitionsAsync(topic).GetAwaiter().GetResult();

    /// <summary>The connection to a partition's leader.</summary>
    public async Task<BrokerConnection> ConnectionForAsync(string topic, int partition, CancellationToken cancellationToken = default)
    {
        var metadata = await MetadataAsync(new[] { topic }, false, cancellationToken).ConfigureAwait(false);
        int leader = metadata.LeaderOf(topic, partition);
        if (leader < 0)
        {
            metadata = await RefreshAsync(topic, cancellationToken).ConfigureAwait(false);
            leader = metadata.LeaderOf(topic, partition);
        }
        if (leader < 0) throw new BrahmaputraException($"no leader for {topic}-{partition}");

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_conns.TryGetValue(leader, out var existing) && !existing.IsBroken) return existing;
            _conns.Remove(leader);
            BrokerInfo? broker = metadata.Brokers.FirstOrDefault(b => b.NodeId == leader);
            if (broker == null) throw new BrahmaputraException($"broker {leader} is not in the metadata");
            // A single-broker cluster advertises the address it was configured
            // with, which may not be the one we dialled; reuse the seed rather
            // than opening a second connection to ourselves.
            if (metadata.Brokers.Count == 1)
            {
                var seed = await SeedLockedAsync(cancellationToken).ConfigureAwait(false);
                _conns[leader] = seed;
                return seed;
            }
            var conn = await BrokerConnection.ConnectAsync(
                $"{broker.Host}:{broker.Port}", _clientId, _connectTimeout, _requestTimeout, cancellationToken).ConfigureAwait(false);
            _conns[leader] = conn;
            return conn;
        }
        finally
        {
            _lock.Release();
        }
    }
}
