// Checks beyond the Go suite: every item of the client feature checklist that
// Program.cs's sections do not already exercise (batch.size, linger.ms,
// partitioners, timestamps, send-and-wait, codec registration, retries and
// request/delivery timeouts through a fault-injecting proxy, bounds-checked
// decoding, fetch limits, the high watermark, offsets by timestamp, metadata,
// multi-topic groups, auto-commit, heartbeats and eviction, static
// membership, LeaveGroup and rebalances).

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Brahmaputra;

internal static partial class Program
{
    private static int Drain(GroupConsumer member, int want, int timeoutMs)
    {
        int got = 0;
        var clock = Stopwatch.StartNew();
        while (got < want && clock.ElapsedMilliseconds < timeoutMs) got += PollQuietly(member, 300).Count;
        return got;
    }

    private static void AwaitAssignment(GroupConsumer member, int timeoutMs)
    {
        var clock = Stopwatch.StartNew();
        while (member.Assignment.Count == 0 && clock.ElapsedMilliseconds < timeoutMs) PollQuietly(member, 200);
    }

    private static IReadOnlyList<ConsumeResult> PollQuietly(GroupConsumer member, int timeoutMs)
    {
        try { return member.Poll(TimeSpan.FromMilliseconds(timeoutMs)); }
        catch (BrahmaputraException) { return Array.Empty<ConsumeResult>(); }
    }

    private static long Now() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static Exception? Try(Action body)
    {
        try { body(); return null; }
        catch (Exception e) { return e; }
    }

    private static void RunChecklist(string address)
    {
        Section("producer: batch.size and linger.ms");
        {
            string batchTopic = Unique("dotnet-batchsize");
            var value = Enumerable.Repeat((byte)'b', 200).ToArray();
            // Only batch.size can send anything during this check.
            var config = new ProducerConfig { BootstrapServers = address, LingerMs = 60_000, BatchSize = 1024 };
            using (var producer = new Producer(config))
            using (var consumer = new Consumer(ConsumerConf(address)))
            {
                producer.Router.Partitions(batchTopic);
                for (int i = 0; i < 8; i++) producer.SendTo(batchTopic, 0, value);
                Thread.Sleep(500);
                int early = consumer.Fetch(batchTopic, 0, 0, 0).Count;
                Check("a batch that reaches batch.size is sent before linger.ms", early >= 1 && early < 8,
                    $"{early} of 8 sent before any flush");
                producer.Flush();
                int after = consumer.Fetch(batchTopic, 0, 0, 500).Count;
                Check("flush sends the partial batch that is left", after == 8, $"got {after}");
            }

            string lingerTopic = Unique("dotnet-linger");
            using (var producer = new Producer(new ProducerConfig { BootstrapServers = address, LingerMs = 500 }))
            using (var consumer = new Consumer(ConsumerConf(address)))
            {
                producer.Router.Partitions(lingerTopic);
                producer.SendTo(lingerTopic, 0, B("lingering"));
                int immediate = consumer.Fetch(lingerTopic, 0, 0, 0).Count;
                Thread.Sleep(1500);
                int later = consumer.Fetch(lingerTopic, 0, 0, 0).Count;
                Check("linger.ms holds a record back, then sends it without a flush", immediate == 0 && later == 1,
                    $"immediately {immediate}, after linger {later}");
            }
        }

        Section("producer: partitioners");
        {
            string rrTopic = Unique("dotnet-rr");
            string pinTopic = Unique("dotnet-pinned");
            IReadOnlyList<int> partitions;
            using (var producer = new Producer(ProducerConf(address)))
            {
                partitions = producer.Router.Partitions(rrTopic);
                for (int i = 0; i < partitions.Count * 2; i++) producer.Send(rrTopic, B($"rr{i}"));
                producer.Router.Partitions(pinTopic);
                producer.SendTo(pinTopic, partitions[^1], B("pinned"));
                producer.Flush();
            }
            using var consumer = new Consumer(ConsumerConf(address));
            var counts = partitions.ToDictionary(p => p, p => consumer.Fetch(rrTopic, p, 0, 0).Count);
            var pinned = partitions.ToDictionary(p => p, p => consumer.Fetch(pinTopic, p, 0, 0).Count);
            Check("a null key round-robins across every partition", counts.Values.All(c => c == 2),
                string.Join(" ", counts.Select(kv => $"{kv.Key}={kv.Value}")));
            Check("an explicit partition is honoured", pinned[partitions[^1]] == 1 && pinned.Values.Sum() == 1,
                string.Join(" ", pinned.Select(kv => $"{kv.Key}={kv.Value}")));
        }

        Section("producer: record timestamps and send-and-wait");
        {
            string timeTopic = Unique("dotnet-timestamps");
            string syncTopic = Unique("dotnet-sync");
            long baseTime = Now() - 60_000;
            long beforeSend = Now();
            var offsets = new List<long>();
            using (var producer = new Producer(ProducerConf(address)))
            {
                // Produce waits for each, so every record is a batch of its own.
                for (int i = 0; i < 3; i++)
                    producer.Produce(new ProducerRecord(timeTopic, B($"t{i}")) { Partition = 0, Timestamp = baseTime + i * 1000L });
                producer.Produce(new ProducerRecord(timeTopic, B("now")) { Partition = 0 });
                for (int i = 0; i < 2; i++)
                    offsets.Add(producer.Produce(new ProducerRecord(syncTopic, B($"s{i}")) { Partition = 0 }).Offset);
            }
            using var consumer = new Consumer(ConsumerConf(address));
            var got = consumer.Fetch(timeTopic, 0, 0, 500);
            Check("an explicit record timestamp round-trips exactly",
                got.Count == 4 && Enumerable.Range(0, 3).All(i => got[i].Timestamp == baseTime + i * 1000L),
                string.Join(",", got.Select(r => r.Timestamp)));
            Check("a record without one is stamped with the wall clock",
                got.Count == 4 && got[3].Timestamp >= beforeSend - 1000 && got[3].Timestamp <= Now() + 1000,
                got.Count == 4 ? got[3].Timestamp.ToString() : "");
            Check("send-and-wait returns each record's offset", offsets.SequenceEqual(new[] { 0L, 1L }),
                string.Join(",", offsets));
            long atHalf = consumer.ListOffsets(timeTopic, 0, baseTime + 500);
            long atLast = consumer.ListOffsets(timeTopic, 0, baseTime + 2000);
            Check("list offsets by timestamp finds the first record at or after it", atHalf == 1 && atLast == 2,
                $"{atHalf}, {atLast}");
        }

        Section("producer: codec registration");
        {
            int compressed = 0, decompressed = 0;
            Codecs.Register(CompressionType.Lz4,
                payload => { Interlocked.Increment(ref compressed); return Lz4.Literals(payload); },
                data => { Interlocked.Increment(ref decompressed); return Lz4.Decode(data); });
            string lz4Topic = Unique("dotnet-lz4");
            var sent = Enumerable.Range(0, 10).Select(i => B($"lz4 record {i}" + new string(' ', 40))).ToList();
            var config = ProducerConf(address);
            config.CompressionType = CompressionType.Lz4;
            using (var producer = new Producer(config))
            {
                for (int i = 0; i < sent.Count; i++) producer.SendTo(lz4Topic, 0, sent[i], B($"k{i}"));
                producer.Flush();
            }
            using (var consumer = new Consumer(ConsumerConf(address)))
            {
                var got = consumer.Fetch(lz4Topic, 0, 0, 500);
                bool same = got.Count == sent.Count && got.Select((r, i) => r.Value!.AsSpan().SequenceEqual(sent[i])).All(x => x);
                Check("a registered codec (lz4) compresses sends and decodes fetches",
                    same && compressed >= 1 && decompressed >= 1,
                    $"{got.Count} records, {compressed} compressed, {decompressed} decompressed");
            }
            var zstd = ProducerConf(address);
            zstd.CompressionType = CompressionType.Zstd;
            var refused = Try(() =>
            {
                using var producer = new Producer(zstd);
                producer.SendTo(Unique("dotnet-zstd"), 0, B("x"));
                producer.Close();
            });
            Check("an unregistered codec is refused, not sent uncompressed",
                refused != null && refused.Message.Contains("not registered"), refused?.Message ?? "no error");
        }

        Section("producer: retries, request.timeout.ms and delivery.timeout.ms");
        using (var proxy = new FaultProxy(address))
        {
            string retryTopic = Unique("dotnet-retry");
            ProducerConfig Settings() => new()
            {
                BootstrapServers = proxy.Address, LingerMs = 0, Acks = Acks.All,
                RequestTimeoutMs = 1234, Retries = 3, RetryBackoffMs = 150,
            };
            Exception? SendThrough(ProducerConfig config, string value) => Try(() =>
            {
                using var producer = new Producer(config);
                producer.Produce(new ProducerRecord(retryTopic, B(value)) { Partition = 0 });
            });

            proxy.FailProduces(2);
            var clock = Stopwatch.StartNew();
            var error = SendThrough(Settings(), "retried");
            long elapsed = clock.ElapsedMilliseconds;
            Check("request.timeout.ms and acks travel with every produce",
                proxy.LastTimeoutMs == 1234 && proxy.LastAcks == -1, $"timeout={proxy.LastTimeoutMs} acks={proxy.LastAcks}");
            Check("a retriable error is retried after retry.backoff.ms",
                error == null && proxy.Produces == 3 && elapsed >= 300,
                $"attempts={proxy.Produces} elapsed={elapsed} error={error?.Message}");
            using (var consumer = new Consumer(ConsumerConf(address)))
            {
                int stored = consumer.Fetch(retryTopic, 0, 0, 500).Count;
                Check("the retried record is stored exactly once", stored == 1, $"stored {stored}");
            }

            proxy.FailProduces(-1);
            var bounded = Settings();
            bounded.Retries = 2;
            error = SendThrough(bounded, "never");
            Check("retries bounds the attempts: the error surfaces after retries + 1",
                error != null && proxy.Produces == 3, $"attempts={proxy.Produces} error={error?.Message}");

            proxy.FailProduces(-1);
            var late = Settings();
            late.Retries = 1_000_000;
            late.RetryBackoffMs = 50;
            late.DeliveryTimeoutMs = 500;
            clock.Restart();
            error = SendThrough(late, "late");
            elapsed = clock.ElapsedMilliseconds;
            Check("delivery.timeout.ms bounds the time spent retrying",
                error != null && elapsed >= 450 && elapsed < 3000,
                $"elapsed={elapsed} attempts={proxy.Produces} error={error?.Message}");

            proxy.FailProduces(0);
            for (int mode = 1; mode <= 2; mode++)
            {
                proxy.CorruptFetch = mode;
                var decodeError = Try(() =>
                {
                    using var consumer = new Consumer(new ConsumerConfig { BootstrapServers = proxy.Address });
                    consumer.Fetch(retryTopic, 0, 0, 100);
                });
                Check(mode == 1 ? "a negative length on the wire is an error" : "a length past the end of the data is an error",
                    decodeError is BrahmaputraException, decodeError?.GetType().Name + ": " + decodeError?.Message);
            }
            proxy.CorruptFetch = 0;
        }

        Section("consumer: fetch limits, high watermark and metadata");
        {
            string fetchTopic = Unique("dotnet-fetch");
            var value = Enumerable.Repeat((byte)'f', 1000).ToArray();
            using (var producer = new Producer(ProducerConf(address)))
            {
                for (int i = 0; i < 10; i++) producer.Produce(new ProducerRecord(fetchTopic, value) { Partition = 0 });
            }
            using (var consumer = new Consumer(new ConsumerConfig { BootstrapServers = address, FetchMaxBytes = 2500 }))
            {
                int got = consumer.Fetch(fetchTopic, 0, 0, 500).Count;
                Check("fetch.max.bytes caps what one fetch returns", got >= 1 && got < 10, $"got {got} of 10");
            }
            using (var consumer = new Consumer(new ConsumerConfig { BootstrapServers = address, MaxPollRecords = 4 }))
            {
                var got = consumer.Fetch(fetchTopic, 0, 0, 500);
                Check("max.poll.records caps one fetch", got.Count == 4, $"got {got.Count}");
                var next = consumer.Fetch(fetchTopic, 0, 4, 500);
                Check("the records a cap held back come on the next fetch", next.Count == 4 && next[0].Offset == 4,
                    $"got {next.Count}");
            }
            using (var waiting = new Consumer(new ConsumerConfig
                   { BootstrapServers = address, FetchMinBytes = 1_000_000, FetchMaxWaitMs = 600 }))
            using (var eager = new Consumer(ConsumerConf(address)))
            {
                var clock = Stopwatch.StartNew();
                int waitedFor = waiting.Fetch(fetchTopic, 0, 0, 600).Count;
                long waited = clock.ElapsedMilliseconds;
                clock.Restart();
                int eagerGot = eager.Fetch(fetchTopic, 0, 0, 600).Count;
                long quick = clock.ElapsedMilliseconds;
                Check("fetch.min.bytes holds a fetch open until fetch.max.wait.ms",
                    waited >= 450 && quick < 400 && waitedFor == 10 && eagerGot == 10,
                    $"waited {waited}ms, eager {quick}ms");

                var result = eager.FetchVerbose(fetchTopic, 0, 0, 500);
                Check("the high watermark is reported", result.HighWatermark == 10, $"{result.HighWatermark}");

                var metadata = eager.Router.Metadata(new[] { fetchTopic }, refresh: true);
                var brokerIds = metadata.Brokers.Select(b => b.NodeId).ToHashSet();
                var partitions = metadata.PartitionsOf(fetchTopic);
                Check("metadata names a live leader for every partition",
                    partitions.Count > 0 && partitions.All(p => brokerIds.Contains(metadata.LeaderOf(fetchTopic, p))),
                    $"{partitions.Count} partitions");
            }
        }

        Section("consumer group: several topics, auto-commit and max.poll.records");
        {
            string topicA = Unique("dotnet-multi-a");
            string topicB = Unique("dotnet-multi-b");
            using (var producer = new Producer(ProducerConf(address)))
            {
                for (int i = 0; i < 6; i++)
                {
                    producer.Send(topicA, B($"a{i}"));
                    producer.Send(topicB, B($"b{i}"));
                }
                producer.Flush();
            }
            var config = GroupConf(address, Unique("dotnet-multi"));
            config.EnableAutoCommit = true;
            config.AutoCommitIntervalMs = 200;
            config.MaxPollRecords = 5;
            var member = new GroupConsumer(config);
            member.Subscribe(new[] { topicA, topicB });
            var seen = new List<ConsumeResult>();
            int largest = 0;
            var clock = Stopwatch.StartNew();
            while (seen.Count < 12 && clock.ElapsedMilliseconds < 20_000)
            {
                var batch = PollQuietly(member, 500);
                largest = Math.Max(largest, batch.Count);
                seen.AddRange(batch);
            }
            var topics = seen.Select(r => r.Topic).ToHashSet();
            Check("one member consumes every subscribed topic", seen.Count == 12 && topics.Count == 2,
                $"{seen.Count} records from {topics.Count} topics");
            Check("max.poll.records caps each poll", largest >= 1 && largest <= 5, $"largest poll {largest}");
            // Nothing calls Commit(): these polls are what auto-commit rides on.
            clock.Restart();
            while (clock.ElapsedMilliseconds < 1000) PollQuietly(member, 100);
            long total = member.Committed().Values.Sum();
            Check("auto.commit.interval.ms commits delivered positions without Commit()", total == 12, $"committed {total}");
            member.Dispose();
        }

        Section("consumer group: heartbeats, session timeout and rejoin");
        {
            string hbTopic = Unique("dotnet-heartbeat");
            using (var producer = new Producer(ProducerConf(address)))
            {
                for (int i = 0; i < 4; i++) producer.Send(hbTopic, B($"h{i}"));
                producer.Flush();
            }
            var alive = GroupConf(address, Unique("dotnet-hb"));
            alive.SessionTimeoutMs = 1500;
            alive.HeartbeatIntervalMs = 300;
            var steady = new GroupConsumer(alive);
            steady.Subscribe(new[] { hbTopic });
            int got = Drain(steady, 4, 15_000);
            string member = steady.MemberId;
            Thread.Sleep(3500); // over twice the session timeout, with no poll
            var commitError = Try(steady.Commit);
            Check("heartbeats keep an idle member in its group past session.timeout.ms",
                got == 4 && commitError == null && steady.MemberId == member, $"got={got} commit={commitError?.Message}");
            steady.Dispose();

            var silent = GroupConf(address, Unique("dotnet-evicted"));
            silent.SessionTimeoutMs = 1000;
            silent.HeartbeatIntervalMs = 20_000; // effectively never, within this check
            var quiet = new GroupConsumer(silent);
            quiet.Subscribe(new[] { hbTopic });
            got = Drain(quiet, 4, 15_000);
            string evicted = quiet.MemberId;
            Thread.Sleep(2500);
            var fenced = Try(quiet.Commit);
            Check("a member that stops heartbeating is evicted after session.timeout.ms",
                got == 4 && fenced is ServerException { Error: ErrorCode.UnknownMemberId },
                $"got={got} commit={fenced?.Message}");
            // Only the join is checked: with no heartbeats this member is evicted
            // again one session timeout after it rejoins.
            var rejoinError = Try(() => quiet.Poll(TimeSpan.FromSeconds(1)));
            Check("an evicted member rejoins as a new member",
                rejoinError == null && quiet.MemberId.Length > 0 && quiet.MemberId != evicted,
                $"{evicted} -> {quiet.MemberId} error={rejoinError?.Message}");
            quiet.Dispose();
        }

        Section("consumer group: static membership, LeaveGroup and rebalances");
        {
            string staticTopic = Unique("dotnet-static");
            IReadOnlyList<int> partitions;
            using (var producer = new Producer(ProducerConf(address)))
            {
                partitions = producer.Router.Partitions(staticTopic);
                for (int i = 0; i < 4; i++) producer.Send(staticTopic, B($"st{i}"));
                producer.Flush();
            }
            string staticGroup = Unique("dotnet-static-grp");
            string instance = Unique("dotnet-instance");
            GroupConsumerConfig Fixed()
            {
                var config = GroupConf(address, staticGroup);
                config.HeartbeatIntervalMs = 300;
                config.GroupInstanceId = instance;
                return config;
            }
            var first = new GroupConsumer(Fixed());
            first.Subscribe(new[] { staticTopic });
            AwaitAssignment(first, 15_000);
            string firstMember = first.MemberId;
            int firstGeneration = first.Generation;
            var returning = new GroupConsumer(Fixed());
            returning.Subscribe(new[] { staticTopic });
            AwaitAssignment(returning, 15_000);
            Check("a returning group.instance.id reclaims its member id without a rebalance",
                firstMember.Length > 0 && returning.MemberId == firstMember && returning.Generation == firstGeneration,
                $"{firstMember}/{firstGeneration} -> {returning.MemberId}/{returning.Generation}");
            returning.Dispose();
            first.Dispose();

            // LeaveGroup: with a 30 s session and a 10 s rebalance timeout, a
            // successor could only get the partitions quickly if the first member
            // told the coordinator it left.
            string leaveGroup = Unique("dotnet-leave-grp");
            GroupConsumerConfig Leaving()
            {
                var config = GroupConf(address, leaveGroup);
                config.SessionTimeoutMs = 30_000;
                config.RebalanceTimeoutMs = 10_000;
                return config;
            }
            var departing = new GroupConsumer(Leaving());
            departing.Subscribe(new[] { staticTopic });
            AwaitAssignment(departing, 15_000);
            departing.Dispose();
            var clock = Stopwatch.StartNew();
            var successor = new GroupConsumer(Leaving());
            successor.Subscribe(new[] { staticTopic });
            AwaitAssignment(successor, 15_000);
            long took = clock.ElapsedMilliseconds;
            Check("close sends LeaveGroup, so a successor is not kept waiting",
                successor.Assignment.Count == partitions.Count && took < 6_000,
                $"{successor.Assignment.Count} partitions after {took}ms");
            successor.Dispose();

            // Two members: the second's join makes the coordinator fence the first's
            // generation; its heartbeat learns that, it rejoins, and the partitions split.
            string shareGroup = Unique("dotnet-share-grp");
            GroupConsumerConfig Sharing()
            {
                var config = GroupConf(address, shareGroup);
                config.HeartbeatIntervalMs = 200;
                return config;
            }
            var one = new GroupConsumer(Sharing());
            one.Subscribe(new[] { staticTopic });
            AwaitAssignment(one, 15_000);
            int before = one.Generation;
            var two = new GroupConsumer(Sharing());
            two.Subscribe(new[] { staticTopic });
            var stop = new ManualResetEventSlim();
            var other = new Thread(() =>
            {
                while (!stop.IsSet) PollQuietly(two, 200);
            }) { IsBackground = true };
            other.Start();
            bool split = false;
            clock.Restart();
            while (!split && clock.ElapsedMilliseconds < 20_000)
            {
                PollQuietly(one, 200);
                var mine = one.Assignment;
                var theirs = two.Assignment;
                split = mine.Count > 0 && theirs.Count > 0 &&
                        mine.Concat(theirs).Distinct().Count() == partitions.Count &&
                        mine.Count + theirs.Count == partitions.Count;
            }
            stop.Set();
            other.Join(10_000);
            Check("a second member rebalances the group and the partitions split between them", split,
                $"{string.Join(",", one.Assignment)} / {string.Join(",", two.Assignment)}");
            Check("the generation advances when the group rebalances", one.Generation > before,
                $"{before} -> {one.Generation}");
            two.Dispose();
            one.Dispose();
        }
    }
}

/// <summary>
/// lz4 in the broker's format (little-endian uncompressed length, then a raw
/// LZ4 block). It compresses by emitting one literal run (valid LZ4 any decoder
/// reads) and decodes full LZ4, matches included, so it reads what the
/// broker's lz4 writes too.
/// </summary>
internal static class Lz4
{
    public static byte[] Literals(byte[] payload)
    {
        int size = payload.Length;
        var output = new MemoryStream(size + size / 255 + 16);
        var prefix = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(prefix, size);
        output.Write(prefix);
        output.WriteByte((byte)(Math.Min(size, 15) << 4));
        if (size >= 15)
        {
            int rest = size - 15;
            for (; rest >= 255; rest -= 255) output.WriteByte(255);
            output.WriteByte((byte)rest);
        }
        output.Write(payload);
        return output.ToArray();
    }

    public static byte[] Decode(byte[] data)
    {
        try
        {
            int size = BinaryPrimitives.ReadInt32LittleEndian(data);
            if (size < 0 || size > 256 * 1024 * 1024) throw new BrahmaputraException($"lz4 size {size}");
            var output = new byte[size];
            int input = 4, at = 0;
            int Length(int start)
            {
                int total = start;
                if (start != 15) return total;
                int more;
                do
                {
                    more = data[input++];
                    total += more;
                } while (more == 255);
                return total;
            }
            while (input < data.Length)
            {
                int token = data[input++];
                int literals = Length(token >> 4);
                Array.Copy(data, input, output, at, literals);
                input += literals;
                at += literals;
                if (input >= data.Length) break;
                int distance = data[input] | (data[input + 1] << 8);
                input += 2;
                int matched = Length(token & 15) + 4;
                if (distance == 0 || distance > at) throw new BrahmaputraException("lz4 match before the output");
                for (int i = 0; i < matched; i++, at++) output[at] = output[at - distance];
            }
            if (at != size) throw new BrahmaputraException($"lz4 decoded {at} of {size}");
            return output;
        }
        catch (Exception e) when (e is IndexOutOfRangeException or ArgumentException)
        {
            throw new BrahmaputraException("truncated lz4 block");
        }
    }
}

/// <summary>
/// Sits between a client and the broker, forwarding frames one request at a
/// time, and can answer a produce with a retriable error or a fetch with a
/// corrupt batch. It records the acks and timeout of every produce it sees.
/// </summary>
internal sealed class FaultProxy : IDisposable
{
    private readonly TcpListener _listener;
    private readonly List<TcpClient> _live = new();
    private readonly object _lock = new();
    private int _failuresLeft;
    private int _produces;

    public string Address { get; }
    public volatile int CorruptFetch;
    public volatile int LastAcks = int.MinValue;
    public volatile int LastTimeoutMs = int.MinValue;
    public int Produces => Volatile.Read(ref _produces);

    public FaultProxy(string target)
    {
        int colon = target.LastIndexOf(':');
        string host = target[..colon];
        int port = int.Parse(target[(colon + 1)..]);
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Address = $"127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";
        new Thread(() =>
        {
            while (true)
            {
                TcpClient client;
                try { client = _listener.AcceptTcpClient(); }
                catch (Exception) { return; }
                var upstream = new TcpClient();
                try { upstream.Connect(host, port); }
                catch (Exception) { client.Dispose(); upstream.Dispose(); continue; }
                lock (_live) { _live.Add(client); _live.Add(upstream); }
                new Thread(() => Serve(client, upstream)) { IsBackground = true }.Start();
            }
        }) { IsBackground = true }.Start();
    }

    /// <summary>Fails the next count produces (-1: every one) and resets the counters.</summary>
    public void FailProduces(int count)
    {
        lock (_lock)
        {
            _failuresLeft = count;
            Volatile.Write(ref _produces, 0);
        }
    }

    private bool TakeFailure()
    {
        lock (_lock)
        {
            if (_failuresLeft == 0) return false;
            if (_failuresLeft > 0) _failuresLeft--;
            return true;
        }
    }

    private static byte[] ReadFrame(Stream stream)
    {
        var prefix = new byte[4];
        stream.ReadExactly(prefix);
        var frame = new byte[BinaryPrimitives.ReadInt32BigEndian(prefix)];
        stream.ReadExactly(frame);
        return frame;
    }

    private static void WriteFrame(Stream stream, byte[] payload)
    {
        var framed = new byte[4 + payload.Length];
        BinaryPrimitives.WriteInt32BigEndian(framed, payload.Length);
        payload.CopyTo(framed, 4);
        stream.Write(framed);
        stream.Flush();
    }

    private void Serve(TcpClient client, TcpClient upstream)
    {
        try
        {
            var down = client.GetStream();
            var up = upstream.GetStream();
            while (true)
            {
                byte[] frame = ReadFrame(down);
                var apiKey = (ApiKey)BinaryPrimitives.ReadInt16BigEndian(frame);
                int correlation = BinaryPrimitives.ReadInt32BigEndian(frame.AsSpan(4));
                int bodyAt = 10 + Math.Max((int)BinaryPrimitives.ReadInt16BigEndian(frame.AsSpan(8)), 0);
                byte[] body = frame[bodyAt..];
                byte[]? reply = null;
                bool oneway = false;
                if (apiKey == ApiKey.Produce)
                {
                    var r = new BodyReader(body);
                    string topic = r.String();
                    int partition = r.Int32();
                    int acks = r.Int32();
                    LastTimeoutMs = r.Int32();
                    LastAcks = acks;
                    Interlocked.Increment(ref _produces);
                    oneway = acks == 0;
                    if (TakeFailure())
                    {
                        var w = new BodyWriter();
                        w.String(topic);
                        w.Int32(partition);
                        w.Int32((int)ErrorCode.NotEnoughReplicas);
                        w.Int64(-1);
                        w.Int64(-1);
                        reply = w.ToArray();
                    }
                }
                else if (apiKey == ApiKey.Fetch && CorruptFetch != 0)
                {
                    var r = new BodyReader(body);
                    string topic = r.String();
                    int partition = r.Int32();
                    // A batch whose batch_length is negative (mode 1) or runs far
                    // past the bytes that follow (mode 2).
                    var batch = new byte[61];
                    BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8), CorruptFetch == 1 ? -1 : 1_000_000);
                    var w = new BodyWriter();
                    w.String(topic);
                    w.Int32(partition);
                    w.Int32(0);
                    w.Int64(1);
                    w.Int64(1);
                    w.Int64(batch.Length);
                    w.Int32(-1);
                    w.Raw(batch);
                    reply = w.ToArray();
                }
                if (reply != null)
                {
                    byte[] framed = Frame.Encode(apiKey, correlation, "", reply);
                    down.Write(framed);
                    down.Flush();
                    continue;
                }
                WriteFrame(up, frame);
                if (!oneway) WriteFrame(down, ReadFrame(up));
            }
        }
        catch (Exception)
        {
            // Either side went away.
        }
        finally
        {
            client.Dispose();
            upstream.Dispose();
        }
    }

    public void Dispose()
    {
        _listener.Stop();
        lock (_live)
        {
            foreach (var c in _live) c.Dispose();
            _live.Clear();
        }
    }
}
