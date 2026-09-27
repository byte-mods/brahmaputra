// End-to-end suite for the .NET driver against a live broker.
//
//   brahmaputra-server --data-dir ./data --default-partitions 4
//   dotnet run --project Brahmaputra.ManualTest -- 127.0.0.1 9092
//
// Every check asserts a property of the system, not that a function ran:
// records come back byte-identical, keys pin partitions, headers survive,
// offsets are contiguous. A setup failure aborts with exit code 2; any
// failed check makes the run exit 1.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Brahmaputra;

internal static class Program
{
    private static int _passed;
    private static int _failed;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            _passed++;
            Console.WriteLine($"  ok   {name}");
            return;
        }
        _failed++;
        Console.WriteLine(detail.Length > 0 ? $"  FAIL {name}: {detail}" : $"  FAIL {name}");
    }

    private static void Section(string title) => Console.WriteLine($"\n{title}");

    private static string Unique(string prefix) =>
        $"{prefix}-{DateTime.UtcNow.Ticks % 1_000_000_000}";

    private static byte[] B(string text) => Encoding.UTF8.GetBytes(text);

    private static string S(byte[]? bytes) => bytes == null ? "<null>" : Encoding.UTF8.GetString(bytes);

    private static int Main(string[] args)
    {
        string host = args.Length > 0 ? args[0] : "127.0.0.1";
        string port = args.Length > 1 ? args[1] : "9092";
        // Also accept a single host:port argument, as the Go suite does.
        string address = args.Length == 1 && host.Contains(':') ? host : $"{host}:{port}";
        try
        {
            Run(address);
        }
        catch (Exception e)
        {
            Console.WriteLine($"  FATAL {e.GetType().Name}: {e.Message}");
            return 2;
        }
        Console.WriteLine($"\n{_passed} passed, {_failed} failed");
        return _failed > 0 ? 1 : 0;
    }

    private static ProducerConfig ProducerConf(string address) => new() { BootstrapServers = address, LingerMs = 0 };

    private static ConsumerConfig ConsumerConf(string address) => new() { BootstrapServers = address };

    private static GroupConsumerConfig GroupConf(string address, string groupId) =>
        new() { BootstrapServers = address, GroupId = groupId, EnableAutoCommit = false };

    private static void Run(string address)
    {
        Section("connection and metadata");
        using (var consumer = new Consumer(ConsumerConf(address)))
        {
            var (versions, brokerVersion) = consumer.Router.Seed().ApiVersions();
            Check("ApiVersions answers", versions.Count > 0, $"{versions.Count} ranges");
            Check("broker reports a version", brokerVersion.Length > 0, brokerVersion);
            var metadata = consumer.Router.Metadata(null, refresh: true);
            Check("metadata lists brokers", metadata.Brokers.Count >= 1, $"{metadata.Brokers.Count} brokers");
        }

        Section("produce and consume round trip");
        string topic = Unique("dotnet-roundtrip");
        var payloads = Enumerable.Range(0, 50).Select(i => B($"record-{i}")).ToList();
        using (var producer = new Producer(ProducerConf(address)))
        {
            foreach (var payload in payloads) producer.SendTo(topic, 0, payload);
            producer.Flush();
        }
        using (var consumer = new Consumer(ConsumerConf(address)))
        {
            var got = consumer.Fetch(topic, 0, 0, 500);
            Check("every record comes back", got.Count == payloads.Count, $"got {got.Count}");
            bool identical = got.Count == payloads.Count;
            for (int i = 0; identical && i < got.Count; i++)
                if (!got[i].Value!.AsSpan().SequenceEqual(payloads[i]) || got[i].Offset != i) identical = false;
            Check("values byte-identical and offsets contiguous", identical);
        }

        Section("compression codecs");
        // Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
        // Codecs.Register so applications that do not want those dependencies
        // do not carry them.
        foreach (var codec in new[] { CompressionType.None, CompressionType.Gzip })
        {
            string name = codec.ToString().ToLowerInvariant();
            string codecTopic = Unique("dotnet-" + name);
            byte[] body = B(string.Concat(Enumerable.Repeat("the same line over and over. ", 40)));
            var config = ProducerConf(address);
            config.CompressionType = codec;
            using (var producer = new Producer(config))
            {
                for (int i = 0; i < 20; i++) producer.SendTo(codecTopic, 0, body.Append((byte)('0' + i % 10)).ToArray());
                producer.Flush();
            }
            using var consumer = new Consumer(ConsumerConf(address));
            var got = consumer.Fetch(codecTopic, 0, 0, 500);
            Check($"{name}: round trips", got.Count == 20 && got[0].Value!.AsSpan().StartsWith(body), $"got {got.Count} records");
        }

        Section("keys, partitioning and ordering");
        {
            string keyTopic = Unique("dotnet-keys");
            IReadOnlyList<int> partitions;
            using (var producer = new Producer(ProducerConf(address)))
            {
                partitions = producer.Router.Partitions(keyTopic);
                for (int i = 0; i < 30; i++) producer.Send(keyTopic, B($"v{i}"), B("user-7"));
                producer.Flush();
            }
            int target = Partitioner.PartitionForKey(B("user-7"), partitions);
            using var consumer = new Consumer(ConsumerConf(address));
            var onTarget = consumer.Fetch(keyTopic, target, 0, 500);
            Check("a key pins every record to one partition", onTarget.Count == 30,
                $"partition {target} holds {onTarget.Count} of 30");
            bool ordered = onTarget.Count == 30;
            for (int i = 0; ordered && i < onTarget.Count; i++)
                if (S(onTarget[i].Value) != $"v{i}") ordered = false;
            Check("per-key order is preserved", ordered);
            int strays = partitions.Where(p => p != target).Sum(p => consumer.Fetch(keyTopic, p, 0, 200).Count);
            Check("no keyed record landed elsewhere", strays == 0, $"{strays} strays");
        }

        Section("murmur2 agrees with the broker's partitioner");
        Check("murmur2(\"\") is stable", Partitioner.Murmur2(ReadOnlySpan<byte>.Empty) == 275646681,
            Partitioner.Murmur2(ReadOnlySpan<byte>.Empty).ToString());
        Check("murmur2 is deterministic", Partitioner.Murmur2(B("user-7")) == Partitioner.Murmur2(B("user-7")));
        Check("different keys hash differently", Partitioner.Murmur2(B("user-7")) != Partitioner.Murmur2(B("user-8")));

        Section("record headers and timestamps");
        {
            string headerTopic = Unique("dotnet-headers");
            long before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1000;
            using (var producer = new Producer(ProducerConf(address)))
            {
                producer.SendTo(headerTopic, 0, B("annotated"), null,
                    new RecordHeader("trace-id", B("abc-123")),
                    new RecordHeader("content-type", B("application/json")),
                    new RecordHeader("tombstone-reason", (byte[]?)null));
                producer.SendTo(headerTopic, 0, B("plain"));
                producer.Flush();
            }
            long after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1000;
            using var consumer = new Consumer(ConsumerConf(address));
            var got = consumer.Fetch(headerTopic, 0, 0, 500);
            Check("both records arrive", got.Count == 2, $"got {got.Count}");
            if (got.Count == 2)
            {
                var (annotated, plain) = (got[0], got[1]);
                Check("headers survive the round trip", annotated.Headers.Count == 3, $"{annotated.Headers.Count} headers");
                Check("header values are exact", S(annotated.Header("trace-id")) == "abc-123");
                Check("a null header value stays null", annotated.Headers.Count == 3 && annotated.Headers[2].Value == null);
                Check("a record with no headers gains none from its batch", plain.Headers.Count == 0,
                    $"{plain.Headers.Count} headers");
                bool inWindow = got.All(r => r.Timestamp >= before && r.Timestamp <= after);
                Check("timestamps are real wall-clock values", inWindow,
                    $"{got[0].Timestamp},{got[1].Timestamp} outside {before}..{after}");
            }
        }

        Section("tombstones");
        {
            string tombTopic = Unique("dotnet-tombstones");
            using (var producer = new Producer(ProducerConf(address)))
            {
                producer.SendTo(tombTopic, 0, B("set"), B("k1"));
                producer.SendTo(tombTopic, 0, Array.Empty<byte>(), B("k2"));
                // A null value is a deletion, and must stay distinguishable from
                // the empty value above all the way through the round trip.
                producer.SendTo(tombTopic, 0, null, B("k3"));
                producer.Flush();
            }
            using var consumer = new Consumer(ConsumerConf(address));
            var got = consumer.Fetch(tombTopic, 0, 0, 500);
            Check("all three records arrive", got.Count == 3, $"got {got.Count}");
            if (got.Count == 3)
            {
                Check("an ordinary value round-trips", S(got[0].Value) == "set");
                Check("an empty value is empty, not null", got[1].Value != null && got[1].Value!.Length == 0, S(got[1].Value));
                Check("a tombstone arrives as a null value", got[2].Value == null, S(got[2].Value));
            }
        }

        Section("offsets");
        using (var consumer = new Consumer(ConsumerConf(address)))
        {
            long earliest = consumer.ListOffsets(topic, 0, Wire.Earliest);
            long latest = consumer.ListOffsets(topic, 0, Wire.Latest);
            Check("earliest is 0 on a fresh topic", earliest == 0, earliest.ToString());
            Check("latest equals the record count", latest == 50, latest.ToString());
        }

        Section("acks");
        foreach (var acks in new[] { Acks.None, Acks.Leader, Acks.All })
        {
            string acksTopic = Unique($"dotnet-acks{(int)acks}");
            var config = ProducerConf(address);
            config.Acks = acks;
            using (var producer = new Producer(config))
            {
                producer.SendTo(acksTopic, 0, B("durable"));
                producer.Flush();
            }
            Thread.Sleep(400);
            using var consumer = new Consumer(ConsumerConf(address));
            var got = consumer.Fetch(acksTopic, 0, 0, 500);
            Check($"acks={(int)acks} stores the record", got.Count == 1, $"got {got.Count}");
        }

        Section("consumer group: assignment, commit, resume");
        {
            string groupTopic = Unique("dotnet-group");
            string groupId = Unique("dotnet-billing");
            using (var producer = new Producer(ProducerConf(address)))
            {
                for (int i = 0; i < 40; i++) producer.Send(groupTopic, B($"g{i}"));
                producer.Flush();
            }

            var seen = new List<ConsumeResult>();
            using (var consumer = new GroupConsumer(GroupConf(address, groupId)))
            {
                consumer.Subscribe(new[] { groupTopic });
                var deadline = Stopwatch.StartNew();
                while (seen.Count < 40 && deadline.Elapsed < TimeSpan.FromSeconds(30))
                    seen.AddRange(consumer.Poll(TimeSpan.FromMilliseconds(500)));
                Check("the group consumes every record", seen.Count == 40, $"got {seen.Count}");
                int distinct = seen.Select(r => (r.Partition, r.Offset)).Distinct().Count();
                Check("no record is delivered twice", distinct == seen.Count);

                consumer.Commit();
                long total = consumer.Committed().Values.Sum();
                Check("commit records a position", total == 40, total.ToString());
            }

            // A second consumer in the same group must resume, not replay.
            using var rejoined = new GroupConsumer(GroupConf(address, groupId));
            rejoined.Subscribe(new[] { groupTopic });
            var replayed = new List<ConsumeResult>();
            var until = Stopwatch.StartNew();
            while (until.Elapsed < TimeSpan.FromSeconds(5))
            {
                try { replayed.AddRange(rejoined.Poll(TimeSpan.FromMilliseconds(300))); }
                catch (BrahmaputraException) { /* keep polling until the window closes */ }
            }
            Check("a rejoining group resumes from its commit", replayed.Count == 0,
                $"replayed {replayed.Count} records it had already committed");
        }

        Section("auto.offset.reset");
        {
            string resetTopic = Unique("dotnet-reset");
            using (var producer = new Producer(ProducerConf(address)))
            {
                for (int i = 0; i < 10; i++) producer.Send(resetTopic, B($"r{i}"));
                producer.Flush();
            }

            var latestConfig = GroupConf(address, Unique("dotnet-latest"));
            latestConfig.AutoOffsetReset = AutoOffsetReset.Latest;
            using (var consumer = new GroupConsumer(latestConfig))
            {
                consumer.Subscribe(new[] { resetTopic });
                var skipped = new List<ConsumeResult>();
                var until = Stopwatch.StartNew();
                while (until.Elapsed < TimeSpan.FromSeconds(4))
                {
                    try { skipped.AddRange(consumer.Poll(TimeSpan.FromMilliseconds(300))); }
                    catch (BrahmaputraException) { /* keep polling until the window closes */ }
                }
                Check("latest skips records produced before the group existed", skipped.Count == 0, $"saw {skipped.Count}");
            }

            var noneConfig = GroupConf(address, Unique("dotnet-none"));
            noneConfig.AutoOffsetReset = AutoOffsetReset.None;
            using (var strict = new GroupConsumer(noneConfig))
            {
                strict.Subscribe(new[] { resetTopic });
                bool raised = false;
                var until = Stopwatch.StartNew();
                while (until.Elapsed < TimeSpan.FromSeconds(5) && !raised)
                {
                    try { strict.Poll(TimeSpan.FromMilliseconds(300)); }
                    catch (NoOffsetForPartitionException) { raised = true; }
                    catch (BrahmaputraException) { /* anything else is not the refusal we want */ }
                }
                Check("none refuses to guess a position", raised);
            }
        }

        Section("assignors");
        foreach (var strategy in new[] { PartitionAssignmentStrategy.Range, PartitionAssignmentStrategy.RoundRobin, PartitionAssignmentStrategy.Sticky })
        {
            string name = strategy.ToString().ToLowerInvariant();
            string assignorTopic = Unique("dotnet-" + name);
            using (var producer = new Producer(ProducerConf(address)))
            {
                for (int i = 0; i < 20; i++) producer.Send(assignorTopic, B($"a{i}"));
                producer.Flush();
            }
            var config = GroupConf(address, Unique("dotnet-grp-" + name));
            config.PartitionAssignmentStrategy = strategy;
            using var consumer = new GroupConsumer(config);
            consumer.Subscribe(new[] { assignorTopic });
            var collected = new List<ConsumeResult>();
            var deadline = Stopwatch.StartNew();
            while (collected.Count < 20 && deadline.Elapsed < TimeSpan.FromSeconds(20))
            {
                try { collected.AddRange(consumer.Poll(TimeSpan.FromMilliseconds(500))); }
                catch (BrahmaputraException) { /* keep polling until the deadline */ }
            }
            Check($"{name}: consumes every record", collected.Count == 20, $"got {collected.Count}");
        }

        Section("bounded client buffer");
        {
            string bufferTopic = Unique("dotnet-buffer");
            var config = ProducerConf(address);
            config.LingerMs = 10_000; // never flush on time during this check
            config.BufferMemory = 2048;
            config.MaxBlockMs = 300;
            using var producer = new Producer(config);
            bool blocked = false;
            for (int i = 0; i < 500 && !blocked; i++)
            {
                try { producer.SendTo(bufferTopic, 0, new byte[256]); }
                catch (BufferFullException e) { blocked = e.Message.Contains("buffer full"); }
            }
            Check("a full buffer blocks and then reports", blocked);
        }

        Section("wire edge cases");
        {
            string edgeTopic = Unique("dotnet-edge");
            byte[] large = new byte[1 << 20];
            for (int i = 0; i < large.Length; i++) large[i] = (byte)(i * 7);
            byte[] unicodeKey = B("ключ-✓-🔑");
            byte[] unicodeValue = B("значение — 数据 — 🚀");
            using (var producer = new Producer(ProducerConf(address)))
            {
                producer.SendTo(edgeTopic, 0, large);
                producer.SendTo(edgeTopic, 0, unicodeValue, unicodeKey, new RecordHeader("ünïcødé-🏷", B("✓")));
                // An empty key and an empty header value are values, not nulls.
                producer.SendTo(edgeTopic, 0, B("empty-key"), Array.Empty<byte>(),
                    new RecordHeader("empty", Array.Empty<byte>()),
                    new RecordHeader("null", (byte[]?)null));
                producer.SendTo(edgeTopic, 0, B("null-key"));
                producer.Close();
            }
            using var consumer = new Consumer(ConsumerConf(address));
            var got = new List<ConsumeResult>();
            for (long offset = 0; got.Count < 4;)
            {
                var batch = consumer.Fetch(edgeTopic, 0, offset, 500);
                if (batch.Count == 0) break;
                got.AddRange(batch);
                offset = batch[^1].Offset + 1;
            }
            Check("edge records all arrive", got.Count == 4, $"got {got.Count}");
            if (got.Count == 4)
            {
                Check("a 1 MiB value round-trips byte-identical", got[0].Value != null && got[0].Value!.AsSpan().SequenceEqual(large),
                    $"{got[0].Value?.Length} bytes");
                Check("unicode key, value and header key round-trip",
                    got[1].Key != null && got[1].Key!.AsSpan().SequenceEqual(unicodeKey) &&
                    got[1].Value != null && got[1].Value!.AsSpan().SequenceEqual(unicodeValue) &&
                    got[1].Headers.Count == 1 && got[1].Headers[0].Key == "ünïcødé-🏷");
                Check("an empty key stays empty, not null", got[2].Key != null && got[2].Key!.Length == 0, S(got[2].Key));
                Check("an empty header value stays empty, not null",
                    got[2].Headers.Count == 2 && got[2].Headers[0].Value is { Length: 0 } && got[2].Headers[1].Value == null,
                    string.Join(",", got[2].Headers.Select(h => $"{h.Key}={S(h.Value)}")));
                Check("a null key stays null", got[3].Key == null, S(got[3].Key));
            }
        }

        Section("ordering under linger flushes");
        {
            string orderTopic = Unique("dotnet-order");
            var config = ProducerConf(address);
            config.LingerMs = 1;
            config.BatchSize = 256;
            const int total = 5000;
            using (var producer = new Producer(config))
            {
                for (int i = 0; i < total; i++) producer.SendTo(orderTopic, 0, B(i.ToString()));
                producer.Close();
            }
            using var consumer = new Consumer(ConsumerConf(address));
            var values = new List<int>();
            for (long offset = 0; values.Count < total;)
            {
                var batch = consumer.Fetch(orderTopic, 0, offset, 500);
                if (batch.Count == 0) break;
                values.AddRange(batch.Select(r => int.Parse(S(r.Value))));
                offset = batch[^1].Offset + 1;
            }
            int inversions = Enumerable.Range(1, Math.Max(0, values.Count - 1)).Count(i => values[i] < values[i - 1]);
            Check("every record of a partition arrives", values.Count == total, $"got {values.Count}");
            Check("a partition's records keep send order", inversions == 0, $"{inversions} inversions");
        }

        Section("background flush failures are reported");
        {
            var config = ProducerConf(address);
            config.LingerMs = 20;
            var producer = new Producer(config);
            Exception? sendError = null, flushError = null;
            // Partition 999 does not exist, so the background send fails. The
            // delivery task is deliberately not awaited: Flush must report it.
            try { _ = producer.SendTo(Unique("dotnet-bgfail"), 999, B("lost")); }
            catch (Exception e) { sendError = e; }
            Thread.Sleep(300);
            try { producer.Flush(); }
            catch (Exception e) { flushError = e; }
            Check("a failed linger flush surfaces on the next Flush", sendError == null && flushError != null,
                $"send={sendError?.Message ?? "ok"} flush={flushError?.Message ?? "ok"}");
            var closing = System.Threading.Tasks.Task.Run(() => { try { producer.Close(); } catch (Exception) { } });
            Check("Close returns after a failed flush", closing.Wait(TimeSpan.FromSeconds(5)), "hung");
        }

        Section("connection failures");
        {
            // A broker that accepts and never answers must cost an error, not a
            // thread blocked forever.
            var silent = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
            silent.Start();
            var held = new List<System.Net.Sockets.TcpClient>();
            _ = System.Threading.Tasks.Task.Run(async () =>
            {
                try { while (true) { var c = await silent.AcceptTcpClientAsync(); lock (held) held.Add(c); } }
                catch (Exception) { /* listener stopped */ }
            });
            string silentAddress = $"127.0.0.1:{((System.Net.IPEndPoint)silent.LocalEndpoint).Port}";
            using (var conn = BrokerConnection.Connect(silentAddress, "dotnet-test", TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(300)))
            {
                var started = Stopwatch.StartNew();
                Exception? requestError = null;
                try { conn.ApiVersions(); }
                catch (Exception e) { requestError = e; }
                Check("a request to an unresponsive broker times out",
                    requestError != null && started.Elapsed < TimeSpan.FromSeconds(3), requestError?.Message ?? "answered");
                Check("a timed-out connection is not reused", conn.IsBroken);
            }
            silent.Stop();
            lock (held) foreach (var c in held) c.Dispose();

            // A connection the broker drops is redialled, not kept forever.
            using var proxy = new Proxy(address);
            string dropTopic = Unique("dotnet-drop");
            var producer = new Producer(ProducerConf(proxy.Address));
            producer.SendTo(dropTopic, 0, B("before")).GetAwaiter().GetResult();
            proxy.DropAll();
            Exception? recovered = new Exception("not attempted");
            for (int attempt = 0; attempt < 3 && recovered != null; attempt++)
            {
                try { producer.SendTo(dropTopic, 0, B("after")).GetAwaiter().GetResult(); recovered = null; }
                catch (Exception e) { recovered = e; }
            }
            Check("a producer recovers after its connection drops", recovered == null, recovered?.Message ?? "");
            producer.Dispose();

            using var consumer = new Consumer(ConsumerConf(proxy.Address));
            consumer.Fetch(dropTopic, 0, 0, 100);
            proxy.DropAll();
            Exception? fetchError = new Exception("not attempted");
            IReadOnlyList<ConsumeResult> fetched = Array.Empty<ConsumeResult>();
            for (int attempt = 0; attempt < 3 && fetchError != null; attempt++)
            {
                try { fetched = consumer.Fetch(dropTopic, 0, 0, 100); fetchError = null; }
                catch (Exception e) { fetchError = e; }
            }
            Check("a consumer recovers after its connection drops", fetchError == null && fetched.Count >= 1,
                fetchError?.Message ?? $"{fetched.Count} records");
        }

        Section("consumer group: max.poll.interval and rejoin");
        {
            string slowTopic = Unique("dotnet-slow");
            var producer = new Producer(ProducerConf(address));
            for (int i = 0; i < 10; i++) producer.Send(slowTopic, B($"s{i}"));
            producer.Flush();
            var groupConfig = GroupConf(address, Unique("dotnet-slow-grp"));
            groupConfig.MaxPollIntervalMs = 1500;
            using var consumer = new GroupConsumer(groupConfig);
            consumer.Subscribe(new[] { slowTopic });
            var first = new List<ConsumeResult>();
            var deadline = Stopwatch.StartNew();
            while (first.Count < 10 && deadline.Elapsed < TimeSpan.FromSeconds(15))
            {
                try { first.AddRange(consumer.Poll(TimeSpan.FromMilliseconds(300))); }
                catch (BrahmaputraException) { break; }
            }
            consumer.Commit();
            // Stall past max.poll.interval.ms: the member leaves the group.
            Thread.Sleep(2500);
            for (int i = 10; i < 20; i++) producer.Send(slowTopic, B($"s{i}"));
            producer.Close();
            var second = new List<ConsumeResult>();
            Exception? pollError = null;
            deadline.Restart();
            while (second.Count < 10 && deadline.Elapsed < TimeSpan.FromSeconds(15))
            {
                try { second.AddRange(consumer.Poll(TimeSpan.FromMilliseconds(300))); }
                catch (BrahmaputraException e) { pollError = e; break; }
            }
            Check("a member that stalled rejoins on its next poll",
                first.Count == 10 && second.Count == 10 && pollError == null,
                $"first={first.Count} second={second.Count} err={pollError?.Message ?? "none"}");
        }

        Section("consumer group: time inside poll does not count against max.poll.interval");
        {
            string joinTopic = Unique("dotnet-inpoll");
            var producer = new Producer(ProducerConf(address));
            producer.Router.Partitions(joinTopic);
            var groupConfig = GroupConf(address, Unique("dotnet-inpoll-grp"));
            // Far shorter than the first poll below, which spends ~1s joining
            // (the broker's initial rebalance delay) and then waits for data.
            groupConfig.MaxPollIntervalMs = 600;
            using var consumer = new GroupConsumer(groupConfig);
            consumer.Subscribe(new[] { joinTopic });
            var sender = System.Threading.Tasks.Task.Run(() =>
            {
                Thread.Sleep(2000);
                for (int i = 0; i < 10; i++) producer.Send(joinTopic, B($"j{i}"));
            });
            // One long poll: it joins, then waits for the records above.
            IReadOnlyList<ConsumeResult> got = Array.Empty<ConsumeResult>();
            Exception? pollError = null, commitError = null;
            try { got = consumer.Poll(TimeSpan.FromSeconds(4)); }
            catch (Exception e) { pollError = e; }
            // Committed straight away, before another poll could quietly
            // rejoin: this fails if the member left the group mid-poll.
            try { consumer.Commit(); }
            catch (Exception e) { commitError = e; }
            Check("a member is still in its group after a long poll",
                pollError == null && got.Count > 0 && commitError == null,
                $"got={got.Count} poll={pollError?.Message ?? "ok"} commit={commitError?.Message ?? "ok"}");
            sender.Wait();
            producer.Close();
        }
    }
}

/// <summary>
/// Forwards TCP to the broker and can sever every live connection, which is
/// how a broker restart or an idle timeout looks to a client.
/// </summary>
internal sealed class Proxy : IDisposable
{
    private readonly System.Net.Sockets.TcpListener _listener;
    private readonly List<System.Net.Sockets.TcpClient> _live = new();

    public string Address { get; }

    public Proxy(string target)
    {
        int colon = target.LastIndexOf(':');
        string host = target[..colon];
        int port = int.Parse(target[(colon + 1)..]);
        _listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        _listener.Start();
        Address = $"127.0.0.1:{((System.Net.IPEndPoint)_listener.LocalEndpoint).Port}";
        _ = System.Threading.Tasks.Task.Run(async () =>
        {
            while (true)
            {
                System.Net.Sockets.TcpClient client;
                try { client = await _listener.AcceptTcpClientAsync(); }
                catch (Exception) { return; }
                var upstream = new System.Net.Sockets.TcpClient();
                try { await upstream.ConnectAsync(host, port); }
                catch (Exception) { client.Dispose(); upstream.Dispose(); continue; }
                lock (_live) { _live.Add(client); _live.Add(upstream); }
                _ = Pipe(client, upstream);
                _ = Pipe(upstream, client);
            }
        });
    }

    private static async System.Threading.Tasks.Task Pipe(System.Net.Sockets.TcpClient from, System.Net.Sockets.TcpClient to)
    {
        try { await from.GetStream().CopyToAsync(to.GetStream()); }
        catch (Exception) { /* either side closed */ }
        to.Dispose();
    }

    public void DropAll()
    {
        lock (_live)
        {
            foreach (var c in _live) c.Dispose();
            _live.Clear();
        }
        Thread.Sleep(50);
    }

    public void Dispose()
    {
        _listener.Stop();
        DropAll();
    }
}
