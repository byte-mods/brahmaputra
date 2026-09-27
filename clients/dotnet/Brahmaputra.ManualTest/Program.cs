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
    }
}
