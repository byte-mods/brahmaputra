/**
 * End-to-end suite for the D driver against a live broker — a port of the
 * Go suite (clients/go/cmd/manualtest) with the same sections and checks.
 *
 *     manual_test HOST PORT
 *
 * Every check asserts a property of the system, not that a function ran.
 */
module manual_test;

import brahmaputra;

import core.atomic : atomicLoad, atomicStore;
import core.stdc.stdlib : exit;
import core.sync.mutex : Mutex;
import core.thread : Thread;
import core.time : Duration, MonoTime, msecs, seconds;
import std.algorithm.searching : canFind, startsWith;
import std.array : replicate;
import std.conv : to;
import std.format : format;
import std.socket;
import std.stdio : stdout, writefln, writeln;

__gshared int passed;
__gshared int failed;

void check(string name, bool ok, string detail = "")
{
    if (ok)
    {
        passed++;
        writefln("  ok   %s", name);
    }
    else
    {
        failed++;
        if (detail.length)
            writefln("  FAIL %s: %s", name, detail);
        else
            writefln("  FAIL %s", name);
    }
    stdout.flush();
}

void section(string title)
{
    writefln("\n%s", title);
}

string unique(string prefix)
{
    return format("%s-%d", prefix, (MonoTime.currTime.ticks + nowMillis() * 1_000_000) % 1_000_000_000);
}

/// Runs `fn`; a failure here is fatal to the suite, as in the Go `must`.
T must(T)(lazy T value)
{
    try
        return value;
    catch (Exception e)
    {
        writefln("  FATAL %s", e.msg);
        stdout.flush();
        exit(2);
        assert(0);
    }
}

void mustDo(lazy void action)
{
    try
        action;
    catch (Exception e)
    {
        writefln("  FATAL %s", e.msg);
        stdout.flush();
        exit(2);
    }
}

const(ubyte)[] b(string text)
{
    return toBytes(text);
}

string errText(Exception e)
{
    return e is null ? "null" : e.msg;
}

ProducerConfig immediate()
{
    ProducerConfig config;
    config.lingerMs = 0;
    return config;
}

GroupConfig manualCommit()
{
    GroupConfig config;
    config.autoCommitIntervalMs = 0;
    return config;
}

ConsumedRecord[] pollQuietly(GroupConsumer consumer, Duration timeout)
{
    try
        return consumer.poll(timeout);
    catch (Exception)
        return null;
}

int main(string[] args)
{
    string address = "127.0.0.1:9092";
    if (args.length > 2)
        address = args[1] ~ ":" ~ args[2];
    else if (args.length > 1)
        address = args[1].canFind(':') ? args[1] : "127.0.0.1:" ~ args[1];

    section("connection and metadata");
    {
        auto consumer = must(new Consumer(address));
        scope (exit)
            consumer.close();
        ApiVersionsResult versions;
        Exception err;
        try
            versions = consumer.router.seed.apiVersions();
        catch (Exception e)
            err = e;
        check("ApiVersions answers", err is null && versions.ranges.length > 0, errText(err));
        check("broker reports a version", versions.brokerVersion.length > 0,
            versions.brokerVersion);
        auto metadata = must(consumer.router.metadata(null, true));
        check("metadata lists brokers", metadata.brokers.length >= 1,
            format("%d brokers", metadata.brokers.length));
    }

    section("produce and consume round trip");
    const topic = unique("d-roundtrip");
    const(ubyte)[][] payloads;
    foreach (i; 0 .. 50)
        payloads ~= b(format("record-%d", i));
    {
        auto producer = must(new Producer(address, immediate()));
        foreach (payload; payloads)
            mustDo(producer.sendTo(topic, 0, payload));
        mustDo(producer.flush());
        mustDo(producer.close());
    }
    {
        auto consumer = must(new Consumer(address));
        auto got = must(consumer.fetch(topic, 0, 0, 500));
        check("every record comes back", got.length == payloads.length,
            format("got %d", got.length));
        bool identical = got.length == payloads.length;
        for (size_t i = 0; identical && i < got.length; i++)
            if (got[i].value != payloads[i] || got[i].offset != cast(long) i)
                identical = false;
        check("values byte-identical and offsets contiguous", identical);
        consumer.close();
    }

    section("compression codecs");
    // Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
    // registerCodec.
    foreach (codec; ["none", "gzip"])
    {
        const codecTopic = unique("d-" ~ codec);
        const body_ = b(replicate("the same line over and over. ", 40));
        auto config = immediate();
        config.compressionType = codec;
        auto producer = must(new Producer(address, config));
        foreach (i; 0 .. 20)
            mustDo(producer.sendTo(codecTopic, 0, body_ ~ cast(ubyte)('0' + i % 10)));
        mustDo(producer.flush());
        mustDo(producer.close());

        auto consumer = must(new Consumer(address));
        auto got = must(consumer.fetch(codecTopic, 0, 0, 500));
        check(codec ~ ": round trips", got.length == 20 && got[0].value.startsWith(body_),
            format("got %d records", got.length));
        consumer.close();
    }

    section("keys, partitioning and ordering");
    {
        const keyTopic = unique("d-keys");
        auto producer = must(new Producer(address, immediate()));
        auto partitions = must(producer.router.partitions(keyTopic));
        foreach (i; 0 .. 30)
            mustDo(producer.send(keyTopic, b(format("v%d", i)), b("user-7")));
        mustDo(producer.flush());
        mustDo(producer.close());

        const target = partitionForKey(b("user-7"), partitions);
        auto consumer = must(new Consumer(address));
        auto onTarget = must(consumer.fetch(keyTopic, target, 0, 500));
        check("a key pins every record to one partition", onTarget.length == 30,
            format("partition %d holds %d of 30", target, onTarget.length));

        bool ordered = onTarget.length == 30;
        for (size_t i = 0; ordered && i < onTarget.length; i++)
            if (cast(const(char)[]) onTarget[i].value != format("v%d", i))
                ordered = false;
        check("per-key order is preserved", ordered);

        size_t strays = 0;
        foreach (partition; partitions)
        {
            if (partition == target)
                continue;
            strays += must(consumer.fetch(keyTopic, partition, 0, 200)).length;
        }
        check("no keyed record landed elsewhere", strays == 0, format("%d strays", strays));
        consumer.close();
    }

    section("murmur2 agrees with the broker's partitioner");
    check(`murmur2("") is stable`, murmur2(null) == 275646681, murmur2(null).to!string);
    check("murmur2 is deterministic", murmur2(b("user-7")) == murmur2(b("user-7")));
    check("different keys hash differently", murmur2(b("user-7")) != murmur2(b("user-8")));

    section("record headers and timestamps");
    {
        const headerTopic = unique("d-headers");
        const before = nowMillis() - 1000;
        auto producer = must(new Producer(address, immediate()));
        mustDo(producer.sendTo(headerTopic, 0, b("annotated"), null, [
                RecordHeader("trace-id", b("abc-123")),
                RecordHeader("content-type", b("application/json")),
                RecordHeader("tombstone-reason", null),
            ]));
        mustDo(producer.sendTo(headerTopic, 0, b("plain")));
        mustDo(producer.flush());
        mustDo(producer.close());
        const after = nowMillis() + 1000;

        auto consumer = must(new Consumer(address));
        auto got = must(consumer.fetch(headerTopic, 0, 0, 500));
        check("both records arrive", got.length == 2, format("got %d", got.length));
        if (got.length == 2)
        {
            auto annotated = got[0];
            auto plain = got[1];
            check("headers survive the round trip", annotated.headers.length == 3,
                format("%d headers", annotated.headers.length));
            check("header values are exact", annotated.header("trace-id") == b("abc-123"));
            check("a null header value stays null",
                annotated.headers.length == 3 && annotated.headers[2].value is null);
            check("a record with no headers gains none from its batch",
                plain.headers.length == 0, format("%d headers", plain.headers.length));
            bool inWindow = true;
            foreach (ref record; got)
                if (record.timestamp < before || record.timestamp > after)
                    inWindow = false;
            check("timestamps are real wall-clock values", inWindow,
                format("%d,%d outside %d..%d", got[0].timestamp, got[1].timestamp, before, after));
        }
        consumer.close();
    }

    section("tombstones");
    {
        const tombTopic = unique("d-tombstones");
        auto producer = must(new Producer(address, immediate()));
        mustDo(producer.sendTo(tombTopic, 0, b("set"), b("k1")));
        mustDo(producer.sendTo(tombTopic, 0, emptyBytes(), b("k2")));
        // A null value is a deletion, and must stay distinguishable from the
        // empty value above all the way through the round trip.
        mustDo(producer.sendTo(tombTopic, 0, null, b("k3")));
        mustDo(producer.flush());
        mustDo(producer.close());

        auto consumer = must(new Consumer(address));
        auto got = must(consumer.fetch(tombTopic, 0, 0, 500));
        check("all three records arrive", got.length == 3, format("got %d", got.length));
        if (got.length == 3)
        {
            check("an ordinary value round-trips", got[0].value == b("set"));
            check("an empty value is empty, not null",
                got[1].value !is null && got[1].value.length == 0, format("%s", got[1].value));
            check("a tombstone arrives as a null value", got[2].value is null,
                format("%s", got[2].value));
        }
        consumer.close();
    }

    section("offsets");
    {
        auto consumer = must(new Consumer(address));
        const earliest = must(consumer.listOffsets(topic, 0, EARLIEST));
        const latest = must(consumer.listOffsets(topic, 0, LATEST));
        check("earliest is 0 on a fresh topic", earliest == 0, earliest.to!string);
        check("latest equals the record count", latest == 50, latest.to!string);
        consumer.close();
    }

    section("acks");
    foreach (acks; [0, 1, -1])
    {
        const acksTopic = unique(format("d-acks%d", acks));
        auto config = immediate();
        config.acks = acks;
        auto producer = must(new Producer(address, config));
        mustDo(producer.sendTo(acksTopic, 0, b("durable")));
        mustDo(producer.flush());
        mustDo(producer.close());
        Thread.sleep(400.msecs);

        auto consumer = must(new Consumer(address));
        auto got = must(consumer.fetch(acksTopic, 0, 0, 500));
        check(format("acks=%d stores the record", acks), got.length == 1,
            format("got %d", got.length));
        consumer.close();
    }

    section("consumer group: assignment, commit, resume");
    {
        const groupTopic = unique("d-group");
        const groupId = unique("d-billing");
        auto producer = must(new Producer(address, immediate()));
        foreach (i; 0 .. 40)
            mustDo(producer.send(groupTopic, b(format("g%d", i))));
        mustDo(producer.flush());
        mustDo(producer.close());

        auto groupConfig = manualCommit();
        auto consumer = must(new GroupConsumer(address, groupId, groupConfig));
        consumer.subscribe([groupTopic]);

        ConsumedRecord[] seen;
        auto deadline = MonoTime.currTime + 30.seconds;
        while (seen.length < 40 && MonoTime.currTime < deadline)
            seen ~= must(consumer.poll(500.msecs));
        check("the group consumes every record", seen.length == 40,
            format("got %d", seen.length));

        bool[string] distinct;
        foreach (ref record; seen)
            distinct[format("%d-%d", record.partition, record.offset)] = true;
        check("no record is delivered twice", distinct.length == seen.length);

        mustDo(consumer.commit());
        auto committedOffsets = must(consumer.committed());
        long total = 0;
        foreach (offset; committedOffsets.byValue)
            total += offset;
        check("commit records a position", total == 40, total.to!string);
        mustDo(consumer.close());

        // A second consumer in the same group must resume, not replay.
        auto rejoined = must(new GroupConsumer(address, groupId, groupConfig));
        rejoined.subscribe([groupTopic]);
        ConsumedRecord[] replayed;
        const until = MonoTime.currTime + 5.seconds;
        while (MonoTime.currTime < until)
            replayed ~= pollQuietly(rejoined, 300.msecs);
        check("a rejoining group resumes from its commit", replayed.length == 0,
            format("replayed %d records it had already committed", replayed.length));
        mustDo(rejoined.close());
    }

    section("auto.offset.reset");
    {
        const resetTopic = unique("d-reset");
        auto producer = must(new Producer(address, immediate()));
        foreach (i; 0 .. 10)
            mustDo(producer.send(resetTopic, b(format("r%d", i))));
        mustDo(producer.flush());
        mustDo(producer.close());

        auto latestConfig = manualCommit();
        latestConfig.autoOffsetReset = AUTO_OFFSET_RESET_LATEST;
        auto consumer = must(new GroupConsumer(address, unique("d-latest"), latestConfig));
        consumer.subscribe([resetTopic]);
        ConsumedRecord[] skipped;
        auto until = MonoTime.currTime + 4.seconds;
        while (MonoTime.currTime < until)
            skipped ~= pollQuietly(consumer, 300.msecs);
        check("latest skips records produced before the group existed",
            skipped.length == 0, format("saw %d", skipped.length));
        mustDo(consumer.close());

        auto noneConfig = manualCommit();
        noneConfig.autoOffsetReset = AUTO_OFFSET_RESET_NONE;
        auto strict = must(new GroupConsumer(address, unique("d-none"), noneConfig));
        strict.subscribe([resetTopic]);
        bool raised = false;
        until = MonoTime.currTime + 5.seconds;
        while (MonoTime.currTime < until && !raised)
        {
            try
                strict.poll(300.msecs);
            catch (NoOffsetForPartitionException)
                raised = true;
            catch (Exception e)
                raised = e.msg.canFind("no committed offset");
        }
        check("none refuses to guess a position", raised);
        mustDo(strict.close());
    }

    section("assignors");
    foreach (assignor; [ASSIGNOR_RANGE, ASSIGNOR_ROUND_ROBIN, ASSIGNOR_STICKY])
    {
        const assignorTopic = unique("d-" ~ assignor);
        auto producer = must(new Producer(address, immediate()));
        foreach (i; 0 .. 20)
            mustDo(producer.send(assignorTopic, b(format("a%d", i))));
        mustDo(producer.flush());
        mustDo(producer.close());

        auto groupConfig = manualCommit();
        groupConfig.assignor = assignor;
        auto consumer = must(new GroupConsumer(address, unique("d-grp-" ~ assignor), groupConfig));
        consumer.subscribe([assignorTopic]);
        ConsumedRecord[] collected;
        const deadline = MonoTime.currTime + 20.seconds;
        while (collected.length < 20 && MonoTime.currTime < deadline)
            collected ~= pollQuietly(consumer, 500.msecs);
        check(assignor ~ ": consumes every record", collected.length == 20,
            format("got %d", collected.length));
        mustDo(consumer.close());
    }

    section("bounded client buffer");
    {
        const bufferTopic = unique("d-buffer");
        ProducerConfig config;
        config.lingerMs = 10_000; // never flush on time during this check
        config.bufferMemory = 2048;
        config.maxBlockMs = 300;
        auto producer = must(new Producer(address, config));
        bool blocked = false;
        const big = b(replicate("x", 256));
        for (int i = 0; i < 500 && !blocked; i++)
        {
            try
                producer.sendTo(bufferTopic, 0, big);
            catch (BufferFullException e)
                blocked = e.msg.canFind("buffer full");
        }
        check("a full buffer blocks and then reports", blocked);
        try
            producer.close();
        catch (Exception)
        {
        }
    }

    section("wire edge cases");
    {
        const edgeTopic = unique("d-edge");
        auto producer = must(new Producer(address, immediate()));
        auto large = new ubyte[1 << 20];
        foreach (i, ref x; large)
            x = cast(ubyte)(i * 7);
        const unicodeKey = b("ключ-✓-🔑");
        const unicodeValue = b("значение — 数据 — 🚀");
        mustDo(producer.sendTo(edgeTopic, 0, large));
        mustDo(producer.sendTo(edgeTopic, 0, unicodeValue, unicodeKey,
                [RecordHeader("ünïcødé-🏷", b("✓"))]));
        // An empty key and an empty header value are values, not nulls.
        mustDo(producer.sendTo(edgeTopic, 0, b("empty-key"), emptyBytes(), [
                    RecordHeader("empty", emptyBytes()), RecordHeader("null", null)
                ]));
        mustDo(producer.sendTo(edgeTopic, 0, b("null-key"), null));
        mustDo(producer.close());

        auto consumer = must(new Consumer(address));
        ConsumedRecord[] got;
        long offset = 0;
        while (got.length < 4)
        {
            ConsumedRecord[] batch;
            try
                batch = consumer.fetch(edgeTopic, 0, offset, 500);
            catch (Exception)
                break;
            if (batch.length == 0)
                break;
            got ~= batch;
            offset = batch[$ - 1].offset + 1;
        }
        check("edge records all arrive", got.length == 4, format("got %d", got.length));
        if (got.length == 4)
        {
            check("a 1 MiB value round-trips byte-identical", got[0].value == large,
                format("%d bytes", got[0].value.length));
            check("unicode key, value and header key round-trip",
                got[1].key == unicodeKey && got[1].value == unicodeValue
                    && got[1].headers.length == 1 && got[1].headers[0].key == "ünïcødé-🏷");
            check("an empty key stays empty, not null",
                got[2].key !is null && got[2].key.length == 0, format("%s", got[2].key));
            check("an empty header value stays empty, not null",
                got[2].headers.length == 2 && got[2].headers[0].value !is null
                    && got[2].headers[0].value.length == 0 && got[2].headers[1].value is null,
                format("%s", got[2].headers));
            check("a null key stays null", got[3].key is null, format("%s", got[3].key));
        }
        consumer.close();
    }

    section("ordering under linger flushes");
    {
        const orderTopic = unique("d-order");
        ProducerConfig config;
        config.lingerMs = 1;
        config.batchSize = 256;
        auto producer = must(new Producer(address, config));
        enum total = 5000;
        foreach (i; 0 .. total)
            mustDo(producer.sendTo(orderTopic, 0, b(i.to!string)));
        mustDo(producer.close());
        auto consumer = must(new Consumer(address));
        int[] values;
        long offset = 0;
        while (values.length < total)
        {
            ConsumedRecord[] batch;
            try
                batch = consumer.fetch(orderTopic, 0, offset, 500);
            catch (Exception)
                break;
            if (batch.length == 0)
                break;
            foreach (ref record; batch)
            {
                int value;
                try
                    value = (cast(const(char)[]) record.value).to!int;
                catch (Exception)
                    value = 0;
                values ~= value;
            }
            offset = batch[$ - 1].offset + 1;
        }
        int inversions = 0;
        foreach (i; 1 .. values.length)
            if (values[i] < values[i - 1])
                inversions++;
        check("every record of a partition arrives", values.length == total,
            format("got %d", values.length));
        check("a partition's records keep send order", inversions == 0,
            format("%d inversions", inversions));
        consumer.close();
    }

    section("background flush failures are reported");
    {
        ProducerConfig config;
        config.lingerMs = 20;
        auto producer = must(new Producer(address, config));
        // Partition 999 does not exist, so the linger thread's flush fails.
        Exception sendErr;
        try
            producer.sendTo(unique("d-bgfail"), 999, b("lost"));
        catch (Exception e)
            sendErr = e;
        Thread.sleep(300.msecs);
        Exception flushErr;
        try
            producer.flush();
        catch (Exception e)
            flushErr = e;
        check("a failed linger flush surfaces on the next Flush",
            sendErr is null && flushErr !is null,
            format("send=%s flush=%s", errText(sendErr), errText(flushErr)));

        shared bool closeReturned = false;
        auto closer = new Thread({
            try
                producer.close();
            catch (Exception)
            {
            }
            atomicStore(closeReturned, true);
        });
        closer.isDaemon = true;
        closer.start();
        const until = MonoTime.currTime + 5.seconds;
        while (!atomicLoad(closeReturned) && MonoTime.currTime < until)
            Thread.sleep(10.msecs);
        check("Close returns after a failed flush", atomicLoad(closeReturned),
            atomicLoad(closeReturned) ? "" : "hung");
    }

    section("connection failures");
    {
        // A broker that accepts and never answers must cost an error, not a
        // thread blocked forever.
        auto silent = new SilentBroker();
        {
            auto conn = must(Connection.dial(silent.address, "d-test", 1.seconds));
            conn.setRequestTimeout(300.msecs);
            const started = MonoTime.currTime;
            Exception requestErr;
            try
                conn.apiVersions();
            catch (Exception e)
                requestErr = e;
            check("a request to an unresponsive broker times out",
                requestErr !is null && MonoTime.currTime - started < 3.seconds,
                errText(requestErr));
            check("a timed-out connection is not reused", conn.broken);
            conn.close();
        }
        silent.close();

        // A connection the broker drops is redialled, not kept forever.
        auto proxy = new Proxy(address);
        const dropTopic = unique("d-drop");
        auto producer = must(new Producer(proxy.address, immediate()));
        mustDo(producer.sendTo(dropTopic, 0, b("before")));
        proxy.dropAll();
        Exception recovered = new Exception("not attempted");
        for (int attempt = 0; attempt < 3 && recovered !is null; attempt++)
        {
            try
            {
                producer.sendTo(dropTopic, 0, b("after"));
                recovered = null;
            }
            catch (Exception e)
                recovered = e;
        }
        check("a producer recovers after its connection drops", recovered is null,
            errText(recovered));
        try
            producer.close();
        catch (Exception)
        {
        }
        auto consumer = must(new Consumer(proxy.address));
        must(consumer.fetch(dropTopic, 0, 0, 100));
        proxy.dropAll();
        Exception fetchErr = new Exception("not attempted");
        ConsumedRecord[] fetched;
        for (int attempt = 0; attempt < 3 && fetchErr !is null; attempt++)
        {
            try
            {
                fetched = consumer.fetch(dropTopic, 0, 0, 100);
                fetchErr = null;
            }
            catch (Exception e)
                fetchErr = e;
        }
        check("a consumer recovers after its connection drops",
            fetchErr is null && fetched.length >= 1, errText(fetchErr));
        consumer.close();
        proxy.close();
    }

    section("consumer group: max.poll.interval and rejoin");
    {
        const slowTopic = unique("d-slow");
        auto producer = must(new Producer(address, immediate()));
        foreach (i; 0 .. 10)
            mustDo(producer.send(slowTopic, b(format("s%d", i))));
        auto groupConfig = manualCommit();
        groupConfig.maxPollIntervalMs = 1500;
        auto consumer = must(new GroupConsumer(address, unique("d-slow-grp"), groupConfig));
        consumer.subscribe([slowTopic]);
        ConsumedRecord[] first;
        auto deadline = MonoTime.currTime + 15.seconds;
        while (first.length < 10 && MonoTime.currTime < deadline)
        {
            try
                first ~= consumer.poll(300.msecs);
            catch (Exception)
                break;
        }
        mustDo(consumer.commit());
        // Stall past max.poll.interval.ms: the member leaves the group.
        Thread.sleep(2500.msecs);
        foreach (i; 10 .. 20)
            mustDo(producer.send(slowTopic, b(format("s%d", i))));
        mustDo(producer.close());
        ConsumedRecord[] second;
        Exception pollErr;
        deadline = MonoTime.currTime + 15.seconds;
        while (second.length < 10 && MonoTime.currTime < deadline)
        {
            try
                second ~= consumer.poll(300.msecs);
            catch (Exception e)
            {
                pollErr = e;
                break;
            }
        }
        check("a member that stalled rejoins on its next poll",
            first.length == 10 && second.length == 10 && pollErr is null,
            format("first=%d second=%d err=%s", first.length, second.length,
                pollErr is null ? "none" : pollErr.msg));
        mustDo(consumer.close());
    }

    section("consumer group: time inside poll does not count against max.poll.interval");
    {
        const joinTopic = unique("d-inpoll");
        auto producer = must(new Producer(address, immediate()));
        must(producer.router.partitions(joinTopic));
        auto groupConfig = manualCommit();
        // Far shorter than the first poll below, which spends ~1s joining
        // (the broker's initial rebalance delay) and then waits for data.
        groupConfig.maxPollIntervalMs = 600;
        auto consumer = must(new GroupConsumer(address, unique("d-inpoll-grp"), groupConfig));
        consumer.subscribe([joinTopic]);
        auto sender = new Thread({
            Thread.sleep(2.seconds);
            foreach (i; 0 .. 10)
            {
                try
                    producer.send(joinTopic, b(format("j%d", i)));
                catch (Exception)
                {
                }
            }
        });
        sender.start();
        // One long poll: it joins, then waits for the records above.
        ConsumedRecord[] got;
        Exception pollErr;
        try
            got = consumer.poll(4.seconds);
        catch (Exception e)
            pollErr = e;
        // Committed straight away, before another poll could quietly
        // rejoin: this fails if the member left the group mid-poll.
        Exception commitErr;
        try
            consumer.commit();
        catch (Exception e)
            commitErr = e;
        check("a member is still in its group after a long poll",
            pollErr is null && got.length > 0 && commitErr is null,
            format("got=%d poll=%s commit=%s", got.length, errText(pollErr), errText(commitErr)));
        sender.join();
        mustDo(consumer.close());
        mustDo(producer.close());
    }

    writefln("\n%d passed, %d failed", passed, failed);
    stdout.flush();
    return failed > 0 ? 1 : 0;
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

private TcpSocket listenLocal()
{
    auto listener = new TcpSocket();
    listener.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, true);
    listener.bind(new InternetAddress("127.0.0.1", InternetAddress.PORT_ANY));
    listener.listen(64);
    return listener;
}

/// Accepts connections and reads them forever without answering.
final class SilentBroker
{
    private TcpSocket listener;
    private shared bool stopping;
    private Thread thread;
    string address;

    this()
    {
        listener = listenLocal();
        address = "127.0.0.1:" ~ listener.localAddress.toPortString;
        thread = new Thread(&run);
        thread.isDaemon = true;
        thread.start();
    }

    private void run()
    {
        Socket[] conns;
        auto readable = new SocketSet();
        ubyte[4096] sink;
        while (!atomicLoad(stopping))
        {
            readable.reset();
            readable.add(listener);
            foreach (c; conns)
                readable.add(c);
            if (Socket.select(readable, null, null, 50.msecs) <= 0)
                continue;
            if (readable.isSet(listener))
                conns ~= listener.accept();
            Socket[] alive;
            foreach (c; conns)
            {
                if (readable.isSet(c) && c.receive(sink[]) <= 0)
                    c.close();
                else
                    alive ~= c;
            }
            conns = alive;
        }
        foreach (c; conns)
            c.close();
        listener.close();
    }

    void close()
    {
        atomicStore(stopping, true);
        thread.join();
    }
}

/// Forwards TCP to the broker and can sever every live connection, which
/// is how a broker restart or an idle timeout looks to a client.
final class Proxy
{
    private TcpSocket listener;
    private string target;
    private shared bool stopping;
    private Thread acceptThread;
    private Mutex mu;
    private Socket[] live;
    string address;

    this(string target)
    {
        this.target = target;
        this.mu = new Mutex;
        listener = listenLocal();
        address = "127.0.0.1:" ~ listener.localAddress.toPortString;
        acceptThread = new Thread(&acceptLoop);
        acceptThread.isDaemon = true;
        acceptThread.start();
    }

    private void acceptLoop()
    {
        auto readable = new SocketSet();
        while (!atomicLoad(stopping))
        {
            readable.reset();
            readable.add(listener);
            if (Socket.select(readable, null, null, 50.msecs) <= 0)
                continue;
            Socket client;
            try
                client = listener.accept();
            catch (Exception)
                continue;
            Socket upstream;
            try
            {
                string host;
                ushort port;
                splitAddress(target, host, port);
                upstream = new TcpSocket(new InternetAddress(host, port));
            }
            catch (Exception)
            {
                client.close();
                continue;
            }
            {
                mu.lock();
                scope (exit)
                    mu.unlock();
                live ~= client;
                live ~= upstream;
            }
            auto pump = new Thread(() => pumpPair(client, upstream));
            pump.isDaemon = true;
            pump.start();
        }
        listener.close();
    }

    // One thread per connection pair; it alone closes both sockets, so
    // dropAll only has to shut them down.
    private static void pumpPair(Socket a, Socket b)
    {
        auto readable = new SocketSet();
        auto buf = new ubyte[64 * 1024];
        bool open = true;
        while (open)
        {
            readable.reset();
            readable.add(a);
            readable.add(b);
            const n = Socket.select(readable, null, null, 100.msecs);
            if (n < 0)
                continue;
            foreach (pair; [[a, b], [b, a]])
            {
                if (!open || !readable.isSet(pair[0]))
                    continue;
                const got = pair[0].receive(buf);
                if (got <= 0 || !sendAll(pair[1], buf[0 .. got]))
                    open = false;
            }
            if (!a.isAlive || !b.isAlive)
                open = false;
        }
        foreach (s; [a, b])
        {
            try
                s.shutdown(SocketShutdown.BOTH);
            catch (Exception)
            {
            }
            s.close();
        }
    }

    private static bool sendAll(Socket s, const(ubyte)[] data)
    {
        while (data.length > 0)
        {
            const n = s.send(data, cast(SocketFlags) 0x4000); // MSG_NOSIGNAL
            if (n <= 0)
                return false;
            data = data[n .. $];
        }
        return true;
    }

    void dropAll()
    {
        {
            mu.lock();
            scope (exit)
                mu.unlock();
            foreach (s; live)
            {
                try
                    s.shutdown(SocketShutdown.BOTH);
                catch (Exception)
                {
                }
            }
            live = null;
        }
        Thread.sleep(50.msecs);
    }

    void close()
    {
        atomicStore(stopping, true);
        acceptThread.join();
        dropAll();
    }
}
