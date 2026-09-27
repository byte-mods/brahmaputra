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

    runExtra(address);

    writefln("\n%d passed, %d failed", passed, failed);
    stdout.flush();
    return failed > 0 ? 1 : 0;
}


long elapsedMs(MonoTime since)
{
    return (MonoTime.currTime - since).total!"msecs";
}

/// Polls until `want` records arrive or `limit` passes.
ConsumedRecord[] pollUntil(GroupConsumer consumer, size_t want, Duration limit,
    size_t* largestPoll = null)
{
    ConsumedRecord[] seen;
    const deadline = MonoTime.currTime + limit;
    while (seen.length < want && MonoTime.currTime < deadline)
    {
        auto batch = pollQuietly(consumer, 300.msecs);
        if (largestPoll !is null && batch.length > *largestPoll)
            *largestPoll = batch.length;
        seen ~= batch;
    }
    return seen;
}

long committedTotal(GroupConsumer consumer)
{
    long total;
    foreach (_, offset; consumer.committed())
        total += offset;
    return total;
}

/// Checks beyond the Go suite's 54: one per feature of the client contract
/// that those do not already exercise.
void runExtra(string address)
{
    section("producer: explicit partition, timestamp and synchronous send");
    {
        const t = unique("d-sync");
        auto producer = must(new Producer(address, immediate()));
        long[] offsets;
        foreach (i; 0 .. 3)
            offsets ~= must(producer.sendSync(ProducerRecord(t, 0, null, b(format("sync-%d", i)))));
        check("sendSync returns each record's offset", offsets == [0L, 1, 2], format("%s", offsets));
        ProducerRecord stamped = ProducerRecord(t, 2, null, b("stamped"));
        stamped.timestampMs = 1_600_000_000_123;
        mustDo(producer.send(stamped));
        mustDo(producer.close());

        auto consumer = must(new Consumer(address));
        auto onTwo = must(consumer.fetch(t, 2, 0, 500));
        check("an explicit partition is honoured",
            onTwo.length == 1 && must(consumer.fetch(t, 0, 0, 500)).length == 3,
            format("partition 2 holds %d", onTwo.length));
        check("an explicit timestamp survives the round trip",
            onTwo.length == 1 && onTwo[0].timestamp == 1_600_000_000_123,
            onTwo.length ? format("%d", onTwo[0].timestamp) : "no record");
        consumer.close();
    }

    section("producer: round-robin for records without a key");
    {
        const t = unique("d-rr");
        auto producer = must(new Producer(address, immediate()));
        auto partitions = must(producer.router.partitions(t));
        foreach (i; 0 .. 8)
            mustDo(producer.send(t, b(format("rr%d", i))));
        mustDo(producer.close());
        auto consumer = must(new Consumer(address));
        size_t[] counts;
        foreach (partition; partitions)
            counts ~= must(consumer.fetch(t, partition, 0, 300)).length;
        check("unkeyed records are spread evenly over every partition",
            counts == [2UL, 2, 2, 2], format("%s", counts));
        consumer.close();
    }

    section("producer: batch.size, linger.ms and close");
    {
        auto consumer = must(new Consumer(address));
        const full = unique("d-batchfull");
        auto config = immediate();
        config.lingerMs = 60_000;
        config.batchSize = 64;
        auto eager = must(new Producer(address, config));
        mustDo(eager.sendTo(full, 0, new ubyte[100]));
        check("a batch that reaches batch.size is sent without waiting for linger.ms",
            must(consumer.fetch(full, 0, 0, 300)).length == 1);

        const lingering = unique("d-linger");
        config.lingerMs = 100;
        config.batchSize = 1 << 20;
        auto lazy_ = must(new Producer(address, config));
        mustDo(lazy_.sendTo(lingering, 0, b("waits")));
        const heldBack = must(consumer.fetch(lingering, 0, 0, 0)).length == 0;
        Thread.sleep(800.msecs);
        check("linger.ms holds a partial batch, then sends it in the background",
            heldBack && must(consumer.fetch(lingering, 0, 0, 300)).length == 1,
            heldBack ? "never sent" : "sent before linger.ms");

        const closing = unique("d-close");
        config.lingerMs = 60_000;
        auto closer = must(new Producer(address, config));
        foreach (i; 0 .. 5)
            mustDo(closer.sendTo(closing, 0, b(format("c%d", i))));
        mustDo(closer.close());
        check("close flushes what is still buffered",
            must(consumer.fetch(closing, 0, 0, 300)).length == 5);
        mustDo(eager.close());
        mustDo(lazy_.close());
        consumer.close();
    }

    section("producer: retries, request.timeout.ms and delivery.timeout.ms");
    {
        auto proxy = new FaultProxy(address);
        scope (exit)
            proxy.close();
        const t = unique("d-retry");
        auto config = immediate();
        config.retries = 3;
        config.retryBackoffMs = 50;
        config.requestTimeoutMs = 4321;
        config.acks = -1;
        auto producer = must(new Producer(proxy.address, config));
        must(producer.router.partitions(t));
        proxy.failProduces(2, ErrorCode.notLeaderOrFollower);
        Exception err;
        try
            producer.sendTo(t, 0, b("persistent"));
        catch (Exception e)
            err = e;
        auto consumer = must(new Consumer(address));
        check("a retriable error is retried until the send succeeds",
            err is null && proxy.produces == 3 && must(consumer.fetch(t, 0, 0, 300)).length == 1,
            format("attempts=%d %s", proxy.produces, errText(err)));
        check("request.timeout.ms and acks travel on the produce request",
            proxy.lastTimeoutMs == 4321 && proxy.lastAcks == -1,
            format("%d/%d", proxy.lastTimeoutMs, proxy.lastAcks));
        consumer.close();

        config.retries = 2;
        config.retryBackoffMs = 150;
        auto bounded = must(new Producer(proxy.address, config));
        must(bounded.router.partitions(t));
        proxy.failProduces(1000, ErrorCode.notLeaderOrFollower);
        auto started = MonoTime.currTime;
        bool failedRight;
        try
            bounded.sendTo(t, 0, b("doomed"));
        catch (ServerException e)
            failedRight = e.code == ErrorCode.notLeaderOrFollower;
        auto took = elapsedMs(started);
        check("retries are bounded and spaced by retry.backoff.ms",
            failedRight && proxy.produces == 3 && took >= 300,
            format("attempts=%d took %dms", proxy.produces, took));

        proxy.failProduces(1000, ErrorCode.invalidRequest);
        try
            bounded.sendTo(t, 0, b("malformed"));
        catch (Exception)
        {
        }
        check("a non-retriable error is not retried", proxy.produces == 1,
            format("attempts=%d", proxy.produces));
        try
            bounded.close();
        catch (Exception)
        {
        }

        config.retries = 1000;
        config.retryBackoffMs = 50;
        config.deliveryTimeoutMs = 400;
        auto capped = must(new Producer(proxy.address, config));
        must(capped.router.partitions(t));
        proxy.failProduces(100_000, ErrorCode.notLeaderOrFollower);
        started = MonoTime.currTime;
        bool gaveUp;
        try
            capped.sendTo(t, 0, b("late"));
        catch (ServerException)
            gaveUp = true;
        took = elapsedMs(started);
        check("delivery.timeout.ms caps the whole retry loop", gaveUp && took < 3000,
            format("took %dms, attempts=%d", took, proxy.produces));
        proxy.failProduces(0, 0);
        try
            capped.close();
        catch (Exception)
        {
        }
        mustDo(producer.close());
    }

    section("compression: registering a codec");
    {
        bool refused;
        try
        {
            auto config = immediate();
            config.compressionType = "snappy";
            auto unregistered = new Producer(address, config);
            unregistered.close();
        }
        catch (BrahmaputraException)
            refused = true;
        check("an unregistered codec is refused up front", refused);

        // A toy reversible codec: enough to prove the hook is used on both
        // the produce and the fetch path. The broker stores batches as-is.
        static const(ubyte)[] flip(const(ubyte)[] input)
        {
            auto out_ = new ubyte[input.length];
            foreach (i, byte_; input)
                out_[$ - 1 - i] = byte_ ^ 0x5a;
            return out_;
        }

        registerCodec(Compression.snappy, &flip, &flip);
        const t = unique("d-codec");
        auto config = immediate();
        config.compressionType = "snappy";
        auto producer = must(new Producer(address, config));
        mustDo(producer.sendTo(t, 0, b("through a registered codec"), b("k"),
                [RecordHeader("h", b("v"))]));
        mustDo(producer.close());
        auto consumer = must(new Consumer(address));
        auto got = must(consumer.fetch(t, 0, 0, 300));
        check("a registered codec compresses on produce and decompresses on fetch",
            got.length == 1 && got[0].value == b("through a registered codec")
                && got[0].key == b("k") && got[0].headers.length == 1);
        consumer.close();
        auto encoded = encodeRecordBatch([Record(b("k"), b("v"))], nowMillis(), Compression.snappy);
        size_t pos;
        auto decoded = decodeRecordBatch(encoded, pos);
        check("a batch encoded with it decodes offline",
            decoded.records.length == 1 && decoded.records[0].value == b("v"));
    }

    section("consumer: fetch limits, watermark, offsets by time, metadata");
    {
        const t = unique("d-fetch");
        auto producer = must(new Producer(address, immediate()));
        const long base = 1_700_000_000_000;
        foreach (i; 0 .. 20)
        {
            auto value = new ubyte[1000];
            value[] = cast(ubyte)('a' + i);
            auto record = ProducerRecord(t, 0, null, value);
            record.timestampMs = base + i * 1000;
            mustDo(producer.send(record));
        }
        mustDo(producer.close());

        ConsumerConfig small;
        small.fetchMaxBytes = 2500;
        auto limited = must(new Consumer(address, small));
        auto capped = must(limited.fetch(t, 0, 0, 300));
        check("fetch.max.bytes caps a response", capped.length > 0 && capped.length < 20,
            format("%d records", capped.length));
        limited.close();

        auto consumer = must(new Consumer(address));
        auto result = must(consumer.fetchVerbose(t, 0, 0, 300));
        check("the high watermark is reported", result.highWatermark == 20,
            format("%d", result.highWatermark));

        ConsumerConfig patient;
        patient.fetchMaxWaitMs = 400;
        patient.fetchMinBytes = 1;
        auto waiter = must(new Consumer(address, patient));
        const started = MonoTime.currTime;
        auto none = must(waiter.fetch(t, 0, 20, 10_000));
        const took = elapsedMs(started);
        check("fetch.max.wait.ms bounds a long poll at the end of the log",
            none.length == 0 && took >= 250 && took < 3000, format("%dms", took));
        waiter.close();

        const byTime = must(consumer.listOffsets(t, 0, base + 5000));
        const between = must(consumer.listOffsets(t, 0, base + 5500));
        check("list offsets by timestamp finds the first record at or after it",
            byTime == 5 && between == 6, format("%d,%d", byTime, between));

        auto metadata = must(consumer.router.metadata([t], true));
        auto partitions = metadata.partitionsOf(t);
        bool led = partitions.length == 4;
        foreach (partition; partitions)
            if (metadata.leaderOf(t, partition) < 0)
                led = false;
        check("metadata lists a topic's partitions and their leaders", led,
            format("%d partitions", partitions.length));
        consumer.close();

        auto groupConfig = manualCommit();
        groupConfig.maxPollRecords = 3;
        auto group = must(new GroupConsumer(address, unique("d-maxpoll"), groupConfig));
        group.subscribe([t]);
        size_t largest;
        auto seen = pollUntil(group, 20, 20.seconds, &largest);
        check("max.poll.records caps every poll", seen.length == 20 && largest == 3,
            format("%d records, largest poll %d", seen.length, largest));
        mustDo(group.close());
    }

    section("decoding is bounds-checked");
    {
        auto w = BodyWriter.start();
        w.i32(-5); // a negative string length
        bool negative;
        try
            cast(void) BodyReader(w.data).str();
        catch (ProtocolException)
            negative = true;
        check("a negative length is an error, not a read", negative);

        auto big = BodyWriter.start();
        big.i32(1 << 30); // claims a gigabyte, carries nothing
        bool oversized;
        try
            cast(void) BodyReader(big.data).str();
        catch (ProtocolException)
            oversized = true;
        auto batch = encodeRecordBatch([Record(b("k"), b("v"))], nowMillis(), Compression.none);
        batch[8] = 0x7f; // batch_length far past the buffer
        bool truncated;
        try
        {
            size_t pos;
            cast(void) decodeRecordBatch(batch, pos);
        }
        catch (ProtocolException)
            truncated = true;
        check("an oversized length is an error, not a read", oversized && truncated);
    }

    section("consumer groups: auto commit, several topics, heartbeats");
    {
        const t1 = unique("d-multi-a");
        const t2 = unique("d-multi-b");
        auto producer = must(new Producer(address, immediate()));
        foreach (i; 0 .. 6)
        {
            mustDo(producer.send(t1, b(format("a%d", i))));
            mustDo(producer.send(t2, b(format("b%d", i))));
        }
        mustDo(producer.close());

        GroupConfig groupConfig;
        groupConfig.enableAutoCommit = true;
        groupConfig.autoCommitIntervalMs = 200;
        auto consumer = must(new GroupConsumer(address, unique("d-multi"), groupConfig));
        consumer.subscribe([t1, t2]);
        auto seen = pollUntil(consumer, 12, 20.seconds);
        bool[string] topics;
        foreach (ref record; seen)
            topics[record.topic] = true;
        check("one member subscribed to two topics consumes both",
            seen.length == 12 && topics.length == 2, format("%d records", seen.length));
        Thread.sleep(300.msecs);
        pollQuietly(consumer, 300.msecs);
        const total = must(committedTotal(consumer));
        check("enable.auto.commit commits on poll after auto.commit.interval.ms", total == 12,
            format("committed %d", total));
        mustDo(consumer.close());

        const idle = unique("d-idle");
        auto seeder = must(new Producer(address, immediate()));
        mustDo(seeder.send(idle, b("x")));
        mustDo(seeder.close());
        auto heartbeatConfig = manualCommit();
        heartbeatConfig.sessionTimeoutMs = 1500;
        heartbeatConfig.heartbeatIntervalMs = 300;
        auto quiet = must(new GroupConsumer(address, unique("d-heartbeat"), heartbeatConfig));
        quiet.subscribe([idle]);
        pollUntil(quiet, 1, 15.seconds);
        const generation = quiet.groupGeneration;
        Thread.sleep(4.seconds); // no poll: only heartbeats keep it in
        Exception err;
        try
            quiet.commit();
        catch (Exception e)
            err = e;
        check("heartbeats keep an idle member in its group past session.timeout.ms",
            err is null && quiet.groupGeneration == generation, errText(err));
        mustDo(quiet.close());
    }

    section("consumer groups: fencing, rejoin, leave and static membership");
    {
        const t = unique("d-fence");
        auto producer = must(new Producer(address, immediate()));
        foreach (i; 0 .. 8)
            mustDo(producer.send(t, b(format("f%d", i))));

        const groupId = unique("d-fence-grp");
        auto groupConfig = manualCommit();
        groupConfig.maxPollIntervalMs = 60_000;
        groupConfig.heartbeatIntervalMs = 200;
        auto first = must(new GroupConsumer(address, groupId, groupConfig));
        first.subscribe([t]);
        pollUntil(first, 8, 15.seconds);

        // The coordinator forgets this member behind its back, as it does
        // when a session expires.
        {
            auto w = BodyWriter.start();
            w.str(groupId);
            w.str(first.groupMemberId);
            must(first.consumer.router.seed.request(ApiKey.leaveGroup, w.data));
        }
        const oldMember = first.groupMemberId;
        const oldGeneration = first.groupGeneration;
        Thread.sleep(1.seconds); // a heartbeat learns UNKNOWN_MEMBER_ID
        foreach (i; 8 .. 12)
            mustDo(producer.send(t, b(format("f%d", i))));
        auto after = pollUntil(first, 4, 15.seconds);
        Exception commitErr;
        try
            first.commit();
        catch (Exception e)
            commitErr = e;
        check("a member the coordinator forgot rejoins on its next poll",
            after.length == 4 && first.groupGeneration > oldGeneration && commitErr is null,
            format("%d records, %s@%d -> %s@%d %s", after.length, oldMember, oldGeneration,
                first.groupMemberId, first.groupGeneration, errText(commitErr)));

        // A second member joins; the first sits out the rebalance and its
        // generation goes stale.
        const staleGeneration = first.groupGeneration;
        auto second = must(new GroupConsumer(address, groupId, groupConfig));
        second.subscribe([t]);
        pollUntil(second, 1000, 8.seconds);
        int fencedCode;
        try
            first.commit();
        catch (ServerException e)
            fencedCode = e.code;
        check("a commit from a stale generation is fenced",
            fencedCode == ErrorCode.illegalGeneration || fencedCode == ErrorCode.unknownMemberId,
            format("generation %d -> code %d", staleGeneration, fencedCode));
        mustDo(second.close());
        mustDo(first.close());

        // Close sends LeaveGroup: the next member gets every partition at
        // once instead of waiting out a long session.
        auto slowSession = groupConfig;
        slowSession.sessionTimeoutMs = 30_000;
        slowSession.rebalanceTimeoutMs = 30_000;
        auto leaver = must(new GroupConsumer(address, groupId ~ "-leave", slowSession));
        leaver.subscribe([t]);
        pollUntil(leaver, 12, 15.seconds);
        mustDo(leaver.close());
        auto successor = must(new GroupConsumer(address, groupId ~ "-leave", slowSession));
        successor.subscribe([t]);
        const started = MonoTime.currTime;
        foreach (i; 12 .. 14)
            mustDo(producer.send(t, b(format("f%d", i))));
        auto handedOver = pollUntil(successor, 2, 15.seconds);
        const took = elapsedMs(started);
        check("close leaves the group so partitions move without a session timeout",
            handedOver.length == 2 && successor.assigned.length == 4 && took < 10_000,
            format("%d records after %dms", handedOver.length, took));
        mustDo(successor.close());

        auto staticConfig = groupConfig;
        staticConfig.groupInstanceId = "d-instance-1";
        const staticGroup = unique("d-static");
        auto original = must(new GroupConsumer(address, staticGroup, staticConfig));
        original.subscribe([t]);
        pollUntil(original, 14, 15.seconds);
        const originalMember = original.groupMemberId;
        const originalGeneration = original.groupGeneration;
        auto restarted = must(new GroupConsumer(address, staticGroup, staticConfig));
        restarted.subscribe([t]);
        pollUntil(restarted, 1000, 3.seconds);
        check("a static member reclaims its member id without a rebalance",
            originalMember.length > 0 && restarted.groupMemberId == originalMember
                && restarted.groupGeneration == originalGeneration,
            format("%s@%d vs %s@%d", originalMember, originalGeneration,
                restarted.groupMemberId, restarted.groupGeneration));
        mustDo(restarted.close());
        mustDo(original.close());
        mustDo(producer.close());
    }

    section("assignors: sticky keeps what members hold");
    {
        auto members = [AssignorMember("m1", ["t"]), AssignorMember("m2", ["t"])];
        int[][string] topics = ["t": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]];
        TopicPartition[][string] previous = [
            "m1": [TopicPartition("t", 2), TopicPartition("t", 10), TopicPartition("t", 11)],
            "m2": [TopicPartition("t", 0), TopicPartition("t", 1)],
        ];
        auto sticky = stickyAssign(members, topics, previous);
        bool holds(string id, int p)
        {
            return sticky[id].canFind(TopicPartition("t", p));
        }

        check("sticky leaves every held partition where it was",
            holds("m1", 2) && holds("m1", 10) && holds("m1", 11) && holds("m2", 0)
                && holds("m2", 1) && sticky["m1"].length == 6 && sticky["m2"].length == 6);
        bool numeric = sticky["m1"].length > 1;
        foreach (i; 1 .. sticky["m1"].length)
            if (sticky["m1"][i - 1].partition >= sticky["m1"][i].partition)
                numeric = false;
        check("sticky orders partitions as numbers, not strings", numeric);
        auto range = rangeAssign(members, topics);
        auto rr = roundRobinAssign(members, topics);
        check("range and roundrobin split twelve partitions six and six",
            range["m1"].length == 6 && range["m2"].length == 6 && rr["m1"].length == 6
                && rr["m1"][1].partition == 2);
    }
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

    static bool sendAll(Socket s, const(ubyte)[] data)
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

/// A proxy that understands frames. It forwards every request to the broker
/// except Produce, which it can answer itself with an error code for the
/// next `failures` requests — how a leader move or an under-replicated
/// partition looks to a producer — and it records what each Produce asked for.
final class FaultProxy
{
    private TcpSocket listener;
    private string target;
    private shared bool stopping;
    private Thread acceptThread;
    private Mutex mu;
    private Socket[] live;
    private int failures;
    private int failCode;
    private shared int produces_;
    private shared int lastAcks_;
    private shared int lastTimeoutMs_;
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

    @property int produces()
    {
        return atomicLoad(produces_);
    }

    @property int lastAcks()
    {
        return atomicLoad(lastAcks_);
    }

    @property int lastTimeoutMs()
    {
        return atomicLoad(lastTimeoutMs_);
    }

    /// Answers the next `count` Produce requests with `code`.
    void failProduces(int count, int code)
    {
        mu.lock();
        scope (exit)
            mu.unlock();
        failures = count;
        failCode = code;
        atomicStore(produces_, 0);
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
            auto worker = new Thread(() => serve(client, upstream));
            worker.isDaemon = true;
            worker.start();
        }
        listener.close();
    }

    private static bool readExact(Socket s, ubyte[] into)
    {
        size_t got;
        while (got < into.length)
        {
            const n = s.receive(into[got .. $]);
            if (n <= 0)
                return false;
            got += n;
        }
        return true;
    }

    private static bool readFrame(Socket s, out ubyte[] frame)
    {
        ubyte[4] header;
        if (!readExact(s, header[]))
            return false;
        const len = (uint(header[0]) << 24) | (uint(header[1]) << 16) | (uint(header[2]) << 8) | header[3];
        frame = new ubyte[4 + len];
        frame[0 .. 4] = header[];
        return readExact(s, frame[4 .. $]);
    }

    private void serve(Socket client, Socket upstream)
    {
        ubyte[] frame;
        try
        {
            while (readFrame(client, frame))
            {
                const apiKey = cast(short)((frame[4] << 8) | frame[5]);
                bool expectReply = true;
                if (apiKey == ApiKey.produce)
                {
                    const correlationId = cast(int)((uint(frame[8]) << 24) | (uint(frame[9]) << 16)
                            | (uint(frame[10]) << 8) | frame[11]);
                    const clientLen = (frame[12] << 8) | frame[13];
                    auto r = BodyReader(frame[14 + clientLen .. $]);
                    const topic = r.str();
                    const partition = r.i32();
                    const acks = r.i32();
                    const timeoutMs = r.i32();
                    atomicStore(lastAcks_, acks);
                    atomicStore(lastTimeoutMs_, timeoutMs);
                    atomicStore(produces_, atomicLoad(produces_) + 1);
                    expectReply = acks != 0;
                    int code;
                    {
                        mu.lock();
                        scope (exit)
                            mu.unlock();
                        if (failures > 0)
                        {
                            failures--;
                            code = failCode;
                        }
                    }
                    if (code != 0)
                    {
                        auto w = BodyWriter.start();
                        w.str(topic);
                        w.i32(partition);
                        w.i32(code);
                        w.i64(-1);
                        w.i64(-1);
                        if (!Proxy.sendAll(client, encodeFrame(apiKey, correlationId, "", w.data)))
                            break;
                        continue;
                    }
                }
                if (!Proxy.sendAll(upstream, frame))
                    break;
                if (!expectReply)
                    continue;
                ubyte[] reply;
                if (!readFrame(upstream, reply) || !Proxy.sendAll(client, reply))
                    break;
            }
        }
        catch (Exception)
        {
        }
        foreach (s; [client, upstream])
        {
            try
                s.shutdown(SocketShutdown.BOTH);
            catch (Exception)
            {
            }
            s.close();
        }
    }

    void close()
    {
        atomicStore(stopping, true);
        acceptThread.join();
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
    }
}
