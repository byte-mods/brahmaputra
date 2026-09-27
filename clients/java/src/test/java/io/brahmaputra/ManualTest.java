package io.brahmaputra;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.brahmaputra.Client.ConsumedRecord;
import io.brahmaputra.Client.Consumer;
import io.brahmaputra.Client.ConsumerConfig;
import io.brahmaputra.Client.Producer;
import io.brahmaputra.Client.ProducerConfig;
import io.brahmaputra.GroupConsumer.GroupConfig;
import io.brahmaputra.Protocol.RecordHeader;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Exercises the Java driver against a live broker.
 *
 * <pre>
 *   brahmaputra-server --data-dir ./data --default-partitions 4
 *   java -cp out io.brahmaputra.ManualTest [host] [port]
 * </pre>
 *
 * <p>A port of the Go driver's {@code cmd/manualtest}, section for section and check for check.
 * Every check asserts a property of the system, not that a function ran: records come back
 * byte-identical, keys pin partitions, headers survive, offsets are contiguous. It exits 1 if
 * any check failed and 2 on an unexpected error.
 */
public final class ManualTest {

    private ManualTest() {}

    private static int passed;
    private static int failed;

    private static void check(String name, boolean ok, String detail) {
        if (ok) {
            passed++;
            System.out.println("  ok   " + name);
            return;
        }
        failed++;
        if (detail != null && !detail.isEmpty()) {
            System.out.println("  FAIL " + name + ": " + detail);
        } else {
            System.out.println("  FAIL " + name);
        }
    }

    private static void section(String title) {
        System.out.println();
        System.out.println(title);
    }

    private static String unique(String prefix) {
        return prefix + "-" + Math.floorMod(System.nanoTime(), 1_000_000_000L);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(UTF_8);
    }

    private static ProducerConfig unbatched() {
        ProducerConfig config = new ProducerConfig();
        config.lingerMs = 0;
        return config;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9092;
        // Accept the Go suite's single host:port argument too.
        if (args.length == 1 && host.contains(":")) {
            port = Integer.parseInt(host.substring(host.lastIndexOf(':') + 1));
            host = host.substring(0, host.lastIndexOf(':'));
        }
        try {
            run(host, port);
        } catch (RuntimeException error) {
            System.out.println("  FATAL " + error);
            error.printStackTrace(System.out);
            System.exit(2);
        }
        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        System.exit(failed > 0 ? 1 : 0);
    }

    private static void run(String host, int port) {
        section("connection and metadata");
        try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
            Client.ApiVersions versions = consumer.router().seed().apiVersions();
            check("ApiVersions answers", !versions.ranges.isEmpty(),
                    versions.ranges.size() + " ranges");
            check("broker reports a version", !versions.brokerVersion.isEmpty(),
                    versions.brokerVersion);
            Client.ClusterMetadata metadata =
                    consumer.router().metadata(Collections.emptyList(), true);
            check("metadata lists brokers", metadata.brokers.size() >= 1,
                    metadata.brokers.size() + " brokers");
        }

        section("produce and consume round trip");
        String topic = unique("java-roundtrip");
        List<byte[]> payloads = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            payloads.add(bytes("record-" + i));
        }
        try (Producer producer = new Producer(host, port, unbatched())) {
            for (byte[] payload : payloads) {
                producer.sendTo(topic, 0, payload, null);
            }
            producer.flush();
        }
        try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
            List<ConsumedRecord> got = consumer.fetch(topic, 0, 0, 500);
            check("every record comes back", got.size() == payloads.size(),
                    "got " + got.size());
            boolean identical = got.size() == payloads.size();
            for (int i = 0; identical && i < got.size(); i++) {
                if (!Arrays.equals(got.get(i).value, payloads.get(i)) || got.get(i).offset != i) {
                    identical = false;
                }
            }
            check("values byte-identical and offsets contiguous", identical, "");
        }

        section("compression codecs");
        // Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via registerCodec
        // so applications that do not want those dependencies do not carry them.
        for (String codec : new String[] {"none", "gzip"}) {
            String codecTopic = unique("java-" + codec);
            StringBuilder repeated = new StringBuilder();
            for (int i = 0; i < 40; i++) {
                repeated.append("the same line over and over. ");
            }
            byte[] body = bytes(repeated.toString());
            ProducerConfig config = unbatched();
            config.compressionType = codec;
            try (Producer producer = new Producer(host, port, config)) {
                for (int i = 0; i < 20; i++) {
                    byte[] value = Arrays.copyOf(body, body.length + 1);
                    value[body.length] = (byte) ('0' + i % 10);
                    producer.sendTo(codecTopic, 0, value, null);
                }
                producer.flush();
            }
            try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                List<ConsumedRecord> got = consumer.fetch(codecTopic, 0, 0, 500);
                check(codec + ": round trips",
                        got.size() == 20 && startsWith(got.get(0).value, body),
                        "got " + got.size() + " records");
            }
        }

        section("keys, partitioning and ordering");
        {
            String keyTopic = unique("java-keys");
            byte[] key = bytes("user-7");
            List<Integer> partitions;
            try (Producer producer = new Producer(host, port, unbatched())) {
                partitions = producer.router().partitions(keyTopic);
                for (int i = 0; i < 30; i++) {
                    producer.send(keyTopic, bytes("v" + i), key);
                }
                producer.flush();
            }
            int target = Protocol.partitionForKey(key, partitions);
            try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                List<ConsumedRecord> onTarget = consumer.fetch(keyTopic, target, 0, 500);
                check("a key pins every record to one partition", onTarget.size() == 30,
                        "partition " + target + " holds " + onTarget.size() + " of 30");

                boolean ordered = onTarget.size() == 30;
                for (int i = 0; ordered && i < onTarget.size(); i++) {
                    if (!new String(onTarget.get(i).value, UTF_8).equals("v" + i)) {
                        ordered = false;
                    }
                }
                check("per-key order is preserved", ordered, "");

                int strays = 0;
                for (int partition : partitions) {
                    if (partition == target) {
                        continue;
                    }
                    strays += consumer.fetch(keyTopic, partition, 0, 200).size();
                }
                check("no keyed record landed elsewhere", strays == 0, strays + " strays");
            }
        }

        section("murmur2 agrees with the broker's partitioner");
        check("murmur2(\"\") is stable", Protocol.murmur2(new byte[0]) == 275646681,
                Integer.toUnsignedString(Protocol.murmur2(new byte[0])));
        check("murmur2 is deterministic",
                Protocol.murmur2(bytes("user-7")) == Protocol.murmur2(bytes("user-7")), "");
        check("different keys hash differently",
                Protocol.murmur2(bytes("user-7")) != Protocol.murmur2(bytes("user-8")), "");

        section("record headers and timestamps");
        {
            String headerTopic = unique("java-headers");
            long before = System.currentTimeMillis() - 1000;
            try (Producer producer = new Producer(host, port, unbatched())) {
                producer.sendTo(headerTopic, 0, bytes("annotated"), null,
                        new RecordHeader("trace-id", bytes("abc-123")),
                        new RecordHeader("content-type", bytes("application/json")),
                        new RecordHeader("tombstone-reason", null));
                producer.sendTo(headerTopic, 0, bytes("plain"), null);
                producer.flush();
            }
            long after = System.currentTimeMillis() + 1000;

            try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                List<ConsumedRecord> got = consumer.fetch(headerTopic, 0, 0, 500);
                check("both records arrive", got.size() == 2, "got " + got.size());
                if (got.size() == 2) {
                    ConsumedRecord annotated = got.get(0);
                    ConsumedRecord plain = got.get(1);
                    check("headers survive the round trip", annotated.headers.size() == 3,
                            annotated.headers.size() + " headers");
                    check("header values are exact",
                            Arrays.equals(annotated.header("trace-id"), bytes("abc-123")), "");
                    check("a null header value stays null",
                            annotated.headers.size() == 3
                                    && annotated.headers.get(2).value == null, "");
                    check("a record with no headers gains none from its batch",
                            plain.headers.isEmpty(), plain.headers.size() + " headers");
                    boolean inWindow = true;
                    for (ConsumedRecord record : got) {
                        if (record.timestamp < before || record.timestamp > after) {
                            inWindow = false;
                        }
                    }
                    check("timestamps are real wall-clock values", inWindow,
                            got.get(0).timestamp + "," + got.get(1).timestamp + " outside "
                                    + before + ".." + after);
                }
            }
        }

        section("tombstones");
        {
            String tombTopic = unique("java-tombstones");
            try (Producer producer = new Producer(host, port, unbatched())) {
                producer.sendTo(tombTopic, 0, bytes("set"), bytes("k1"));
                producer.sendTo(tombTopic, 0, new byte[0], bytes("k2"));
                // A null value is a deletion, and must stay distinguishable from the empty
                // value above all the way through the round trip.
                producer.sendTo(tombTopic, 0, null, bytes("k3"));
                producer.flush();
            }
            try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                List<ConsumedRecord> got = consumer.fetch(tombTopic, 0, 0, 500);
                check("all three records arrive", got.size() == 3, "got " + got.size());
                if (got.size() == 3) {
                    check("an ordinary value round-trips",
                            Arrays.equals(got.get(0).value, bytes("set")), "");
                    check("an empty value is empty, not null",
                            got.get(1).value != null && got.get(1).value.length == 0,
                            String.valueOf(got.get(1).value));
                    check("a tombstone arrives as a null value", got.get(2).value == null,
                            String.valueOf(got.get(2).value));
                }
            }
        }

        section("offsets");
        try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
            long earliest = consumer.listOffsets(topic, 0, Client.EARLIEST);
            long latest = consumer.listOffsets(topic, 0, Client.LATEST);
            check("earliest is 0 on a fresh topic", earliest == 0, String.valueOf(earliest));
            check("latest equals the record count", latest == 50, String.valueOf(latest));
        }

        section("acks");
        for (int acks : new int[] {0, 1, -1}) {
            String acksTopic = unique("java-acks" + acks);
            ProducerConfig config = unbatched();
            config.acks = acks;
            try (Producer producer = new Producer(host, port, config)) {
                producer.sendTo(acksTopic, 0, bytes("durable"), null);
                producer.flush();
            }
            sleep(400);
            try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                List<ConsumedRecord> got = consumer.fetch(acksTopic, 0, 0, 500);
                check("acks=" + acks + " stores the record", got.size() == 1,
                        "got " + got.size());
            }
        }

        section("consumer group: assignment, commit, resume");
        {
            String groupTopic = unique("java-group");
            String groupId = unique("java-billing");
            try (Producer producer = new Producer(host, port, unbatched())) {
                for (int i = 0; i < 40; i++) {
                    producer.send(groupTopic, bytes("g" + i));
                }
                producer.flush();
            }

            GroupConfig groupConfig = new GroupConfig();
            groupConfig.autoCommitIntervalMs = 0;
            GroupConsumer consumer = new GroupConsumer(host, port, groupId, groupConfig);
            consumer.subscribe(Collections.singletonList(groupTopic));

            List<ConsumedRecord> seen = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (seen.size() < 40 && System.currentTimeMillis() < deadline) {
                seen.addAll(consumer.poll(500));
            }
            check("the group consumes every record", seen.size() == 40, "got " + seen.size());

            Set<String> distinct = new HashSet<>();
            for (ConsumedRecord record : seen) {
                distinct.add(record.partition + "-" + record.offset);
            }
            check("no record is delivered twice", distinct.size() == seen.size(), "");

            consumer.commit();
            Map<GroupConsumer.TopicPartition, Long> committed =
                    consumer.committed(Collections.emptyList());
            long total = 0;
            for (long offset : committed.values()) {
                total += offset;
            }
            check("commit records a position", total == 40, String.valueOf(total));
            consumer.close();

            // A second consumer in the same group must resume, not replay.
            GroupConsumer rejoined = new GroupConsumer(host, port, groupId, groupConfig);
            rejoined.subscribe(Collections.singletonList(groupTopic));
            List<ConsumedRecord> replayed = new ArrayList<>();
            long until = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < until) {
                replayed.addAll(pollQuietly(rejoined, 300));
            }
            check("a rejoining group resumes from its commit", replayed.isEmpty(),
                    "replayed " + replayed.size() + " records it had already committed");
            rejoined.close();
        }

        section("auto.offset.reset");
        {
            String resetTopic = unique("java-reset");
            try (Producer producer = new Producer(host, port, unbatched())) {
                for (int i = 0; i < 10; i++) {
                    producer.send(resetTopic, bytes("r" + i));
                }
                producer.flush();
            }

            GroupConfig latestConfig = new GroupConfig();
            latestConfig.autoCommitIntervalMs = 0;
            latestConfig.autoOffsetReset = GroupConsumer.AutoOffsetReset.LATEST;
            GroupConsumer consumer =
                    new GroupConsumer(host, port, unique("java-latest"), latestConfig);
            consumer.subscribe(Collections.singletonList(resetTopic));
            List<ConsumedRecord> skipped = new ArrayList<>();
            long until = System.currentTimeMillis() + 4_000;
            while (System.currentTimeMillis() < until) {
                skipped.addAll(pollQuietly(consumer, 300));
            }
            check("latest skips records produced before the group existed",
                    skipped.isEmpty(), "saw " + skipped.size());
            consumer.close();

            GroupConfig noneConfig = new GroupConfig();
            noneConfig.autoCommitIntervalMs = 0;
            noneConfig.autoOffsetReset = GroupConsumer.AutoOffsetReset.NONE;
            GroupConsumer strict = new GroupConsumer(host, port, unique("java-none"), noneConfig);
            strict.subscribe(Collections.singletonList(resetTopic));
            boolean raised = false;
            until = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < until && !raised) {
                try {
                    strict.poll(300);
                } catch (Protocol.NoOffsetForPartitionException error) {
                    raised = true;
                } catch (Protocol.BrahmaputraException error) {
                    raised = String.valueOf(error.getMessage()).contains("no committed offset");
                }
            }
            check("none refuses to guess a position", raised, "");
            strict.close();
        }

        section("assignors");
        for (GroupConsumer.Assignor assignor : GroupConsumer.Assignor.values()) {
            String name = assignor.name().toLowerCase(java.util.Locale.ROOT);
            String assignorTopic = unique("java-" + name);
            try (Producer producer = new Producer(host, port, unbatched())) {
                for (int i = 0; i < 20; i++) {
                    producer.send(assignorTopic, bytes("a" + i));
                }
                producer.flush();
            }

            GroupConfig groupConfig = new GroupConfig();
            groupConfig.autoCommitIntervalMs = 0;
            groupConfig.assignor = assignor;
            GroupConsumer consumer =
                    new GroupConsumer(host, port, unique("java-grp-" + name), groupConfig);
            consumer.subscribe(Collections.singletonList(assignorTopic));
            List<ConsumedRecord> collected = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 20_000;
            while (collected.size() < 20 && System.currentTimeMillis() < deadline) {
                collected.addAll(pollQuietly(consumer, 500));
            }
            check(name + ": consumes every record", collected.size() == 20,
                    "got " + collected.size());
            consumer.close();
        }

        section("bounded client buffer");
        {
            String bufferTopic = unique("java-buffer");
            ProducerConfig config = new ProducerConfig();
            config.lingerMs = 10_000; // never flush on time during this check
            config.bufferMemory = 2048;
            config.maxBlockMs = 300;
            Producer producer = new Producer(host, port, config);
            byte[] value = new byte[256];
            Arrays.fill(value, (byte) 'x');
            boolean blocked = false;
            for (int i = 0; i < 500 && !blocked; i++) {
                try {
                    producer.sendTo(bufferTopic, 0, value, null);
                } catch (Protocol.BrahmaputraException error) {
                    blocked = String.valueOf(error.getMessage()).contains("buffer full");
                }
            }
            check("a full buffer blocks and then reports", blocked, "");
            // Like the Go suite, this producer is abandoned rather than closed: closing would
            // flush the records the check just proved were held back.
        }

        section("wire edge cases");
        {
            String edgeTopic = unique("java-edge");
            byte[] large = new byte[1 << 20];
            for (int i = 0; i < large.length; i++) {
                large[i] = (byte) (i * 7);
            }
            byte[] unicodeKey = bytes("ключ-✓-🔑");
            byte[] unicodeValue = bytes("значение — 数据 — 🚀");
            try (Producer producer = new Producer(host, port, unbatched())) {
                producer.sendTo(edgeTopic, 0, large, null);
                producer.sendTo(edgeTopic, 0, unicodeValue, unicodeKey,
                        new RecordHeader("ünïcødé-🏷", bytes("✓")));
                // An empty key and an empty header value are values, not nulls.
                producer.sendTo(edgeTopic, 0, bytes("empty-key"), new byte[0],
                        new RecordHeader("empty", new byte[0]),
                        new RecordHeader("null", null));
                producer.sendTo(edgeTopic, 0, bytes("null-key"), null);
            }

            try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                List<ConsumedRecord> got = fetchAll(consumer, edgeTopic, 4);
                check("edge records all arrive", got.size() == 4, "got " + got.size());
                if (got.size() == 4) {
                    check("a 1 MiB value round-trips byte-identical",
                            Arrays.equals(got.get(0).value, large),
                            (got.get(0).value == null ? "null" : got.get(0).value.length)
                                    + " bytes");
                    ConsumedRecord unicode = got.get(1);
                    check("unicode key, value and header key round-trip",
                            Arrays.equals(unicode.key, unicodeKey)
                                    && Arrays.equals(unicode.value, unicodeValue)
                                    && unicode.headers.size() == 1
                                    && unicode.headers.get(0).key.equals("ünïcødé-🏷"), "");
                    ConsumedRecord empty = got.get(2);
                    check("an empty key stays empty, not null",
                            empty.key != null && empty.key.length == 0,
                            String.valueOf(empty.key));
                    check("an empty header value stays empty, not null",
                            empty.headers.size() == 2
                                    && empty.headers.get(0).value != null
                                    && empty.headers.get(0).value.length == 0
                                    && empty.headers.get(1).value == null,
                            empty.headers.size() + " headers");
                    check("a null key stays null", got.get(3).key == null,
                            String.valueOf(got.get(3).key));
                }
            }
        }

        section("ordering under linger flushes");
        {
            String orderTopic = unique("java-order");
            ProducerConfig config = new ProducerConfig();
            config.lingerMs = 1;
            config.batchSize = 256;
            final int total = 5000;
            try (Producer producer = new Producer(host, port, config)) {
                for (int i = 0; i < total; i++) {
                    producer.sendTo(orderTopic, 0, bytes(Integer.toString(i)), null);
                }
            }
            try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                List<ConsumedRecord> got = fetchAll(consumer, orderTopic, total);
                int inversions = 0;
                for (int i = 1; i < got.size(); i++) {
                    if (Integer.parseInt(new String(got.get(i).value, UTF_8))
                            < Integer.parseInt(new String(got.get(i - 1).value, UTF_8))) {
                        inversions++;
                    }
                }
                check("every record of a partition arrives", got.size() == total,
                        "got " + got.size());
                check("a partition's records keep send order", inversions == 0,
                        inversions + " inversions");
            }
        }

        section("background flush failures are reported");
        {
            ProducerConfig config = new ProducerConfig();
            config.lingerMs = 20;
            Producer producer = new Producer(host, port, config);
            // Partition 999 does not exist, so the linger thread's flush fails.
            RuntimeException sendError = null;
            try {
                producer.sendTo(unique("java-bgfail"), 999, bytes("lost"), null);
            } catch (RuntimeException error) {
                sendError = error;
            }
            sleep(300);
            RuntimeException flushError = null;
            try {
                producer.flush();
            } catch (RuntimeException error) {
                flushError = error;
            }
            check("a failed linger flush surfaces on the next flush",
                    sendError == null && flushError != null,
                    "send=" + sendError + " flush=" + flushError);
            Thread closer = new Thread(() -> {
                try {
                    producer.close();
                } catch (RuntimeException ignored) {
                    // Returning with an error is still returning.
                }
            });
            closer.start();
            try {
                closer.join(5_000);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
            check("close returns after a failed flush", !closer.isAlive(),
                    closer.isAlive() ? "hung" : "");
        }

        section("connection failures");
        {
            // A broker that accepts and never answers must cost an error, not a thread
            // blocked forever.
            try (SilentBroker silent = new SilentBroker()) {
                Client.Connection connection =
                        Client.Connection.connect("127.0.0.1", silent.port(), "java-test", 1000);
                connection.setRequestTimeout(300);
                long started = System.currentTimeMillis();
                RuntimeException requestError = null;
                try {
                    connection.apiVersions();
                } catch (RuntimeException error) {
                    requestError = error;
                }
                check("a request to an unresponsive broker times out",
                        requestError != null && System.currentTimeMillis() - started < 3_000,
                        String.valueOf(requestError));
                check("a timed-out connection is not reused", connection.isBroken(), "");
                connection.close();
            }

            // A connection the broker drops is redialled, not kept forever.
            try (DropProxy proxy = new DropProxy(host, port)) {
                String dropTopic = unique("java-drop");
                try (Producer producer = new Producer("127.0.0.1", proxy.port(), unbatched())) {
                    producer.sendTo(dropTopic, 0, bytes("before"), null);
                    proxy.dropAll();
                    RuntimeException recovered = new RuntimeException("not attempted");
                    for (int attempt = 0; attempt < 3 && recovered != null; attempt++) {
                        try {
                            producer.sendTo(dropTopic, 0, bytes("after"), null);
                            recovered = null;
                        } catch (RuntimeException error) {
                            recovered = error;
                        }
                    }
                    check("a producer recovers after its connection drops", recovered == null,
                            String.valueOf(recovered));
                }
                try (Consumer consumer =
                        new Consumer("127.0.0.1", proxy.port(), new ConsumerConfig())) {
                    consumer.fetch(dropTopic, 0, 0, 100);
                    proxy.dropAll();
                    RuntimeException fetchError = new RuntimeException("not attempted");
                    List<ConsumedRecord> fetched = Collections.emptyList();
                    for (int attempt = 0; attempt < 3 && fetchError != null; attempt++) {
                        try {
                            fetched = consumer.fetch(dropTopic, 0, 0, 100);
                            fetchError = null;
                        } catch (RuntimeException error) {
                            fetchError = error;
                        }
                    }
                    check("a consumer recovers after its connection drops",
                            fetchError == null && fetched.size() >= 1,
                            String.valueOf(fetchError));
                }
            }
        }

        section("consumer group: max.poll.interval and rejoin");
        {
            String slowTopic = unique("java-slow");
            Producer producer = new Producer(host, port, unbatched());
            for (int i = 0; i < 10; i++) {
                producer.send(slowTopic, bytes("s" + i));
            }
            GroupConfig groupConfig = new GroupConfig();
            groupConfig.autoCommitIntervalMs = 0;
            groupConfig.maxPollIntervalMs = 1500;
            GroupConsumer consumer =
                    new GroupConsumer(host, port, unique("java-slow-grp"), groupConfig);
            consumer.subscribe(Collections.singletonList(slowTopic));
            List<ConsumedRecord> first = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 15_000;
            while (first.size() < 10 && System.currentTimeMillis() < deadline) {
                try {
                    first.addAll(consumer.poll(300));
                } catch (Protocol.BrahmaputraException error) {
                    break;
                }
            }
            consumer.commit();
            // Stall past max.poll.interval.ms: the member leaves the group.
            sleep(2500);
            for (int i = 10; i < 20; i++) {
                producer.send(slowTopic, bytes("s" + i));
            }
            producer.close();
            List<ConsumedRecord> second = new ArrayList<>();
            RuntimeException pollError = null;
            deadline = System.currentTimeMillis() + 15_000;
            while (second.size() < 10 && System.currentTimeMillis() < deadline) {
                try {
                    second.addAll(consumer.poll(300));
                } catch (Protocol.BrahmaputraException error) {
                    pollError = error;
                    break;
                }
            }
            check("a member that stalled rejoins on its next poll",
                    first.size() == 10 && second.size() == 10 && pollError == null,
                    "first=" + first.size() + " second=" + second.size() + " err=" + pollError);
            consumer.close();
        }

        section("consumer group: time inside poll does not count against max.poll.interval");
        {
            String joinTopic = unique("java-inpoll");
            Producer producer = new Producer(host, port, unbatched());
            producer.router().partitions(joinTopic);
            GroupConfig groupConfig = new GroupConfig();
            groupConfig.autoCommitIntervalMs = 0;
            // Far shorter than the first poll below, which spends ~1s joining (the broker's
            // initial rebalance delay) and then waits for data.
            groupConfig.maxPollIntervalMs = 600;
            GroupConsumer consumer =
                    new GroupConsumer(host, port, unique("java-inpoll-grp"), groupConfig);
            consumer.subscribe(Collections.singletonList(joinTopic));
            Thread late = new Thread(() -> {
                sleep(2000);
                for (int i = 0; i < 10; i++) {
                    try {
                        producer.send(joinTopic, bytes("j" + i));
                    } catch (RuntimeException ignored) {
                        // The check below reports what did not arrive.
                    }
                }
            });
            late.start();
            // One long poll: it joins, then waits for the records above.
            List<ConsumedRecord> got = Collections.emptyList();
            RuntimeException pollError = null;
            try {
                got = consumer.poll(4000);
            } catch (RuntimeException error) {
                pollError = error;
            }
            // Committed straight away, before another poll could quietly rejoin: this fails
            // if the member left the group mid-poll.
            RuntimeException commitError = null;
            try {
                consumer.commit();
            } catch (RuntimeException error) {
                commitError = error;
            }
            check("a member is still in its group after a long poll",
                    pollError == null && !got.isEmpty() && commitError == null,
                    "got=" + got.size() + " poll=" + pollError + " commit=" + commitError);
            try {
                late.join();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
            consumer.close();
            producer.close();
        }

        runExtra(host, port);
    }

    // -----------------------------------------------------------------------
    // Checks beyond the Go suite: every item of the client feature checklist
    // that the sections above do not already exercise.
    // -----------------------------------------------------------------------

    private static void runExtra(String host, int port) {
        section("producer: batch.size and linger.ms");
        {
            String batchTopic = unique("java-batchsize");
            ProducerConfig config = new ProducerConfig();
            config.lingerMs = 60_000; // only batch.size can send anything during this check
            config.batchSize = 1024;
            byte[] value = new byte[200];
            Arrays.fill(value, (byte) 'b');
            try (Producer producer = new Producer(host, port, config);
                    Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                for (int i = 0; i < 8; i++) {
                    producer.sendTo(batchTopic, 0, value, null);
                }
                int early = consumer.fetch(batchTopic, 0, 0, 0).size();
                check("a batch that reaches batch.size is sent before linger.ms",
                        early >= 1 && early < 8, early + " of 8 sent before any flush");
                producer.flush();
                int after = fetchAll(consumer, batchTopic, 8).size();
                check("flush sends the partial batch that is left", after == 8,
                        "got " + after);
            }

            String lingerTopic = unique("java-linger");
            ProducerConfig lingering = new ProducerConfig();
            lingering.lingerMs = 500;
            try (Producer producer = new Producer(host, port, lingering);
                    Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                producer.router().partitions(lingerTopic);
                producer.sendTo(lingerTopic, 0, bytes("lingering"), null);
                int immediate = consumer.fetch(lingerTopic, 0, 0, 0).size();
                sleep(1500);
                int later = consumer.fetch(lingerTopic, 0, 0, 0).size();
                check("linger.ms holds a record back, then sends it without a flush",
                        immediate == 0 && later == 1,
                        "immediately " + immediate + ", after linger " + later);
            }
        }

        section("producer: partitioners");
        {
            String rrTopic = unique("java-rr");
            String pinTopic = unique("java-pinned");
            List<Integer> partitions;
            try (Producer producer = new Producer(host, port, unbatched())) {
                partitions = producer.router().partitions(rrTopic);
                for (int i = 0; i < partitions.size() * 2; i++) {
                    producer.send(rrTopic, bytes("rr" + i));
                }
                producer.router().partitions(pinTopic);
                producer.sendTo(pinTopic, partitions.size() - 1, bytes("pinned"), null);
            }
            try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                boolean even = true;
                StringBuilder counts = new StringBuilder();
                int pinnedElsewhere = 0;
                int pinnedThere = 0;
                for (int partition : partitions) {
                    int count = consumer.fetch(rrTopic, partition, 0, 0).size();
                    counts.append(partition).append('=').append(count).append(' ');
                    even &= count == 2;
                    int pinned = consumer.fetch(pinTopic, partition, 0, 0).size();
                    if (partition == partitions.size() - 1) {
                        pinnedThere += pinned;
                    } else {
                        pinnedElsewhere += pinned;
                    }
                }
                check("a null key round-robins across every partition", even,
                        counts.toString().trim());
                check("an explicit partition is honoured",
                        pinnedThere == 1 && pinnedElsewhere == 0,
                        pinnedThere + " there, " + pinnedElsewhere + " elsewhere");
            }
        }

        section("producer: record timestamps and send-and-wait");
        {
            String timeTopic = unique("java-timestamps");
            String syncTopic = unique("java-sync");
            long base = System.currentTimeMillis() - 60_000;
            long first;
            long second;
            long beforeSend = System.currentTimeMillis();
            try (Producer producer = new Producer(host, port, unbatched())) {
                for (int i = 0; i < 3; i++) {
                    producer.sendTo(timeTopic, 0, bytes("t" + i), null, base + i * 1000L,
                            Collections.emptyList());
                }
                producer.sendTo(timeTopic, 0, bytes("now"), null);
                first = producer.sendSyncTo(syncTopic, 0, bytes("s0"), null,
                        Client.NO_TIMESTAMP, Collections.emptyList());
                second = producer.sendSyncTo(syncTopic, 0, bytes("s1"), null,
                        Client.NO_TIMESTAMP, Collections.emptyList());
            }
            try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                List<ConsumedRecord> got = consumer.fetch(timeTopic, 0, 0, 500);
                boolean exact = got.size() == 4;
                for (int i = 0; exact && i < 3; i++) {
                    exact = got.get(i).timestamp == base + i * 1000L;
                }
                check("an explicit record timestamp round-trips exactly", exact,
                        got.size() + " records");
                check("a record without one is stamped with the wall clock",
                        got.size() == 4 && got.get(3).timestamp >= beforeSend - 1000
                                && got.get(3).timestamp <= System.currentTimeMillis() + 1000,
                        got.size() == 4 ? String.valueOf(got.get(3).timestamp) : "");
                check("send-and-wait returns each record's offset", first == 0 && second == 1,
                        first + ", " + second);
                long atHalf = consumer.listOffsets(timeTopic, 0, base + 500);
                long atLast = consumer.listOffsets(timeTopic, 0, base + 2000);
                check("list offsets by timestamp finds the first record at or after it",
                        atHalf == 1 && atLast == 2, atHalf + ", " + atLast);
            }
        }

        section("producer: codec registration");
        {
            Lz4Literals lz4 = new Lz4Literals();
            Protocol.registerCodec(Protocol.Compression.LZ4, lz4);
            String lz4Topic = unique("java-lz4");
            ProducerConfig config = unbatched();
            config.compressionType = "lz4";
            List<byte[]> sent = new ArrayList<>();
            try (Producer producer = new Producer(host, port, config)) {
                for (int i = 0; i < 10; i++) {
                    byte[] value = bytes("lz4 record " + i + " ".repeat(40));
                    sent.add(value);
                    producer.sendTo(lz4Topic, 0, value, bytes("k" + i));
                }
            }
            try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                List<ConsumedRecord> got = consumer.fetch(lz4Topic, 0, 0, 500);
                boolean same = got.size() == sent.size();
                for (int i = 0; same && i < got.size(); i++) {
                    same = Arrays.equals(got.get(i).value, sent.get(i));
                }
                check("a registered codec (lz4) compresses sends and decodes fetches",
                        same && lz4.compressed.get() >= 10 && lz4.decompressed.get() >= 10,
                        got.size() + " records, " + lz4.compressed.get() + " compressed, "
                                + lz4.decompressed.get() + " decompressed");
            }
            ProducerConfig zstd = unbatched();
            zstd.compressionType = "zstd";
            RuntimeException refused = null;
            try (Producer producer = new Producer(host, port, zstd)) {
                producer.sendTo(unique("java-zstd"), 0, bytes("x"), null);
            } catch (RuntimeException error) {
                refused = error;
            }
            check("an unregistered codec is refused, not sent uncompressed",
                    refused != null && String.valueOf(refused.getMessage()).contains("not registered"),
                    String.valueOf(refused));
        }

        section("producer: retries, request.timeout.ms and delivery.timeout.ms");
        try (FaultProxy proxy = new FaultProxy(host, port)) {
            String retryTopic = unique("java-retry");
            ProducerConfig config = unbatched();
            config.acks = -1;
            config.requestTimeoutMs = 1234;
            config.retries = 3;
            config.retryBackoffMs = 150;
            proxy.failProduces(2);
            RuntimeException error = null;
            long started = System.currentTimeMillis();
            try (Producer producer = new Producer("127.0.0.1", proxy.port(), config)) {
                producer.sendTo(retryTopic, 0, bytes("retried"), null);
            } catch (RuntimeException failure) {
                error = failure;
            }
            long elapsed = System.currentTimeMillis() - started;
            check("request.timeout.ms and acks travel with every produce",
                    proxy.lastTimeoutMs == 1234 && proxy.lastAcks == -1,
                    "timeout=" + proxy.lastTimeoutMs + " acks=" + proxy.lastAcks);
            check("a retriable error is retried after retry.backoff.ms",
                    error == null && proxy.produces == 3 && elapsed >= 300,
                    "attempts=" + proxy.produces + " elapsed=" + elapsed + " error=" + error);
            try (Consumer consumer = new Consumer(host, port, new ConsumerConfig())) {
                int stored = consumer.fetch(retryTopic, 0, 0, 500).size();
                check("the retried record is stored exactly once", stored == 1,
                        "stored " + stored);
            }

            proxy.failProduces(-1);
            config.retries = 2;
            error = null;
            try (Producer producer = new Producer("127.0.0.1", proxy.port(), config)) {
                producer.sendTo(retryTopic, 0, bytes("never"), null);
            } catch (RuntimeException failure) {
                error = failure;
            }
            check("retries bounds the attempts: the error surfaces after retries + 1",
                    error != null && proxy.produces == 3,
                    "attempts=" + proxy.produces + " error=" + error);

            proxy.failProduces(-1);
            config.retries = 1_000_000;
            config.retryBackoffMs = 50;
            config.deliveryTimeoutMs = 500;
            error = null;
            started = System.currentTimeMillis();
            try (Producer producer = new Producer("127.0.0.1", proxy.port(), config)) {
                producer.sendTo(retryTopic, 0, bytes("late"), null);
            } catch (RuntimeException failure) {
                error = failure;
            }
            elapsed = System.currentTimeMillis() - started;
            check("delivery.timeout.ms bounds the time spent retrying",
                    error != null && elapsed >= 450 && elapsed < 3000,
                    "elapsed=" + elapsed + " attempts=" + proxy.produces + " error=" + error);

            proxy.failProduces(0);
            for (int mode = 1; mode <= 2; mode++) {
                proxy.corruptFetch = mode;
                RuntimeException decodeError = null;
                try (Consumer consumer =
                        new Consumer("127.0.0.1", proxy.port(), new ConsumerConfig())) {
                    consumer.fetch(retryTopic, 0, 0, 100);
                } catch (RuntimeException failure) {
                    decodeError = failure;
                }
                check(mode == 1
                                ? "a negative length on the wire is an error"
                                : "a length past the end of the data is an error",
                        decodeError instanceof Protocol.BrahmaputraException,
                        String.valueOf(decodeError));
            }
            proxy.corruptFetch = 0;
        }

        section("consumer: fetch limits, high watermark and metadata");
        {
            String fetchTopic = unique("java-fetch");
            byte[] value = new byte[1000];
            Arrays.fill(value, (byte) 'f');
            try (Producer producer = new Producer(host, port, unbatched())) {
                for (int i = 0; i < 10; i++) {
                    producer.sendTo(fetchTopic, 0, value, null);
                }
            }
            ConsumerConfig small = new ConsumerConfig();
            small.fetchMaxBytes = 2500;
            try (Consumer consumer = new Consumer(host, port, small)) {
                int got = consumer.fetch(fetchTopic, 0, 0, 500).size();
                check("fetch.max.bytes caps what one fetch returns", got >= 1 && got < 10,
                        "got " + got + " of 10");
            }
            ConsumerConfig capped = new ConsumerConfig();
            capped.maxPollRecords = 4;
            try (Consumer consumer = new Consumer(host, port, capped)) {
                List<ConsumedRecord> got = consumer.fetch(fetchTopic, 0, 0, 500);
                check("max.poll.records caps one fetch", got.size() == 4,
                        "got " + got.size());
                List<ConsumedRecord> next = consumer.fetch(fetchTopic, 0, 4, 500);
                check("the records a cap held back come on the next fetch",
                        next.size() == 4 && next.get(0).offset == 4, "got " + next.size());
            }
            ConsumerConfig patient = new ConsumerConfig();
            patient.fetchMinBytes = 1_000_000;
            patient.fetchMaxWaitMs = 600;
            try (Consumer waiting = new Consumer(host, port, patient);
                    Consumer eager = new Consumer(host, port, new ConsumerConfig())) {
                long started = System.currentTimeMillis();
                int waitedFor = waiting.fetch(fetchTopic, 0, 0, 600).size();
                long waited = System.currentTimeMillis() - started;
                started = System.currentTimeMillis();
                int eagerGot = eager.fetch(fetchTopic, 0, 0, 600).size();
                long quick = System.currentTimeMillis() - started;
                check("fetch.min.bytes holds a fetch open until fetch.max.wait.ms",
                        waited >= 450 && quick < 400 && waitedFor == 10 && eagerGot == 10,
                        "waited " + waited + "ms, eager " + quick + "ms");

                Client.FetchResult result = eager.fetchVerbose(fetchTopic, 0, 0, 500);
                check("the high watermark is reported", result.highWatermark == 10,
                        String.valueOf(result.highWatermark));

                Client.ClusterMetadata metadata = eager.router()
                        .metadata(Collections.singletonList(fetchTopic), true);
                Set<Integer> brokerIds = new HashSet<>();
                for (Client.BrokerInfo broker : metadata.brokers) {
                    brokerIds.add(broker.nodeId);
                }
                List<Integer> partitions = metadata.partitionsOf(fetchTopic);
                boolean led = !partitions.isEmpty();
                for (int partition : partitions) {
                    led &= brokerIds.contains(metadata.leaderOf(fetchTopic, partition));
                }
                check("metadata names a live leader for every partition", led,
                        partitions.size() + " partitions");
            }
        }

        section("consumer group: several topics, auto-commit and max.poll.records");
        {
            String topicA = unique("java-multi-a");
            String topicB = unique("java-multi-b");
            try (Producer producer = new Producer(host, port, unbatched())) {
                for (int i = 0; i < 6; i++) {
                    producer.send(topicA, bytes("a" + i));
                    producer.send(topicB, bytes("b" + i));
                }
            }
            GroupConfig config = new GroupConfig();
            config.autoCommitIntervalMs = 200;
            config.maxPollRecords = 5;
            GroupConsumer consumer = new GroupConsumer(host, port, unique("java-multi"), config);
            consumer.subscribe(Arrays.asList(topicA, topicB));
            List<ConsumedRecord> seen = new ArrayList<>();
            int largest = 0;
            long deadline = System.currentTimeMillis() + 20_000;
            while (seen.size() < 12 && System.currentTimeMillis() < deadline) {
                List<ConsumedRecord> batch = pollQuietly(consumer, 500);
                largest = Math.max(largest, batch.size());
                seen.addAll(batch);
            }
            Set<String> topics = new HashSet<>();
            for (ConsumedRecord record : seen) {
                topics.add(record.topic);
            }
            check("one member consumes every subscribed topic",
                    seen.size() == 12 && topics.size() == 2,
                    seen.size() + " records from " + topics);
            check("max.poll.records caps each poll", largest >= 1 && largest <= 5,
                    "largest poll " + largest);
            // Nothing calls commit(): these polls are what auto-commit rides on.
            long until = System.currentTimeMillis() + 1_000;
            while (System.currentTimeMillis() < until) {
                pollQuietly(consumer, 100);
            }
            long total = 0;
            for (long offset : consumer.committed(Collections.emptyList()).values()) {
                total += offset;
            }
            check("auto.commit.interval.ms commits delivered positions without commit()",
                    total == 12, "committed " + total);
            consumer.close();
        }

        section("consumer group: heartbeats, session timeout and rejoin");
        {
            String hbTopic = unique("java-heartbeat");
            try (Producer producer = new Producer(host, port, unbatched())) {
                for (int i = 0; i < 4; i++) {
                    producer.send(hbTopic, bytes("h" + i));
                }
            }
            GroupConfig alive = new GroupConfig();
            alive.autoCommitIntervalMs = 0;
            alive.sessionTimeoutMs = 1500;
            alive.heartbeatIntervalMs = 300;
            GroupConsumer steady = new GroupConsumer(host, port, unique("java-hb"), alive);
            steady.subscribe(Collections.singletonList(hbTopic));
            int got = drain(steady, 4, 15_000);
            String member = steady.memberId();
            sleep(3500); // over twice the session timeout, with no poll
            RuntimeException commitError = null;
            try {
                steady.commit();
            } catch (RuntimeException error) {
                commitError = error;
            }
            check("heartbeats keep an idle member in its group past session.timeout.ms",
                    got == 4 && commitError == null && steady.memberId().equals(member),
                    "got=" + got + " commit=" + commitError);
            steady.close();

            GroupConfig silent = new GroupConfig();
            silent.autoCommitIntervalMs = 0;
            silent.sessionTimeoutMs = 1000;
            silent.heartbeatIntervalMs = 20_000; // effectively never, within this check
            GroupConsumer quiet = new GroupConsumer(host, port, unique("java-evicted"), silent);
            quiet.subscribe(Collections.singletonList(hbTopic));
            got = drain(quiet, 4, 15_000);
            String evicted = quiet.memberId();
            sleep(2500);
            RuntimeException fenced = null;
            try {
                quiet.commit();
            } catch (RuntimeException error) {
                fenced = error;
            }
            check("a member that stops heartbeating is evicted after session.timeout.ms",
                    got == 4 && fenced instanceof Protocol.ServerException
                            && ((Protocol.ServerException) fenced).code
                                    == Protocol.ErrorCode.UNKNOWN_MEMBER_ID,
                    "got=" + got + " commit=" + fenced);
            RuntimeException rejoinError = null;
            try {
                // Only the join is checked: with no heartbeats this member will be evicted
                // again one session timeout after it rejoins.
                quiet.poll(1000);
            } catch (RuntimeException error) {
                rejoinError = error;
            }
            check("an evicted member rejoins as a new member",
                    rejoinError == null && !quiet.memberId().isEmpty()
                            && !quiet.memberId().equals(evicted),
                    evicted + " -> " + quiet.memberId() + " error=" + rejoinError);
            quiet.close();
        }

        section("consumer group: static membership, LeaveGroup and rebalances");
        {
            String staticTopic = unique("java-static");
            List<Integer> partitions;
            try (Producer producer = new Producer(host, port, unbatched())) {
                partitions = producer.router().partitions(staticTopic);
                for (int i = 0; i < 4; i++) {
                    producer.send(staticTopic, bytes("st" + i));
                }
            }
            GroupConfig fixed = new GroupConfig();
            fixed.autoCommitIntervalMs = 0;
            fixed.heartbeatIntervalMs = 300;
            fixed.groupInstanceId = unique("java-instance");
            String staticGroup = unique("java-static-grp");
            GroupConsumer first = new GroupConsumer(host, port, staticGroup, fixed);
            first.subscribe(Collections.singletonList(staticTopic));
            awaitAssignment(first, 15_000);
            String firstMember = first.memberId();
            int firstGeneration = first.generation();
            GroupConsumer returning = new GroupConsumer(host, port, staticGroup, fixed);
            returning.subscribe(Collections.singletonList(staticTopic));
            awaitAssignment(returning, 15_000);
            check("a returning group.instance.id reclaims its member id without a rebalance",
                    !firstMember.isEmpty() && returning.memberId().equals(firstMember)
                            && returning.generation() == firstGeneration,
                    firstMember + "/" + firstGeneration + " -> " + returning.memberId() + "/"
                            + returning.generation());
            returning.close();
            first.close();

            // LeaveGroup: with a 30 s session and a 10 s rebalance timeout, a successor could
            // only get the partitions quickly if the first member told the coordinator it left.
            GroupConfig leaving = new GroupConfig();
            leaving.autoCommitIntervalMs = 0;
            leaving.sessionTimeoutMs = 30_000;
            leaving.rebalanceTimeoutMs = 10_000;
            String leaveGroup = unique("java-leave-grp");
            GroupConsumer departing = new GroupConsumer(host, port, leaveGroup, leaving);
            departing.subscribe(Collections.singletonList(staticTopic));
            awaitAssignment(departing, 15_000);
            departing.close();
            long started = System.currentTimeMillis();
            GroupConsumer successor = new GroupConsumer(host, port, leaveGroup, leaving);
            successor.subscribe(Collections.singletonList(staticTopic));
            awaitAssignment(successor, 15_000);
            long took = System.currentTimeMillis() - started;
            check("close sends LeaveGroup, so a successor is not kept waiting",
                    successor.assignment().size() == partitions.size() && took < 6_000,
                    successor.assignment().size() + " partitions after " + took + "ms");
            successor.close();

            // Two members: the second's join makes the coordinator fence the first's
            // generation; its heartbeat learns that, it rejoins, and the partitions split.
            GroupConfig sharing = new GroupConfig();
            sharing.autoCommitIntervalMs = 0;
            sharing.heartbeatIntervalMs = 200;
            String shareGroup = unique("java-share-grp");
            GroupConsumer one = new GroupConsumer(host, port, shareGroup, sharing);
            one.subscribe(Collections.singletonList(staticTopic));
            awaitAssignment(one, 15_000);
            int before = one.generation();
            java.util.concurrent.atomic.AtomicReference<List<GroupConsumer.TopicPartition>>
                    otherHolds = new java.util.concurrent.atomic.AtomicReference<>(
                            Collections.emptyList());
            java.util.concurrent.atomic.AtomicBoolean stop =
                    new java.util.concurrent.atomic.AtomicBoolean();
            Thread second = new Thread(() -> {
                GroupConsumer two = new GroupConsumer(host, port, shareGroup, sharing);
                two.subscribe(Collections.singletonList(staticTopic));
                while (!stop.get()) {
                    pollQuietly(two, 200);
                    otherHolds.set(two.assignment());
                }
                two.close();
            });
            second.start();
            boolean split = false;
            long deadline = System.currentTimeMillis() + 20_000;
            while (!split && System.currentTimeMillis() < deadline) {
                pollQuietly(one, 200);
                Set<GroupConsumer.TopicPartition> union = new HashSet<>(one.assignment());
                union.addAll(otherHolds.get());
                split = !one.assignment().isEmpty() && !otherHolds.get().isEmpty()
                        && union.size() == partitions.size()
                        && one.assignment().size() + otherHolds.get().size()
                                == partitions.size();
            }
            stop.set(true);
            try {
                second.join(10_000);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
            check("a second member rebalances the group and the partitions split between them",
                    split, one.assignment() + " / " + otherHolds.get());
            check("the generation advances when the group rebalances",
                    one.generation() > before, before + " -> " + one.generation());
            one.close();
        }
    }

    /** Poll until {@code want} records have arrived; returns how many did. */
    private static int drain(GroupConsumer consumer, int want, long timeoutMs) {
        int got = 0;
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (got < want && System.currentTimeMillis() < deadline) {
            got += pollQuietly(consumer, 300).size();
        }
        return got;
    }

    /** Poll until the member holds partitions. */
    private static void awaitAssignment(GroupConsumer consumer, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
            pollQuietly(consumer, 200);
        }
    }

    /**
     * A minimal lz4 codec in the broker's format (little-endian uncompressed length, then a raw
     * LZ4 block). It compresses by emitting one literal run — valid LZ4 that any decoder reads —
     * and decodes full LZ4, matches included, so it reads what the broker's lz4 writes too.
     */
    private static final class Lz4Literals implements Protocol.Codec {
        final AtomicInteger compressed = new AtomicInteger();
        final AtomicInteger decompressed = new AtomicInteger();

        @Override
        public byte[] compress(byte[] payload) {
            compressed.incrementAndGet();
            int size = payload.length;
            ByteArrayOutputStream out = new ByteArrayOutputStream(size + size / 255 + 16);
            out.write(size);
            out.write(size >>> 8);
            out.write(size >>> 16);
            out.write(size >>> 24);
            out.write(Math.min(size, 15) << 4);
            if (size >= 15) {
                int rest = size - 15;
                for (; rest >= 255; rest -= 255) {
                    out.write(255);
                }
                out.write(rest);
            }
            out.write(payload, 0, size);
            return out.toByteArray();
        }

        @Override
        public byte[] decompress(byte[] data) {
            decompressed.incrementAndGet();
            try {
                int size = (data[0] & 0xFF) | (data[1] & 0xFF) << 8 | (data[2] & 0xFF) << 16
                        | (data[3] & 0xFF) << 24;
                if (size < 0 || size > 256 * 1024 * 1024) {
                    throw new Protocol.ProtocolException("lz4 size " + size);
                }
                byte[] out = new byte[size];
                int in = 4;
                int at = 0;
                while (in < data.length) {
                    int token = data[in++] & 0xFF;
                    int literals = token >>> 4;
                    if (literals == 15) {
                        int more;
                        do {
                            more = data[in++] & 0xFF;
                            literals += more;
                        } while (more == 255);
                    }
                    System.arraycopy(data, in, out, at, literals);
                    in += literals;
                    at += literals;
                    if (in >= data.length) {
                        break;
                    }
                    int distance = (data[in] & 0xFF) | (data[in + 1] & 0xFF) << 8;
                    in += 2;
                    int length = token & 15;
                    if (length == 15) {
                        int more;
                        do {
                            more = data[in++] & 0xFF;
                            length += more;
                        } while (more == 255);
                    }
                    length += 4;
                    if (distance == 0 || distance > at) {
                        throw new Protocol.ProtocolException("lz4 match before the output");
                    }
                    for (int i = 0; i < length; i++, at++) {
                        out[at] = out[at - distance];
                    }
                }
                if (at != size) {
                    throw new Protocol.ProtocolException("lz4 decoded " + at + " of " + size);
                }
                return out;
            } catch (IndexOutOfBoundsException error) {
                throw new Protocol.ProtocolException("truncated lz4 block");
            }
        }
    }

    /**
     * Sits between a client and the broker, forwarding frames one request at a time, and can
     * answer a produce with a retriable error or a fetch with a corrupt batch. It records the
     * acks and timeout of every produce it sees.
     */
    private static final class FaultProxy implements AutoCloseable {
        private final ServerSocket server;
        private final List<Socket> live = Collections.synchronizedList(new ArrayList<>());
        private int failProduces;
        volatile int corruptFetch;
        volatile int produces;
        volatile int lastAcks = Integer.MIN_VALUE;
        volatile int lastTimeoutMs = Integer.MIN_VALUE;

        FaultProxy(String targetHost, int targetPort) {
            try {
                server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            } catch (IOException error) {
                throw new UncheckedIOException(error);
            }
            daemon(() -> {
                while (true) {
                    Socket client;
                    Socket upstream;
                    try {
                        client = server.accept();
                    } catch (IOException closed) {
                        return;
                    }
                    try {
                        upstream = new Socket(targetHost, targetPort);
                    } catch (IOException error) {
                        closeQuietly(client);
                        continue;
                    }
                    live.add(client);
                    live.add(upstream);
                    daemon(() -> serve(client, upstream));
                }
            });
        }

        int port() {
            return server.getLocalPort();
        }

        /** Fail the next {@code count} produces (-1: every one) and reset the counters. */
        synchronized void failProduces(int count) {
            failProduces = count;
            produces = 0;
        }

        private synchronized boolean takeFailure() {
            if (failProduces == 0) {
                return false;
            }
            if (failProduces > 0) {
                failProduces--;
            }
            return true;
        }

        private void serve(Socket client, Socket upstream) {
            try {
                DataInputStream in = new DataInputStream(client.getInputStream());
                OutputStream out = client.getOutputStream();
                DataInputStream upIn = new DataInputStream(upstream.getInputStream());
                OutputStream upOut = upstream.getOutputStream();
                while (true) {
                    byte[] frame = new byte[in.readInt()];
                    in.readFully(frame);
                    ByteBuffer header = ByteBuffer.wrap(frame);
                    short apiKey = header.getShort(0);
                    int correlation = header.getInt(4);
                    int bodyAt = 10 + Math.max(header.getShort(8), 0);
                    byte[] body = Arrays.copyOfRange(frame, bodyAt, frame.length);
                    byte[] reply = null;
                    boolean oneway = false;
                    if (apiKey == Protocol.ApiKey.PRODUCE) {
                        Protocol.Reader reader = Protocol.Reader.body(body);
                        String topic = reader.string();
                        int partition = reader.int32();
                        int acks = reader.int32();
                        int timeout = reader.int32();
                        produces++;
                        lastAcks = acks;
                        lastTimeoutMs = timeout;
                        oneway = acks == 0;
                        if (takeFailure()) {
                            reply = Protocol.Writer.body().string(topic).int32(partition)
                                    .int32(Protocol.ErrorCode.NOT_ENOUGH_REPLICAS)
                                    .int64(-1).int64(-1).bytes();
                        }
                    } else if (apiKey == Protocol.ApiKey.FETCH && corruptFetch != 0) {
                        Protocol.Reader reader = Protocol.Reader.body(body);
                        String topic = reader.string();
                        int partition = reader.int32();
                        // A batch whose batch_length is negative (mode 1) or runs far past
                        // the bytes that follow (mode 2).
                        byte[] batch = new byte[61];
                        ByteBuffer.wrap(batch).putInt(8, corruptFetch == 1 ? -1 : 1_000_000);
                        reply = Protocol.Writer.body().string(topic).int32(partition).int32(0)
                                .int64(1).int64(1).int64(batch.length).int32(-1).raw(batch)
                                .bytes();
                    }
                    if (reply != null) {
                        out.write(Protocol.encodeFrame(apiKey, correlation, null, reply));
                        out.flush();
                        continue;
                    }
                    writeFrame(upOut, frame);
                    if (oneway) {
                        continue;
                    }
                    byte[] response = new byte[upIn.readInt()];
                    upIn.readFully(response);
                    writeFrame(out, response);
                }
            } catch (IOException | RuntimeException closed) {
                // Either side went away.
            } finally {
                closeQuietly(client);
                closeQuietly(upstream);
            }
        }

        private static void writeFrame(OutputStream out, byte[] payload) throws IOException {
            byte[] framed = new byte[4 + payload.length];
            ByteBuffer.wrap(framed).putInt(payload.length).put(payload);
            out.write(framed);
            out.flush();
        }

        @Override
        public void close() {
            closeQuietly(server);
            synchronized (live) {
                for (Socket socket : live) {
                    closeQuietly(socket);
                }
            }
        }
    }

    /** Fetch partition 0 from the start until {@code want} records arrive or it runs dry. */
    private static List<ConsumedRecord> fetchAll(Consumer consumer, String topic, int want) {
        List<ConsumedRecord> got = new ArrayList<>();
        long offset = 0;
        while (got.size() < want) {
            List<ConsumedRecord> batch;
            try {
                batch = consumer.fetch(topic, 0, offset, 500);
            } catch (Protocol.BrahmaputraException error) {
                break;
            }
            if (batch.isEmpty()) {
                break;
            }
            got.addAll(batch);
            offset = batch.get(batch.size() - 1).offset + 1;
        }
        return got;
    }

    /** Accepts connections and reads them forever without ever answering. */
    private static final class SilentBroker implements AutoCloseable {
        private final ServerSocket server;
        private final List<Socket> accepted = Collections.synchronizedList(new ArrayList<>());

        SilentBroker() {
            try {
                server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            } catch (IOException error) {
                throw new UncheckedIOException(error);
            }
            daemon(() -> {
                while (true) {
                    Socket socket;
                    try {
                        socket = server.accept();
                    } catch (IOException closed) {
                        return;
                    }
                    accepted.add(socket);
                    daemon(() -> drain(socket));
                }
            });
        }

        int port() {
            return server.getLocalPort();
        }

        @Override
        public void close() {
            closeQuietly(server);
            synchronized (accepted) {
                for (Socket socket : accepted) {
                    closeQuietly(socket);
                }
            }
        }
    }

    /**
     * Forwards TCP to the broker and can sever every live connection, which is how a broker
     * restart or an idle timeout looks to a client.
     */
    private static final class DropProxy implements AutoCloseable {
        private final ServerSocket server;
        private final List<Socket> live = new ArrayList<>();

        DropProxy(String targetHost, int targetPort) {
            try {
                server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            } catch (IOException error) {
                throw new UncheckedIOException(error);
            }
            daemon(() -> {
                while (true) {
                    Socket client;
                    try {
                        client = server.accept();
                    } catch (IOException closed) {
                        return;
                    }
                    Socket upstream;
                    try {
                        upstream = new Socket(targetHost, targetPort);
                    } catch (IOException error) {
                        closeQuietly(client);
                        continue;
                    }
                    synchronized (live) {
                        live.add(client);
                        live.add(upstream);
                    }
                    daemon(() -> pipe(client, upstream));
                    daemon(() -> pipe(upstream, client));
                }
            });
        }

        int port() {
            return server.getLocalPort();
        }

        void dropAll() {
            synchronized (live) {
                for (Socket socket : live) {
                    closeQuietly(socket);
                }
                live.clear();
            }
            sleep(50);
        }

        @Override
        public void close() {
            closeQuietly(server);
            dropAll();
        }
    }

    private static void pipe(Socket from, Socket to) {
        byte[] chunk = new byte[64 * 1024];
        try {
            InputStream in = from.getInputStream();
            OutputStream out = to.getOutputStream();
            int read;
            while ((read = in.read(chunk)) >= 0) {
                out.write(chunk, 0, read);
            }
        } catch (IOException closed) {
            // Either side went away; the finally closes the other.
        } finally {
            closeQuietly(from);
            closeQuietly(to);
        }
    }

    private static void drain(Socket socket) {
        byte[] chunk = new byte[4096];
        try {
            InputStream in = socket.getInputStream();
            while (in.read(chunk) >= 0) {
                // Discard: this broker never answers.
            }
        } catch (IOException closed) {
            // Done.
        } finally {
            closeQuietly(socket);
        }
    }

    private static void daemon(Runnable body) {
        Thread thread = new Thread(body);
        thread.setDaemon(true);
        thread.start();
    }

    private static void closeQuietly(java.io.Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException ignored) {
            // Nothing useful to do.
        }
    }

    /** A poll whose failure counts as "nothing delivered", as the Go suite ignores it. */
    private static List<ConsumedRecord> pollQuietly(GroupConsumer consumer, long timeoutMs) {
        try {
            return consumer.poll(timeoutMs);
        } catch (Protocol.BrahmaputraException error) {
            return Collections.emptyList();
        }
    }

    private static boolean startsWith(byte[] data, byte[] prefix) {
        return data != null
                && data.length >= prefix.length
                && Arrays.equals(Arrays.copyOf(data, prefix.length), prefix);
    }
}
