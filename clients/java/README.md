# Brahmaputra client for Java

Java 17+. No dependencies — standard library only (gzip from `java.util.zip`,
CRC32C from `java.util.zip.CRC32C`).

Verified end to end against a live broker: **54/54 checks**, the same
sections and checks as the Go suite (`./test.sh HOST PORT`).

## Build

With Maven:

```bash
mvn package                  # target/brahmaputra-client-0.1.0.jar
```

or with nothing but a JDK:

```bash
javac --release 17 -d out $(find src/main/java -name '*.java')
```

`groupId` is `io.brahmaputra`, `artifactId` is `brahmaputra-client`. The
whole driver is three files under `src/main/java/io/brahmaputra/`:
`Protocol` (framing, BitPacker bodies, record batches, murmur2, codecs),
`Client` (connection, router, producer, consumer) and `GroupConsumer`.

## Produce

```java
Client.ProducerConfig config = new Client.ProducerConfig();
config.acks = 1;                 // 0, 1 or -1 (all)
config.lingerMs = 5;
config.compressionType = "gzip";

try (Client.Producer producer = new Client.Producer("127.0.0.1", 9092, config)) {
    // Keyed: murmur2(key) % partitions, so records sharing a key keep order.
    producer.send(
            "orders",
            "{\"id\":1}".getBytes(UTF_8),
            "user-7".getBytes(UTF_8),
            new Protocol.RecordHeader("trace-id", "abc-123".getBytes(UTF_8)),
            new Protocol.RecordHeader("retry-of", null));   // null header value survives

    // No key: round-robin across partitions.
    producer.send("orders", "{\"id\":2}".getBytes(UTF_8));

    // An explicit partition, bypassing the partitioner.
    producer.sendTo("orders", 3, "{\"id\":3}".getBytes(UTF_8), null);

    // A tombstone: a null value, distinct from an empty one.
    producer.send("orders", null, "user-7".getBytes(UTF_8));

    producer.flush();   // send everything buffered and wait for the acks

    // Or wait for one record's offset. A full round trip — correct, and slow.
    long offset = producer.sendSync("orders", "{\"id\":4}".getBytes(UTF_8), null, null);
}
```

`send` buffers and returns; errors from a batch surface from the `flush`
(or the `send` that filled the batch) that sent it. A batch the background
linger thread failed to send is reported by the next `flush()` or
`close()` — those records have left the buffer, so nothing else would say
so. `close()` flushes, then releases the connections even if that flush
failed.

A partition has at most one batch in flight: the linger thread and a send
that fills a batch never race each other to the wire, so a partition's
records land in the order they were sent, retries included.

## Consume one partition

```java
try (Client.Consumer consumer =
        new Client.Consumer("127.0.0.1", 9092, new Client.ConsumerConfig())) {
    for (Client.ConsumedRecord record : consumer.fetch("orders", 0, 0, 500)) {
        System.out.println(record.offset + " " + record.timestamp + " "
                + (record.value == null ? "<tombstone>" : new String(record.value, UTF_8)));
    }

    Client.FetchResult result = consumer.fetchVerbose("orders", 0, 0, 500);
    long highWatermark = result.highWatermark;

    long start = consumer.listOffsets("orders", 0, Client.EARLIEST);
    long end = consumer.listOffsets("orders", 0, Client.LATEST);
    long anHourAgo = consumer.listOffsets("orders", 0, System.currentTimeMillis() - 3_600_000);
}
```

## Consume as a group

```java
GroupConsumer.GroupConfig config = new GroupConsumer.GroupConfig();
config.assignor = GroupConsumer.Assignor.STICKY;
config.autoOffsetReset = GroupConsumer.AutoOffsetReset.EARLIEST;
config.autoCommitIntervalMs = 0;      // commit explicitly
config.groupInstanceId = "worker-3";  // static membership

try (GroupConsumer consumer =
        new GroupConsumer("127.0.0.1", 9092, "billing", config)) {
    consumer.subscribe(List.of("orders"));
    while (true) {
        for (Client.ConsumedRecord record : consumer.poll(500)) {
            handle(record.value);
        }
        // At-least-once: commit after processing, never before.
        consumer.commit();
    }
}
```

Closing commits and then leaves the group, so its partitions move
immediately rather than after a session timeout.

A `GroupConsumer` is single-threaded, like Kafka's: one per thread. Its
heartbeat runs on a daemon thread of its own, but joining a group happens
inside `poll`, so two consumers polled alternately from *one* thread will
keep evicting each other — each join waits for the other member, which
cannot rejoin while the thread is busy.

`max.poll.interval.ms` bounds the time *between* polls. Time spent inside
`poll` — including a slow join — does not count; a member that does stall
leaves the group and rejoins on its next `poll`. A member the coordinator
no longer knows (`UNKNOWN_MEMBER_ID`) rejoins as a new one.

`AutoOffsetReset.NONE` makes `poll` throw
`Protocol.NoOffsetForPartitionException` rather than guess where to start.

## Configuration

Public fields on the config classes, named after Kafka's settings.

**`Client.ProducerConfig`**

| Field | Kafka name | Default |
|---|---|---|
| `clientId` | `client.id` | `brahmaputra-java` |
| `acks` | `acks` | `1` (`0`, `1`, `-1`) |
| `batchSize` | `batch.size` | 16384 |
| `lingerMs` | `linger.ms` | 5 (0 sends each record at once) |
| `compressionType` | `compression.type` | `none` |
| `requestTimeoutMs` | `request.timeout.ms` | 30000 |
| `retries` | `retries` | 5 |
| `retryBackoffMs` | `retry.backoff.ms` | 100 |
| `deliveryTimeoutMs` | `delivery.timeout.ms` | 120000 |
| `bufferMemory` | `buffer.memory` | 33554432 |
| `maxBlockMs` | `max.block.ms` | 60000 |
| `dialTimeoutMs` | — | 30000 |

Only errors the broker returns *before* appending are retried
(`NOT_LEADER_OR_FOLLOWER`, leader-epoch errors, `NOT_ENOUGH_REPLICAS`,
`COORDINATOR_LOAD_IN_PROGRESS`, `INTERNAL`), so a retry cannot duplicate
a record. When `buffer.memory` is full, `send` blocks for up to
`max.block.ms` and then throws `producer buffer full`.

**`Client.ConsumerConfig`**

| Field | Kafka name | Default |
|---|---|---|
| `clientId` | `client.id` | `brahmaputra-java` |
| `fetchMaxBytes` | `fetch.max.bytes` | 8388608 |
| `fetchMinBytes` | `fetch.min.bytes` | 1 |
| `fetchMaxWaitMs` | `fetch.max.wait.ms` | 500 |
| `isolationLevel` | `isolation.level` | `Protocol.READ_UNCOMMITTED` |
| `rack` | `client.rack` | `""` |
| `maxPollRecords` | `max.poll.records` | 500 |
| `dialTimeoutMs` | — | 30000 |

**`GroupConsumer.GroupConfig`**

| Field | Kafka name | Default |
|---|---|---|
| `clientId` | `client.id` | `brahmaputra-java` |
| `sessionTimeoutMs` | `session.timeout.ms` | 10000 |
| `rebalanceTimeoutMs` | `rebalance.timeout.ms` | 3000 |
| `maxPollIntervalMs` | `max.poll.interval.ms` | 300000 |
| `autoCommitIntervalMs` | `auto.commit.interval.ms` | 5000 (0 disables) |
| `autoOffsetReset` | `auto.offset.reset` | `EARLIEST` (`LATEST`, `NONE`) |
| `assignor` | `partition.assignment.strategy` | `RANGE` (`ROUNDROBIN`, `STICKY`) |
| `groupInstanceId` | `group.instance.id` | `""` (dynamic member) |
| `maxPollRecords` | `max.poll.records` | 500 |
| `fetchMaxBytes` | `fetch.max.bytes` | 8388608 |
| `dialTimeoutMs` | — | 30000 |

## Compression

`none` and `gzip` are built in. Register anything else yourself, so this
library stays dependency-free:

```java
Protocol.registerCodec(Protocol.Compression.ZSTD, new Protocol.Codec() {
    @Override public byte[] compress(byte[] payload) { ... }
    @Override public byte[] decompress(byte[] payload) { ... }
});
```

If you register lz4, note that the broker expects a little-endian `uint32`
of the uncompressed length followed by a raw LZ4 **block** — not the LZ4
frame format, which a frame-format library would silently produce instead.

## Connections

Each broker connection carries one request at a time and bounds each
round trip with `Client.DEFAULT_REQUEST_TIMEOUT_MS` (2 minutes — longer
than any legitimate long-poll or rebalance wait;
`Connection.setRequestTimeout` changes it). A timeout, I/O error or
correlation mismatch closes the connection and marks it broken
(`isBroken()`), because the byte stream is at an unknown position; the
router redials it on next use, so a dropped socket costs one failed
request, not the client.

## Errors

Everything the driver throws is an unchecked
`Protocol.BrahmaputraException`. A broker error code arrives as
`Protocol.ServerException` with the code in `.code`; malformed bytes on the
wire as `Protocol.ProtocolException`.

## Running the end-to-end suite

Start a broker, then:

```bash
./test.sh 127.0.0.1 9092
```

`test.sh` compiles with plain `javac -Xlint:all -Werror` into `out/` and
runs `io.brahmaputra.ManualTest` (`src/test/java`), a port of the Go
driver's `cmd/manualtest`. It prints `54 passed, 0 failed` and exits
non-zero on any failure. With Maven instead:

```bash
mvn test-compile exec:exec -Dbroker.host=127.0.0.1 -Dbroker.port=9092
```

`mvn test` does not run it — it needs a live broker, so it is not a unit
test.

## Not implemented

Same as the other drivers: no TLS (so no usable authentication — the
broker refuses credentials on plaintext), no idempotent or transactional
producer.
