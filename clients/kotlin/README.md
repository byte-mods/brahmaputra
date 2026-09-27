# Brahmaputra client for Kotlin

Idiomatic Kotlin over the verified [Java driver](../java) — the same arrangement as
Kafka's Kotlin users on the Java client. The wire protocol is not reimplemented here:
`io.brahmaputra.kt` adds coroutines, `Flow`, a configuration DSL and data classes, and
the Java driver (compiled from `../java/src/main/java` into the same build, never copied)
does the work.

Kotlin 2.4, JVM 17+. Dependencies: `kotlin-stdlib` and `kotlinx-coroutines-core`.

Verified end to end against a live broker: **88/88 checks** — the Go suite's 54
checks plus 34 covering the rest of the client feature checklist (batch.size, linger.ms,
partitioners, record timestamps, send-and-wait offsets, a registered codec, retries and
request / delivery timeouts through a fault-injecting proxy, bounds-checked decoding, fetch
limits, the high watermark, offsets by timestamp, multi-topic groups, auto-commit,
heartbeats and eviction, static membership, LeaveGroup, rebalances), all driven through
this wrapper's API (`./test.sh HOST PORT`).

## Build

Any of three ways, all compiling the Java driver sources alongside the Kotlin:

```bash
./test.sh 127.0.0.1 9092     # JDK + curl only; fetches kotlinc 2.4 from Maven Central once
mvn package                  # target/brahmaputra-kotlin-0.1.0.jar
gradle build                 # build/libs/brahmaputra-kotlin-0.1.0.jar
```

## Produce

```kotlin
import io.brahmaputra.kt.*

producer {
    bootstrapServers = "127.0.0.1:9092"
    acks = Acks.ALL
    lingerMs = 5
    compression = Compression.GZIP
}.use { producer ->
    // Keyed: murmur2(key) % partitions, so records sharing a key keep their order.
    producer.send(ProducerRecord(
        topic = "orders",
        value = """{"id":1}""".encodeToByteArray(),
        key = "user-7".encodeToByteArray(),
        headers = listOf(Header("trace-id", "abc-123"), Header("retry-of", null)),
    ))

    producer.send("orders", "{\"id\":2}".encodeToByteArray())                 // round-robin
    producer.send("orders", "{\"id\":3}".encodeToByteArray(), partition = 3)  // explicit partition
    producer.send(ProducerRecord.tombstone("orders", "user-7".encodeToByteArray()))
    producer.send(ProducerRecord.of("orders", value = "text", key = "user-9")) // UTF-8 helpers

    producer.flush()   // send everything buffered and wait for the acks

    // A record with its own timestamp (unix ms); null (the default) stamps the wall clock.
    producer.send("orders", "{\"id\":5}".encodeToByteArray(), timestamp = System.currentTimeMillis() - 60_000)

    val offset = producer.sendAndAwait(ProducerRecord.of("orders", "{\"id\":4}"))  // one round trip
    val pinned = producer.sendAndAwait(ProducerRecord.of("orders", "{\"id\":6}", partition = 3))
}
```

`send`, `flush` and `sendAndAwait` are `suspend` functions that run the Java driver's
blocking call on `Dispatchers.IO`, so they never block the caller's dispatcher; each has a
`...Blocking` twin for code outside a coroutine. `send` buffers and returns; a batch's
error surfaces from the `flush` (or the `send` that filled the batch) that sent it, and a
batch the background linger thread failed to send is reported by the next `flush()` or
`close()`. A partition has at most one batch in flight, so sequential sends land in order.

## Consume one partition

```kotlin
consumer("127.0.0.1", 9092).use { consumer ->
    for (record in consumer.fetch("orders", partition = 0, offset = 0)) {
        println("${record.offset} ${record.timestamp} ${record.valueAsString ?: "<tombstone>"}")
    }

    val (records, highWatermark) = consumer.fetchWithWatermark("orders", 0, 0)

    // A cold Flow: to the high watermark, or forever with follow = true.
    consumer.records("orders", 0, fromOffset = 0).collect { println(it) }

    val start = consumer.listOffsets("orders", 0, OffsetSpec.Earliest)
    val end = consumer.listOffsets("orders", 0, OffsetSpec.Latest)
    val anHourAgo = consumer.listOffsets("orders", 0, OffsetSpec.AtTimestamp(System.currentTimeMillis() - 3_600_000))

    val metadata = consumer.metadata(listOf("orders"))   // brokers, partitions, leaders
    val leader = metadata.leaderOf("orders", 0)
}
```

Records are data classes (`Record`, `ProducerRecord`, `Header`, `TopicPartition`,
`FetchResult`) whose equality compares byte contents. A null `value` is a tombstone
(`isTombstone`), distinct from an empty array; a null key and a null header value survive
the round trip as null, and empty ones as empty.

## Consume as a group

```kotlin
groupConsumer {
    bootstrapServers = "127.0.0.1:9092"
    groupId = "billing"
    topics = listOf("orders", "refunds")   // any number of topics
    assignor = Assignor.STICKY
    autoOffsetReset = AutoOffsetReset.EARLIEST
    autoCommitIntervalMs = 0        // commit explicitly
    groupInstanceId = "worker-3"    // static membership
    sessionTimeoutMs = 10_000
    heartbeatIntervalMs = 3_000
}.use { member ->
    member.records().collect { record ->   // Flow<Record>: polls until cancelled
        handle(record)
        member.commit()                    // at-least-once: after processing
    }
    // or batch by batch:  val batch = member.poll(timeoutMs = 500)
}
```

The Java group member is single-threaded, like Kafka's, so the wrapper gives each member a
thread of its own and runs every call there: coroutines may `poll` from any dispatcher, and
a poll suspends rather than blocking the caller. Closing commits and then leaves the group,
so its partitions move immediately rather than after a session timeout.

`maxPollIntervalMs` bounds the time *between* polls; time spent inside `poll`, a slow join
included, does not count. A member that stalls leaves the group and rejoins on its next
`poll`; one the coordinator no longer knows (`UNKNOWN_MEMBER_ID`, from a heartbeat or a
commit) rejoins as a new member, and a commit fenced by a newer generation throws and the
next `poll` rejoins.
`AutoOffsetReset.NONE` makes `poll` throw `NoOffsetForPartitionException`. Cancelling a
coroutine while its poll is in flight discards that poll's records, as abandoning any
consumer's poll would.

## Configuration

Properties of the DSL receivers, named after Kafka's settings.

**`producer { }`** (`ProducerSettings`)

| Property | Kafka name | Default |
|---|---|---|
| `bootstrapServers` | `bootstrap.servers` | `127.0.0.1:9092` (first entry used as seed) |
| `clientId` | `client.id` | `brahmaputra-kotlin` |
| `acks` | `acks` | `Acks.LEADER` (`NONE`, `LEADER`, `ALL`) |
| `batchSize` | `batch.size` | 16384 |
| `lingerMs` | `linger.ms` | 5 (0 sends each record at once) |
| `compression` | `compression.type` | `Compression.NONE` (`GZIP`; others via `Codecs.register`) |
| `requestTimeoutMs` | `request.timeout.ms` | 30000 |
| `retries` | `retries` | 5 |
| `retryBackoffMs` | `retry.backoff.ms` | 100 |
| `deliveryTimeoutMs` | `delivery.timeout.ms` | 120000 |
| `bufferMemory` | `buffer.memory` | 33554432 |
| `maxBlockMs` | `max.block.ms` | 60000 |
| `dialTimeoutMs` | — | 30000 |

Only errors the broker returns before appending are retried, so a retry cannot duplicate a
record; `retries` bounds the attempts after the first and `deliveryTimeoutMs` the whole send.

**`consumer { }`** (`ConsumerSettings`): `bootstrapServers`, `clientId`, `fetchMaxBytes`
(8 MiB), `fetchMinBytes` (1), `fetchMaxWaitMs` (500), `readCommitted` (false),
`rack` (`client.rack`, ""), `maxPollRecords` (`max.poll.records`, the most one `fetch`
returns; 500), `dialTimeoutMs` (30000).

**`groupConsumer { }`** (`GroupConsumerSettings`)

| Property | Kafka name | Default |
|---|---|---|
| `groupId` | `group.id` | required |
| `topics` | `subscribe(...)` | none (or call `subscribe`) |
| `sessionTimeoutMs` | `session.timeout.ms` | 10000 |
| `heartbeatIntervalMs` | `heartbeat.interval.ms` | 3000 (0: a third of the session timeout) |
| `rebalanceTimeoutMs` | `rebalance.timeout.ms` | 3000 |
| `maxPollIntervalMs` | `max.poll.interval.ms` | 300000 |
| `autoCommitIntervalMs` | `auto.commit.interval.ms` | 5000 (0 disables) |
| `autoOffsetReset` | `auto.offset.reset` | `EARLIEST` (`LATEST`, `NONE`) |
| `assignor` | `partition.assignment.strategy` | `RANGE` (`ROUNDROBIN`, `STICKY`) |
| `groupInstanceId` | `group.instance.id` | `""` (dynamic member) |
| `maxPollRecords` | `max.poll.records` | 500 |
| `fetchMaxBytes` | `fetch.max.bytes` | 8388608 |

## Compression

`NONE` and `GZIP` are built in. Register others yourself:

```kotlin
Codecs.register(Compression.ZSTD, compress = { zstd.compress(it) }, decompress = { zstd.decompress(it) })
```

LZ4 must be a little-endian `uint32` uncompressed length followed by a raw LZ4 **block**,
not the LZ4 frame format.

## Connections, errors, and the Java layer

Each wrapper exposes the Java object it wraps as `underlying` (`Client.Producer`,
`Client.Consumer`, `GroupConsumer`, `Client.Connection`). Every request has a round-trip
timeout (2 minutes by default); a timeout, I/O error or correlation mismatch closes the
connection and marks it broken, and the router redials on next use.
`BrokerConnection.connect(host, port)` gives one connection with a settable
`requestTimeoutMs` and `isBroken`, for diagnostics.

Everything thrown is an unchecked `BrahmaputraException` (a typealias of the Java class): a
broker error code arrives as `ServerException` (`.code`, compare with `ErrorCode.*`),
malformed bytes as `ProtocolException`.

## Running the end-to-end suite

Start a broker, then:

```bash
./test.sh 127.0.0.1 9092
```

`test.sh` works from any directory. It downloads `kotlin-compiler-embeddable` 2.4.20 and its
runtime, `kotlin-stdlib` and `kotlinx-coroutines-core` from Maven Central into
`~/.cache/brahmaputra-jvm` (override with `BRAHMAPUTRA_JVM_CACHE`) on first use, verifies
their SHA-1s, compiles the Java driver with `javac -Werror` and then the wrapper and
`src/test/kotlin/.../ManualTest.kt` with `kotlinc -Werror` into `build/test-sh`, and runs
the suite. It prints `88 passed, 0 failed` and exits non-zero on any failure. With a build
tool instead:

```bash
mvn test-compile exec:exec -Dbroker.host=127.0.0.1 -Dbroker.port=9092
gradle manualTest -Pbroker=127.0.0.1:9092
```

## Not implemented

Same as the Java driver: no TLS (so no usable authentication), no idempotent or
transactional producer.
