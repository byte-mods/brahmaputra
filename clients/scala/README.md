# Brahmaputra client for Scala

Idiomatic Scala 3 over the verified [Java driver](../java) — the same arrangement as
Kafka's Scala users on the Java client. The wire protocol is not reimplemented here:
`io.brahmaputra.scaladsl` adds case classes, `Option` for nullable bytes, `Try` and `Future`
results, `FiniteDuration` settings and `Iterator`/`LazyList` consumption, and the Java driver
(compiled from `../java/src/main/java` into the same build, never copied) does the work.

Scala 3.3 LTS, JVM 17+. No dependencies beyond the Scala library.

Verified end to end against a live broker: **88/88 checks** — the Go suite's 54 checks plus
34 covering the rest of the client feature checklist (batch.size, linger.ms, partitioners,
record timestamps, send-and-wait offsets, a registered codec, retries and request / delivery
timeouts through a fault-injecting proxy, bounds-checked decoding, fetch limits, the high
watermark, offsets by timestamp, multi-topic groups, auto-commit, heartbeats and eviction,
static membership, LeaveGroup, rebalances), all driven through this wrapper's API
(`./test.sh HOST PORT`).

## Build

```bash
./test.sh 127.0.0.1 9092     # JDK + curl only; fetches the Scala 3 compiler from Maven Central once
sbt package                  # mixed Scala/Java compilation of this dir and ../java/src/main/java
```

## Produce

```scala
import io.brahmaputra.scaladsl.*
import scala.concurrent.duration.*
import scala.util.Using

val settings = ProducerSettings("127.0.0.1:9092", acks = Acks.All, linger = 5.millis,
  compression = Compression.Gzip)

Using.resource(Producer(settings)) { producer =>
  // Keyed: murmur2(key) % partitions, so records sharing a key keep their order.
  producer.send(ProducerRecord("orders", """{"id":1}""").withKey("user-7")
    .withHeaders(Header("trace-id", "abc-123"), Header.empty("retry-of"))) // Try[Unit]

  producer.send(ProducerRecord("orders", """{"id":2}"""))                  // round-robin
  producer.send(ProducerRecord("orders", """{"id":3}""").toPartition(3))   // explicit partition
  producer.send(ProducerRecord.tombstone("orders", "user-7".getBytes))     // value = None

  producer.flush().get          // send everything buffered and wait for the acks

  // A record with its own timestamp (unix ms); without one it gets the wall clock.
  producer.send(ProducerRecord("orders", """{"id":5}""").withTimestamp(System.currentTimeMillis() - 60000))

  val offset: Try[Long] = producer.sendAndAwait(ProducerRecord("orders", """{"id":4}"""))
  val pinned: Try[Long] = producer.sendAndAwait(ProducerRecord("orders", """{"id":6}""").toPartition(3))

  // Future-based: runs on the producer's own thread, one at a time, in call order.
  val done: Future[Unit] = producer.sendAsync(ProducerRecord("orders", "async"))
}
```

Blocking calls return `Try` rather than throwing a driver error; `...Async` calls return a
`Future`. `send` buffers and returns; a batch's error surfaces from the `flush` (or the
`send` that filled the batch) that sent it, and a batch the background linger thread failed
to send is reported by the next `flush()` or `close()`. A partition has at most one batch
in flight, so records sent in order — including through `sendAsync` — land in order.

## Consume one partition

```scala
Using.resource(Consumer(ConsumerSettings("127.0.0.1:9092"))) { consumer =>
  for (record <- consumer.fetch("orders", partition = 0, offset = 0).get)
    println(s"${record.offset} ${record.timestamp} ${record.valueString.getOrElse("<tombstone>")}")

  val FetchResult(records, highWatermark) = consumer.fetchWithWatermark("orders", 0, 0).get

  // Lazily, to the high watermark (or forever with follow = true).
  consumer.iterator("orders", 0, from = 0).foreach(println)
  val all: LazyList[ConsumerRecord] = consumer.lazyList("orders", 0)

  val start = consumer.listOffsets("orders", 0, OffsetSpec.Earliest)
  val end = consumer.listOffsets("orders", 0, OffsetSpec.Latest)
  val anHourAgo = consumer.listOffsets("orders", 0, OffsetSpec.AtTimestamp(System.currentTimeMillis() - 3600000))

  val metadata = consumer.metadata(Seq("orders")).get   // brokers, partitions and their leaders
}
```

`ConsumerRecord`, `ProducerRecord`, `Header`, `TopicPartition` and friends are case classes.
Keys, values and header values are `Option[Array[Byte]]`: `None` is null on the wire, so
`value = None` is a tombstone (`isTombstone`), distinct from `Some(Array.emptyByteArray)`.
(Arrays compare by identity, as everywhere in Scala.)

## Consume as a group

```scala
val settings = GroupSettings(
  groupId = "billing",
  bootstrapServers = "127.0.0.1:9092",
  topics = Seq("orders", "refunds"),   // any number of topics
  assignor = Assignor.Sticky,
  autoOffsetReset = AutoOffsetReset.Earliest,
  autoCommitInterval = None,           // commit explicitly
  groupInstanceId = Some("worker-3"),  // static membership
  sessionTimeout = 10.seconds,
  heartbeatInterval = 3.seconds)

Using.resource(GroupConsumer(settings)) { member =>
  for (record <- member.iterator(pollTimeout = 500.millis)) {   // endless; a failed poll throws
    handle(record)
    member.commit().get                                         // at-least-once: after processing
  }
  // or poll by poll:  member.poll(500.millis): Try[Vector[ConsumerRecord]]
  //                   member.pollAsync(500.millis): Future[Vector[ConsumerRecord]]
  //                   member.batches(500.millis): Iterator[Vector[ConsumerRecord]], empty ones included
}
```

The Java group member is single-threaded, like Kafka's, so the wrapper gives each member a
thread of its own and runs every call there; blocking and `Future` calls may be mixed and made
from any thread. Closing commits and then leaves the group, so its partitions move
immediately rather than after a session timeout.

`maxPollInterval` bounds the time *between* polls; time spent inside `poll`, a slow join
included, does not count. A member that stalls leaves the group and rejoins on its next
`poll`; one the coordinator no longer knows (`UNKNOWN_MEMBER_ID`, from a heartbeat or a
commit) rejoins as a new member, and a commit fenced by a newer generation fails and the next
`poll` rejoins.
`AutoOffsetReset.Fail` (Kafka's `none`) makes `poll` fail with
`NoOffsetForPartitionException`.

## Configuration

Case classes with defaults; change them with named arguments or `copy`. Times are
`FiniteDuration`.

**`ProducerSettings`**

| Field | Kafka name | Default |
|---|---|---|
| `bootstrapServers` | `bootstrap.servers` | `127.0.0.1:9092` (first entry used as seed) |
| `clientId` | `client.id` | `brahmaputra-scala` |
| `acks` | `acks` | `Acks.Leader` (`Zero`, `Leader`, `All`) |
| `batchSize` | `batch.size` | 16384 |
| `linger` | `linger.ms` | 5 ms (`Duration.Zero` sends each record at once) |
| `compression` | `compression.type` | `Compression.Uncompressed` (`Gzip`; others via `Codecs.register`) |
| `requestTimeout` | `request.timeout.ms` | 30 s |
| `retries` | `retries` | 5 |
| `retryBackoff` | `retry.backoff.ms` | 100 ms |
| `deliveryTimeout` | `delivery.timeout.ms` | 2 min |
| `bufferMemory` | `buffer.memory` | 33554432 |
| `maxBlock` | `max.block.ms` | 60 s |
| `dialTimeout` | — | 30 s |

Only errors the broker returns before appending are retried, so a retry cannot duplicate a
record; `retries` bounds the attempts after the first and `deliveryTimeout` the whole send.

**`ConsumerSettings`**: `bootstrapServers`, `clientId`, `fetchMaxBytes` (8 MiB),
`fetchMinBytes` (1), `fetchMaxWait` (500 ms), `readCommitted` (false), `rack`
(`client.rack`, ""), `maxPollRecords` (`max.poll.records`: the most one `fetch` returns;
500), `dialTimeout` (30 s).

**`GroupSettings`**

| Field | Kafka name | Default |
|---|---|---|
| `groupId` | `group.id` | required |
| `topics` | `subscribe(...)` | none (or call `subscribe`) |
| `sessionTimeout` | `session.timeout.ms` | 10 s |
| `heartbeatInterval` | `heartbeat.interval.ms` | 3 s (zero: a third of the session timeout) |
| `rebalanceTimeout` | `rebalance.timeout.ms` | 3 s |
| `maxPollInterval` | `max.poll.interval.ms` | 5 min |
| `autoCommitInterval` | `auto.commit.interval.ms` | `Some(5.seconds)` (`None` disables) |
| `autoOffsetReset` | `auto.offset.reset` | `Earliest` (`Latest`, `Fail`) |
| `assignor` | `partition.assignment.strategy` | `Range` (`RoundRobin`, `Sticky`) |
| `groupInstanceId` | `group.instance.id` | `None` (dynamic member) |
| `maxPollRecords` | `max.poll.records` | 500 |
| `fetchMaxBytes` | `fetch.max.bytes` | 8388608 |

## Compression

`Uncompressed` and `Gzip` are built in. Register others yourself:

```scala
Codecs.register(Compression.Zstd, compress = zstd.compress, decompress = zstd.decompress)
```

LZ4 must be a little-endian `uint32` uncompressed length followed by a raw LZ4 **block**,
not the LZ4 frame format.

## Connections, errors, and the Java layer

Each wrapper exposes the Java object it wraps as `underlying`. Every request has a
round-trip timeout (2 minutes by default); a timeout, I/O error or correlation mismatch
closes the connection and marks it broken, and the router redials on next use.
`BrokerConnection.open(host, port)` gives one connection with `setRequestTimeout` and
`isBroken`, for diagnostics.

Failures are the Java driver's unchecked exceptions, carried in `Failure`/failed `Future`s:
`BrahmaputraException`, `ServerException` (broker error code in `.code`, see
`io.brahmaputra.Protocol.ErrorCode`), `ProtocolException` and
`NoOffsetForPartitionException` are type aliases for them.

## Running the end-to-end suite

Start a broker, then:

```bash
./test.sh 127.0.0.1 9092
```

`test.sh` works from any directory and needs only a JDK and curl (no sbt, no scala-cli). It
downloads the Scala 3.3.8 compiler and library from Maven Central into
`~/.cache/brahmaputra-jvm` (override with `BRAHMAPUTRA_JVM_CACHE`) on first use, verifies
their SHA-1s, compiles the Java driver with `javac -Werror` and then the wrapper and
`src/test/scala/.../ManualTest.scala` with `scalac -Werror` into `build/test-sh`, and runs
the suite. It prints `88 passed, 0 failed` and exits non-zero on any failure. With sbt:

```bash
sbt "Test/runMain io.brahmaputra.scaladsl.ManualTest 127.0.0.1 9092"
```

## Not implemented

Same as the Java driver: no TLS (so no usable authentication), no idempotent or
transactional producer.
