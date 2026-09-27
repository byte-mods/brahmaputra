package io.brahmaputra.scaladsl

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import io.brahmaputra.{Client, Protocol}
import io.brahmaputra.GroupConsumer as JavaGroupConsumer

// ---------------------------------------------------------------------------
// Enumerations
// ---------------------------------------------------------------------------

/** `acks`: how many replicas must hold a batch before the broker answers. */
enum Acks(val wire: Int) {
  /** Fire and forget (`acks=0`): the broker does not answer. */
  case Zero extends Acks(0)
  /** The partition leader appended the batch (`acks=1`). */
  case Leader extends Acks(1)
  /** Every in-sync replica has the batch (`acks=all`). */
  case All extends Acks(-1)
}

/** `compression.type`. Uncompressed and Gzip are built in; others need [[Codecs.register]]. */
enum Compression(val java: Protocol.Compression) {
  case Uncompressed extends Compression(Protocol.Compression.NONE)
  case Gzip extends Compression(Protocol.Compression.GZIP)
  case Lz4 extends Compression(Protocol.Compression.LZ4)
  case Zstd extends Compression(Protocol.Compression.ZSTD)
  case Snappy extends Compression(Protocol.Compression.SNAPPY)

  /** Kafka's spelling: none, gzip, lz4, zstd, snappy. */
  def label: String = java.label
}

/** `auto.offset.reset`: where a member starts on a partition its group never committed. */
enum AutoOffsetReset(val java: JavaGroupConsumer.AutoOffsetReset) {
  case Earliest extends AutoOffsetReset(JavaGroupConsumer.AutoOffsetReset.EARLIEST)
  case Latest extends AutoOffsetReset(JavaGroupConsumer.AutoOffsetReset.LATEST)
  /** Kafka's `none`: fail the poll with [[NoOffsetForPartitionException]] rather than guess. */
  case Fail extends AutoOffsetReset(JavaGroupConsumer.AutoOffsetReset.NONE)
}

/** `partition.assignment.strategy`. */
enum Assignor(val java: JavaGroupConsumer.Assignor) {
  case Range extends Assignor(JavaGroupConsumer.Assignor.RANGE)
  case RoundRobin extends Assignor(JavaGroupConsumer.Assignor.ROUNDROBIN)
  case Sticky extends Assignor(JavaGroupConsumer.Assignor.STICKY)
}

/** Where [[Consumer.listOffsets]] should look. */
enum OffsetSpec {
  /** The oldest record still retained. */
  case Earliest
  /** The log end: the offset the next record will get. */
  case Latest
  /** The first offset whose timestamp is at or after `epochMillis`. */
  case AtTimestamp(epochMillis: Long)

  def wire: Long = this match {
    case Earliest => Client.EARLIEST
    case Latest => Client.LATEST
    case AtTimestamp(ms) => ms
  }
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/**
 * One record header. `None` is a real header with a null value, distinct from an empty
 * array, and it survives the round trip. (Arrays compare by identity, as everywhere in Scala.)
 */
final case class Header(key: String, value: Option[Array[Byte]]) {
  def valueString: Option[String] = value.map(new String(_, UTF_8))

  override def toString: String = s"Header($key=${Bytes.describe(value)})"
}

object Header {
  def apply(key: String, value: Array[Byte]): Header = Header(key, Option(value))
  def apply(key: String, value: String): Header = Header(key, Some(value.getBytes(UTF_8)))
  /** A header whose value is null. */
  def empty(key: String): Header = Header(key, None)
}

/**
 * A record to send.
 *
 *  - `value = None` is a tombstone, distinct from `Some(Array.emptyByteArray)`.
 *  - `key = None` round-robins across partitions; a key pins the record to
 *    `murmur2(key) % partitions`, so records sharing a key keep their order.
 *  - `partition` bypasses the partitioner.
 *  - `timestamp` is the record's own time in unix milliseconds; `None` stamps the wall clock
 *    when it is sent.
 */
final case class ProducerRecord(
    topic: String,
    value: Option[Array[Byte]],
    key: Option[Array[Byte]] = None,
    partition: Option[Int] = None,
    headers: Seq[Header] = Nil,
    timestamp: Option[Long] = None
) {
  def withKey(key: Array[Byte]): ProducerRecord = copy(key = Option(key))
  def withKey(key: String): ProducerRecord = copy(key = Some(key.getBytes(UTF_8)))
  def toPartition(partition: Int): ProducerRecord = copy(partition = Some(partition))
  def withHeaders(headers: Header*): ProducerRecord = copy(headers = this.headers ++ headers)
  /** Carry this timestamp (unix ms) instead of the wall clock at send time. */
  def withTimestamp(epochMillis: Long): ProducerRecord = copy(timestamp = Some(epochMillis))

  override def toString: String =
    s"ProducerRecord($topic, partition=$partition, key=${Bytes.describe(key)}, " +
      s"value=${Bytes.describe(value)}, headers=$headers, timestamp=$timestamp)"
}

object ProducerRecord {
  def apply(topic: String, value: Array[Byte]): ProducerRecord = ProducerRecord(topic, Option(value))
  def apply(topic: String, value: String): ProducerRecord =
    ProducerRecord(topic, Some(value.getBytes(UTF_8)))
  /** A tombstone: the log-compaction delete marker for `key`. */
  def tombstone(topic: String, key: Array[Byte]): ProducerRecord =
    ProducerRecord(topic, None, key = Some(key))
}

/** A topic and one of its partitions. */
final case class TopicPartition(topic: String, partition: Int) {
  override def toString: String = s"$topic-$partition"
}

/** One record delivered to the application. `timestamp` is absolute unix milliseconds. */
final case class ConsumerRecord(
    topic: String,
    partition: Int,
    offset: Long,
    key: Option[Array[Byte]],
    value: Option[Array[Byte]],
    timestamp: Long,
    headers: Vector[Header]
) {
  /** A tombstone: no value at all, which an empty value is not. */
  def isTombstone: Boolean = value.isEmpty
  def topicPartition: TopicPartition = TopicPartition(topic, partition)
  def keyString: Option[String] = key.map(new String(_, UTF_8))
  def valueString: Option[String] = value.map(new String(_, UTF_8))
  /** The first header named `name`, if there is one. */
  def header(name: String): Option[Header] = headers.find(_.key == name)

  override def toString: String =
    s"ConsumerRecord($topic-$partition@$offset, key=${Bytes.describe(key)}, " +
      s"value=${Bytes.describe(value)}, timestamp=$timestamp, headers=$headers)"
}

/** The records one fetch returned, and the partition's high watermark at the time. */
final case class FetchResult(records: Vector[ConsumerRecord], highWatermark: Long)

// ---------------------------------------------------------------------------
// Cluster information
// ---------------------------------------------------------------------------

final case class ApiVersionRange(apiKey: Int, minVersion: Int, maxVersion: Int)
final case class ApiVersions(ranges: Vector[ApiVersionRange], brokerVersion: String)
final case class BrokerInfo(nodeId: Int, host: String, port: Int, rack: String)
final case class PartitionInfo(partition: Int, leader: Int, replicas: Vector[Int], isr: Vector[Int], leaderEpoch: Int)
final case class TopicInfo(name: String, partitions: Vector[PartitionInfo])
final case class ClusterMetadata(brokers: Vector[BrokerInfo], topics: Vector[TopicInfo])

// ---------------------------------------------------------------------------
// Settings (Kafka names, scala.concurrent.duration for times)
// ---------------------------------------------------------------------------

/**
 * Producer settings. Build with named arguments and `copy`:
 * {{{
 *   ProducerSettings("127.0.0.1:9092", acks = Acks.All, linger = 5.millis)
 * }}}
 */
final case class ProducerSettings(
    bootstrapServers: String = "127.0.0.1:9092",
    clientId: String = "brahmaputra-scala",
    acks: Acks = Acks.Leader,
    /** `batch.size`: flush a partition's buffer once it holds this many bytes. */
    batchSize: Int = 16 * 1024,
    /** `linger.ms`: flush every non-empty buffer at least this often; zero sends at once. */
    linger: FiniteDuration = 5.millis,
    compression: Compression = Compression.Uncompressed,
    requestTimeout: FiniteDuration = 30.seconds,
    /** Retries of errors the broker returns before appending, so a retry cannot duplicate. */
    retries: Int = 5,
    retryBackoff: FiniteDuration = 100.millis,
    /** Caps a whole send, first attempt through last retry. */
    deliveryTimeout: FiniteDuration = 2.minutes,
    /** `buffer.memory`: caps unflushed record bytes held client-side. */
    bufferMemory: Int = 32 * 1024 * 1024,
    /** `max.block.ms`: how long a send may block on a full buffer before failing. */
    maxBlock: FiniteDuration = 60.seconds,
    dialTimeout: FiniteDuration = 30.seconds
) {
  def toJava: Client.ProducerConfig = {
    val c = new Client.ProducerConfig()
    c.clientId = clientId
    c.acks = acks.wire
    c.batchSize = batchSize
    c.lingerMs = Settings.millis(linger)
    c.compressionType = compression.label
    c.requestTimeoutMs = Settings.millis(requestTimeout)
    c.retries = retries
    c.retryBackoffMs = Settings.millis(retryBackoff)
    c.deliveryTimeoutMs = Settings.millis(deliveryTimeout)
    c.bufferMemory = bufferMemory
    c.maxBlockMs = Settings.millis(maxBlock)
    c.dialTimeoutMs = Settings.millis(dialTimeout)
    c
  }
}

/** Settings for a [[Consumer]] that reads partitions directly, with no group. */
final case class ConsumerSettings(
    bootstrapServers: String = "127.0.0.1:9092",
    clientId: String = "brahmaputra-scala",
    fetchMaxBytes: Int = 8 * 1024 * 1024,
    fetchMinBytes: Int = 1,
    fetchMaxWait: FiniteDuration = 500.millis,
    /** `isolation.level = read_committed`: stop at the last stable offset. */
    readCommitted: Boolean = false,
    /** `client.rack`: read from an in-sync replica in this rack when there is one. */
    rack: String = "",
    /** `max.poll.records`: the most records one fetch returns (0: unlimited). */
    maxPollRecords: Int = 500,
    dialTimeout: FiniteDuration = 30.seconds
) {
  def toJava: Client.ConsumerConfig = {
    val c = new Client.ConsumerConfig()
    c.clientId = clientId
    c.fetchMaxBytes = fetchMaxBytes
    c.fetchMinBytes = fetchMinBytes
    c.fetchMaxWaitMs = Settings.millis(fetchMaxWait)
    c.isolationLevel = if (readCommitted) Protocol.READ_COMMITTED else Protocol.READ_UNCOMMITTED
    c.rack = rack
    c.maxPollRecords = maxPollRecords
    c.dialTimeoutMs = Settings.millis(dialTimeout)
    c
  }
}

/** Settings for a [[GroupConsumer]]. */
final case class GroupSettings(
    groupId: String,
    bootstrapServers: String = "127.0.0.1:9092",
    clientId: String = "brahmaputra-scala",
    sessionTimeout: FiniteDuration = 10.seconds,
    /** `heartbeat.interval.ms`: keep it well under `sessionTimeout`. */
    heartbeatInterval: FiniteDuration = 3.seconds,
    rebalanceTimeout: FiniteDuration = 3.seconds,
    /** `max.poll.interval.ms`: bounds the time *between* polls, not time inside one. */
    maxPollInterval: FiniteDuration = 5.minutes,
    /** `auto.commit.interval.ms`; `None` disables auto commit. */
    autoCommitInterval: Option[FiniteDuration] = Some(5.seconds),
    autoOffsetReset: AutoOffsetReset = AutoOffsetReset.Earliest,
    assignor: Assignor = Assignor.Range,
    /** `group.instance.id`: static membership. */
    groupInstanceId: Option[String] = None,
    maxPollRecords: Int = 500,
    fetchMaxBytes: Int = 8 * 1024 * 1024,
    dialTimeout: FiniteDuration = 30.seconds,
    /** Topics to subscribe to at once (or call [[GroupConsumer.subscribe]] later). */
    topics: Seq[String] = Nil
) {
  def toJava: JavaGroupConsumer.GroupConfig = {
    val c = new JavaGroupConsumer.GroupConfig()
    c.clientId = clientId
    c.sessionTimeoutMs = Settings.millis(sessionTimeout)
    c.heartbeatIntervalMs = Settings.millis(heartbeatInterval)
    c.rebalanceTimeoutMs = Settings.millis(rebalanceTimeout)
    c.maxPollIntervalMs = Settings.millis(maxPollInterval)
    c.autoCommitIntervalMs = autoCommitInterval.fold(0)(Settings.millis)
    c.autoOffsetReset = autoOffsetReset.java
    c.assignor = assignor.java
    c.groupInstanceId = groupInstanceId.getOrElse("")
    c.maxPollRecords = maxPollRecords
    c.fetchMaxBytes = fetchMaxBytes
    c.dialTimeoutMs = Settings.millis(dialTimeout)
    c
  }
}

private[scaladsl] object Settings {
  def millis(d: FiniteDuration): Int = math.min(d.toMillis, Int.MaxValue.toLong).toInt

  /** Splits `host:port` (the first entry of a comma-separated list). */
  def bootstrap(servers: String): (String, Int) = {
    val first = servers.split(',').head.trim
    val colon = first.lastIndexOf(':')
    require(colon > 0, s"bootstrapServers must be host:port, got '$servers'")
    val port = first.substring(colon + 1).toIntOption
      .getOrElse(throw new IllegalArgumentException(s"bad port in '$servers'"))
    (first.substring(0, colon).stripPrefix("[").stripSuffix("]"), port)
  }
}

// ---------------------------------------------------------------------------
// Partitioner and codecs
// ---------------------------------------------------------------------------

/** The producer's partitioner: Kafka's murmur2, as the broker computes it. */
object Partitioner {
  /** `murmur2("") == 275646681`. */
  def murmur2(data: Array[Byte]): Int = Protocol.murmur2(data)

  def partitionFor(key: Array[Byte], partitions: Seq[Int]): Int =
    Protocol.partitionForKey(key, partitions.map(Int.box).asJava)
}

/** Plug-in compression, so the driver itself stays dependency-free. */
object Codecs {
  /**
   * Register a codec that is not built in. Lz4 must be a little-endian uint32 uncompressed
   * length plus a raw LZ4 block, not the LZ4 frame format.
   */
  def register(codec: Compression, compress: Array[Byte] => Array[Byte], decompress: Array[Byte] => Array[Byte]): Unit = {
    val (pack, unpack) = (compress, decompress)
    Protocol.registerCodec(
      codec.java,
      new Protocol.Codec {
        def compress(payload: Array[Byte]): Array[Byte] = pack(payload)
        def decompress(payload: Array[Byte]): Array[Byte] = unpack(payload)
      }
    )
  }
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

private[scaladsl] object Bytes {
  def describe(bytes: Option[Array[Byte]]): String = bytes match {
    case None => "None"
    case Some(b) if b.length <= 64 => "\"" + new String(b, UTF_8) + "\""
    case Some(b) => s"<${b.length} bytes>"
  }
}

private[scaladsl] object Convert {
  def header(h: Protocol.RecordHeader): Header = Header(h.key, Option(h.value))

  def header(h: Header): Protocol.RecordHeader = new Protocol.RecordHeader(h.key, h.value.orNull)

  def record(r: Client.ConsumedRecord): ConsumerRecord =
    ConsumerRecord(r.topic, r.partition, r.offset, Option(r.key), Option(r.value), r.timestamp,
      r.headers.asScala.iterator.map(header).toVector)

  def records(rs: java.util.List[Client.ConsumedRecord]): Vector[ConsumerRecord] =
    rs.asScala.iterator.map(record).toVector

  def ints(xs: java.util.List[Integer]): Vector[Int] = xs.asScala.iterator.map(_.intValue).toVector

  def topicPartition(tp: JavaGroupConsumer.TopicPartition): TopicPartition = TopicPartition(tp.topic, tp.partition)

  def topicPartition(tp: TopicPartition): JavaGroupConsumer.TopicPartition =
    new JavaGroupConsumer.TopicPartition(tp.topic, tp.partition)

  def apiVersions(v: Client.ApiVersions): ApiVersions =
    ApiVersions(v.ranges.asScala.iterator.map(r => ApiVersionRange(r.apiKey, r.minVersion, r.maxVersion)).toVector,
      v.brokerVersion)

  def metadata(m: Client.ClusterMetadata): ClusterMetadata =
    ClusterMetadata(
      m.brokers.asScala.iterator.map(b => BrokerInfo(b.nodeId, b.host, b.port, b.rack)).toVector,
      m.topics.asScala.iterator.map { t =>
        TopicInfo(t.name, t.partitions.asScala.iterator.map { p =>
          PartitionInfo(p.partition, p.leader, ints(p.replicas), ints(p.isr), p.leaderEpoch)
        }.toVector)
      }.toVector
    )
}
