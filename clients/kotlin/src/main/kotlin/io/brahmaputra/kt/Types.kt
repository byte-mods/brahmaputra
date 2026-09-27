package io.brahmaputra.kt

import io.brahmaputra.Client
import io.brahmaputra.Protocol

// ---------------------------------------------------------------------------
// Re-exports of the Java driver's types, so callers need only this package.
// ---------------------------------------------------------------------------

/** Everything the driver throws. Unchecked, like every Kotlin exception. */
typealias BrahmaputraException = Protocol.BrahmaputraException

/** A broker error code; the code is in [ServerException.code] (see [ErrorCode]). */
typealias ServerException = Protocol.ServerException

/** Malformed bytes on the wire. */
typealias ProtocolException = Protocol.ProtocolException

/** Thrown by a group poll under `auto.offset.reset = none` when a partition has no commit. */
typealias NoOffsetForPartitionException = Protocol.NoOffsetForPartitionException

/** Broker error codes (`ErrorCode.NOT_LEADER_OR_FOLLOWER`, ...). */
typealias ErrorCode = Protocol.ErrorCode

/** `compression.type`: NONE and GZIP are built in; register others with [Codecs.register]. */
typealias Compression = Protocol.Compression

/** `auto.offset.reset`: EARLIEST, LATEST or NONE. */
typealias AutoOffsetReset = io.brahmaputra.GroupConsumer.AutoOffsetReset

/** `partition.assignment.strategy`: RANGE, ROUNDROBIN or STICKY. */
typealias Assignor = io.brahmaputra.GroupConsumer.Assignor

/** What ApiVersions returns: the key ranges and the broker's version string. */
typealias ApiVersions = Client.ApiVersions

/** A metadata image: brokers, topics, partitions and their leaders. */
typealias ClusterMetadata = Client.ClusterMetadata

/** `acks`: how many replicas must hold a batch before the broker answers. */
enum class Acks(val wire: Int) {
    /** Fire and forget: the broker does not answer at all. */
    NONE(0),

    /** The partition leader has appended the batch. */
    LEADER(1),

    /** Every in-sync replica has the batch. */
    ALL(-1),
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/**
 * One record header. A null [value] is a real, distinct state (not the same as an empty
 * array), and survives the round trip.
 *
 * Equality compares the bytes, not the array identity.
 */
data class Header(val key: String, val value: ByteArray?) {
    constructor(key: String, value: String) : this(key, value.encodeToByteArray())

    /** The value decoded as UTF-8, or null for a null value. */
    val valueAsString: String? get() = value?.decodeToString()

    override fun equals(other: Any?): Boolean =
        other is Header && key == other.key && bytesEqual(value, other.value)

    override fun hashCode(): Int = 31 * key.hashCode() + (value?.contentHashCode() ?: -1)

    override fun toString(): String = "Header($key=${describe(value)})"
}

/**
 * A record to send.
 *
 * - `value == null` is a tombstone, distinct from an empty value.
 * - `key == null` round-robins across partitions; a key pins the record to
 *   `murmur2(key) % partitions`, so records sharing a key keep their order.
 * - [partition] bypasses the partitioner entirely.
 * - [timestamp] is the record's own time in unix milliseconds; null stamps the wall clock
 *   when it is sent.
 */
data class ProducerRecord(
    val topic: String,
    val value: ByteArray?,
    val key: ByteArray? = null,
    val partition: Int? = null,
    val headers: List<Header> = emptyList(),
    val timestamp: Long? = null,
) {
    companion object {
        /** A record whose key and value are UTF-8 text. */
        fun of(
            topic: String,
            value: String?,
            key: String? = null,
            partition: Int? = null,
            headers: List<Header> = emptyList(),
            timestamp: Long? = null,
        ): ProducerRecord =
            ProducerRecord(
                topic, value?.encodeToByteArray(), key?.encodeToByteArray(), partition, headers, timestamp,
            )

        /** A tombstone: a null value, the log-compaction delete marker for [key]. */
        fun tombstone(topic: String, key: ByteArray, partition: Int? = null): ProducerRecord =
            ProducerRecord(topic, null, key, partition)
    }

    override fun equals(other: Any?): Boolean =
        other is ProducerRecord && topic == other.topic && partition == other.partition &&
            bytesEqual(key, other.key) && bytesEqual(value, other.value) &&
            headers == other.headers && timestamp == other.timestamp

    override fun hashCode(): Int =
        listOf(topic, partition, key?.contentHashCode(), value?.contentHashCode(), headers, timestamp)
            .hashCode()

    override fun toString(): String =
        "ProducerRecord(topic=$topic, partition=$partition, key=${describe(key)}, " +
            "value=${describe(value)}, headers=$headers, timestamp=$timestamp)"
}

/** A topic and one of its partitions. */
data class TopicPartition(val topic: String, val partition: Int) {
    override fun toString(): String = "$topic-$partition"
}

/** One record delivered to the application. [timestamp] is absolute unix milliseconds. */
data class Record(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val key: ByteArray?,
    val value: ByteArray?,
    val timestamp: Long,
    val headers: List<Header>,
) {
    /** True for a tombstone: a null value, which an empty value is not. */
    val isTombstone: Boolean get() = value == null

    val topicPartition: TopicPartition get() = TopicPartition(topic, partition)

    /** The key decoded as UTF-8, or null. */
    val keyAsString: String? get() = key?.decodeToString()

    /** The value decoded as UTF-8, or null for a tombstone. */
    val valueAsString: String? get() = value?.decodeToString()

    /** The value of the first header named [name], or null if absent (or null-valued). */
    fun header(name: String): ByteArray? = headers.firstOrNull { it.key == name }?.value

    override fun equals(other: Any?): Boolean =
        other is Record && topic == other.topic && partition == other.partition &&
            offset == other.offset && timestamp == other.timestamp &&
            bytesEqual(key, other.key) && bytesEqual(value, other.value) &&
            headers == other.headers

    override fun hashCode(): Int = listOf(topic, partition, offset).hashCode()

    override fun toString(): String =
        "Record($topic-$partition@$offset, key=${describe(key)}, value=${describe(value)}, " +
            "timestamp=$timestamp, headers=$headers)"
}

/** The records one fetch returned, and the partition's high watermark at the time. */
data class FetchResult(val records: List<Record>, val highWatermark: Long)

/** Where [Consumer.listOffsets] should look. */
sealed interface OffsetSpec {
    /** The oldest record still retained. */
    data object Earliest : OffsetSpec

    /** The log end: the offset the next record will get. */
    data object Latest : OffsetSpec

    /** The first offset whose timestamp is at or after [epochMillis]. */
    data class AtTimestamp(val epochMillis: Long) : OffsetSpec

    val wire: Long
        get() = when (this) {
            Earliest -> Client.EARLIEST
            Latest -> Client.LATEST
            is AtTimestamp -> epochMillis
        }
}

// ---------------------------------------------------------------------------
// Partitioner and codecs
// ---------------------------------------------------------------------------

/** The producer's partitioner, the same Kafka-compatible murmur2 the broker uses. */
object Partitioner {
    /** Kafka's murmur2; `murmur2("") == 275646681`. */
    fun murmur2(data: ByteArray): Int = Protocol.murmur2(data)

    /** The partition a keyed record lands on. */
    fun partitionFor(key: ByteArray, partitions: List<Int>): Int =
        Protocol.partitionForKey(key, partitions)
}

/** Plug-in compression, so the driver itself stays dependency-free. */
object Codecs {
    /**
     * Register an implementation of a codec that is not built in (LZ4, ZSTD, SNAPPY).
     * LZ4 must be a little-endian uint32 uncompressed length plus a raw LZ4 *block*, not
     * the LZ4 frame format.
     */
    fun register(
        codec: Compression,
        compress: (ByteArray) -> ByteArray,
        decompress: (ByteArray) -> ByteArray,
    ) {
        Protocol.registerCodec(codec, object : Protocol.Codec {
            override fun compress(payload: ByteArray): ByteArray = compress(payload)
            override fun decompress(payload: ByteArray): ByteArray = decompress(payload)
        })
    }
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

internal fun bytesEqual(a: ByteArray?, b: ByteArray?): Boolean =
    if (a == null || b == null) a === b else a.contentEquals(b)

private fun describe(bytes: ByteArray?): String = when {
    bytes == null -> "null"
    bytes.size <= 64 -> "\"" + bytes.decodeToString() + "\""
    else -> "<${bytes.size} bytes>"
}

internal fun Header.toJava(): Protocol.RecordHeader = Protocol.RecordHeader(key, value)

internal fun Protocol.RecordHeader.toKotlin(): Header = Header(key, value)

internal fun Client.ConsumedRecord.toKotlin(): Record =
    Record(topic, partition, offset, key, value, timestamp, headers.map { it.toKotlin() })

internal fun io.brahmaputra.GroupConsumer.TopicPartition.toKotlin(): TopicPartition =
    TopicPartition(topic, partition)

internal fun TopicPartition.toJava(): io.brahmaputra.GroupConsumer.TopicPartition =
    io.brahmaputra.GroupConsumer.TopicPartition(topic, partition)

/** Splits `host:port` (the first entry of a comma-separated list). */
internal fun parseBootstrap(servers: String): Pair<String, Int> {
    val first = servers.split(',').first().trim()
    val colon = first.lastIndexOf(':')
    require(colon > 0) { "bootstrapServers must be host:port, got '$servers'" }
    val port = first.substring(colon + 1).toIntOrNull()
        ?: throw IllegalArgumentException("bad port in bootstrapServers '$servers'")
    return first.substring(0, colon).removePrefix("[").removeSuffix("]") to port
}
