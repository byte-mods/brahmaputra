package io.brahmaputra.kt

import io.brahmaputra.Client
import io.brahmaputra.GroupConsumer as JavaGroupConsumer

/** Keeps nested configuration lambdas from reaching into an enclosing builder. */
@DslMarker
annotation class BrahmaputraDsl

/**
 * Producer settings, named as Kafka names them (`linger.ms` is [lingerMs], ...).
 *
 * ```
 * producer {
 *     bootstrapServers = "127.0.0.1:9092"
 *     acks = Acks.ALL
 *     lingerMs = 5
 *     compression = Compression.GZIP
 * }
 * ```
 */
@BrahmaputraDsl
class ProducerSettings {
    /** `host:port` of one broker; the rest of the cluster is discovered from it. */
    var bootstrapServers: String = "127.0.0.1:9092"
    var clientId: String = "brahmaputra-kotlin"
    var acks: Acks = Acks.LEADER
    /** Flush a partition's buffer once it holds this many bytes. */
    var batchSize: Int = 16 * 1024
    /** Flush every non-empty buffer at least this often; 0 sends each record at once. */
    var lingerMs: Int = 5
    var compression: Compression = Compression.NONE
    var requestTimeoutMs: Int = 30_000
    /** Retries of errors the broker returns before appending, so a retry cannot duplicate. */
    var retries: Int = 5
    var retryBackoffMs: Int = 100
    /** Caps a whole send, first attempt through last retry. */
    var deliveryTimeoutMs: Int = 120_000
    /** Caps unflushed record bytes held client-side. */
    var bufferMemory: Int = 32 * 1024 * 1024
    /** How long a send may block on a full buffer before failing with `producer buffer full`. */
    var maxBlockMs: Int = 60_000
    var dialTimeoutMs: Int = 30_000

    fun toJava(): Client.ProducerConfig = Client.ProducerConfig().also {
        it.clientId = clientId
        it.acks = acks.wire
        it.batchSize = batchSize
        it.lingerMs = lingerMs
        it.compressionType = compression.label
        it.requestTimeoutMs = requestTimeoutMs
        it.retries = retries
        it.retryBackoffMs = retryBackoffMs
        it.deliveryTimeoutMs = deliveryTimeoutMs
        it.bufferMemory = bufferMemory
        it.maxBlockMs = maxBlockMs
        it.dialTimeoutMs = dialTimeoutMs
    }
}

/** Settings for a [Consumer] that reads partitions directly, with no group. */
@BrahmaputraDsl
class ConsumerSettings {
    var bootstrapServers: String = "127.0.0.1:9092"
    var clientId: String = "brahmaputra-kotlin"
    var fetchMaxBytes: Int = 8 * 1024 * 1024
    var fetchMinBytes: Int = 1
    var fetchMaxWaitMs: Int = 500
    /** Read up to the last stable offset only (`isolation.level = read_committed`). */
    var readCommitted: Boolean = false
    /** `client.rack`: read from an in-sync replica in this rack when there is one. */
    var rack: String = ""
    /** `max.poll.records`: the most records one fetch returns (0: unlimited). */
    var maxPollRecords: Int = 500
    var dialTimeoutMs: Int = 30_000

    fun toJava(): Client.ConsumerConfig = Client.ConsumerConfig().also {
        it.clientId = clientId
        it.fetchMaxBytes = fetchMaxBytes
        it.fetchMinBytes = fetchMinBytes
        it.fetchMaxWaitMs = fetchMaxWaitMs
        it.isolationLevel =
            if (readCommitted) io.brahmaputra.Protocol.READ_COMMITTED
            else io.brahmaputra.Protocol.READ_UNCOMMITTED
        it.rack = rack
        it.maxPollRecords = maxPollRecords
        it.dialTimeoutMs = dialTimeoutMs
    }
}

/** Settings for a [GroupConsumer]. [groupId] is required. */
@BrahmaputraDsl
class GroupConsumerSettings {
    var bootstrapServers: String = "127.0.0.1:9092"
    var groupId: String = ""
    var clientId: String = "brahmaputra-kotlin"
    var sessionTimeoutMs: Int = 10_000
    /** How often the member heartbeats; keep it well under [sessionTimeoutMs]. 0: a third of it. */
    var heartbeatIntervalMs: Int = 3_000
    var rebalanceTimeoutMs: Int = 3_000
    /** Bounds the time *between* polls; time spent inside a poll does not count. */
    var maxPollIntervalMs: Int = 300_000
    /** 0 disables auto commit. */
    var autoCommitIntervalMs: Int = 5_000
    var autoOffsetReset: AutoOffsetReset = AutoOffsetReset.EARLIEST
    var assignor: Assignor = Assignor.RANGE
    /** `group.instance.id`: static membership when non-empty. */
    var groupInstanceId: String = ""
    var maxPollRecords: Int = 500
    var fetchMaxBytes: Int = 8 * 1024 * 1024
    var dialTimeoutMs: Int = 30_000

    /** Topics to subscribe to straight away (or call [GroupConsumer.subscribe] later). */
    var topics: List<String> = emptyList()

    fun toJava(): JavaGroupConsumer.GroupConfig = JavaGroupConsumer.GroupConfig().also {
        it.clientId = clientId
        it.sessionTimeoutMs = sessionTimeoutMs
        it.heartbeatIntervalMs = heartbeatIntervalMs
        it.rebalanceTimeoutMs = rebalanceTimeoutMs
        it.maxPollIntervalMs = maxPollIntervalMs
        it.autoCommitIntervalMs = autoCommitIntervalMs
        it.autoOffsetReset = autoOffsetReset
        it.assignor = assignor
        it.groupInstanceId = groupInstanceId
        it.maxPollRecords = maxPollRecords
        it.fetchMaxBytes = fetchMaxBytes
        it.dialTimeoutMs = dialTimeoutMs
    }
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/** Open a producer. Connects to the bootstrap broker before returning. */
fun producer(configure: ProducerSettings.() -> Unit): Producer {
    val settings = ProducerSettings().apply(configure)
    val (host, port) = parseBootstrap(settings.bootstrapServers)
    return Producer(Client.Producer(host, port, settings.toJava()))
}

/** Open a producer on [host]:[port]. */
fun producer(host: String, port: Int, configure: ProducerSettings.() -> Unit = {}): Producer =
    producer {
        configure()
        bootstrapServers = "$host:$port"
    }

/** Open a partition consumer (no group). */
fun consumer(configure: ConsumerSettings.() -> Unit): Consumer {
    val settings = ConsumerSettings().apply(configure)
    val (host, port) = parseBootstrap(settings.bootstrapServers)
    return Consumer(Client.Consumer(host, port, settings.toJava()))
}

/** Open a partition consumer on [host]:[port]. */
fun consumer(host: String, port: Int, configure: ConsumerSettings.() -> Unit = {}): Consumer =
    consumer {
        configure()
        bootstrapServers = "$host:$port"
    }

/** Open a group member. It joins on its first poll. */
fun groupConsumer(configure: GroupConsumerSettings.() -> Unit): GroupConsumer {
    val settings = GroupConsumerSettings().apply(configure)
    require(settings.groupId.isNotEmpty()) { "groupId is required" }
    val (host, port) = parseBootstrap(settings.bootstrapServers)
    val member = GroupConsumer(JavaGroupConsumer(host, port, settings.groupId, settings.toJava()))
    if (settings.topics.isNotEmpty()) {
        member.subscribe(settings.topics)
    }
    return member
}

/** Open a member of [groupId] on [host]:[port]. */
fun groupConsumer(
    host: String,
    port: Int,
    groupId: String,
    configure: GroupConsumerSettings.() -> Unit = {},
): GroupConsumer = groupConsumer {
    this.groupId = groupId
    configure()
    bootstrapServers = "$host:$port"
}
