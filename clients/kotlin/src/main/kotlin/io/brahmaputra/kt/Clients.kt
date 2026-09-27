package io.brahmaputra.kt

import io.brahmaputra.Client
import io.brahmaputra.GroupConsumer as JavaGroupConsumer
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext

/*
 * Every suspend function here runs the Java driver's blocking call on another thread
 * (Dispatchers.IO, or a group member's own thread) so it never blocks the caller's
 * dispatcher. Each has a `...Blocking` twin for code that is not in a coroutine.
 */

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

/**
 * A batching producer over [Client.Producer]. Share one instance: the batching is the point.
 *
 * [send] buffers and returns; errors from a batch surface from the [flush] (or the [send]
 * that filled the batch) that sent it, and a batch the background linger thread failed to
 * send is reported by the next [flush] or [close]. A partition has at most one batch in
 * flight, so a partition's records land in the order they were sent — provided the sends
 * themselves are sequential (one coroutine awaiting each, or one thread).
 */
class Producer(
    /** The Java producer doing the work, for anything this wrapper does not cover. */
    val underlying: Client.Producer,
) : AutoCloseable {

    /** Buffer one record. Suspends (off the caller's dispatcher) while the buffer is full. */
    suspend fun send(record: ProducerRecord) {
        withContext(Dispatchers.IO) { sendBlocking(record) }
    }

    /** Buffer one record; see [ProducerRecord] for what null key/value/partition mean. */
    suspend fun send(
        topic: String,
        value: ByteArray?,
        key: ByteArray? = null,
        partition: Int? = null,
        headers: List<Header> = emptyList(),
        timestamp: Long? = null,
    ) = send(ProducerRecord(topic, value, key, partition, headers, timestamp))

    /** [send] for callers outside a coroutine: blocks the calling thread. */
    fun sendBlocking(record: ProducerRecord) {
        val headers = record.headers.map { it.toJava() }
        val partition = record.partition
        val timestamp = record.timestamp ?: Client.NO_TIMESTAMP
        if (partition != null) {
            underlying.sendTo(record.topic, partition, record.value, record.key, timestamp, headers)
        } else {
            underlying.send(record.topic, record.value, record.key, timestamp, headers)
        }
    }

    /**
     * Send one record on its own and return its offset. A full round trip per record:
     * correct, and slow. Records already buffered for the same partition go first, so the
     * offset never lands ahead of an earlier send.
     */
    suspend fun sendAndAwait(record: ProducerRecord): Long = withContext(Dispatchers.IO) {
        sendAndAwaitBlocking(record)
    }

    /** [sendAndAwait] for callers outside a coroutine. */
    fun sendAndAwaitBlocking(record: ProducerRecord): Long {
        val headers = record.headers.map { it.toJava() }
        val timestamp = record.timestamp ?: Client.NO_TIMESTAMP
        val partition = record.partition
        return if (partition != null) {
            underlying.sendSyncTo(record.topic, partition, record.value, record.key, timestamp, headers)
        } else {
            underlying.sendSync(record.topic, record.value, record.key, timestamp, headers)
        }
    }

    /** Send everything buffered and wait for the acks; also reports failed linger flushes. */
    suspend fun flush() {
        withContext(Dispatchers.IO) { underlying.flush() }
    }

    fun flushBlocking() = underlying.flush()

    /** The partitions of [topic] (auto-creating it when the broker does that). */
    suspend fun partitionsFor(topic: String): List<Int> =
        withContext(Dispatchers.IO) { underlying.router().partitions(topic).toList() }

    /** Flush, then release connections — even when that flush fails, which is rethrown. */
    override fun close() = underlying.close()
}

// ---------------------------------------------------------------------------
// Partition consumer
// ---------------------------------------------------------------------------

/** Reads partitions directly, with no group coordination. Wraps [Client.Consumer]. */
class Consumer(
    val underlying: Client.Consumer,
) : AutoCloseable {

    /** Records of one partition from [offset], waiting up to [maxWaitMs] for some to arrive. */
    suspend fun fetch(topic: String, partition: Int, offset: Long, maxWaitMs: Int = 500): List<Record> =
        fetchWithWatermark(topic, partition, offset, maxWaitMs).records

    fun fetchBlocking(topic: String, partition: Int, offset: Long, maxWaitMs: Int = 500): List<Record> =
        underlying.fetch(topic, partition, offset, maxWaitMs).map { it.toKotlin() }

    /** Like [fetch], and also returns the partition's high watermark. */
    suspend fun fetchWithWatermark(
        topic: String,
        partition: Int,
        offset: Long,
        maxWaitMs: Int = 500,
    ): FetchResult = withContext(Dispatchers.IO) {
        val result = underlying.fetchVerbose(topic, partition, offset, maxWaitMs)
        FetchResult(result.records.map { it.toKotlin() }, result.highWatermark)
    }

    /**
     * The records of one partition as a cold [Flow], starting at [fromOffset].
     *
     * With `follow = false` it completes once it has caught up with the high watermark;
     * with `follow = true` it keeps long-polling for new records until cancelled.
     */
    fun records(
        topic: String,
        partition: Int,
        fromOffset: Long = 0,
        follow: Boolean = false,
        maxWaitMs: Int = 500,
    ): Flow<Record> = flow {
        var next = fromOffset
        while (true) {
            currentCoroutineContext().ensureActive()
            val batch = fetchWithWatermark(topic, partition, next, maxWaitMs)
            for (record in batch.records) {
                emit(record)
                next = record.offset + 1
            }
            if (!follow && (batch.records.isEmpty() || next >= batch.highWatermark)) {
                return@flow
            }
        }
    }

    /** Resolve [OffsetSpec.Earliest], [OffsetSpec.Latest] or a timestamp to an offset. */
    suspend fun listOffsets(topic: String, partition: Int, spec: OffsetSpec): Long =
        withContext(Dispatchers.IO) { underlying.listOffsets(topic, partition, spec.wire) }

    suspend fun partitionsFor(topic: String): List<Int> =
        withContext(Dispatchers.IO) { underlying.partitions(topic).toList() }

    /** The seed broker's ApiVersions answer. */
    suspend fun apiVersions(): ApiVersions =
        withContext(Dispatchers.IO) { underlying.router().seed().apiVersions() }

    /** Fresh metadata for [topics] (all topics when empty). */
    suspend fun metadata(topics: List<String> = emptyList()): ClusterMetadata =
        withContext(Dispatchers.IO) { underlying.router().metadata(topics, true) }

    override fun close() = underlying.close()
}

// ---------------------------------------------------------------------------
// Group consumer
// ---------------------------------------------------------------------------

/**
 * A consumer-group member over [JavaGroupConsumer].
 *
 * Like Kafka's consumer the Java member is single-threaded, so this wrapper gives each member
 * a thread of its own and runs every call on it: suspend functions hop there and back, the
 * `...Blocking` twins wait for it. Coroutines may therefore poll from any dispatcher without
 * breaking the one-thread rule, and a poll never blocks the caller's dispatcher.
 *
 * `max.poll.interval.ms` bounds the time between polls; time spent inside a poll (joining
 * included) does not count. Cancelling a coroutine while its poll is in flight discards that
 * poll's records, as abandoning any consumer's poll would.
 */
class GroupConsumer(
    val underlying: JavaGroupConsumer,
) : AutoCloseable {

    private val thread: ExecutorService = Executors.newSingleThreadExecutor { body ->
        Thread(body, "brahmaputra-group-${SEQUENCE.incrementAndGet()}").apply { isDaemon = true }
    }
    private val dispatcher: CoroutineDispatcher = thread.asCoroutineDispatcher()
    @Volatile private var closed = false

    private fun <T> onMemberThread(body: () -> T): T {
        try {
            return thread.submit<T> { body() }.get()
        } catch (error: ExecutionException) {
            throw error.cause ?: error
        }
    }

    fun subscribe(topics: Collection<String>) = onMemberThread { underlying.subscribe(topics.toList()) }

    fun subscribe(vararg topics: String) = subscribe(topics.toList())

    /** Up to `max.poll.records` records, joining (or rejoining) the group first if needed. */
    suspend fun poll(timeoutMs: Long = 500): List<Record> =
        withContext(dispatcher) { underlying.poll(timeoutMs).map { it.toKotlin() } }

    fun pollBlocking(timeoutMs: Long = 500): List<Record> =
        onMemberThread { underlying.poll(timeoutMs).map { it.toKotlin() } }

    /**
     * Every record this member is assigned, as a [Flow] that polls until cancelled.
     * Poll errors end the flow with that exception (use `retry`/`catch` to taste).
     */
    fun records(pollTimeoutMs: Long = 500): Flow<Record> = flow {
        while (true) {
            currentCoroutineContext().ensureActive()
            for (record in poll(pollTimeoutMs)) {
                emit(record)
            }
        }
    }

    /** Commit the position of every assigned partition. At-least-once: after processing. */
    suspend fun commit() = withContext(dispatcher) { underlying.commit() }

    fun commitBlocking() = onMemberThread { underlying.commit() }

    /** Committed offsets for [partitions] (every assigned partition when empty). */
    suspend fun committed(partitions: List<TopicPartition> = emptyList()): Map<TopicPartition, Long> =
        withContext(dispatcher) {
            underlying.committed(partitions.map { it.toJava() })
                .entries.associate { (tp, offset) -> tp.toKotlin() to offset }
        }

    /** The partitions this member owns; empty before its first poll joins. */
    val assignment: List<TopicPartition> get() = underlying.assignment().map { it.toKotlin() }

    val memberId: String get() = underlying.memberId()

    val generation: Int get() = underlying.generation()

    /** Commit, leave the group (so partitions move at once), and release the member thread. */
    override fun close() {
        if (closed) return
        closed = true
        try {
            onMemberThread { underlying.close() }
        } finally {
            thread.shutdown()
        }
    }

    private companion object {
        val SEQUENCE = AtomicInteger()
    }
}

// ---------------------------------------------------------------------------
// A single connection (diagnostics)
// ---------------------------------------------------------------------------

/**
 * One broker connection, for diagnostics and tests. A request that times out, hits an I/O
 * error or sees a correlation mismatch closes it and marks it [isBroken].
 */
class BrokerConnection(val underlying: Client.Connection) : AutoCloseable {

    /** Bound on one request/response round trip (default 120 s); 0 disables it. */
    var requestTimeoutMs: Int = Client.DEFAULT_REQUEST_TIMEOUT_MS
        set(value) {
            underlying.setRequestTimeout(value)
            field = value
        }

    val isBroken: Boolean get() = underlying.isBroken

    suspend fun apiVersions(): ApiVersions = withContext(Dispatchers.IO) { underlying.apiVersions() }

    override fun close() = underlying.close()

    companion object {
        fun connect(
            host: String,
            port: Int,
            clientId: String = "brahmaputra-kotlin",
            dialTimeoutMs: Int = 30_000,
        ): BrokerConnection = BrokerConnection(Client.Connection.connect(host, port, clientId, dialTimeoutMs))
    }
}
