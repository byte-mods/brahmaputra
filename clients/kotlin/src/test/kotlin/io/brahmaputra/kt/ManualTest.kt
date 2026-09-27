package io.brahmaputra.kt

import java.io.Closeable
import java.io.IOException
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.Collections
import kotlin.system.exitProcess
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull

/*
 * Exercises the Kotlin wrapper (and through it the Java driver) against a live broker:
 *
 *   brahmaputra-server --data-dir ./data --default-partitions 4
 *   ./test.sh 127.0.0.1 9092
 *
 * A port of the Go driver's cmd/manualtest, section for section and check for check, written
 * against the wrapper's API. Only the connection-failure checks reach one layer down, through
 * BrokerConnection. Exits 1 if any check failed and 2 on an unexpected error.
 */

private var passed = 0
private var failed = 0

private fun check(name: String, ok: Boolean, detail: String = "") {
    if (ok) {
        passed++
        println("  ok   $name")
    } else {
        failed++
        println(if (detail.isEmpty()) "  FAIL $name" else "  FAIL $name: $detail")
    }
}

private fun section(title: String) {
    println()
    println(title)
}

private fun unique(prefix: String) = "$prefix-${Math.floorMod(System.nanoTime(), 1_000_000_000L)}"

private fun bytes(text: String) = text.encodeToByteArray()

private fun now() = System.currentTimeMillis()

fun main(args: Array<String>) {
    var host = args.getOrElse(0) { "127.0.0.1" }
    var port = args.getOrNull(1)?.toInt() ?: 9092
    // Accept the Go suite's single host:port argument too.
    if (args.size == 1 && host.contains(':')) {
        port = host.substringAfterLast(':').toInt()
        host = host.substringBeforeLast(':')
    }
    try {
        runBlocking { runSuite(host, port) }
    } catch (error: Exception) {
        println("  FATAL $error")
        error.printStackTrace(System.out)
        exitProcess(2)
    }
    println()
    println("$passed passed, $failed failed")
    exitProcess(if (failed > 0) 1 else 0)
}

/** A group poll whose failure counts as "nothing delivered", as the Go suite ignores it. */
private suspend fun GroupConsumer.pollQuietly(timeoutMs: Long): List<Record> =
    try {
        poll(timeoutMs)
    } catch (error: BrahmaputraException) {
        emptyList()
    }

/** Partition 0 from the start until [want] records arrive or it runs dry (or errors). */
private suspend fun Consumer.fetchAll(topic: String, want: Int): List<Record> =
    records(topic, 0).take(want).catch { }.toList()

private suspend fun runSuite(host: String, port: Int) {
    section("connection and metadata")
    consumer(host, port).use { consumer ->
        val versions = consumer.apiVersions()
        check("ApiVersions answers", versions.ranges.isNotEmpty(), "${versions.ranges.size} ranges")
        check("broker reports a version", versions.brokerVersion.isNotEmpty(), versions.brokerVersion)
        val metadata = consumer.metadata()
        check("metadata lists brokers", metadata.brokers.size >= 1, "${metadata.brokers.size} brokers")
    }

    section("produce and consume round trip")
    val topic = unique("kotlin-roundtrip")
    val payloads = List(50) { bytes("record-$it") }
    producer(host, port) { lingerMs = 0 }.use { producer ->
        for (payload in payloads) {
            producer.send(topic, payload, partition = 0)
        }
        producer.flush()
    }
    consumer(host, port).use { consumer ->
        val got = consumer.fetch(topic, 0, 0)
        check("every record comes back", got.size == payloads.size, "got ${got.size}")
        val identical = got.size == payloads.size &&
            got.withIndex().all { (i, r) -> r.value.contentEquals(payloads[i]) && r.offset == i.toLong() }
        check("values byte-identical and offsets contiguous", identical)
    }

    section("compression codecs")
    // Only none and gzip ship in the driver; others are opt-in via Codecs.register.
    for (codec in listOf(Compression.NONE, Compression.GZIP)) {
        val codecTopic = unique("kotlin-${codec.label}")
        val body = bytes("the same line over and over. ".repeat(40))
        producer(host, port) {
            lingerMs = 0
            compression = codec
        }.use { producer ->
            repeat(20) { i -> producer.send(codecTopic, body + ('0' + i % 10).code.toByte(), partition = 0) }
            producer.flush()
        }
        consumer(host, port).use { consumer ->
            val got = consumer.fetch(codecTopic, 0, 0)
            val first = got.firstOrNull()?.value
            check(
                "${codec.label}: round trips",
                got.size == 20 && first != null && first.size > body.size &&
                    first.copyOf(body.size).contentEquals(body),
                "got ${got.size} records",
            )
        }
    }

    section("keys, partitioning and ordering")
    run {
        val keyTopic = unique("kotlin-keys")
        val key = bytes("user-7")
        val partitions = producer(host, port) { lingerMs = 0 }.use { producer ->
            val partitions = producer.partitionsFor(keyTopic)
            repeat(30) { producer.send(keyTopic, bytes("v$it"), key) }
            producer.flush()
            partitions
        }
        val target = Partitioner.partitionFor(key, partitions)
        consumer(host, port).use { consumer ->
            val onTarget = consumer.fetch(keyTopic, target, 0)
            check("a key pins every record to one partition", onTarget.size == 30,
                "partition $target holds ${onTarget.size} of 30")
            check("per-key order is preserved",
                onTarget.size == 30 && onTarget.withIndex().all { (i, r) -> r.valueAsString == "v$i" })
            val strays = partitions.filter { it != target }.sumOf { consumer.fetch(keyTopic, it, 0, 200).size }
            check("no keyed record landed elsewhere", strays == 0, "$strays strays")
        }
    }

    section("murmur2 agrees with the broker's partitioner")
    check("murmur2(\"\") is stable", Partitioner.murmur2(ByteArray(0)) == 275646681,
        Integer.toUnsignedString(Partitioner.murmur2(ByteArray(0))))
    check("murmur2 is deterministic", Partitioner.murmur2(bytes("user-7")) == Partitioner.murmur2(bytes("user-7")))
    check("different keys hash differently",
        Partitioner.murmur2(bytes("user-7")) != Partitioner.murmur2(bytes("user-8")))

    section("record headers and timestamps")
    run {
        val headerTopic = unique("kotlin-headers")
        val before = now() - 1000
        producer(host, port) { lingerMs = 0 }.use { producer ->
            producer.send(ProducerRecord(headerTopic, bytes("annotated"), partition = 0, headers = listOf(
                Header("trace-id", "abc-123"),
                Header("content-type", "application/json"),
                Header("tombstone-reason", null),
            )))
            producer.send(ProducerRecord.of(headerTopic, "plain", partition = 0))
            producer.flush()
        }
        val after = now() + 1000
        consumer(host, port).use { consumer ->
            val got = consumer.fetch(headerTopic, 0, 0)
            check("both records arrive", got.size == 2, "got ${got.size}")
            if (got.size == 2) {
                val (annotated, plain) = got
                check("headers survive the round trip", annotated.headers.size == 3,
                    "${annotated.headers.size} headers")
                check("header values are exact", annotated.header("trace-id").contentEquals(bytes("abc-123")))
                check("a null header value stays null",
                    annotated.headers.size == 3 && annotated.headers[2] == Header("tombstone-reason", null))
                check("a record with no headers gains none from its batch", plain.headers.isEmpty(),
                    "${plain.headers.size} headers")
                check("timestamps are real wall-clock values", got.all { it.timestamp in before..after },
                    "${got.map { it.timestamp }} outside $before..$after")
            }
        }
    }

    section("tombstones")
    run {
        val tombTopic = unique("kotlin-tombstones")
        producer(host, port) { lingerMs = 0 }.use { producer ->
            producer.send(tombTopic, bytes("set"), bytes("k1"), partition = 0)
            producer.send(tombTopic, ByteArray(0), bytes("k2"), partition = 0)
            // A null value is a deletion, and must stay distinguishable from the empty value.
            producer.send(ProducerRecord.tombstone(tombTopic, bytes("k3"), partition = 0))
            producer.flush()
        }
        consumer(host, port).use { consumer ->
            val got = consumer.fetch(tombTopic, 0, 0)
            check("all three records arrive", got.size == 3, "got ${got.size}")
            if (got.size == 3) {
                check("an ordinary value round-trips", got[0].valueAsString == "set")
                check("an empty value is empty, not null", got[1].value?.isEmpty() == true, "${got[1]}")
                check("a tombstone arrives as a null value", got[2].isTombstone, "${got[2]}")
            }
        }
    }

    section("offsets")
    consumer(host, port).use { consumer ->
        val earliest = consumer.listOffsets(topic, 0, OffsetSpec.Earliest)
        val latest = consumer.listOffsets(topic, 0, OffsetSpec.Latest)
        check("earliest is 0 on a fresh topic", earliest == 0L, "$earliest")
        check("latest equals the record count", latest == 50L, "$latest")
    }

    section("acks")
    for (level in listOf(Acks.NONE, Acks.LEADER, Acks.ALL)) {
        val acksTopic = unique("kotlin-acks${level.wire}")
        producer(host, port) {
            lingerMs = 0
            acks = level
        }.use { producer ->
            producer.send(acksTopic, bytes("durable"), partition = 0)
            producer.flush()
        }
        delay(400)
        consumer(host, port).use { consumer ->
            val got = consumer.fetch(acksTopic, 0, 0)
            check("acks=${level.wire} stores the record", got.size == 1, "got ${got.size}")
        }
    }

    section("consumer group: assignment, commit, resume")
    run {
        val groupTopic = unique("kotlin-group")
        val group = unique("kotlin-billing")
        producer(host, port) { lingerMs = 0 }.use { producer ->
            repeat(40) { producer.send(groupTopic, bytes("g$it")) }
            producer.flush()
        }
        val settings: GroupConsumerSettings.() -> Unit = {
            groupId = group
            autoCommitIntervalMs = 0
            topics = listOf(groupTopic)
        }
        val member = groupConsumer(host, port, group, settings)
        // Collected through the Flow API: take(40) stops polling once all 40 arrived.
        val seen = mutableListOf<Record>()
        withTimeoutOrNull(30_000) {
            member.records(pollTimeoutMs = 500).onEach { seen += it }.take(40).toList()
        }
        check("the group consumes every record", seen.size == 40, "got ${seen.size}")
        check("no record is delivered twice", seen.map { it.topicPartition to it.offset }.toSet().size == seen.size)
        member.commit()
        val total = member.committed().values.sum()
        check("commit records a position", total == 40L, "$total")
        member.close()

        // A second member of the same group must resume, not replay.
        groupConsumer(host, port, group, settings).use { rejoined ->
            val replayed = mutableListOf<Record>()
            val until = now() + 5_000
            while (now() < until) {
                replayed += rejoined.pollQuietly(300)
            }
            check("a rejoining group resumes from its commit", replayed.isEmpty(),
                "replayed ${replayed.size} records it had already committed")
        }
    }

    section("auto.offset.reset")
    run {
        val resetTopic = unique("kotlin-reset")
        producer(host, port) { lingerMs = 0 }.use { producer ->
            repeat(10) { producer.send(resetTopic, bytes("r$it")) }
            producer.flush()
        }
        groupConsumer(host, port, unique("kotlin-latest")) {
            autoCommitIntervalMs = 0
            autoOffsetReset = AutoOffsetReset.LATEST
            topics = listOf(resetTopic)
        }.use { member ->
            val skipped = mutableListOf<Record>()
            val until = now() + 4_000
            while (now() < until) {
                skipped += member.pollQuietly(300)
            }
            check("latest skips records produced before the group existed", skipped.isEmpty(),
                "saw ${skipped.size}")
        }

        groupConsumer(host, port, unique("kotlin-none")) {
            autoCommitIntervalMs = 0
            autoOffsetReset = AutoOffsetReset.NONE
            topics = listOf(resetTopic)
        }.use { strict ->
            var raised = false
            val until = now() + 5_000
            while (now() < until && !raised) {
                try {
                    strict.poll(300)
                } catch (error: NoOffsetForPartitionException) {
                    raised = true
                } catch (error: BrahmaputraException) {
                    raised = error.message.orEmpty().contains("no committed offset")
                }
            }
            check("none refuses to guess a position", raised)
        }
    }

    section("assignors")
    for (strategy in Assignor.entries) {
        val name = strategy.name.lowercase()
        val assignorTopic = unique("kotlin-$name")
        producer(host, port) { lingerMs = 0 }.use { producer ->
            repeat(20) { producer.send(assignorTopic, bytes("a$it")) }
            producer.flush()
        }
        groupConsumer(host, port, unique("kotlin-grp-$name")) {
            autoCommitIntervalMs = 0
            assignor = strategy
            topics = listOf(assignorTopic)
        }.use { member ->
            val collected = mutableListOf<Record>()
            val deadline = now() + 20_000
            while (collected.size < 20 && now() < deadline) {
                collected += member.pollQuietly(500)
            }
            check("$name: consumes every record", collected.size == 20, "got ${collected.size}")
        }
    }

    section("bounded client buffer")
    run {
        val bufferTopic = unique("kotlin-buffer")
        val producer = producer(host, port) {
            lingerMs = 10_000 // never flush on time during this check
            bufferMemory = 2048
            maxBlockMs = 300
        }
        val value = ByteArray(256) { 'x'.code.toByte() }
        var blocked = false
        var attempts = 0
        while (attempts++ < 500 && !blocked) {
            try {
                producer.send(bufferTopic, value, partition = 0)
            } catch (error: BrahmaputraException) {
                blocked = error.message.orEmpty().contains("buffer full")
            }
        }
        check("a full buffer blocks and then reports", blocked)
        // Like the Go suite, this producer is abandoned rather than closed: closing would flush
        // the records the check just proved were held back.
    }

    section("wire edge cases")
    run {
        val edgeTopic = unique("kotlin-edge")
        val large = ByteArray(1 shl 20) { (it * 7).toByte() }
        val unicodeKey = bytes("ключ-✓-🔑")
        val unicodeValue = bytes("значение — 数据 — 🚀")
        producer(host, port) { lingerMs = 0 }.use { producer ->
            producer.send(edgeTopic, large, partition = 0)
            producer.send(edgeTopic, unicodeValue, unicodeKey, 0, listOf(Header("ünïcødé-🏷", "✓")))
            // An empty key and an empty header value are values, not nulls.
            producer.send(edgeTopic, bytes("empty-key"), ByteArray(0), 0,
                listOf(Header("empty", ByteArray(0)), Header("null", null)))
            producer.send(edgeTopic, bytes("null-key"), null, 0)
        }
        consumer(host, port).use { consumer ->
            val got = consumer.fetchAll(edgeTopic, 4)
            check("edge records all arrive", got.size == 4, "got ${got.size}")
            if (got.size == 4) {
                check("a 1 MiB value round-trips byte-identical", got[0].value.contentEquals(large),
                    "${got[0].value?.size} bytes")
                val unicode = got[1]
                check("unicode key, value and header key round-trip",
                    unicode.key.contentEquals(unicodeKey) && unicode.value.contentEquals(unicodeValue) &&
                        unicode.headers.map { it.key } == listOf("ünïcødé-🏷"))
                val empty = got[2]
                check("an empty key stays empty, not null", empty.key?.isEmpty() == true, "${empty.key}")
                check("an empty header value stays empty, not null",
                    empty.headers == listOf(Header("empty", ByteArray(0)), Header("null", null)),
                    "${empty.headers}")
                check("a null key stays null", got[3].key == null, "${got[3].key}")
            }
        }
    }

    section("ordering under linger flushes")
    run {
        val orderTopic = unique("kotlin-order")
        val total = 5000
        producer(host, port) {
            lingerMs = 1
            batchSize = 256
        }.use { producer ->
            for (i in 0 until total) {
                producer.send(orderTopic, bytes(i.toString()), partition = 0)
            }
        }
        consumer(host, port).use { consumer ->
            val got = consumer.fetchAll(orderTopic, total)
            val numbers = got.map { it.valueAsString!!.toInt() }
            val inversions = numbers.zipWithNext().count { (a, b) -> b < a }
            check("every record of a partition arrives", got.size == total, "got ${got.size}")
            check("a partition's records keep send order", inversions == 0, "$inversions inversions")
        }
    }

    section("background flush failures are reported")
    run {
        val producer = producer(host, port) { lingerMs = 20 }
        // Partition 999 does not exist, so the linger thread's flush fails.
        val sendError = runCatching { producer.send(unique("kotlin-bgfail"), bytes("lost"), partition = 999) }
            .exceptionOrNull()
        delay(300)
        val flushError = runCatching { producer.flush() }.exceptionOrNull()
        check("a failed linger flush surfaces on the next flush", sendError == null && flushError != null,
            "send=$sendError flush=$flushError")
        val closer = Thread { runCatching { producer.close() } } // returning with an error is still returning
        closer.start()
        closer.join(5_000)
        check("close returns after a failed flush", !closer.isAlive, if (closer.isAlive) "hung" else "")
    }

    section("connection failures")
    run {
        // A broker that accepts and never answers must cost an error, not a thread blocked forever.
        SilentBroker().use { silent ->
            BrokerConnection.connect("127.0.0.1", silent.port, "kotlin-test", 1000).use { connection ->
                connection.requestTimeoutMs = 300
                val started = now()
                val requestError = runCatching { connection.apiVersions() }.exceptionOrNull()
                check("a request to an unresponsive broker times out",
                    requestError != null && now() - started < 3_000, "$requestError")
                check("a timed-out connection is not reused", connection.isBroken)
            }
        }

        // A connection the broker drops is redialled, not kept forever.
        DropProxy(host, port).use { proxy ->
            val dropTopic = unique("kotlin-drop")
            producer("127.0.0.1", proxy.port) { lingerMs = 0 }.use { producer ->
                producer.send(dropTopic, bytes("before"), partition = 0)
                proxy.dropAll()
                var recovered: Throwable? = RuntimeException("not attempted")
                for (attempt in 1..3) {
                    recovered = runCatching { producer.send(dropTopic, bytes("after"), partition = 0) }
                        .exceptionOrNull()
                    if (recovered == null) break
                }
                check("a producer recovers after its connection drops", recovered == null, "$recovered")
            }
            consumer("127.0.0.1", proxy.port).use { consumer ->
                consumer.fetch(dropTopic, 0, 0, 100)
                proxy.dropAll()
                var fetchError: Throwable? = RuntimeException("not attempted")
                var fetched = emptyList<Record>()
                for (attempt in 1..3) {
                    try {
                        fetched = consumer.fetch(dropTopic, 0, 0, 100)
                        fetchError = null
                        break
                    } catch (error: RuntimeException) {
                        fetchError = error
                    }
                }
                check("a consumer recovers after its connection drops",
                    fetchError == null && fetched.isNotEmpty(), "$fetchError")
            }
        }
    }

    section("consumer group: max.poll.interval and rejoin")
    run {
        val slowTopic = unique("kotlin-slow")
        val producer = producer(host, port) { lingerMs = 0 }
        repeat(10) { producer.send(slowTopic, bytes("s$it")) }
        val member = groupConsumer(host, port, unique("kotlin-slow-grp")) {
            autoCommitIntervalMs = 0
            maxPollIntervalMs = 1500
            topics = listOf(slowTopic)
        }
        val first = mutableListOf<Record>()
        var deadline = now() + 15_000
        while (first.size < 10 && now() < deadline) {
            try {
                first += member.poll(300)
            } catch (error: BrahmaputraException) {
                break
            }
        }
        member.commit()
        // Stall past max.poll.interval.ms: the member leaves the group.
        delay(2500)
        for (i in 10 until 20) producer.send(slowTopic, bytes("s$i"))
        producer.close()
        val second = mutableListOf<Record>()
        var pollError: Throwable? = null
        deadline = now() + 15_000
        while (second.size < 10 && now() < deadline) {
            try {
                second += member.poll(300)
            } catch (error: BrahmaputraException) {
                pollError = error
                break
            }
        }
        check("a member that stalled rejoins on its next poll",
            first.size == 10 && second.size == 10 && pollError == null,
            "first=${first.size} second=${second.size} err=$pollError")
        member.close()
    }

    section("consumer group: time inside poll does not count against max.poll.interval")
    run {
        val joinTopic = unique("kotlin-inpoll")
        val producer = producer(host, port) { lingerMs = 0 }
        producer.partitionsFor(joinTopic)
        // Far shorter than the first poll below, which spends ~1s joining (the broker's initial
        // rebalance delay) and then waits for data.
        val member = groupConsumer(host, port, unique("kotlin-inpoll-grp")) {
            autoCommitIntervalMs = 0
            maxPollIntervalMs = 600
            topics = listOf(joinTopic)
        }
        kotlinx.coroutines.coroutineScope {
            // The poll below suspends rather than blocking this coroutine's thread, so a
            // sibling coroutine can produce the records it is waiting for.
            val late = launch(Dispatchers.IO) {
                delay(2000)
                repeat(10) { runCatching { producer.send(joinTopic, bytes("j$it")) } }
            }
            // One long poll: it joins, then waits for the records above.
            val got = runCatching { member.poll(4000) }
            // Committed straight away, before another poll could quietly rejoin: this fails if
            // the member left the group mid-poll.
            val commitError = runCatching { member.commit() }.exceptionOrNull()
            check("a member is still in its group after a long poll",
                got.isSuccess && got.getOrThrow().isNotEmpty() && commitError == null,
                "got=${got.getOrNull()?.size} poll=${got.exceptionOrNull()} commit=$commitError")
            late.join()
        }
        member.close()
        producer.close()
    }
}

// ---------------------------------------------------------------------------
// Fake brokers for the connection-failure checks
// ---------------------------------------------------------------------------

/** Accepts connections and reads them forever without ever answering. */
private class SilentBroker : AutoCloseable {
    private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
    private val accepted = Collections.synchronizedList(mutableListOf<Socket>())
    val port: Int get() = server.localPort

    init {
        daemon {
            while (true) {
                val socket = try { server.accept() } catch (closed: IOException) { return@daemon }
                accepted += socket
                daemon { drain(socket) }
            }
        }
    }

    override fun close() {
        closeQuietly(server)
        synchronized(accepted) { accepted.forEach(::closeQuietly) }
    }
}

/**
 * Forwards TCP to the broker and can sever every live connection, which is how a broker
 * restart or an idle timeout looks to a client.
 */
private class DropProxy(targetHost: String, targetPort: Int) : AutoCloseable {
    private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
    private val live = mutableListOf<Socket>()
    val port: Int get() = server.localPort

    init {
        daemon {
            while (true) {
                val client = try { server.accept() } catch (closed: IOException) { return@daemon }
                val upstream = try {
                    Socket(targetHost, targetPort)
                } catch (error: IOException) {
                    closeQuietly(client)
                    continue
                }
                synchronized(live) {
                    live += client
                    live += upstream
                }
                daemon { pipe(client, upstream) }
                daemon { pipe(upstream, client) }
            }
        }
    }

    fun dropAll() {
        synchronized(live) {
            live.forEach(::closeQuietly)
            live.clear()
        }
        Thread.sleep(50)
    }

    override fun close() {
        closeQuietly(server)
        dropAll()
    }
}

private fun pipe(from: Socket, to: Socket) {
    try {
        from.getInputStream().copyTo(to.getOutputStream(), 64 * 1024)
    } catch (closed: IOException) {
        // Either side went away; the finally closes the other.
    } finally {
        closeQuietly(from)
        closeQuietly(to)
    }
}

private fun drain(socket: Socket) {
    val chunk = ByteArray(4096)
    try {
        val input = socket.getInputStream()
        while (input.read(chunk) >= 0) {
            // Discard: this broker never answers.
        }
    } catch (closed: IOException) {
        // Done.
    } finally {
        closeQuietly(socket)
    }
}

private fun daemon(body: () -> Unit) {
    Thread(body).apply { isDaemon = true }.start()
}

private fun closeQuietly(closeable: Closeable) {
    try {
        closeable.close()
    } catch (ignored: IOException) {
        // Nothing useful to do.
    }
}
