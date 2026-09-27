package io.brahmaputra.kt

import io.brahmaputra.Protocol
import java.io.Closeable
import java.io.DataInputStream
import java.io.IOException
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.ByteBuffer
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
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
 * against the wrapper's API, followed by checks for the rest of the client feature checklist.
 * Only the connection-failure checks reach one layer down, through BrokerConnection, and the
 * fault-injecting proxy builds its fake answers with the Java driver's frame codec. Exits 1 if
 * any check failed and 2 on an unexpected error.
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

    runExtra(host, port)
}

// ---------------------------------------------------------------------------
// Checks beyond the Go suite: every item of the client feature checklist that the sections
// above do not already exercise, each through the wrapper's own API.
// ---------------------------------------------------------------------------

private suspend fun runExtra(host: String, port: Int) {
    section("producer: batch.size and linger.ms")
    run {
        val batchTopic = unique("kotlin-batchsize")
        val value = ByteArray(200) { 'b'.code.toByte() }
        producer(host, port) {
            lingerMs = 60_000 // only batch.size can send anything during this check
            batchSize = 1024
        }.use { producer ->
            consumer(host, port).use { consumer ->
                repeat(8) { producer.send(batchTopic, value, partition = 0) }
                val early = consumer.fetch(batchTopic, 0, 0, 0).size
                check("a batch that reaches batch.size is sent before linger.ms",
                    early in 1..7, "$early of 8 sent before any flush")
                producer.flush()
                val after = consumer.fetchAll(batchTopic, 8).size
                check("flush sends the partial batch that is left", after == 8, "got $after")
            }
        }

        val lingerTopic = unique("kotlin-linger")
        producer(host, port) { lingerMs = 500 }.use { producer ->
            consumer(host, port).use { consumer ->
                producer.partitionsFor(lingerTopic)
                producer.send(lingerTopic, bytes("lingering"), partition = 0)
                val immediate = consumer.fetch(lingerTopic, 0, 0, 0).size
                delay(1500)
                val later = consumer.fetch(lingerTopic, 0, 0, 0).size
                check("linger.ms holds a record back, then sends it without a flush",
                    immediate == 0 && later == 1, "immediately $immediate, after linger $later")
            }
        }
    }

    section("producer: partitioners")
    run {
        val rrTopic = unique("kotlin-rr")
        val pinTopic = unique("kotlin-pinned")
        val partitions: List<Int>
        producer(host, port) { lingerMs = 0 }.use { producer ->
            partitions = producer.partitionsFor(rrTopic)
            repeat(partitions.size * 2) { producer.send(rrTopic, bytes("rr$it")) }
            producer.partitionsFor(pinTopic)
            producer.send(pinTopic, bytes("pinned"), partition = partitions.last())
        }
        consumer(host, port).use { consumer ->
            val counts = partitions.associateWith { consumer.fetch(rrTopic, it, 0, 0).size }
            val pinned = partitions.associateWith { consumer.fetch(pinTopic, it, 0, 0).size }
            check("a null key round-robins across every partition", counts.values.all { it == 2 }, "$counts")
            check("an explicit partition is honoured",
                pinned[partitions.last()] == 1 && pinned.values.sum() == 1, "$pinned")
        }
    }

    section("producer: record timestamps and send-and-wait")
    run {
        val timeTopic = unique("kotlin-timestamps")
        val syncTopic = unique("kotlin-sync")
        val base = now() - 60_000
        val beforeSend = now()
        val offsets: List<Long>
        producer(host, port) { lingerMs = 0 }.use { producer ->
            for (i in 0 until 3) {
                producer.send(timeTopic, bytes("t$i"), partition = 0, timestamp = base + i * 1000L)
            }
            producer.send(timeTopic, bytes("now"), partition = 0)
            offsets = List(2) {
                producer.sendAndAwait(ProducerRecord(syncTopic, bytes("s$it"), partition = 0))
            }
        }
        consumer(host, port).use { consumer ->
            val got = consumer.fetch(timeTopic, 0, 0)
            check("an explicit record timestamp round-trips exactly",
                got.size == 4 && (0 until 3).all { got[it].timestamp == base + it * 1000L },
                "${got.map { it.timestamp }}")
            check("a record without one is stamped with the wall clock",
                got.size == 4 && got[3].timestamp in (beforeSend - 1000)..(now() + 1000),
                "${got.lastOrNull()?.timestamp}")
            check("send-and-wait returns each record's offset", offsets == listOf(0L, 1L), "$offsets")
            val atHalf = consumer.listOffsets(timeTopic, 0, OffsetSpec.AtTimestamp(base + 500))
            val atLast = consumer.listOffsets(timeTopic, 0, OffsetSpec.AtTimestamp(base + 2000))
            check("list offsets by timestamp finds the first record at or after it",
                atHalf == 1L && atLast == 2L, "$atHalf, $atLast")
        }
    }

    section("producer: codec registration")
    run {
        val compressed = AtomicInteger()
        val decompressed = AtomicInteger()
        Codecs.register(
            Compression.LZ4,
            compress = { compressed.incrementAndGet(); lz4Literals(it) },
            decompress = { decompressed.incrementAndGet(); lz4Decode(it) },
        )
        val lz4Topic = unique("kotlin-lz4")
        val sent = List(10) { bytes("lz4 record $it" + " ".repeat(40)) }
        producer(host, port) {
            lingerMs = 0
            compression = Compression.LZ4
        }.use { producer ->
            sent.forEachIndexed { i, value -> producer.send(lz4Topic, value, bytes("k$i"), partition = 0) }
        }
        consumer(host, port).use { consumer ->
            val got = consumer.fetch(lz4Topic, 0, 0)
            check("a registered codec (lz4) compresses sends and decodes fetches",
                got.size == sent.size && got.indices.all { got[it].value.contentEquals(sent[it]) } &&
                    compressed.get() >= 10 && decompressed.get() >= 10,
                "${got.size} records, ${compressed.get()} compressed, ${decompressed.get()} decompressed")
        }
        val refused = runCatching {
            producer(host, port) {
                lingerMs = 0
                compression = Compression.ZSTD
            }.use { it.send(unique("kotlin-zstd"), bytes("x"), partition = 0) }
        }.exceptionOrNull()
        check("an unregistered codec is refused, not sent uncompressed",
            refused?.message?.contains("not registered") == true, "$refused")
    }

    section("producer: retries, request.timeout.ms and delivery.timeout.ms")
    FaultProxy(host, port).use { proxy ->
        val retryTopic = unique("kotlin-retry")
        val settings: ProducerSettings.() -> Unit = {
            lingerMs = 0
            acks = Acks.ALL
            requestTimeoutMs = 1234
            retries = 3
            retryBackoffMs = 150
        }
        proxy.failProduces(2)
        var started = now()
        var error = runCatching {
            producer("127.0.0.1", proxy.port, settings).use { it.send(retryTopic, bytes("retried"), partition = 0) }
        }.exceptionOrNull()
        var elapsed = now() - started
        check("request.timeout.ms and acks travel with every produce",
            proxy.lastTimeoutMs == 1234 && proxy.lastAcks == -1,
            "timeout=${proxy.lastTimeoutMs} acks=${proxy.lastAcks}")
        check("a retriable error is retried after retry.backoff.ms",
            error == null && proxy.produces == 3 && elapsed >= 300,
            "attempts=${proxy.produces} elapsed=$elapsed error=$error")
        consumer(host, port).use { consumer ->
            val stored = consumer.fetch(retryTopic, 0, 0).size
            check("the retried record is stored exactly once", stored == 1, "stored $stored")
        }

        proxy.failProduces(-1)
        error = runCatching {
            producer("127.0.0.1", proxy.port) {
                settings()
                retries = 2
            }.use { it.send(retryTopic, bytes("never"), partition = 0) }
        }.exceptionOrNull()
        check("retries bounds the attempts: the error surfaces after retries + 1",
            error != null && proxy.produces == 3, "attempts=${proxy.produces} error=$error")

        proxy.failProduces(-1)
        started = now()
        error = runCatching {
            producer("127.0.0.1", proxy.port) {
                settings()
                retries = 1_000_000
                retryBackoffMs = 50
                deliveryTimeoutMs = 500
            }.use { it.send(retryTopic, bytes("late"), partition = 0) }
        }.exceptionOrNull()
        elapsed = now() - started
        check("delivery.timeout.ms bounds the time spent retrying",
            error != null && elapsed in 450..2999,
            "elapsed=$elapsed attempts=${proxy.produces} error=$error")

        proxy.failProduces(0)
        for (mode in 1..2) {
            proxy.corruptFetch = mode
            val decodeError = runCatching {
                consumer("127.0.0.1", proxy.port).use { it.fetch(retryTopic, 0, 0, 100) }
            }.exceptionOrNull()
            check(if (mode == 1) "a negative length on the wire is an error"
                  else "a length past the end of the data is an error",
                decodeError is BrahmaputraException, "$decodeError")
        }
        proxy.corruptFetch = 0
    }

    section("consumer: fetch limits, high watermark and metadata")
    run {
        val fetchTopic = unique("kotlin-fetch")
        val value = ByteArray(1000) { 'f'.code.toByte() }
        producer(host, port) { lingerMs = 0 }.use { producer ->
            repeat(10) { producer.send(fetchTopic, value, partition = 0) }
        }
        consumer(host, port) { fetchMaxBytes = 2500 }.use { consumer ->
            val got = consumer.fetch(fetchTopic, 0, 0).size
            check("fetch.max.bytes caps what one fetch returns", got in 1..9, "got $got of 10")
        }
        consumer(host, port) { maxPollRecords = 4 }.use { consumer ->
            val got = consumer.fetch(fetchTopic, 0, 0)
            check("max.poll.records caps one fetch", got.size == 4, "got ${got.size}")
            val next = consumer.fetch(fetchTopic, 0, 4)
            check("the records a cap held back come on the next fetch",
                next.size == 4 && next.first().offset == 4L, "got ${next.size}")
        }
        consumer(host, port) {
            fetchMinBytes = 1_000_000
            fetchMaxWaitMs = 600
        }.use { waiting ->
            consumer(host, port).use { eager ->
                var started = now()
                val waitedFor = waiting.fetch(fetchTopic, 0, 0, 600).size
                val waited = now() - started
                started = now()
                val eagerGot = eager.fetch(fetchTopic, 0, 0, 600).size
                val quick = now() - started
                check("fetch.min.bytes holds a fetch open until fetch.max.wait.ms",
                    waited >= 450 && quick < 400 && waitedFor == 10 && eagerGot == 10,
                    "waited ${waited}ms, eager ${quick}ms")

                val result = eager.fetchWithWatermark(fetchTopic, 0, 0)
                check("the high watermark is reported", result.highWatermark == 10L, "${result.highWatermark}")

                val metadata = eager.metadata(listOf(fetchTopic))
                val brokerIds = metadata.brokers.map { it.nodeId }.toSet()
                val partitions = metadata.partitionsOf(fetchTopic)
                check("metadata names a live leader for every partition",
                    partitions.isNotEmpty() && partitions.all { metadata.leaderOf(fetchTopic, it) in brokerIds },
                    "${partitions.size} partitions")
            }
        }
    }

    section("consumer group: several topics, auto-commit and max.poll.records")
    run {
        val topicA = unique("kotlin-multi-a")
        val topicB = unique("kotlin-multi-b")
        producer(host, port) { lingerMs = 0 }.use { producer ->
            repeat(6) {
                producer.send(topicA, bytes("a$it"))
                producer.send(topicB, bytes("b$it"))
            }
        }
        val member = groupConsumer(host, port, unique("kotlin-multi")) {
            autoCommitIntervalMs = 200
            maxPollRecords = 5
            topics = listOf(topicA, topicB)
        }
        val seen = mutableListOf<Record>()
        var largest = 0
        val deadline = now() + 20_000
        while (seen.size < 12 && now() < deadline) {
            val batch = member.pollQuietly(500)
            largest = maxOf(largest, batch.size)
            seen += batch
        }
        val topics = seen.map { it.topic }.toSet()
        check("one member consumes every subscribed topic",
            seen.size == 12 && topics.size == 2, "${seen.size} records from $topics")
        check("max.poll.records caps each poll", largest in 1..5, "largest poll $largest")
        // Nothing calls commit(): these polls are what auto-commit rides on.
        val until = now() + 1_000
        while (now() < until) member.pollQuietly(100)
        val total = member.committed().values.sum()
        check("auto.commit.interval.ms commits delivered positions without commit()",
            total == 12L, "committed $total")
        member.close()
    }

    section("consumer group: heartbeats, session timeout and rejoin")
    run {
        val hbTopic = unique("kotlin-heartbeat")
        producer(host, port) { lingerMs = 0 }.use { producer ->
            repeat(4) { producer.send(hbTopic, bytes("h$it")) }
        }
        val steady = groupConsumer(host, port, unique("kotlin-hb")) {
            autoCommitIntervalMs = 0
            sessionTimeoutMs = 1500
            heartbeatIntervalMs = 300
            topics = listOf(hbTopic)
        }
        var got = steady.drain(4, 15_000)
        val member = steady.memberId
        delay(3500) // over twice the session timeout, with no poll
        val commitError = runCatching { steady.commit() }.exceptionOrNull()
        check("heartbeats keep an idle member in its group past session.timeout.ms",
            got == 4 && commitError == null && steady.memberId == member, "got=$got commit=$commitError")
        steady.close()

        val quiet = groupConsumer(host, port, unique("kotlin-evicted")) {
            autoCommitIntervalMs = 0
            sessionTimeoutMs = 1000
            heartbeatIntervalMs = 20_000 // effectively never, within this check
            topics = listOf(hbTopic)
        }
        got = quiet.drain(4, 15_000)
        val evicted = quiet.memberId
        delay(2500)
        val fenced = runCatching { quiet.commit() }.exceptionOrNull()
        check("a member that stops heartbeating is evicted after session.timeout.ms",
            got == 4 && fenced is ServerException && fenced.code == ErrorCode.UNKNOWN_MEMBER_ID,
            "got=$got commit=$fenced")
        // Only the join is checked: with no heartbeats this member is evicted again one
        // session timeout after it rejoins.
        val rejoinError = runCatching { quiet.poll(1000) }.exceptionOrNull()
        check("an evicted member rejoins as a new member",
            rejoinError == null && quiet.memberId.isNotEmpty() && quiet.memberId != evicted,
            "$evicted -> ${quiet.memberId} error=$rejoinError")
        quiet.close()
    }

    section("consumer group: static membership, LeaveGroup and rebalances")
    run {
        val staticTopic = unique("kotlin-static")
        val partitions: List<Int>
        producer(host, port) { lingerMs = 0 }.use { producer ->
            partitions = producer.partitionsFor(staticTopic)
            repeat(4) { producer.send(staticTopic, bytes("st$it")) }
        }
        val staticGroup = unique("kotlin-static-grp")
        val instance = unique("kotlin-instance")
        val fixed: GroupConsumerSettings.() -> Unit = {
            autoCommitIntervalMs = 0
            heartbeatIntervalMs = 300
            groupInstanceId = instance
            topics = listOf(staticTopic)
        }
        val first = groupConsumer(host, port, staticGroup, fixed)
        first.awaitAssignment(15_000)
        val firstMember = first.memberId
        val firstGeneration = first.generation
        val returning = groupConsumer(host, port, staticGroup, fixed)
        returning.awaitAssignment(15_000)
        check("a returning group.instance.id reclaims its member id without a rebalance",
            firstMember.isNotEmpty() && returning.memberId == firstMember &&
                returning.generation == firstGeneration,
            "$firstMember/$firstGeneration -> ${returning.memberId}/${returning.generation}")
        returning.close()
        first.close()

        // LeaveGroup: with a 30 s session and a 10 s rebalance timeout, a successor could only
        // get the partitions quickly if the first member told the coordinator it left.
        val leaveGroup = unique("kotlin-leave-grp")
        val leaving: GroupConsumerSettings.() -> Unit = {
            autoCommitIntervalMs = 0
            sessionTimeoutMs = 30_000
            rebalanceTimeoutMs = 10_000
            topics = listOf(staticTopic)
        }
        val departing = groupConsumer(host, port, leaveGroup, leaving)
        departing.awaitAssignment(15_000)
        departing.close()
        val started = now()
        val successor = groupConsumer(host, port, leaveGroup, leaving)
        successor.awaitAssignment(15_000)
        val took = now() - started
        check("close sends LeaveGroup, so a successor is not kept waiting",
            successor.assignment.size == partitions.size && took < 6_000,
            "${successor.assignment.size} partitions after ${took}ms")
        successor.close()

        // Two members: the second's join makes the coordinator fence the first's generation;
        // its heartbeat learns that, it rejoins, and the partitions split.
        val shareGroup = unique("kotlin-share-grp")
        val sharing: GroupConsumerSettings.() -> Unit = {
            autoCommitIntervalMs = 0
            heartbeatIntervalMs = 200
            topics = listOf(staticTopic)
        }
        val one = groupConsumer(host, port, shareGroup, sharing)
        one.awaitAssignment(15_000)
        val before = one.generation
        val two = groupConsumer(host, port, shareGroup, sharing)
        var split = false
        kotlinx.coroutines.coroutineScope {
            // Each member polls on its own thread, so both can take part in the rebalance.
            val other = launch(Dispatchers.IO) {
                while (true) two.pollQuietly(200)
            }
            val deadline = now() + 20_000
            while (!split && now() < deadline) {
                one.pollQuietly(200)
                val mine = one.assignment
                val theirs = two.assignment
                split = mine.isNotEmpty() && theirs.isNotEmpty() &&
                    (mine + theirs).toSet().size == partitions.size && mine.size + theirs.size == partitions.size
            }
            other.cancel()
        }
        check("a second member rebalances the group and the partitions split between them",
            split, "${one.assignment} / ${two.assignment}")
        check("the generation advances when the group rebalances",
            one.generation > before, "$before -> ${one.generation}")
        two.close()
        one.close()
    }
}

/** Poll until [want] records have arrived; returns how many did. */
private suspend fun GroupConsumer.drain(want: Int, timeoutMs: Long): Int {
    var got = 0
    val deadline = now() + timeoutMs
    while (got < want && now() < deadline) got += pollQuietly(300).size
    return got
}

/** Poll until the member holds partitions. */
private suspend fun GroupConsumer.awaitAssignment(timeoutMs: Long) {
    val deadline = now() + timeoutMs
    while (assignment.isEmpty() && now() < deadline) pollQuietly(200)
}

/**
 * lz4 in the broker's format (little-endian uncompressed length, then a raw LZ4 block),
 * compressing by emitting one literal run — valid LZ4 any decoder reads.
 */
private fun lz4Literals(payload: ByteArray): ByteArray {
    val size = payload.size
    val out = java.io.ByteArrayOutputStream(size + size / 255 + 16)
    for (shift in 0 until 32 step 8) out.write(size ushr shift)
    out.write(minOf(size, 15) shl 4)
    if (size >= 15) {
        var rest = size - 15
        while (rest >= 255) {
            out.write(255)
            rest -= 255
        }
        out.write(rest)
    }
    out.write(payload)
    return out.toByteArray()
}

/** Full LZ4 block decoding, matches included, so it reads what the broker's lz4 writes too. */
private fun lz4Decode(data: ByteArray): ByteArray {
    try {
        val size = (0 until 4).fold(0) { acc, i -> acc or ((data[i].toInt() and 0xFF) shl (8 * i)) }
        if (size < 0 || size > 256 * 1024 * 1024) throw ProtocolException("lz4 size $size")
        val out = ByteArray(size)
        var input = 4
        var at = 0
        fun length(start: Int): Int {
            var total = start
            if (start == 15) {
                do {
                    val more = data[input++].toInt() and 0xFF
                    total += more
                } while (more == 255)
            }
            return total
        }
        while (input < data.size) {
            val token = data[input++].toInt() and 0xFF
            val literals = length(token ushr 4)
            System.arraycopy(data, input, out, at, literals)
            input += literals
            at += literals
            if (input >= data.size) break
            val distance = (data[input].toInt() and 0xFF) or ((data[input + 1].toInt() and 0xFF) shl 8)
            input += 2
            val matched = length(token and 15) + 4
            if (distance == 0 || distance > at) throw ProtocolException("lz4 match before the output")
            repeat(matched) {
                out[at] = out[at - distance]
                at++
            }
        }
        if (at != size) throw ProtocolException("lz4 decoded $at of $size")
        return out
    } catch (error: IndexOutOfBoundsException) {
        throw ProtocolException("truncated lz4 block")
    }
}

/**
 * Sits between a client and the broker, forwarding frames one request at a time, and can
 * answer a produce with a retriable error or a fetch with a corrupt batch. It records the acks
 * and timeout of every produce it sees. Built on the Java driver's frame codec.
 */
private class FaultProxy(targetHost: String, targetPort: Int) : AutoCloseable {
    private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
    private val live = Collections.synchronizedList(mutableListOf<Socket>())
    private var failuresLeft = 0
    @Volatile var corruptFetch = 0
    @Volatile var produces = 0
    @Volatile var lastAcks = Int.MIN_VALUE
    @Volatile var lastTimeoutMs = Int.MIN_VALUE
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
                live += client
                live += upstream
                daemon { serve(client, upstream) }
            }
        }
    }

    /** Fail the next [count] produces (-1: every one) and reset the counters. */
    @Synchronized
    fun failProduces(count: Int) {
        failuresLeft = count
        produces = 0
    }

    @Synchronized
    private fun takeFailure(): Boolean {
        if (failuresLeft == 0) return false
        if (failuresLeft > 0) failuresLeft--
        return true
    }

    private fun serve(client: Socket, upstream: Socket) {
        try {
            val input = DataInputStream(client.getInputStream())
            val output = client.getOutputStream()
            val upInput = DataInputStream(upstream.getInputStream())
            val upOutput = upstream.getOutputStream()
            while (true) {
                val frame = ByteArray(input.readInt()).also { input.readFully(it) }
                val header = ByteBuffer.wrap(frame)
                val apiKey = header.getShort(0)
                val correlation = header.getInt(4)
                val body = frame.copyOfRange(10 + maxOf(header.getShort(8).toInt(), 0), frame.size)
                var reply: ByteArray? = null
                var oneway = false
                if (apiKey == Protocol.ApiKey.PRODUCE) {
                    val reader = Protocol.Reader.body(body)
                    val topic = reader.string()
                    val partition = reader.int32()
                    val acks = reader.int32()
                    lastTimeoutMs = reader.int32()
                    lastAcks = acks
                    produces++
                    oneway = acks == 0
                    if (takeFailure()) {
                        reply = Protocol.Writer.body().string(topic).int32(partition)
                            .int32(ErrorCode.NOT_ENOUGH_REPLICAS).int64(-1).int64(-1).bytes()
                    }
                } else if (apiKey == Protocol.ApiKey.FETCH && corruptFetch != 0) {
                    val reader = Protocol.Reader.body(body)
                    val topic = reader.string()
                    val partition = reader.int32()
                    // A batch whose batch_length is negative (mode 1) or runs far past the bytes
                    // that follow (mode 2).
                    val batch = ByteArray(61)
                    ByteBuffer.wrap(batch).putInt(8, if (corruptFetch == 1) -1 else 1_000_000)
                    reply = Protocol.Writer.body().string(topic).int32(partition).int32(0)
                        .int64(1).int64(1).int64(batch.size.toLong()).int32(-1).raw(batch).bytes()
                }
                if (reply != null) {
                    output.write(Protocol.encodeFrame(apiKey, correlation, null, reply))
                    output.flush()
                    continue
                }
                writeFrame(upOutput, frame)
                if (oneway) continue
                writeFrame(output, ByteArray(upInput.readInt()).also { upInput.readFully(it) })
            }
        } catch (closed: IOException) {
            // Either side went away.
        } catch (closed: RuntimeException) {
            // Likewise.
        } finally {
            closeQuietly(client)
            closeQuietly(upstream)
        }
    }

    private fun writeFrame(out: OutputStream, payload: ByteArray) {
        out.write(ByteBuffer.allocate(4 + payload.size).putInt(payload.size).put(payload).array())
        out.flush()
    }

    override fun close() {
        closeQuietly(server)
        synchronized(live) { live.forEach(::closeQuietly) }
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
