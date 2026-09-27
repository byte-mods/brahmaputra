package io.brahmaputra.scaladsl

import java.io.{Closeable, IOException}
import java.net.{InetAddress, ServerSocket, Socket}
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try, Using}

/**
 * Exercises the Scala wrapper (and through it the Java driver) against a live broker:
 * {{{
 *   brahmaputra-server --data-dir ./data --default-partitions 4
 *   ./test.sh 127.0.0.1 9092
 * }}}
 * A port of the Go driver's cmd/manualtest, section for section and check for check, written
 * against the wrapper's API. Only the connection-failure checks go a layer down, through
 * [[BrokerConnection]]. Exits 1 if any check failed and 2 on an unexpected error.
 */
object ManualTest {
  private var passed = 0
  private var failed = 0

  private def check(name: String, ok: Boolean, detail: => String = ""): Unit =
    if (ok) {
      passed += 1
      println(s"  ok   $name")
    } else {
      failed += 1
      val d = detail
      println(if (d.isEmpty) s"  FAIL $name" else s"  FAIL $name: $d")
    }

  private def section(title: String): Unit = {
    println()
    println(title)
  }

  private def unique(prefix: String): String = s"$prefix-${Math.floorMod(System.nanoTime(), 1000000000L)}"

  private def bytes(text: String): Array[Byte] = text.getBytes(UTF_8)

  private def now(): Long = System.currentTimeMillis()

  private def same(a: Option[Array[Byte]], b: Array[Byte]): Boolean = a.exists(java.util.Arrays.equals(_, b))

  def main(args: Array[String]): Unit = {
    var host = args.headOption.getOrElse("127.0.0.1")
    var port = args.lift(1).map(_.toInt).getOrElse(9092)
    // Accept the Go suite's single host:port argument too.
    if (args.length == 1 && host.contains(":")) {
      port = host.substring(host.lastIndexOf(':') + 1).toInt
      host = host.substring(0, host.lastIndexOf(':'))
    }
    Try(run(host, port)) match {
      case Failure(error) =>
        println(s"  FATAL $error")
        error.printStackTrace(System.out)
        sys.exit(2)
      case Success(_) =>
    }
    println()
    println(s"$passed passed, $failed failed")
    sys.exit(if (failed > 0) 1 else 0)
  }

  private def run(host: String, port: Int): Unit = {
    val bootstrap = s"$host:$port"
    val unbatched = ProducerSettings(bootstrap, linger = Duration.Zero)
    val consumerSettings = ConsumerSettings(bootstrap)
    def group(id: String, topic: String): GroupSettings =
      GroupSettings(id, bootstrap, autoCommitInterval = None, topics = Seq(topic))

    /** A group poll whose failure counts as "nothing delivered", as the Go suite ignores it. */
    def pollQuietly(member: GroupConsumer, timeout: FiniteDuration): Vector[ConsumerRecord] =
      member.poll(timeout).getOrElse(Vector.empty)

    /** Partition 0 from the start until `want` records arrive, it runs dry, or a fetch fails. */
    def fetchAll(consumer: Consumer, topic: String, want: Int): Vector[ConsumerRecord] = {
      val it = consumer.iterator(topic, 0)
      val got = Vector.newBuilder[ConsumerRecord]
      var n = 0
      while (n < want && Try(it.hasNext).getOrElse(false)) {
        got += it.next()
        n += 1
      }
      got.result()
    }

    section("connection and metadata")
    Using.resource(Consumer(consumerSettings)) { consumer =>
      val versions = consumer.apiVersions().get
      check("ApiVersions answers", versions.ranges.nonEmpty, s"${versions.ranges.size} ranges")
      check("broker reports a version", versions.brokerVersion.nonEmpty, versions.brokerVersion)
      val metadata = consumer.metadata().get
      check("metadata lists brokers", metadata.brokers.nonEmpty, s"${metadata.brokers.size} brokers")
    }

    section("produce and consume round trip")
    val topic = unique("scala-roundtrip")
    val payloads = Vector.tabulate(50)(i => bytes(s"record-$i"))
    Using.resource(Producer(unbatched)) { producer =>
      payloads.foreach(p => producer.send(ProducerRecord(topic, p).toPartition(0)).get)
      producer.flush().get
    }
    Using.resource(Consumer(consumerSettings)) { consumer =>
      val got = consumer.fetch(topic, 0, 0).get
      check("every record comes back", got.size == payloads.size, s"got ${got.size}")
      val identical = got.size == payloads.size && got.zipWithIndex.forall { case (r, i) =>
        same(r.value, payloads(i)) && r.offset == i
      }
      check("values byte-identical and offsets contiguous", identical)
    }

    section("compression codecs")
    // Only none and gzip ship in the driver; others are opt-in via Codecs.register.
    for (codec <- Seq(Compression.Uncompressed, Compression.Gzip)) {
      val codecTopic = unique(s"scala-${codec.label}")
      val body = bytes("the same line over and over. " * 40)
      Using.resource(Producer(unbatched.copy(compression = codec))) { producer =>
        for (i <- 0 until 20) producer.send(ProducerRecord(codecTopic, body :+ ('0' + i % 10).toByte).toPartition(0)).get
        producer.flush().get
      }
      Using.resource(Consumer(consumerSettings)) { consumer =>
        val got = consumer.fetch(codecTopic, 0, 0).get
        check(s"${codec.label}: round trips",
          got.size == 20 && got.head.value.exists(_.startsWith(body)), s"got ${got.size} records")
      }
    }

    section("keys, partitioning and ordering")
    locally {
      val keyTopic = unique("scala-keys")
      val key = bytes("user-7")
      val partitions = Using.resource(Producer(unbatched)) { producer =>
        val partitions = producer.partitionsFor(keyTopic).get
        for (i <- 0 until 30) producer.send(ProducerRecord(keyTopic, s"v$i").withKey(key)).get
        producer.flush().get
        partitions
      }
      val target = Partitioner.partitionFor(key, partitions)
      Using.resource(Consumer(consumerSettings)) { consumer =>
        val onTarget = consumer.fetch(keyTopic, target, 0).get
        check("a key pins every record to one partition", onTarget.size == 30,
          s"partition $target holds ${onTarget.size} of 30")
        check("per-key order is preserved",
          onTarget.size == 30 && onTarget.zipWithIndex.forall { case (r, i) => r.valueString.contains(s"v$i") })
        val strays = partitions.filter(_ != target).map(p => consumer.fetch(keyTopic, p, 0, 200.millis).get.size).sum
        check("no keyed record landed elsewhere", strays == 0, s"$strays strays")
      }
    }

    section("murmur2 agrees with the broker's partitioner")
    check("murmur2(\"\") is stable", Partitioner.murmur2(Array.emptyByteArray) == 275646681,
      Integer.toUnsignedString(Partitioner.murmur2(Array.emptyByteArray)))
    check("murmur2 is deterministic", Partitioner.murmur2(bytes("user-7")) == Partitioner.murmur2(bytes("user-7")))
    check("different keys hash differently", Partitioner.murmur2(bytes("user-7")) != Partitioner.murmur2(bytes("user-8")))

    section("record headers and timestamps")
    locally {
      val headerTopic = unique("scala-headers")
      val before = now() - 1000
      Using.resource(Producer(unbatched)) { producer =>
        producer.send(ProducerRecord(headerTopic, "annotated").toPartition(0).withHeaders(
          Header("trace-id", "abc-123"),
          Header("content-type", "application/json"),
          Header.empty("tombstone-reason"))).get
        producer.send(ProducerRecord(headerTopic, "plain").toPartition(0)).get
        producer.flush().get
      }
      val after = now() + 1000
      Using.resource(Consumer(consumerSettings)) { consumer =>
        val got = consumer.fetch(headerTopic, 0, 0).get
        check("both records arrive", got.size == 2, s"got ${got.size}")
        if (got.size == 2) {
          val Vector(annotated, plain) = got: @unchecked
          check("headers survive the round trip", annotated.headers.size == 3, s"${annotated.headers.size} headers")
          check("header values are exact", same(annotated.header("trace-id").flatMap(_.value), bytes("abc-123")))
          check("a null header value stays null",
            annotated.headers.size == 3 && annotated.headers(2).value.isEmpty)
          check("a record with no headers gains none from its batch", plain.headers.isEmpty,
            s"${plain.headers.size} headers")
          check("timestamps are real wall-clock values", got.forall(r => r.timestamp >= before && r.timestamp <= after),
            s"${got.map(_.timestamp)} outside $before..$after")
        }
      }
    }

    section("tombstones")
    locally {
      val tombTopic = unique("scala-tombstones")
      Using.resource(Producer(unbatched)) { producer =>
        producer.send(ProducerRecord(tombTopic, "set").withKey("k1").toPartition(0)).get
        producer.send(ProducerRecord(tombTopic, Array.emptyByteArray).withKey("k2").toPartition(0)).get
        // A None value is a deletion, and must stay distinguishable from the empty value above.
        producer.send(ProducerRecord.tombstone(tombTopic, bytes("k3")).toPartition(0)).get
        producer.flush().get
      }
      Using.resource(Consumer(consumerSettings)) { consumer =>
        val got = consumer.fetch(tombTopic, 0, 0).get
        check("all three records arrive", got.size == 3, s"got ${got.size}")
        if (got.size == 3) {
          check("an ordinary value round-trips", got(0).valueString.contains("set"))
          check("an empty value is empty, not null", got(1).value.exists(_.isEmpty), got(1).toString)
          check("a tombstone arrives as a null value", got(2).isTombstone, got(2).toString)
        }
      }
    }

    section("offsets")
    Using.resource(Consumer(consumerSettings)) { consumer =>
      val earliest = consumer.listOffsets(topic, 0, OffsetSpec.Earliest).get
      val latest = consumer.listOffsets(topic, 0, OffsetSpec.Latest).get
      check("earliest is 0 on a fresh topic", earliest == 0, earliest.toString)
      check("latest equals the record count", latest == 50, latest.toString)
    }

    section("acks")
    for (acks <- Seq(Acks.Zero, Acks.Leader, Acks.All)) {
      val acksTopic = unique(s"scala-acks${acks.wire}")
      Using.resource(Producer(unbatched.copy(acks = acks))) { producer =>
        producer.send(ProducerRecord(acksTopic, "durable").toPartition(0)).get
        producer.flush().get
      }
      Thread.sleep(400)
      Using.resource(Consumer(consumerSettings)) { consumer =>
        val got = consumer.fetch(acksTopic, 0, 0).get
        check(s"acks=${acks.wire} stores the record", got.size == 1, s"got ${got.size}")
      }
    }

    section("consumer group: assignment, commit, resume")
    locally {
      val groupTopic = unique("scala-group")
      val groupId = unique("scala-billing")
      Using.resource(Producer(unbatched)) { producer =>
        for (i <- 0 until 40) producer.send(ProducerRecord(groupTopic, s"g$i")).get
        producer.flush().get
      }
      val member = GroupConsumer(group(groupId, groupTopic))
      // Through the iterator API: take(40) stops polling once all 40 have arrived, and the
      // deadline is checked after every poll, empty ones included.
      val deadline = now() + 30000
      val seen = member.batches(500.millis).takeWhile(_ => now() < deadline).flatten.take(40).toVector
      check("the group consumes every record", seen.size == 40, s"got ${seen.size}")
      check("no record is delivered twice", seen.map(r => (r.topicPartition, r.offset)).distinct.size == seen.size)
      member.commit().get
      val total = member.committed().get.values.sum
      check("commit records a position", total == 40, total.toString)
      member.close()

      // A second member of the same group must resume, not replay.
      Using.resource(GroupConsumer(group(groupId, groupTopic))) { rejoined =>
        val replayed = mutable.Buffer.empty[ConsumerRecord]
        val until = now() + 5000
        while (now() < until) replayed ++= pollQuietly(rejoined, 300.millis)
        check("a rejoining group resumes from its commit", replayed.isEmpty,
          s"replayed ${replayed.size} records it had already committed")
      }
    }

    section("auto.offset.reset")
    locally {
      val resetTopic = unique("scala-reset")
      Using.resource(Producer(unbatched)) { producer =>
        for (i <- 0 until 10) producer.send(ProducerRecord(resetTopic, s"r$i")).get
        producer.flush().get
      }
      Using.resource(GroupConsumer(group(unique("scala-latest"), resetTopic).copy(autoOffsetReset = AutoOffsetReset.Latest))) {
        member =>
          val skipped = mutable.Buffer.empty[ConsumerRecord]
          val until = now() + 4000
          while (now() < until) skipped ++= pollQuietly(member, 300.millis)
          check("latest skips records produced before the group existed", skipped.isEmpty, s"saw ${skipped.size}")
      }
      Using.resource(GroupConsumer(group(unique("scala-none"), resetTopic).copy(autoOffsetReset = AutoOffsetReset.Fail))) {
        strict =>
          var raised = false
          val until = now() + 5000
          while (now() < until && !raised) {
            strict.poll(300.millis) match {
              case Failure(_: NoOffsetForPartitionException) => raised = true
              case Failure(error: BrahmaputraException) =>
                raised = String.valueOf(error.getMessage).contains("no committed offset")
              case _ =>
            }
          }
          check("none refuses to guess a position", raised)
      }
    }

    section("assignors")
    for (assignor <- Assignor.values) {
      val name = assignor.java.name.toLowerCase(java.util.Locale.ROOT)
      val assignorTopic = unique(s"scala-$name")
      Using.resource(Producer(unbatched)) { producer =>
        for (i <- 0 until 20) producer.send(ProducerRecord(assignorTopic, s"a$i")).get
        producer.flush().get
      }
      Using.resource(GroupConsumer(group(unique(s"scala-grp-$name"), assignorTopic).copy(assignor = assignor))) { member =>
        val collected = mutable.Buffer.empty[ConsumerRecord]
        val deadline = now() + 20000
        while (collected.size < 20 && now() < deadline) collected ++= pollQuietly(member, 500.millis)
        check(s"$name: consumes every record", collected.size == 20, s"got ${collected.size}")
      }
    }

    section("bounded client buffer")
    locally {
      val bufferTopic = unique("scala-buffer")
      val producer = Producer(ProducerSettings(bootstrap,
        linger = 10.seconds, // never flush on time during this check
        bufferMemory = 2048,
        maxBlock = 300.millis))
      val value = Array.fill[Byte](256)('x'.toByte)
      var blocked = false
      var attempts = 0
      while (attempts < 500 && !blocked) {
        attempts += 1
        producer.send(ProducerRecord(bufferTopic, value).toPartition(0)) match {
          case Failure(error: BrahmaputraException) =>
            blocked = String.valueOf(error.getMessage).contains("buffer full")
          case _ =>
        }
      }
      check("a full buffer blocks and then reports", blocked)
      // Like the Go suite, this producer is abandoned rather than closed: closing would flush
      // the records the check just proved were held back.
    }

    section("wire edge cases")
    locally {
      val edgeTopic = unique("scala-edge")
      val large = Array.tabulate[Byte](1 << 20)(i => (i * 7).toByte)
      val unicodeKey = bytes("ключ-✓-🔑")
      val unicodeValue = bytes("значение — 数据 — 🚀")
      Using.resource(Producer(unbatched)) { producer =>
        producer.send(ProducerRecord(edgeTopic, large).toPartition(0)).get
        producer.send(ProducerRecord(edgeTopic, unicodeValue).withKey(unicodeKey).toPartition(0)
          .withHeaders(Header("ünïcødé-🏷", "✓"))).get
        // An empty key and an empty header value are values, not nulls.
        producer.send(ProducerRecord(edgeTopic, "empty-key").withKey(Array.emptyByteArray).toPartition(0)
          .withHeaders(Header("empty", Array.emptyByteArray), Header.empty("null"))).get
        producer.send(ProducerRecord(edgeTopic, "null-key").toPartition(0)).get
      }
      Using.resource(Consumer(consumerSettings)) { consumer =>
        val got = fetchAll(consumer, edgeTopic, 4)
        check("edge records all arrive", got.size == 4, s"got ${got.size}")
        if (got.size == 4) {
          check("a 1 MiB value round-trips byte-identical", same(got(0).value, large),
            s"${got(0).value.map(_.length)} bytes")
          val unicode = got(1)
          check("unicode key, value and header key round-trip",
            same(unicode.key, unicodeKey) && same(unicode.value, unicodeValue) &&
              unicode.headers.map(_.key) == Vector("ünïcødé-🏷"))
          val empty = got(2)
          check("an empty key stays empty, not null", empty.key.exists(_.isEmpty), empty.toString)
          check("an empty header value stays empty, not null",
            empty.headers.size == 2 && empty.headers(0).value.exists(_.isEmpty) && empty.headers(1).value.isEmpty,
            empty.headers.toString)
          check("a null key stays null", got(3).key.isEmpty, got(3).toString)
        }
      }
    }

    section("ordering under linger flushes")
    locally {
      val orderTopic = unique("scala-order")
      val total = 5000
      // Sent through sendAsync: the futures run one at a time on the producer's own thread,
      // in call order, so the log must come out in send order.
      Using.resource(Producer(ProducerSettings(bootstrap, linger = 1.milli, batchSize = 256))) { producer =>
        val sends = (0 until total).map(i => producer.sendAsync(ProducerRecord(orderTopic, i.toString).toPartition(0)))
        Await.result(Future.sequence(sends), 60.seconds)
      }
      Using.resource(Consumer(consumerSettings)) { consumer =>
        val got = fetchAll(consumer, orderTopic, total)
        val numbers = got.flatMap(_.valueString).map(_.toInt)
        val inversions = numbers.zip(numbers.drop(1)).count { case (a, b) => b < a }
        check("every record of a partition arrives", got.size == total, s"got ${got.size}")
        check("a partition's records keep send order", inversions == 0, s"$inversions inversions")
      }
    }

    section("background flush failures are reported")
    locally {
      val producer = Producer(ProducerSettings(bootstrap, linger = 20.millis))
      // Partition 999 does not exist, so the linger thread's flush fails.
      val sent = producer.send(ProducerRecord(unique("scala-bgfail"), "lost").toPartition(999))
      Thread.sleep(300)
      val flushed = producer.flush()
      check("a failed linger flush surfaces on the next flush", sent.isSuccess && flushed.isFailure,
        s"send=$sent flush=$flushed")
      val closer = new Thread(() => { Try(producer.close()); () }) // returning with an error is still returning
      closer.start()
      closer.join(5000)
      check("close returns after a failed flush", !closer.isAlive, if (closer.isAlive) "hung" else "")
    }

    section("connection failures")
    locally {
      // A broker that accepts and never answers must cost an error, not a thread blocked forever.
      Using.resource(new SilentBroker) { silent =>
        Using.resource(BrokerConnection.open("127.0.0.1", silent.port, "scala-test", 1.second).get) { connection =>
          connection.setRequestTimeout(300.millis)
          val started = now()
          val answer = connection.apiVersions()
          check("a request to an unresponsive broker times out",
            answer.isFailure && now() - started < 3000, answer.toString)
          check("a timed-out connection is not reused", connection.isBroken)
        }
      }

      // A connection the broker drops is redialled, not kept forever.
      Using.resource(new DropProxy(host, port)) { proxy =>
        val dropTopic = unique("scala-drop")
        val proxied = s"127.0.0.1:${proxy.port}"
        Using.resource(Producer(unbatched.copy(bootstrapServers = proxied))) { producer =>
          producer.send(ProducerRecord(dropTopic, "before").toPartition(0)).get
          proxy.dropAll()
          val attempts = Iterator.continually(producer.send(ProducerRecord(dropTopic, "after").toPartition(0)))
          val outcome = attempts.take(3).find(_.isSuccess).getOrElse(Failure(new RuntimeException("3 failures")))
          check("a producer recovers after its connection drops", outcome.isSuccess, outcome.toString)
        }
        Using.resource(Consumer(consumerSettings.copy(bootstrapServers = proxied))) { consumer =>
          consumer.fetch(dropTopic, 0, 0, 100.millis)
          proxy.dropAll()
          val attempts = Iterator.continually(consumer.fetch(dropTopic, 0, 0, 100.millis))
          val outcome = attempts.take(3).find(_.isSuccess).getOrElse(Failure(new RuntimeException("3 failures")))
          check("a consumer recovers after its connection drops", outcome.toOption.exists(_.nonEmpty), outcome.toString)
        }
      }
    }

    section("consumer group: max.poll.interval and rejoin")
    locally {
      val slowTopic = unique("scala-slow")
      val producer = Producer(unbatched)
      for (i <- 0 until 10) producer.send(ProducerRecord(slowTopic, s"s$i")).get
      val member = GroupConsumer(group(unique("scala-slow-grp"), slowTopic).copy(maxPollInterval = 1500.millis))
      val first = mutable.Buffer.empty[ConsumerRecord]
      var deadline = now() + 15000
      var stop = false
      while (!stop && first.size < 10 && now() < deadline) {
        member.poll(300.millis) match {
          case Success(records) => first ++= records
          case Failure(_) => stop = true
        }
      }
      member.commit()
      // Stall past max.poll.interval: the member leaves the group.
      Thread.sleep(2500)
      for (i <- 10 until 20) producer.send(ProducerRecord(slowTopic, s"s$i")).get
      producer.close()
      val second = mutable.Buffer.empty[ConsumerRecord]
      var pollError: Option[Throwable] = None
      deadline = now() + 15000
      while (pollError.isEmpty && second.size < 10 && now() < deadline) {
        member.poll(300.millis) match {
          case Success(records) => second ++= records
          case Failure(error) => pollError = Some(error)
        }
      }
      check("a member that stalled rejoins on its next poll",
        first.size == 10 && second.size == 10 && pollError.isEmpty,
        s"first=${first.size} second=${second.size} err=$pollError")
      member.close()
    }

    section("consumer group: time inside poll does not count against max.poll.interval")
    locally {
      val joinTopic = unique("scala-inpoll")
      val producer = Producer(unbatched)
      producer.partitionsFor(joinTopic).get
      // Far shorter than the first poll below, which spends ~1s joining (the broker's initial
      // rebalance delay) and then waits for data.
      val member = GroupConsumer(group(unique("scala-inpoll-grp"), joinTopic).copy(maxPollInterval = 600.millis))
      val late = Future {
        Thread.sleep(2000)
        for (i <- 0 until 10) producer.send(ProducerRecord(joinTopic, s"j$i"))
      }
      // One long poll, as a Future on the member's thread: it joins, then waits for the records.
      val got = Try(Await.result(member.pollAsync(4.seconds), 30.seconds))
      // Committed straight away, before another poll could quietly rejoin: this fails if the
      // member left the group mid-poll.
      val committed = member.commit()
      check("a member is still in its group after a long poll",
        got.toOption.exists(_.nonEmpty) && committed.isSuccess,
        s"got=${got.map(_.size)} commit=$committed")
      Await.ready(late, 30.seconds)
      member.close()
      producer.close()
    }
  }

  // -------------------------------------------------------------------------
  // Fake brokers for the connection-failure checks
  // -------------------------------------------------------------------------

  /** Accepts connections and reads them forever without ever answering. */
  private final class SilentBroker extends AutoCloseable {
    private val server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    private val accepted = mutable.Buffer.empty[Socket]
    def port: Int = server.getLocalPort

    daemon {
      var open = true
      while (open) {
        Try(server.accept()) match {
          case Success(socket) =>
            accepted.synchronized(accepted += socket)
            daemon(drain(socket))
          case Failure(_) => open = false
        }
      }
    }

    override def close(): Unit = {
      closeQuietly(server)
      accepted.synchronized(accepted.foreach(closeQuietly))
    }
  }

  /**
   * Forwards TCP to the broker and can sever every live connection, which is how a broker
   * restart or an idle timeout looks to a client.
   */
  private final class DropProxy(targetHost: String, targetPort: Int) extends AutoCloseable {
    private val server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    private val live = mutable.Buffer.empty[Socket]
    def port: Int = server.getLocalPort

    daemon {
      var open = true
      while (open) {
        Try(server.accept()) match {
          case Failure(_) => open = false
          case Success(client) =>
            Try(new Socket(targetHost, targetPort)) match {
              case Failure(_) => closeQuietly(client)
              case Success(upstream) =>
                live.synchronized { live += client; live += upstream }
                daemon(pipe(client, upstream))
                daemon(pipe(upstream, client))
            }
        }
      }
    }

    def dropAll(): Unit = {
      live.synchronized {
        live.foreach(closeQuietly)
        live.clear()
      }
      Thread.sleep(50)
    }

    override def close(): Unit = {
      closeQuietly(server)
      dropAll()
    }
  }

  private def pipe(from: Socket, to: Socket): Unit =
    try from.getInputStream.transferTo(to.getOutputStream)
    catch { case _: IOException => () } // either side went away
    finally {
      closeQuietly(from)
      closeQuietly(to)
    }

  private def drain(socket: Socket): Unit = {
    val chunk = new Array[Byte](4096)
    try {
      val in = socket.getInputStream
      while (in.read(chunk) >= 0) () // discard: this broker never answers
    } catch { case _: IOException => () }
    finally closeQuietly(socket)
  }

  private def daemon(body: => Unit): Unit = {
    val thread = new Thread(() => body)
    thread.setDaemon(true)
    thread.start()
  }

  private def closeQuietly(closeable: Closeable): Unit =
    try closeable.close()
    catch { case _: IOException => () }
}
