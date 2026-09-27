package io.brahmaputra.scaladsl

import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

import io.brahmaputra.{Client, Protocol}
import io.brahmaputra.GroupConsumer as JavaGroupConsumer

/** Everything the driver throws: an unchecked exception, surfaced here inside `Try`/`Future`. */
type BrahmaputraException = Protocol.BrahmaputraException
/** A broker error code, in `.code` (see `io.brahmaputra.Protocol.ErrorCode`). */
type ServerException = Protocol.ServerException
/** Malformed bytes on the wire. */
type ProtocolException = Protocol.ProtocolException
/** A group poll under `AutoOffsetReset.Fail` found a partition with no committed offset. */
type NoOffsetForPartitionException = Protocol.NoOffsetForPartitionException

/*
 * Conventions: blocking calls return `Try` (never throw for a driver error); `...Async` calls
 * return a `Future`. Futures of one producer or one group member run on a thread that object
 * owns, one at a time in call order, so async sends keep their order and a group member keeps
 * Kafka's one-thread rule. Every class exposes the Java object it wraps as `underlying`.
 */

private[scaladsl] final class SerialThread(name: String) {
  private val executor: ExecutorService = Executors.newSingleThreadExecutor(new ThreadFactory {
    def newThread(body: Runnable): Thread = {
      val t = new Thread(body, s"$name-${SerialThread.sequence.incrementAndGet()}")
      t.setDaemon(true)
      t
    }
  })
  val context: ExecutionContext = ExecutionContext.fromExecutorService(executor)

  def apply[T](body: => T): Future[T] = Future(body)(using context)

  /** Run `body` on this thread and wait for it. */
  def await[T](body: => T): Try[T] = Try(Await.result(apply(body), Duration.Inf))

  def shutdown(): Unit = executor.shutdown()
}

private[scaladsl] object SerialThread {
  val sequence = new AtomicInteger()
}

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

/**
 * A batching producer over [[io.brahmaputra.Client.Producer]]. Share one instance.
 *
 * `send` buffers and returns; errors from a batch surface from the `flush` (or the `send`
 * that filled the batch) that sent it, and a batch the background linger thread failed to
 * send is reported by the next `flush` or `close`. A partition has at most one batch in
 * flight, so records sent in order land in order.
 */
final class Producer(val underlying: Client.Producer) extends AutoCloseable {
  private lazy val serial = new SerialThread("brahmaputra-producer")
  @volatile private var serialStarted = false

  /** Buffer one record; blocks while the buffer is full, up to `maxBlock`. */
  def send(record: ProducerRecord): Try[Unit] = Try(sendNow(record))

  /** Buffer one record from the producer's own thread; futures complete in call order. */
  def sendAsync(record: ProducerRecord): Future[Unit] = onSerial(sendNow(record))

  /** Send everything buffered and wait for acknowledgement (including failed linger flushes). */
  def flush(): Try[Unit] = Try(underlying.flush())

  def flushAsync(): Future[Unit] = onSerial(underlying.flush())

  /**
   * Send one record on its own and return its offset: a full round trip, correct and slow.
   * The partitioner chooses the partition, so `record.partition` must be empty.
   */
  def sendAndAwait(record: ProducerRecord): Try[Long] = Try {
    require(record.partition.isEmpty, "sendAndAwait picks the partition itself")
    underlying.sendSync(record.topic, record.value.orNull, record.key.orNull,
      record.headers.map(Convert.header).asJava)
  }

  /** The partitions of `topic` (auto-creating it where the broker does that). */
  def partitionsFor(topic: String): Try[Vector[Int]] = Try(Convert.ints(underlying.router().partitions(topic)))

  /** Flush, then release connections — even if that flush fails; its error is thrown. */
  override def close(): Unit =
    try underlying.close()
    finally if (serialStarted) serial.shutdown()

  private def sendNow(record: ProducerRecord): Unit = {
    val headers = record.headers.map(Convert.header).asJava
    record.partition match {
      case Some(p) => underlying.sendTo(record.topic, p, record.value.orNull, record.key.orNull, headers)
      case None => underlying.send(record.topic, record.value.orNull, record.key.orNull, headers)
    }
  }

  private def onSerial[T](body: => T): Future[T] = {
    serialStarted = true
    serial(body)
  }
}

object Producer {
  /** Connect to the bootstrap broker. Throws if it cannot (see [[open]] for a `Try`). */
  def apply(settings: ProducerSettings = ProducerSettings()): Producer = {
    val (host, port) = Settings.bootstrap(settings.bootstrapServers)
    new Producer(new Client.Producer(host, port, settings.toJava))
  }

  def open(settings: ProducerSettings = ProducerSettings()): Try[Producer] = Try(apply(settings))
}

// ---------------------------------------------------------------------------
// Partition consumer
// ---------------------------------------------------------------------------

/** Reads partitions directly, with no group. Thread-safe; wraps [[io.brahmaputra.Client.Consumer]]. */
final class Consumer(val underlying: Client.Consumer) extends AutoCloseable {

  /** Records of one partition from `offset`, waiting up to `maxWait` for some to arrive. */
  def fetch(topic: String, partition: Int, offset: Long, maxWait: FiniteDuration = 500.millis): Try[Vector[ConsumerRecord]] =
    fetchWithWatermark(topic, partition, offset, maxWait).map(_.records)

  /** Like [[fetch]], and also returns the partition's high watermark. */
  def fetchWithWatermark(topic: String, partition: Int, offset: Long, maxWait: FiniteDuration = 500.millis): Try[FetchResult] =
    Try {
      val result = underlying.fetchVerbose(topic, partition, offset, Settings.millis(maxWait))
      FetchResult(Convert.records(result.records), result.highWatermark)
    }

  def fetchAsync(topic: String, partition: Int, offset: Long, maxWait: FiniteDuration = 500.millis)(using
      ec: ExecutionContext
  ): Future[Vector[ConsumerRecord]] =
    Future(scala.concurrent.blocking(fetch(topic, partition, offset, maxWait).get))

  /**
   * One partition's records from `from` as a lazy iterator. With `follow = false` it ends
   * once it has caught up with the high watermark; with `follow = true` it long-polls forever.
   * A failed fetch is thrown from `hasNext`/`next`.
   */
  def iterator(
      topic: String,
      partition: Int,
      from: Long = 0L,
      follow: Boolean = false,
      maxWait: FiniteDuration = 500.millis
  ): Iterator[ConsumerRecord] = {
    val (t, p) = (topic, partition) // Iterator has members named `partition`
    new Iterator[ConsumerRecord] {
      private var next_ = from
      private var pending: Iterator[ConsumerRecord] = Iterator.empty
      private var done = false

      def hasNext: Boolean = {
        while (!pending.hasNext && !done) {
          val batch = fetchWithWatermark(t, p, next_, maxWait).get
          batch.records.lastOption.foreach(last => next_ = last.offset + 1)
          pending = batch.records.iterator
          if (!follow && (batch.records.isEmpty || next_ >= batch.highWatermark)) done = true
        }
        pending.hasNext
      }

      def next(): ConsumerRecord =
        if (hasNext) pending.next() else throw new NoSuchElementException("caught up")
    }
  }

  /** [[iterator]] as a memoising `LazyList`. */
  def lazyList(topic: String, partition: Int, from: Long = 0L): LazyList[ConsumerRecord] =
    LazyList.from(iterator(topic, partition, from))

  /** Resolve `OffsetSpec.Earliest`, `Latest` or `AtTimestamp(ms)` to an offset. */
  def listOffsets(topic: String, partition: Int, spec: OffsetSpec): Try[Long] =
    Try(underlying.listOffsets(topic, partition, spec.wire))

  def partitionsFor(topic: String): Try[Vector[Int]] = Try(Convert.ints(underlying.partitions(topic)))

  /** The seed broker's ApiVersions answer. */
  def apiVersions(): Try[ApiVersions] = Try(Convert.apiVersions(underlying.router().seed().apiVersions()))

  /** Fresh metadata for `topics` (every topic when empty). */
  def metadata(topics: Seq[String] = Nil): Try[ClusterMetadata] =
    Try(Convert.metadata(underlying.router().metadata(topics.asJava, true)))

  override def close(): Unit = underlying.close()
}

object Consumer {
  def apply(settings: ConsumerSettings = ConsumerSettings()): Consumer = {
    val (host, port) = Settings.bootstrap(settings.bootstrapServers)
    new Consumer(new Client.Consumer(host, port, settings.toJava))
  }

  def open(settings: ConsumerSettings = ConsumerSettings()): Try[Consumer] = Try(apply(settings))
}

// ---------------------------------------------------------------------------
// Group consumer
// ---------------------------------------------------------------------------

/**
 * A consumer-group member over [[io.brahmaputra.GroupConsumer]].
 *
 * The Java member is single-threaded, like Kafka's, so this one owns a thread and runs every
 * call there: blocking calls wait for it, `...Async` calls return its `Future`. Either may be
 * used from any thread.
 *
 * `maxPollInterval` bounds the time between polls; time inside a poll (joining included)
 * does not count, and a member that stalls rejoins on its next poll.
 */
final class GroupConsumer(val underlying: JavaGroupConsumer) extends AutoCloseable {
  private val member = new SerialThread("brahmaputra-group")
  @volatile private var closed = false

  def subscribe(topics: String*): Unit = subscribe(topics)

  def subscribe(topics: Iterable[String]): Unit =
    member.await(underlying.subscribe(topics.toList.asJava)).get

  /** Up to `maxPollRecords` records, joining (or rejoining) the group first if needed. */
  def poll(timeout: FiniteDuration = 500.millis): Try[Vector[ConsumerRecord]] =
    member.await(Convert.records(underlying.poll(timeout.toMillis)))

  def pollAsync(timeout: FiniteDuration = 500.millis): Future[Vector[ConsumerRecord]] =
    member(Convert.records(underlying.poll(timeout.toMillis)))

  /**
   * Every record this member is assigned, as an endless iterator that polls as it goes.
   * A failed poll is thrown from `hasNext`.
   */
  def iterator(pollTimeout: FiniteDuration = 500.millis): Iterator[ConsumerRecord] =
    batches(pollTimeout).flatten

  /**
   * One element per poll, empty when a poll timed out with nothing — so a caller can stop
   * on a deadline even when no records come (`batches(t).takeWhile(_ => ...).flatten`).
   */
  def batches(pollTimeout: FiniteDuration = 500.millis): Iterator[Vector[ConsumerRecord]] =
    Iterator.continually(poll(pollTimeout).get)

  /** Commit the position of every assigned partition. At-least-once: after processing. */
  def commit(): Try[Unit] = member.await(underlying.commit())

  def commitAsync(): Future[Unit] = member(underlying.commit())

  /** Committed offsets of `partitions` (every assigned partition when empty). */
  def committed(partitions: Seq[TopicPartition] = Nil): Try[Map[TopicPartition, Long]] =
    member.await {
      underlying.committed(partitions.map(Convert.topicPartition).asJava).asScala.iterator.map {
        case (tp, offset) => Convert.topicPartition(tp) -> offset.longValue
      }.toMap
    }

  /** The partitions this member owns; empty before its first poll joins. */
  def assignment: Vector[TopicPartition] = underlying.assignment().asScala.iterator.map(Convert.topicPartition).toVector

  def memberId: String = underlying.memberId()

  def generation: Int = underlying.generation()

  /** Commit, leave the group (so its partitions move at once), and stop the member thread. */
  override def close(): Unit =
    if (!closed) {
      closed = true
      try member.await(underlying.close()).get
      finally member.shutdown()
    }
}

object GroupConsumer {
  def apply(settings: GroupSettings): GroupConsumer = {
    require(settings.groupId.nonEmpty, "groupId is required")
    val (host, port) = Settings.bootstrap(settings.bootstrapServers)
    val consumer = new GroupConsumer(new JavaGroupConsumer(host, port, settings.groupId, settings.toJava))
    if (settings.topics.nonEmpty) consumer.subscribe(settings.topics)
    consumer
  }

  def open(settings: GroupSettings): Try[GroupConsumer] = Try(apply(settings))
}

// ---------------------------------------------------------------------------
// One connection (diagnostics)
// ---------------------------------------------------------------------------

/**
 * One broker connection, for diagnostics and tests. A request that times out, hits an I/O
 * error or sees a correlation mismatch closes it and marks it broken.
 */
final class BrokerConnection(val underlying: Client.Connection) extends AutoCloseable {
  /** Bound one request/response round trip (default two minutes); zero disables it. */
  def setRequestTimeout(timeout: FiniteDuration): Unit = underlying.setRequestTimeout(Settings.millis(timeout))

  def isBroken: Boolean = underlying.isBroken

  def apiVersions(): Try[ApiVersions] = Try(Convert.apiVersions(underlying.apiVersions()))

  override def close(): Unit = underlying.close()
}

object BrokerConnection {
  def open(host: String, port: Int, clientId: String = "brahmaputra-scala", dialTimeout: FiniteDuration = 30.seconds): Try[BrokerConnection] =
    Try(new BrokerConnection(Client.Connection.connect(host, port, clientId, Settings.millis(dialTimeout))))
}
