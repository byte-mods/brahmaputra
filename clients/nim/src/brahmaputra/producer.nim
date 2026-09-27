## The producer: per-partition batching, a linger thread, bounded buffer.

import std/[options, tables, locks, os]
import ./protocol, ./conn

type
  TopicPartition* = tuple[topic: string, partition: int32]

  ProducerConfig* = object
    ## Named as Kafka names its producer settings. Where a default differs
    ## from Kafka's it is called out.
    clientId*: string           ## `client.id`
    acks*: int32                ## `acks`: 0 fire-and-forget, 1 leader, -1 all in-sync replicas.
    batchSize*: int             ## `batch.size`: flush a partition once it holds this many bytes.
    lingerMs*: int
      ## `linger.ms`: flush every non-empty buffer at least this often; 0
      ## sends each record immediately. Kafka defaults to 0; this to 5.
    compressionType*: string    ## `compression.type`: none, gzip, or a registered lz4/zstd/snappy.
    requestTimeoutMs*: int32    ## `request.timeout.ms`: the broker-side wait for acknowledgements.
    retries*: int               ## `retries` of a send refused with a retriable error.
    retryBackoffMs*: int        ## `retry.backoff.ms`
    deliveryTimeoutMs*: int     ## `delivery.timeout.ms`: caps a send, first attempt to last retry.
    bufferMemory*: int          ## `buffer.memory`: cap on unflushed record bytes held client-side.
    maxBlockMs*: int            ## `max.block.ms`: how long a send blocks on a full buffer.
    dialTimeoutMs*: int         ## TCP connect timeout.
    socketTimeoutMs*: int
      ## Client-side bound on one request/response round trip (default
      ## 120 s). A connection that exceeds it is closed and redialled.

  Buffered = object
    record: Record
    createdMs: int64

  ProducerObj = object
    config: ProducerConfig
    codec: Compression
    router: Router
    stateLock: Lock
    flushLock: Lock
    buffers: Table[TopicPartition, seq[Buffered]]
    sizes: Table[TopicPartition, int]
    bufferedBytes: int
    roundRobin: int
    closed: bool
    backgroundErr: string
    hasBackgroundErr: bool
    lingerThread: Thread[ptr ProducerObj]
    lingerStarted: bool

  Producer* {.acyclic.} = ref ProducerObj
    ## Batches records per partition and sends each batch as one Produce
    ## request. Thread-safe; share one rather than creating one per message.
    ##
    ## A background thread flushes every `lingerMs`. All flushes (linger,
    ## batch-full, explicit) are serialised, so a partition has at most one
    ## batch in flight and batches leave in the order they were filled.

proc defaultProducerConfig*(): ProducerConfig =
  ProducerConfig(clientId: DefaultClientId, acks: 1, batchSize: 16 * 1024,
                 lingerMs: 5, compressionType: "none", requestTimeoutMs: 30_000,
                 retries: 5, retryBackoffMs: 100, deliveryTimeoutMs: 120_000,
                 bufferMemory: 32 * 1024 * 1024, maxBlockMs: 60_000,
                 dialTimeoutMs: DefaultDialTimeoutMs,
                 socketTimeoutMs: DefaultRequestTimeoutMs)

proc router*(p: Producer): Router = p.router
  ## The routing layer, for callers that need metadata.

# --- internals shared by the caller's thread and the linger thread --------

proc produce(p: ptr ProducerObj, topic: string, partition: int32,
             batch: openArray[Buffered]): int64 =
  if batch.len == 0: return -1
  # The batch stores one base timestamp and a delta per record, so the
  # rebasing happens here: maxTimestamp is the newest record's time.
  var maxTimestamp = batch[0].createdMs
  for item in batch: maxTimestamp = max(maxTimestamp, item.createdMs)
  var records = newSeqOfCap[Record](batch.len)
  for item in batch:
    var record = item.record
    record.timestampDelta = item.createdMs - maxTimestamp
    records.add record
  let encoded = encodeRecordBatch(records, maxTimestamp, p.codec)
  var w = initBodyWriter()
  w.writeString(topic)
  w.writeInt32(partition)
  w.writeInt32(p.config.acks)
  w.writeInt32(p.config.requestTimeoutMs)
  w.writeInt64(int64(encoded.len))
  w.writeRaw(encoded)

  if p.config.acks == 0:
    p.router.sendOneway(topic, partition, ApiProduce, w.buf)
    return -1

  let deadline = monoMillis() + int64(p.config.deliveryTimeoutMs)
  var attemptsLeft = p.config.retries
  while true:
    var r = initBodyReader(p.router.request(topic, partition, ApiProduce, w.buf))
    discard r.readString()      # topic
    discard r.readInt32()       # partition
    let code = r.readInt32()
    let baseOffset = r.readInt64()
    discard r.readInt64()       # log_append_time_ms
    if code == ErrNone:
      return baseOffset
    if not retriable(code) or attemptsLeft <= 0 or monoMillis() >= deadline:
      raise newServerError(code, "produce to " & topic & "-" & $partition)
    dec attemptsLeft
    if code in [ErrNotLeaderOrFollower, ErrFencedLeaderEpoch, ErrUnknownLeaderEpoch]:
      # A stale route is the most common retriable cause.
      try: p.router.refresh(topic)
      except CatchableError: discard
    sleep(p.config.retryBackoffMs)

proc release(p: ptr ProducerObj, size: int) =
  withLock p.stateLock:
    p.bufferedBytes = max(0, p.bufferedBytes - size)

proc flushPartition(p: ptr ProducerObj, slot: TopicPartition) =
  # Held across the round trip and any retries: at most one batch in
  # flight, and batches leave in the order they were taken.
  withLock p.flushLock:
    var batch: seq[Buffered]
    var size = 0
    withLock p.stateLock:
      discard p.buffers.pop(slot, batch)
      discard p.sizes.pop(slot, size)
    if batch.len > 0:
      p.release(size)
      discard p.produce(slot.topic, slot.partition, batch)

proc flushAll(p: ptr ProducerObj) =
  ## Flushes every partition; tries them all, then raises the first failure.
  var slots: seq[TopicPartition]
  withLock p.stateLock:
    for slot, records in p.buffers:
      if records.len > 0: slots.add slot
  var first: ref CatchableError = nil
  for slot in slots:
    try:
      p.flushPartition(slot)
    except CatchableError as e:
      if first == nil: first = e
  if first != nil:
    raise first

proc lingerLoop(p: ptr ProducerObj) {.thread.} =
  let interval = int64(max(p.config.lingerMs, 1))
  var next = monoMillis() + interval
  while true:
    var closed = false
    withLock p.stateLock:
      closed = p.closed
    if closed: break
    let now = monoMillis()
    if now < next:
      sleep(int(min(next - now, 20)))
      continue
    # A background flush that fails must not kill the thread; the next
    # explicit flush()/close() surfaces the error to a caller who can act.
    try:
      p.flushAll()
    except CatchableError as e:
      withLock p.stateLock:
        if not p.hasBackgroundErr:
          p.hasBackgroundErr = true
          p.backgroundErr = e.msg
    next = monoMillis() + interval

# --- public API -----------------------------------------------------------

proc raw(p: Producer): ptr ProducerObj {.inline.} = cast[ptr ProducerObj](p)

proc newProducer*(address: string, config = defaultProducerConfig()): Producer =
  ## Connects to the seed broker at `host:port` and starts the linger thread.
  let codec = parseCompression(config.compressionType)
  if not codecAvailable(codec):
    raise newException(CodecError, $codec &
      " compression is not registered; call registerCodec first")
  result = Producer(config: config, codec: codec,
                    router: newRouter(address, config.clientId, config.dialTimeoutMs,
                                      config.socketTimeoutMs))
  initLock(result.stateLock)
  initLock(result.flushLock)
  if config.lingerMs > 0:
    createThread(result.lingerThread, lingerLoop, result.raw)
    result.lingerStarted = true

proc reserve(p: Producer, size: int) =
  ## Blocks until `size` more bytes may be buffered: a producer faster than
  ## its broker is slowed down here rather than growing without limit.
  let limit = p.config.bufferMemory
  if limit <= 0 or size >= limit:
    # Larger than the whole budget: admitted rather than waiting forever
    # on a condition that can never hold.
    withLock p.stateLock:
      p.bufferedBytes += size
    return
  let deadline = monoMillis() + int64(p.config.maxBlockMs)
  while true:
    var used = 0
    var admitted = false
    withLock p.stateLock:
      if p.bufferedBytes + size <= limit:
        p.bufferedBytes += size
        admitted = true
      used = p.bufferedBytes
    if admitted: return
    if monoMillis() >= deadline:
      raise newException(BufferFullError, "producer buffer full: " & $used & " of " &
        $limit & " bytes unflushed after max.block.ms=" & $p.config.maxBlockMs)
    sleep(5)

proc choosePartition(p: Producer, topic: string, key: Option[string]): int32 =
  let partitions = p.router.partitions(topic)
  if key.isSome:
    return partitionForKey(key.get, partitions)
  var index = 0
  withLock p.stateLock:
    index = p.roundRobin mod partitions.len
    inc p.roundRobin
  partitions[index]

proc sendTo*(p: Producer, topic: string, partition: int32, value: Option[string],
             key = none(string), headers: openArray[RecordHeader] = [],
             timestamp: int64 = -1) =
  ## Buffers one record on an explicit partition, bypassing the partitioner.
  ## `value = none(string)` is a tombstone; `some("")` is an empty value.
  ## `timestamp` is unix ms; negative means now.
  var size = 16
  if value.isSome: size += value.get.len
  if key.isSome: size += key.get.len
  for h in headers:
    size += h.key.len + 4
    if h.value.isSome: size += h.value.get.len
  p.reserve(size)
  let slot: TopicPartition = (topic, partition)
  let created = if timestamp >= 0: timestamp else: nowMillis()
  var full = false
  withLock p.stateLock:
    p.buffers.mgetOrPut(slot, @[]).add Buffered(
      record: Record(key: key, value: value, headers: @headers), createdMs: created)
    let total = p.sizes.getOrDefault(slot) + size
    p.sizes[slot] = total
    full = total >= p.config.batchSize
  if p.config.lingerMs == 0 or full:
    p.raw.flushPartition(slot)

proc sendTo*(p: Producer, topic: string, partition: int32, value: string,
             key = none(string), headers: openArray[RecordHeader] = [],
             timestamp: int64 = -1) =
  p.sendTo(topic, partition, some(value), key, headers, timestamp)

proc send*(p: Producer, topic: string, value: Option[string], key = none(string),
           headers: openArray[RecordHeader] = [], timestamp: int64 = -1) =
  ## Buffers one record. With a key the partition is `murmur2(key)`, so
  ## records sharing a key keep their order; without one it round-robins.
  ## Call `flush` to await delivery.
  p.sendTo(topic, p.choosePartition(topic, key), value, key, headers, timestamp)

proc send*(p: Producer, topic: string, value: string, key = none(string),
           headers: openArray[RecordHeader] = [], timestamp: int64 = -1) =
  p.send(topic, some(value), key, headers, timestamp)

proc sendSync*(p: Producer, topic: string, value: Option[string], key = none(string),
               headers: openArray[RecordHeader] = [], partition: int32 = -1,
               timestamp: int64 = -1): int64 =
  ## Sends one record on its own and returns its offset (-1 with acks=0).
  ## A full round trip per record — correct, and slow. A non-negative
  ## `partition` bypasses the partitioner; `timestamp` is unix ms
  ## (negative means now).
  let partition = if partition >= 0: partition else: p.choosePartition(topic, key)
  let created = if timestamp >= 0: timestamp else: nowMillis()
  let item = Buffered(record: Record(key: key, value: value, headers: @headers),
                      createdMs: created)
  withLock p.flushLock:
    result = p.raw.produce(topic, partition, [item])

proc sendSync*(p: Producer, topic: string, value: string, key = none(string),
               headers: openArray[RecordHeader] = [], partition: int32 = -1,
               timestamp: int64 = -1): int64 =
  p.sendSync(topic, some(value), key, headers, partition, timestamp)

proc flush*(p: Producer) =
  ## Sends every buffered record and waits for acknowledgement. Also raises
  ## the failure of any background (linger) flush since the last call,
  ## because those records are gone and nothing else would say so.
  var flushErr: ref CatchableError = nil
  try:
    p.raw.flushAll()
  except CatchableError as e:
    flushErr = e
  var background = ""
  var hadBackground = false
  withLock p.stateLock:
    if p.hasBackgroundErr:
      hadBackground = true
      background = p.backgroundErr
      p.hasBackgroundErr = false
      p.backgroundErr = ""
  if flushErr != nil:
    raise flushErr
  if hadBackground:
    raise newException(BrahmaputraError, "background flush failed: " & background)

proc close*(p: Producer) =
  ## Flushes, stops the linger thread and releases connections. The thread
  ## and connections are released even when the final flush fails; that
  ## failure is still raised.
  var alreadyClosed = false
  withLock p.stateLock:
    alreadyClosed = p.closed
  if alreadyClosed: return
  var flushErr: ref CatchableError = nil
  try:
    p.flush()
  except CatchableError as e:
    flushErr = e
  withLock p.stateLock:
    p.closed = true
  if p.lingerStarted:
    joinThread(p.lingerThread)
    p.lingerStarted = false
  p.router.close()
  if flushErr != nil:
    raise flushErr
