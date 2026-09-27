## The partition consumer: fetch, offsets, high watermark.

import std/[options]
import ./protocol, ./conn

type
  ConsumedRecord* = object
    ## One record delivered to the application.
    topic*: string
    partition*: int32
    offset*: int64
    key*: Option[string]        ## `none` is a null key; `some("")` an empty one.
    value*: Option[string]      ## `none` is a tombstone; `some("")` an empty value.
    timestamp*: int64           ## Absolute unix milliseconds.
    headers*: seq[RecordHeader]

  ConsumerConfig* = object
    ## Named as Kafka names its consumer settings.
    clientId*: string
    fetchMaxBytes*: int32       ## `fetch.max.bytes`: caps one response.
    fetchMinBytes*: int32       ## `fetch.min.bytes`: return early once this many bytes are ready.
    fetchMaxWaitMs*: int32      ## `fetch.max.wait.ms`: the long-poll ceiling when caught up.
    rack*: string               ## `client.rack`, empty when none.
    isolationLevel*: int32      ## `ReadUncommitted` (default) or `ReadCommitted`.
    maxPollRecords*: int        ## `max.poll.records` (used by the group consumer).
    dialTimeoutMs*: int
    socketTimeoutMs*: int       ## Client-side round-trip bound (default 120 s).

  Consumer* {.acyclic.} = ref object
    ## Reads one partition at a time, with no group coordination. Use it
    ## from one thread at a time.
    config: ConsumerConfig
    router: Router

  FetchResult* = object
    records*: seq[ConsumedRecord]
    highWatermark*: int64
    lastStableOffset*: int64

proc header*(r: ConsumedRecord, key: string): Option[string] =
  ## The first value stored under `key`, if any.
  for h in r.headers:
    if h.key == key: return h.value
  none(string)

proc defaultConsumerConfig*(): ConsumerConfig =
  ConsumerConfig(clientId: DefaultClientId, fetchMaxBytes: 8 * 1024 * 1024,
                 fetchMinBytes: 1, fetchMaxWaitMs: 500,
                 isolationLevel: ReadUncommitted, maxPollRecords: 500,
                 dialTimeoutMs: DefaultDialTimeoutMs,
                 socketTimeoutMs: DefaultRequestTimeoutMs)

proc newConsumer*(address: string, config = defaultConsumerConfig()): Consumer =
  Consumer(config: config, router: newRouter(address, config.clientId,
           config.dialTimeoutMs, config.socketTimeoutMs))

proc close*(c: Consumer) = c.router.close()
proc router*(c: Consumer): Router = c.router
proc config*(c: Consumer): ConsumerConfig = c.config

proc partitions*(c: Consumer, topic: string): seq[int32] = c.router.partitions(topic)

proc listOffsets*(c: Consumer, topic: string, partition: int32, timestamp: int64): int64 =
  ## Resolves `Earliest`, `Latest` or a unix-ms timestamp to an offset.
  var w = initBodyWriter()
  w.writeString(topic)
  w.writeInt32(partition)
  w.writeInt64(timestamp)
  var r = initBodyReader(c.router.request(topic, partition, ApiListOffsets, w.buf))
  discard r.readString()   # topic
  discard r.readInt32()    # partition
  let code = r.readInt32()
  result = r.readInt64()
  discard r.readInt64()    # timestamp
  if code != ErrNone:
    raise newServerError(code, "list_offsets " & topic & "-" & $partition)

type RawFetch = object
  code: int32
  highWatermark: int64
  lastStable: int64
  batches: seq[DecodedBatch]

proc fetchOnce(c: Consumer, topic: string, partition: int32, body: string): RawFetch =
  var r = initBodyReader(c.router.request(topic, partition, ApiFetch, body))
  discard r.readString()   # topic
  discard r.readInt32()    # partition
  result.code = r.readInt32()
  result.highWatermark = r.readInt64()
  result.lastStable = r.readInt64()
  let batchesLength = r.readInt64()
  # Read even though unused: the batches trail the whole struct.
  discard r.readInt32()    # preferred_read_replica
  let trailing = r.rest()
  if batchesLength < 0 or batchesLength > int64(trailing.len):
    raise newException(DecodeError, "fetch response claims more batch bytes than it carries")
  let raw = trailing[0 ..< int(batchesLength)]
  var pos = 0
  while pos < raw.len:
    let (batch, next) = decodeRecordBatch(raw, pos)
    result.batches.add batch
    pos = next

proc fetchVerbose*(c: Consumer, topic: string, partition: int32, offset: int64,
                   maxWaitMs: int32 = 500): FetchResult =
  ## Reads from one partition starting at `offset`, and also reports the
  ## partition's high watermark.
  var w = initBodyWriter()
  w.writeString(topic)
  w.writeInt32(partition)
  w.writeInt64(offset)
  w.writeInt32(c.config.fetchMaxBytes)
  w.writeInt32(min(maxWaitMs, c.config.fetchMaxWaitMs))
  w.writeInt32(c.config.fetchMinBytes)
  w.writeInt32(c.config.isolationLevel)
  w.writeString(c.config.rack)
  var raw = c.fetchOnce(topic, partition, w.buf)
  if raw.code == ErrNotLeaderOrFollower:
    c.router.refresh(topic)
    raw = c.fetchOnce(topic, partition, w.buf)
  if raw.code != ErrNone:
    raise newServerError(raw.code, "fetch " & topic & "-" & $partition)
  result.highWatermark = raw.highWatermark
  result.lastStableOffset = raw.lastStable
  for batch in raw.batches:
    for index, record in batch.records:
      let recordOffset = batch.baseOffset + int64(index)
      # A batch can start before the requested offset; skip what the
      # caller has already seen.
      if recordOffset < offset: continue
      result.records.add ConsumedRecord(topic: topic, partition: partition,
        offset: recordOffset, key: record.key, value: record.value,
        timestamp: batch.maxTimestamp + record.timestampDelta,
        headers: record.headers)

proc fetch*(c: Consumer, topic: string, partition: int32, offset: int64,
            maxWaitMs: int32 = 500): seq[ConsumedRecord] =
  ## Reads from one partition starting at `offset`.
  c.fetchVerbose(topic, partition, offset, maxWaitMs).records

proc highWatermark*(c: Consumer, topic: string, partition: int32): int64 =
  ## The offset one past the last committed-to-the-log record.
  c.fetchVerbose(topic, partition, c.listOffsets(topic, partition, Latest), 0).highWatermark
