# Brahmaputra client for Haskell

A native Haskell driver: it speaks Brahmaputra's wire protocol directly
over `network` sockets, with no FFI and no sidecar.

Verified end to end against a live broker: **85/85 checks**
(`./test.sh HOST PORT`, which compiles the library and the suite with
`ghc -threaded -O1 -Wall -Werror` and runs it).

## Build

GHC 9.4 or newer. It depends only on GHC boot packages plus `network` and
`zlib`: `base`, `bytestring`, `containers`, `stm`, `text`, `time`, `array`,
`network`, `zlib`.

```bash
# plain ghc: no package manager needed
ghc -threaded -O1 -Wall -isrc -outputdir build -o build/app app/Main.hs

# or, with cabal
cabal build
```

**Link with `-threaded`.** The linger sender and the group heartbeat are
`forkIO` threads that block on sockets and STM, and the request timeout
relies on the threaded I/O manager.

Every failure is thrown as a `BrahmaputraError` exception:
`ServerError code context`, `ProtocolError`, `ConnectionError`,
`BufferFull`, `NoOffsetForPartition`, `ClientError`.

## Produce

```haskell
{-# LANGUAGE OverloadedStrings #-}
import Brahmaputra

main :: IO ()
main = withProducer "127.0.0.1:9092" defaultProducerConfig
         { pcAcks = AcksLeader, pcLingerMs = 5, pcCompression = Gzip } $ \producer -> do
  -- Keyed: murmur2(key) % partitions, so records sharing a key keep order.
  send producer (producerRecord "orders" (Just "{\"id\":1}"))
    { prKey = Just "user-7"
    , prHeaders = [Header "trace-id" (Just "abc-123"), Header "note" Nothing] }

  -- Explicit partition, bypassing the partitioner.
  send producer (producerRecord "orders" (Just "{\"id\":2}")) { prPartition = Just 3 }

  -- A tombstone: a Nothing value deletes the key on a compacted topic. It is
  -- distinct from Just "", an ordinary record with an empty value.
  send producer (producerRecord "orders" Nothing) { prKey = Just "user-7" }

  -- Or wait for one record's offset. A full round trip — correct, and slow.
  offset <- sendSync producer (producerRecord "orders" (Just "{\"id\":3}"))
  print offset

  flush producer          -- closeProducer (via withProducer) also flushes
```

`send` buffers per partition and returns at once; `flush` sends and waits
for acknowledgement. When `pcBufferMemory` bytes are buffered, `send`
blocks for up to `pcMaxBlockMs` and then throws `BufferFull`. A failed
background (linger) flush is thrown from the next `flush` or
`closeProducer`, and `closeProducer` still stops the linger thread and
releases the connections. Each partition has at most one batch in flight,
so a linger flush and a batch-full flush never reorder a partition.

## Consume one partition

```haskell
withConsumer "127.0.0.1:9092" defaultConsumerConfig $ \consumer -> do
  records <- fetch consumer "orders" 0 0 500       -- topic partition offset maxWaitMs
  mapM_ (\r -> print (crOffset r, crKey r, crValue r, crTimestamp r, crHeaders r)) records

  (_, highWatermark) <- fetchVerbose consumer "orders" 0 0 500
  start <- listOffsets consumer "orders" 0 Earliest
  end   <- listOffsets consumer "orders" 0 Latest
  at    <- listOffsets consumer "orders" 0 (AtTimestamp 1700000000000)
  print (highWatermark, start, end, at)
```

## Consume as a group

```haskell
withGroupConsumer "127.0.0.1:9092" "billing" defaultGroupConfig
    { gcAssignor = StickyAssignor
    , gcAutoOffsetReset = ResetEarliest
    , gcAutoCommitIntervalMs = 0              -- commit explicitly
    , gcGroupInstanceId = Just "worker-3"     -- static membership
    } $ \consumer -> do
  subscribe consumer ["orders"]
  forever $ do
    records <- poll consumer 500              -- timeout in ms
    mapM_ (handle . crValue) records
    -- At-least-once: commit after processing, never before.
    commit consumer
```

Closing commits and then sends `LeaveGroup`, so its partitions move at
once rather than after a session timeout. With `ResetNone` and no
committed offset, `poll` throws `NoOffsetForPartition`.
`gcMaxPollIntervalMs` bounds the time *between* polls: time inside `poll`
(a slow join included) never counts, and a member that did stall past it
leaves the group and rejoins on its next `poll`. A member the coordinator
no longer knows (`UNKNOWN_MEMBER_ID`) rejoins as a new member, and a
heartbeat reply for a generation the member has already left does not
make it rejoin again.

## Compression

`NoCompression` and `Gzip` (via `zlib`) are built in. The rest are opt-in,
so this package pulls in no compression library you did not ask for:

```haskell
registerCodec Zstd zstdCompress zstdDecompress   -- ByteString -> IO ByteString each
```

If you register lz4, the broker expects a little-endian `Word32` of the
uncompressed length followed by a raw LZ4 **block** — not the LZ4 frame
format.

## Connections

Every request has a client-side round-trip timeout (`pcSocketTimeoutMs` /
`ccSocketTimeoutMs` / `gcSocketTimeoutMs`, default 120 s; `setRequestTimeout`
on a raw `Conn`). After a timeout, an I/O error or a correlation-id
mismatch the connection is closed and marked broken (`isBroken`), and the
router redials it — the seed connection included — on next use.

```haskell
conn <- dial "127.0.0.1:9092" "my-tool" 1000     -- dial timeout in ms
setRequestTimeout conn 300
(ranges, brokerVersion) <- apiVersions conn
```

## Configuration

`ProducerConfig` (`defaultProducerConfig`)

| Field | Kafka name | Default |
|---|---|---|
| `pcClientId` | `client.id` | `"brahmaputra-haskell"` |
| `pcAcks` | `acks` | `AcksLeader` (`AcksNone` = 0, `AcksAll` = -1) |
| `pcBatchSize` | `batch.size` | 16384 bytes |
| `pcLingerMs` | `linger.ms` | 5 (0 sends every record immediately) |
| `pcCompression` | `compression.type` | `NoCompression` |
| `pcRequestTimeoutMs` | `request.timeout.ms` (broker-side ack wait) | 30000 |
| `pcRetries` | `retries` | 5 |
| `pcRetryBackoffMs` | `retry.backoff.ms` | 100 |
| `pcDeliveryTimeoutMs` | `delivery.timeout.ms` | 120000 |
| `pcBufferMemory` | `buffer.memory` | 33554432 |
| `pcMaxBlockMs` | `max.block.ms` | 60000 |
| `pcDialTimeoutMs` | connect timeout | 30000 |
| `pcSocketTimeoutMs` | client-side round-trip bound | 120000 |

`ConsumerConfig` (`defaultConsumerConfig`)

| Field | Kafka name | Default |
|---|---|---|
| `ccClientId` | `client.id` | `"brahmaputra-haskell"` |
| `ccFetchMaxBytes` | `fetch.max.bytes` | 8388608 |
| `ccFetchMinBytes` | `fetch.min.bytes` | 1 |
| `ccFetchMaxWaitMs` | `fetch.max.wait.ms` | 500 |
| `ccRack` | `client.rack` | `""` |
| `ccIsolationLevel` | `isolation.level` | `ReadUncommitted` |
| `ccMaxPollRecords` | `max.poll.records` | 500 |
| `ccDialTimeoutMs` / `ccSocketTimeoutMs` | | 30000 / 120000 |

`GroupConfig` (`defaultGroupConfig`)

| Field | Kafka name | Default |
|---|---|---|
| `gcClientId` | `client.id` | `"brahmaputra-haskell"` |
| `gcSessionTimeoutMs` | `session.timeout.ms` | 10000 |
| `gcHeartbeatIntervalMs` | `heartbeat.interval.ms` (0 = a third of the session timeout) | 0 |
| `gcRebalanceTimeoutMs` | `rebalance.timeout.ms` | 3000 |
| `gcMaxPollIntervalMs` | `max.poll.interval.ms` | 300000 |
| `gcAutoCommitIntervalMs` | `auto.commit.interval.ms` (0 disables) | 5000 |
| `gcAutoOffsetReset` | `auto.offset.reset` | `ResetEarliest` (`ResetLatest`, `ResetNone`) |
| `gcAssignor` | `partition.assignment.strategy` | `RangeAssignor` (`RoundRobinAssignor`, `StickyAssignor`) |
| `gcGroupInstanceId` | `group.instance.id` | `Nothing` |
| `gcMaxPollRecords` | `max.poll.records` | 500 |
| `gcFetchMaxBytes` | `fetch.max.bytes` | 8388608 |
| `gcDialTimeoutMs` / `gcSocketTimeoutMs` | | 30000 / 120000 |

## Modules

| Module | Contents |
|---|---|
| `Brahmaputra` | re-exports everything below |
| `Brahmaputra.Protocol` | BitPacker writer/reader, frames, CRC32C, murmur2, codecs, record batches, errors |
| `Brahmaputra.Connection` | `Conn` (serialised requests, timeouts, broken flag), metadata, `Router` |
| `Brahmaputra.Producer` | batching producer, linger thread, bounded buffer |
| `Brahmaputra.Consumer` | fetch, list offsets, high watermark |
| `Brahmaputra.Group` | consumer groups, heartbeat thread |
| `Brahmaputra.Assignor` | range / roundrobin / sticky (pure) |

## End-to-end test

```bash
brahmaputra-server --data-dir ./data --default-partitions 4
clients/haskell/test.sh 127.0.0.1 9092
```

`test/ManualTest.hs` is a port of the Go suite's 19 sections and 54 checks,
including a silent broker and a TCP proxy that drops connections, both
written inside the test, followed by six coverage sections (31 checks) for
every other setting above: batch.size, linger.ms, retries/backoff/delivery
timeout against a fake refusing broker, fetch.max/min bytes, list offsets by
timestamp, max.poll.records, auto-commit, static membership, LeaveGroup,
session timeout, generation fencing, a registered codec and decoder bounds.
It prints `85 passed, 0 failed`
and exits non-zero on any failure. Set `GHC=` to pick a compiler, or
`GHC_FLAGS=` to override `-Wall -Werror`.

## Not implemented

As with the other non-Rust drivers: no TLS/QUIC, no SASL authentication,
and no idempotent or transactional producer, so retrying an ambiguous
send (a connection dropped mid-request) can duplicate a record.
