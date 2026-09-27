# Brahmaputra client for F#

An idiomatic F# API over the verified .NET driver
([`clients/dotnet`](../dotnet)). The wire protocol, batching, retries,
routing and group membership all come from that driver. This layer adds
the F# surface: records for configs and messages, `option` where the
driver uses `null`, `Result` for failures, `Async` and `Task` variants of
every call, and module functions that read well in a pipeline.

It needs no NuGet packages beyond FSharp.Core. The .NET SDK ships that in
`sdk/<version>/FSharp/library-packs`. `nuget.config` clears the remote
feeds, so the build works offline.

```bash
dotnet build clients/fsharp/Brahmaputra.FSharp -c Release
```

To use it from your own project, add a project reference. The driver comes
with it:

```xml
<ProjectReference Include="path/to/clients/fsharp/Brahmaputra.FSharp/Brahmaputra.FSharp.fsproj" />
```

Tested end to end against a live broker: **54/54 checks**
(`./test.sh 127.0.0.1 9092`).

## Conventions

- **Three shapes per operation.** Each call comes as `op`, which blocks and
  returns `Result<'T, BrahmaputraError>`. It also comes as `opAsync`
  (`Async<Result<…>>`) and `opTask` (`Task<Result<…>>`). No shape throws for
  a broker or network failure. An `opAsync` passes the Async's cancellation
  token to the driver, and cancelling it cancels the Async in the normal F#
  way.
- **The handle comes last**, so calls pipe:
  `producer |> Producer.send record`.
- **`option`, never `null`.** A tombstone is `Value = None`, and it is
  distinct from `Some [||]`, an empty value. The same holds for keys and
  header values.
- **Lists, not lazy sequences.** `poll` and `fetch` return
  `ConsumerRecord list`.
- **`IDisposable`.** `Producer`, `Consumer`, `GroupConsumer` and
  `Connection` all work with `use`. `Producer` and `GroupConsumer` also
  implement `IAsyncDisposable`.
- **`.Underlying`** exposes the driver object when you need something this
  layer does not wrap.

Errors are a union:

```fsharp
type BrahmaputraError =
    | Server of ServerError          // broker error code: .Code, .Error, .Context, .Retriable
    | Timeout of string              // request round-trip timeout; the connection was closed
    | BufferFull of string           // buffer.memory full for max.block.ms
    | NoOffsetForPartition of TopicPartition   // auto.offset.reset = None
    | Connection of string           // connect failed / connection broke (redialled on next use)
    | Client of string               // corrupt response, no partitions, group would not stabilise
    | Disposed of string | Cancelled | InvalidArgument of string | InvalidState of string
    | Unexpected of exn
```

## Produce

```fsharp
open Brahmaputra.FSharp

let config =
    { ProducerConfig.create "127.0.0.1:9092" with
        Acks = Acks.Leader
        LingerMs = 5
        Compression = Compression.Gzip }

use producer = Producer.create config          // or Producer.tryCreate for a Result

// Keyed: murmur2(key) % partitions, so records that share a key keep their
// order. send buffers the record and returns a Delivery. It blocks only while
// the buffer is full, for at most max.block.ms.
let delivery =
    ProducerRecord.ofString "orders" """{"id":1}"""
    |> ProducerRecord.withStringKey "user-7"
    |> ProducerRecord.withHeader (Header.ofString "trace-id" "abc-123")
    |> fun record -> producer |> Producer.send record

// Or wait for the acknowledgement: one round trip per record.
match producer |> Producer.produce (ProducerRecord.ofString "orders" "x" |> ProducerRecord.withPartition 3) with
| Ok meta -> printfn "stored at %d-%A" meta.Partition meta.Offset    // Offset = None with acks=0
| Error e -> eprintfn "failed: %s" e.Message

// A tombstone: Value = None, distinct from an empty value.
producer |> Producer.send (ProducerRecord.tombstone "orders" (Text.Encoding.UTF8.GetBytes "user-7")) |> ignore

// Returns the first delivery failure, including one from an earlier
// background (linger) send that nobody waited on.
match producer |> Producer.flush with
| Ok () -> ()
| Error e -> eprintfn "a send failed: %s" e.Message
```

A `Delivery` can be awaited with `Delivery.wait`, `Delivery.waitAsync` or
`Delivery.waitTask`, or all at once with `Delivery.waitAll`.
`Producer.close` flushes and releases resources, and then reports any
failure not yet reported. `Dispose` drains the buffer the same way but does
not report.

In an async workflow:

```fsharp
let publish (producer: Producer) (events: (string * byte[]) list) = async {
    for key, body in events do
        let! _ = producer |> Producer.sendAsync (ProducerRecord.create "events" body
                                                 |> ProducerRecord.withStringKey key)
        ()
    return! producer |> Producer.flushAsync
}
```

## Consume one partition

```fsharp
use consumer = Consumer.create (ConsumerConfig.create "127.0.0.1:9092")

match consumer |> Consumer.fetch "orders" 0 0L 500 with
| Ok records ->
    for r in records do
        printfn "%d %d %A" r.Offset r.Timestamp (ConsumerRecord.valueString r)   // None = tombstone
| Error e -> eprintfn "%s" e.Message

let latest = consumer |> Consumer.listOffsets "orders" 0 OffsetSpec.Latest
let hourAgo = consumer |> Consumer.listOffsets "orders" 0
                            (OffsetSpec.AtTimestamp(DateTimeOffset.UtcNow.AddHours(-1.).ToUnixTimeMilliseconds()))
let verbose = consumer |> Consumer.fetchVerbose "orders" 0 0L 500    // FetchResult.HighWatermark
```

## Consume as a group

```fsharp
let config =
    { GroupConsumerConfig.create "127.0.0.1:9092" "billing" with
        Assignor = Assignor.Sticky
        AutoOffsetReset = AutoOffsetReset.Earliest
        EnableAutoCommit = false               // commit explicitly
        GroupInstanceId = Some "worker-3" }    // static membership

use consumer = GroupConsumer.create config    // on dispose: commit if auto, then LeaveGroup
consumer |> GroupConsumer.subscribe [ "orders" ]

let rec loop () = async {
    match! consumer |> GroupConsumer.pollAsync (TimeSpan.FromMilliseconds 500.) with
    | Ok records ->
        records |> List.iter handle
        // At-least-once: commit after processing, never before.
        let! _ = consumer |> GroupConsumer.commitAsync
        return! loop ()
    | Error (BrahmaputraError.NoOffsetForPartition tp) -> failwithf "no position for %O" tp
    | Error e -> eprintfn "poll: %s" e.Message; return! loop ()
}
```

A `GroupConsumer` is not thread-safe, which matches Kafka's consumer. Poll
and commit from one thread or one async flow. Heartbeats run in the
background. If the application stops polling for longer than
`MaxPollIntervalMs`, the member leaves the group, and the next poll
rejoins. Time spent inside `poll` never counts toward that limit.
`GroupConsumer.committed`, `assignment`, `memberId` and `generation` report
the member's state.

## Configuration

Field names follow Kafka's names. Build a config from `…Config.create` or
`…Config.defaults` and change fields with `{ c with … }`.

**`ProducerConfig`**

| Field | Kafka name | Default |
|---|---|---|
| `BootstrapServers` | `bootstrap.servers` | `127.0.0.1:9092` (comma-separated) |
| `ClientId` | `client.id` | `brahmaputra-fsharp` |
| `Acks` | `acks` | `Acks.Leader` (`None` = 0, `Leader` = 1, `All` = all) |
| `BatchSize` | `batch.size` | 16384 |
| `LingerMs` | `linger.ms` | 5 (Kafka: 0) |
| `Compression` | `compression.type` | `Compression.None` (`Gzip` built in; others via `Codecs.register`) |
| `RequestTimeoutMs` | `request.timeout.ms` | 30000 |
| `Retries` | `retries` | 5 (retriable broker errors only) |
| `RetryBackoffMs` | `retry.backoff.ms` | 100 |
| `DeliveryTimeoutMs` | `delivery.timeout.ms` | 120000 |
| `BufferMemory` | `buffer.memory` | 32 MiB |
| `MaxBlockMs` | `max.block.ms` | 60000, then `BrahmaputraError.BufferFull` |
| `ConnectTimeoutMs` | `socket.connection.setup.timeout.ms` | 30000 |

**`ConsumerConfig`**

| Field | Kafka name | Default |
|---|---|---|
| `FetchMaxBytes` | `fetch.max.bytes` | 8 MiB |
| `FetchMinBytes` | `fetch.min.bytes` | 1 |
| `FetchMaxWaitMs` | `fetch.max.wait.ms` | 500 |
| `ClientRack` | `client.rack` | `None` |
| `IsolationLevel` | `isolation.level` | `ReadUncommitted` |
| `RequestTimeoutMs` | `request.timeout.ms` | 30000 (client-side round-trip timeout per request) |
| `BootstrapServers`, `ClientId`, `ConnectTimeoutMs` | | as above |

**`GroupConsumerConfig`** has every `ConsumerConfig` field, plus:

| Field | Kafka name | Default |
|---|---|---|
| `GroupId` | `group.id` | required |
| `GroupInstanceId` | `group.instance.id` | `None` (dynamic member) |
| `SessionTimeoutMs` | `session.timeout.ms` | 10000 (Kafka: 45000) |
| `RebalanceTimeoutMs` | | 3000 |
| `MaxPollIntervalMs` | `max.poll.interval.ms` | 300000 |
| `MaxPollRecords` | `max.poll.records` | 500 |
| `EnableAutoCommit` | `enable.auto.commit` | true |
| `AutoCommitIntervalMs` | `auto.commit.interval.ms` | 5000 |
| `AutoOffsetReset` | `auto.offset.reset` | `Earliest` (`Latest`; `None` returns `NoOffsetForPartition`) |
| `Assignor` | `partition.assignment.strategy` | `Range` (`RoundRobin`, `Sticky`) |

## Compression

`None` and `Gzip` are built in. The others are opt-in:

```fsharp
Codecs.register Compression.Zstd MyZstd.compress MyZstd.decompress |> ignore
```

For lz4, the broker expects a little-endian `uint32` holding the
uncompressed length, followed by a raw LZ4 **block**. It does not accept
the LZ4 frame format.

## Lower level

`Partitioner.murmur2` and `Partitioner.partitionForKey` expose the
partitioner. Kafka's murmur2 gives `murmur2 [||] = 275646681u`.
`Connection.connect address clientId connectTimeout requestTimeout` opens a
raw broker connection. It offers `Connection.apiVersions` and
`Connection.isBroken`: a connection is marked broken after a timeout, an
I/O error or a correlation mismatch. `Consumer.metadata` and
`Producer.metadata` return a `ClusterMetadata` record.

## Running the end-to-end suite

Start a broker, then run:

```bash
./clients/fsharp/test.sh 127.0.0.1 9092
```

The script builds the suite and the two libraries it uses in Release. All
build output, including the C# driver's, goes to `clients/fsharp/artifacts`.
It then runs `Brahmaputra.FSharp.ManualTest`. That suite is a check-for-check
port of the Go suite, driven through the F# API, and it ends with
`54 passed, 0 failed`. The script exits 1 if a check fails and 2 if setup
fails, for example when no broker is reachable. You can also run the suite
directly:

```bash
cd clients/fsharp
dotnet run -c Release --project Brahmaputra.FSharp.ManualTest -- 127.0.0.1 9092
```
