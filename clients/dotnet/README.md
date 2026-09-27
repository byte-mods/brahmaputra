# Brahmaputra client for .NET

A native C# driver for .NET 8. It speaks the wire protocol directly and needs
no NuGet packages: gzip comes from `System.IO.Compression`, and CRC32C and
Kafka's murmur2 are written out in the library.

```bash
dotnet build clients/dotnet/Brahmaputra.sln -c Release
```

To use it from your own project, add a project reference:

```xml
<ProjectReference Include="path/to/clients/dotnet/Brahmaputra/Brahmaputra.csproj" />
```

Tested end to end against a live broker: **54/54 checks** (`./test.sh 127.0.0.1 9092`).

Every blocking call has an `…Async` twin that takes a `CancellationToken`.
The sync methods wrap the async ones. The library uses `ConfigureAwait(false)`
throughout, so calling the sync methods from a UI or ASP.NET context will not
deadlock.

## Produce

```csharp
using Brahmaputra;
using System.Text;

var config = new ProducerConfig
{
    BootstrapServers = "127.0.0.1:9092",
    Acks = Acks.Leader,
    LingerMs = 5,
    CompressionType = CompressionType.Gzip,
};
await using var producer = new Producer(config);

// Keyed: murmur2(key) % partitions, so records sharing a key keep their order.
// Send buffers the record and returns a delivery task. It blocks only while
// the buffer is full, for at most max.block.ms.
Task<RecordMetadata> delivery = producer.Send("orders",
    Encoding.UTF8.GetBytes("{\"id\":1}"), Encoding.UTF8.GetBytes("user-7"),
    new RecordHeader("trace-id", "abc-123"));

// Or wait for the acknowledgement and get the offset. This is one full round
// trip per record: correct, but slow.
RecordMetadata md = await producer.ProduceAsync(
    new ProducerRecord("orders", Encoding.UTF8.GetBytes("{\"id\":2}"))
    {
        Partition = 3,                   // explicit partition, bypasses the partitioner
        Timestamp = 1_700_000_000_000,   // optional; defaults to the time of the send
    });

// A null value is a tombstone. It stays distinct from an empty value.
producer.Send("orders", value: null, key: Encoding.UTF8.GetBytes("user-7"));

await producer.FlushAsync();   // throws the first delivery failure, if any
```

`FlushAsync` also throws when an earlier background send failed and nobody
awaited that record's delivery task, so a failed linger flush is never
silent. `CloseAsync` flushes, releases everything, and then throws any
failure that has not been reported yet. `Dispose` and `DisposeAsync` drain
the buffer the same way but do not throw; failures stay on each record's task.

## Consume one partition

```csharp
using var consumer = new Consumer(new ConsumerConfig { BootstrapServers = "127.0.0.1:9092" });

IReadOnlyList<ConsumeResult> records = consumer.Fetch("orders", partition: 0, offset: 0, maxWaitMs: 500);
foreach (var r in records)
    Console.WriteLine($"{r.Offset} {r.Timestamp} {Encoding.UTF8.GetString(r.Value ?? [])}");

long end   = consumer.ListOffsets("orders", 0, Wire.Latest);
long start = consumer.ListOffsets("orders", 0, Wire.Earliest);
long atT   = consumer.ListOffsets("orders", 0, DateTimeOffset.UtcNow.AddHours(-1).ToUnixTimeMilliseconds());
FetchResult fr = await consumer.FetchVerboseAsync("orders", 0, end, 500);   // fr.HighWatermark
```

## Consume as a group

```csharp
var config = new GroupConsumerConfig
{
    BootstrapServers = "127.0.0.1:9092",
    GroupId = "billing",
    PartitionAssignmentStrategy = PartitionAssignmentStrategy.Sticky,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,        // commit explicitly
    GroupInstanceId = "worker-3",    // static membership
};
await using var consumer = new GroupConsumer(config);   // on dispose: commit if auto, then LeaveGroup
consumer.Subscribe(new[] { "orders" });

while (!stopping.IsCancellationRequested)
{
    var records = await consumer.PollAsync(TimeSpan.FromMilliseconds(500), stopping);
    foreach (var r in records) Handle(r);
    // At-least-once: commit after processing, never before.
    await consumer.CommitAsync(stopping);
}
```

A `GroupConsumer` is not thread-safe, which matches Kafka's consumer. Poll
and commit from one thread or one async flow. Heartbeats run on a
background task. If the application stops polling for longer than
`MaxPollIntervalMs`, the member leaves the group on its own, so a live but
stuck process does not keep its partitions. The next poll rejoins.

## Configuration

Names follow Kafka's. Where a default differs from Kafka's, the table says so.

**`ProducerConfig`**

| Property | Kafka name | Default |
|---|---|---|
| `BootstrapServers` | `bootstrap.servers` | `127.0.0.1:9092` (comma-separated list) |
| `ClientId` | `client.id` | `brahmaputra-dotnet` |
| `Acks` | `acks` | `Acks.Leader` (`None`=0, `Leader`=1, `All`=-1) |
| `BatchSize` | `batch.size` | 16384 |
| `LingerMs` | `linger.ms` | 5 (Kafka: 0) |
| `CompressionType` | `compression.type` | `None` (`Gzip` built in; others via `Codecs.Register`) |
| `RequestTimeoutMs` | `request.timeout.ms` | 30000 |
| `Retries` | `retries` | 5 (retriable broker errors only, so a retry never duplicates) |
| `RetryBackoffMs` | `retry.backoff.ms` | 100 |
| `DeliveryTimeoutMs` | `delivery.timeout.ms` | 120000 |
| `BufferMemory` | `buffer.memory` | 32 MiB |
| `MaxBlockMs` | `max.block.ms` | 60000, then `BufferFullException` |
| `ConnectTimeoutMs` | `socket.connection.setup.timeout.ms` | 30000 |

**`ConsumerConfig`**

| Property | Kafka name | Default |
|---|---|---|
| `FetchMaxBytes` | `fetch.max.bytes` | 8 MiB |
| `FetchMinBytes` | `fetch.min.bytes` | 1 |
| `FetchMaxWaitMs` | `fetch.max.wait.ms` | 500 |
| `MaxPollRecords` | `max.poll.records` | 500 |
| `ClientRack` | `client.rack` | empty |
| `IsolationLevel` | `isolation.level` | `Wire.ReadUncommitted` |
| `RequestTimeoutMs`, `ConnectTimeoutMs`, `BootstrapServers`, `ClientId` | | as above |

**`GroupConsumerConfig`** (extends `ConsumerConfig`)

| Property | Kafka name | Default |
|---|---|---|
| `GroupId` | `group.id` | required |
| `SessionTimeoutMs` | `session.timeout.ms` | 10000 (Kafka: 45000) |
| `RebalanceTimeoutMs` | | 3000 |
| `MaxPollIntervalMs` | `max.poll.interval.ms` | 300000 |
| `EnableAutoCommit` | `enable.auto.commit` | true |
| `AutoCommitIntervalMs` | `auto.commit.interval.ms` | 5000 |
| `AutoOffsetReset` | `auto.offset.reset` | `Earliest` (`Latest`; `None` throws `NoOffsetForPartitionException`) |
| `PartitionAssignmentStrategy` | `partition.assignment.strategy` | `Range` (`RoundRobin`, `Sticky`) |
| `GroupInstanceId` | `group.instance.id` | null (dynamic member) |

## Compression

`None` and `Gzip` are built in. The other codecs are opt-in, so this
library brings no compression dependencies with it:

```csharp
Codecs.Register(CompressionType.Zstd,
    compress:   payload => MyZstd.Compress(payload),
    decompress: payload => MyZstd.Decompress(payload));
```

If you register lz4, the broker expects a little-endian `uint32` of the
uncompressed length followed by a raw LZ4 **block**. It does not accept the
LZ4 frame format, and a frame-format library will produce batches the
broker cannot read.

## Errors

Every error is a `BrahmaputraException`. A broker error code arrives as
`ServerException`, with `.Code` and `.Error` (`ErrorCode` enum). A client
request timeout is a `TimeoutException`. When a request times out or is
cancelled partway, the connection is left in an unknown state, so it is
closed, and the router re-dials it on the next use.

## Running the end-to-end suite

Start a broker, then:

```bash
./clients/dotnet/test.sh 127.0.0.1 9092
```

The script builds the solution in Release and runs `Brahmaputra.ManualTest`.
That suite has the same sections and checks as the Go suite, and ends with
`N passed, 0 failed`. It exits 1 if any check fails and 2 if setup fails
(for example, when no broker is reachable). You can also run it directly:

```bash
cd clients/dotnet
dotnet run -c Release --project Brahmaputra.ManualTest -- 127.0.0.1 9092
```
