namespace Brahmaputra.FSharp

open System

/// Error codes the broker returns (re-exported from the .NET driver).
type ErrorCode = Brahmaputra.ErrorCode

/// A topic and one of its partitions. Compares by topic (ordinal), then by
/// partition as an integer.
[<StructuredFormatDisplay("{Topic}-{Partition}")>]
type TopicPartition =
    { Topic: string
      Partition: int }

    override this.ToString() = sprintf "%s-%d" this.Topic this.Partition

/// A non-zero error code from the broker.
type ServerError =
    { /// The raw code.
      Code: int
      /// The code as an enum.
      Error: ErrorCode
      /// What the client was doing when the broker refused.
      Context: string
      /// Human-readable description.
      Message: string }

    /// True when the broker returned the code before appending, so a retry cannot duplicate.
    member this.Retriable = Brahmaputra.ServerException.IsRetriable this.Code

/// Every failure the F# API reports through `Result`.
[<RequireQualifiedAccess; NoComparison>]
type BrahmaputraError =
    /// The broker answered with a non-zero error code.
    | Server of ServerError
    /// A request got no answer within its round-trip timeout. The connection was closed.
    | Timeout of message: string
    /// `buffer.memory` stayed full for `max.block.ms`.
    | BufferFull of message: string
    /// `auto.offset.reset` is `None` and the group has no committed position here.
    | NoOffsetForPartition of TopicPartition
    /// Connecting failed, or the connection broke mid-request. The router redials on next use.
    | Connection of message: string
    /// Any other client-side failure: a corrupt response, no partitions, a group that would not stabilise.
    | Client of message: string
    /// The producer or consumer was already closed.
    | Disposed of message: string
    /// The operation was cancelled through its cancellation token.
    | Cancelled
    /// A configuration or argument was invalid.
    | InvalidArgument of message: string
    /// The call was made in the wrong state (for example, poll before subscribe).
    | InvalidState of message: string
    /// An exception this API does not classify.
    | Unexpected of exn

    /// A one-line description.
    member this.Message =
        match this with
        | Server s -> s.Message
        | Timeout m
        | BufferFull m
        | Connection m
        | Client m
        | Disposed m
        | InvalidArgument m
        | InvalidState m -> m
        | NoOffsetForPartition tp -> sprintf "no committed offset for %O and auto.offset.reset is none" tp
        | Cancelled -> "operation cancelled"
        | Unexpected e -> e.Message

    override this.ToString() = this.Message

/// A record header. `Value = None` is a null header value, distinct from an empty one.
type Header =
    { Key: string
      Value: byte[] option }

/// Builds headers.
[<RequireQualifiedAccess>]
module Header =
    /// A header with a byte value.
    let create (key: string) (value: byte[]) : Header = { Key = key; Value = Some value }

    /// A header with a UTF-8 string value.
    let ofString (key: string) (value: string) : Header =
        { Key = key; Value = Some(Text.Encoding.UTF8.GetBytes value) }

    /// A header whose value is null.
    let nullValue (key: string) : Header = { Key = key; Value = None }

/// A record to send.
type ProducerRecord =
    { Topic: string
      /// Explicit partition; `None` lets the partitioner choose (murmur2 of the key, or round-robin).
      Partition: int option
      /// `None` is a null key; `Some [||]` is an empty key.
      Key: byte[] option
      /// `None` is a tombstone, distinct from `Some [||]` (an empty value).
      Value: byte[] option
      /// Headers, in order.
      Headers: Header list
      /// Unix-ms timestamp; `None` stamps the time of the send.
      Timestamp: int64 option }

/// Builds records in a pipeline: `ProducerRecord.create "t" bytes |> ProducerRecord.withKey k`.
[<RequireQualifiedAccess>]
module ProducerRecord =
    /// A record with a value and nothing else.
    let create (topic: string) (value: byte[]) : ProducerRecord =
        { Topic = topic
          Partition = None
          Key = None
          Value = Some value
          Headers = []
          Timestamp = None }

    /// A record with a UTF-8 string value.
    let ofString (topic: string) (value: string) : ProducerRecord =
        create topic (Text.Encoding.UTF8.GetBytes value)

    /// A tombstone (null value) for a key.
    let tombstone (topic: string) (key: byte[]) : ProducerRecord =
        { create topic [||] with
            Value = None
            Key = Some key }

    /// Sets the key.
    let withKey (key: byte[]) (record: ProducerRecord) = { record with Key = Some key }

    /// Sets a UTF-8 string key.
    let withStringKey (key: string) (record: ProducerRecord) =
        { record with Key = Some(Text.Encoding.UTF8.GetBytes key) }

    /// Sets the value; `None` makes it a tombstone.
    let withValue (value: byte[] option) (record: ProducerRecord) = { record with Value = value }

    /// Pins the record to a partition.
    let withPartition (partition: int) (record: ProducerRecord) = { record with Partition = Some partition }

    /// Appends a header.
    let withHeader (header: Header) (record: ProducerRecord) =
        { record with Headers = record.Headers @ [ header ] }

    /// Replaces the headers.
    let withHeaders (headers: Header list) (record: ProducerRecord) = { record with Headers = headers }

    /// Sets the unix-ms timestamp.
    let withTimestamp (timestamp: int64) (record: ProducerRecord) = { record with Timestamp = Some timestamp }

/// Where a record landed.
type RecordMetadata =
    { Topic: string
      Partition: int
      /// The offset; `None` with acks=0, where the broker does not answer.
      Offset: int64 option
      /// The record's timestamp, or the broker's append time if it uses one.
      Timestamp: int64 }

/// One record read back.
type ConsumerRecord =
    { Topic: string
      Partition: int
      Offset: int64
      /// `None` is a null key; `Some [||]` is an empty key.
      Key: byte[] option
      /// `None` is a tombstone; `Some [||]` is an empty value.
      Value: byte[] option
      /// Absolute unix milliseconds.
      Timestamp: int64
      /// Headers, in order.
      Headers: Header list }

    /// The record's topic and partition.
    member this.TopicPartition = { Topic = this.Topic; Partition = this.Partition }

/// Reads fields of consumed records.
[<RequireQualifiedAccess>]
module ConsumerRecord =
    /// The first header with this key.
    let tryHeader (key: string) (record: ConsumerRecord) : Header option =
        record.Headers |> List.tryFind (fun h -> h.Key = key)

    /// The value of the first header with this key; `None` if absent or null.
    let headerValue (key: string) (record: ConsumerRecord) : byte[] option =
        tryHeader key record |> Option.bind (fun h -> h.Value)

    /// The value decoded as UTF-8; `None` for a tombstone.
    let valueString (record: ConsumerRecord) : string option =
        record.Value |> Option.map Text.Encoding.UTF8.GetString

    /// The key decoded as UTF-8; `None` for a null key.
    let keyString (record: ConsumerRecord) : string option =
        record.Key |> Option.map Text.Encoding.UTF8.GetString

/// A fetch's records plus the partition's high watermark.
type FetchResult =
    { Records: ConsumerRecord list
      HighWatermark: int64 }

/// `acks`: how much durability a send waits for.
[<RequireQualifiedAccess>]
type Acks =
    /// acks=0: fire and forget; no offset is known.
    | None
    /// acks=1: the leader appended it.
    | Leader
    /// acks=all: every in-sync replica has it.
    | All

/// `compression.type`. `None` and `Gzip` are built in; register the others with `Codecs.register`.
[<RequireQualifiedAccess>]
type Compression =
    | None
    | Gzip
    | Lz4
    | Zstd
    | Snappy

/// `isolation.level`.
[<RequireQualifiedAccess>]
type IsolationLevel =
    | ReadUncommitted
    | ReadCommitted

/// `auto.offset.reset`.
[<RequireQualifiedAccess>]
type AutoOffsetReset =
    /// The oldest retained record.
    | Earliest
    /// The end of the log.
    | Latest
    /// Refuse to guess: poll returns `BrahmaputraError.NoOffsetForPartition`.
    | None

/// `partition.assignment.strategy`.
[<RequireQualifiedAccess>]
type Assignor =
    | Range
    | RoundRobin
    | Sticky

/// What `listOffsets` resolves.
[<RequireQualifiedAccess>]
type OffsetSpec =
    /// The first retained offset.
    | Earliest
    /// The log end offset (the next offset to be written).
    | Latest
    /// The first offset whose timestamp is at or after this unix-ms time.
    | AtTimestamp of int64

/// Producer settings, named after Kafka's.
type ProducerConfig =
    { /// `bootstrap.servers`: comma-separated host:port list.
      BootstrapServers: string
      /// `client.id`.
      ClientId: string
      /// `acks`.
      Acks: Acks
      /// `batch.size` in bytes.
      BatchSize: int
      /// `linger.ms`.
      LingerMs: int
      /// `compression.type`.
      Compression: Compression
      /// `request.timeout.ms`: the broker-side wait for acknowledgements.
      RequestTimeoutMs: int
      /// `retries` of retriable broker errors.
      Retries: int
      /// `retry.backoff.ms`.
      RetryBackoffMs: int
      /// `delivery.timeout.ms`.
      DeliveryTimeoutMs: int
      /// `buffer.memory` in bytes.
      BufferMemory: int64
      /// `max.block.ms`: how long a send may block on a full buffer.
      MaxBlockMs: int
      /// `socket.connection.setup.timeout.ms`.
      ConnectTimeoutMs: int }

/// Producer config defaults.
[<RequireQualifiedAccess>]
module ProducerConfig =
    /// Defaults, which match the .NET driver's (linger.ms 5, acks=1).
    let defaults: ProducerConfig =
        { BootstrapServers = "127.0.0.1:9092"
          ClientId = "brahmaputra-fsharp"
          Acks = Acks.Leader
          BatchSize = 16 * 1024
          LingerMs = 5
          Compression = Compression.None
          RequestTimeoutMs = 30_000
          Retries = 5
          RetryBackoffMs = 100
          DeliveryTimeoutMs = 120_000
          BufferMemory = 32L * 1024L * 1024L
          MaxBlockMs = 60_000
          ConnectTimeoutMs = 30_000 }

    /// Defaults with the given bootstrap servers.
    let create (bootstrapServers: string) = { defaults with BootstrapServers = bootstrapServers }

/// Partition-consumer settings, named after Kafka's.
type ConsumerConfig =
    { BootstrapServers: string
      ClientId: string
      /// `fetch.max.bytes`.
      FetchMaxBytes: int
      /// `fetch.min.bytes`.
      FetchMinBytes: int
      /// `fetch.max.wait.ms`: the long-poll ceiling.
      FetchMaxWaitMs: int
      /// `max.poll.records`: the most records one fetch returns (0: unlimited); the
      /// rest come on the next fetch from the last returned offset + 1.
      MaxPollRecords: int
      /// `client.rack`; `None` for no rack.
      ClientRack: string option
      /// `isolation.level`.
      IsolationLevel: IsolationLevel
      /// `request.timeout.ms`: the client-side round-trip timeout of one request.
      RequestTimeoutMs: int
      /// `socket.connection.setup.timeout.ms`.
      ConnectTimeoutMs: int }

/// Consumer config defaults.
[<RequireQualifiedAccess>]
module ConsumerConfig =
    /// Defaults.
    let defaults: ConsumerConfig =
        { BootstrapServers = "127.0.0.1:9092"
          ClientId = "brahmaputra-fsharp"
          FetchMaxBytes = 8 * 1024 * 1024
          FetchMinBytes = 1
          FetchMaxWaitMs = 500
          MaxPollRecords = 500
          ClientRack = None
          IsolationLevel = IsolationLevel.ReadUncommitted
          RequestTimeoutMs = 30_000
          ConnectTimeoutMs = 30_000 }

    /// Defaults with the given bootstrap servers.
    let create (bootstrapServers: string) = { defaults with BootstrapServers = bootstrapServers }

/// Consumer-group settings, named after Kafka's.
type GroupConsumerConfig =
    { BootstrapServers: string
      ClientId: string
      /// `group.id` (required).
      GroupId: string
      /// `group.instance.id`: static membership; `None` for a dynamic member.
      GroupInstanceId: string option
      /// `session.timeout.ms`.
      SessionTimeoutMs: int
      /// `heartbeat.interval.ms`: keep it well under the session timeout; 0 uses a third of it.
      HeartbeatIntervalMs: int
      /// How long the coordinator waits for members to rejoin during a rebalance.
      RebalanceTimeoutMs: int
      /// `max.poll.interval.ms`. Time spent inside poll never counts against it.
      MaxPollIntervalMs: int
      /// `max.poll.records`.
      MaxPollRecords: int
      /// `enable.auto.commit`.
      EnableAutoCommit: bool
      /// `auto.commit.interval.ms`.
      AutoCommitIntervalMs: int
      /// `auto.offset.reset`.
      AutoOffsetReset: AutoOffsetReset
      /// `partition.assignment.strategy`.
      Assignor: Assignor
      FetchMaxBytes: int
      FetchMinBytes: int
      FetchMaxWaitMs: int
      ClientRack: string option
      IsolationLevel: IsolationLevel
      RequestTimeoutMs: int
      ConnectTimeoutMs: int }

/// Group consumer config defaults.
[<RequireQualifiedAccess>]
module GroupConsumerConfig =
    /// Defaults (session.timeout.ms 10 s, range assignor, auto commit on, earliest).
    let defaults: GroupConsumerConfig =
        { BootstrapServers = "127.0.0.1:9092"
          ClientId = "brahmaputra-fsharp"
          GroupId = ""
          GroupInstanceId = None
          SessionTimeoutMs = 10_000
          HeartbeatIntervalMs = 3_000
          RebalanceTimeoutMs = 3_000
          MaxPollIntervalMs = 300_000
          MaxPollRecords = 500
          EnableAutoCommit = true
          AutoCommitIntervalMs = 5_000
          AutoOffsetReset = AutoOffsetReset.Earliest
          Assignor = Assignor.Range
          FetchMaxBytes = 8 * 1024 * 1024
          FetchMinBytes = 1
          FetchMaxWaitMs = 500
          ClientRack = None
          IsolationLevel = IsolationLevel.ReadUncommitted
          RequestTimeoutMs = 30_000
          ConnectTimeoutMs = 30_000 }

    /// Defaults with the given bootstrap servers and group id.
    let create (bootstrapServers: string) (groupId: string) =
        { defaults with
            BootstrapServers = bootstrapServers
            GroupId = groupId }

/// One broker in the cluster.
type BrokerInfo =
    { NodeId: int
      Host: string
      Port: int
      /// `None` when the broker has no rack.
      Rack: string option }

/// One partition's replica placement.
type PartitionMetadata =
    { Partition: int
      Leader: int
      Replicas: int list
      Isr: int list
      LeaderEpoch: int }

/// A snapshot of the cluster.
type ClusterMetadata =
    { Brokers: BrokerInfo list
      ControllerId: int
      /// Topic name to its partitions, in ascending partition order.
      Topics: Map<string, PartitionMetadata list> }

/// One entry of an ApiVersions response.
type ApiVersionRange =
    { ApiKey: int
      MinVersion: int
      MaxVersion: int }

/// What a broker speaks.
type ApiVersions =
    { Ranges: ApiVersionRange list
      BrokerVersion: string }
