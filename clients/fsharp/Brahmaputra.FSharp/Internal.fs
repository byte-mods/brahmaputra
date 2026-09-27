namespace Brahmaputra.FSharp

open System
open System.IO
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks

/// Maps exceptions from the .NET driver onto `BrahmaputraError`.
[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module BrahmaputraError =
    let private isConnectionMessage (message: string) =
        message.StartsWith("connect to ", StringComparison.Ordinal)
        || message.StartsWith("connection to ", StringComparison.Ordinal)
        || message.StartsWith("no bootstrap server reachable", StringComparison.Ordinal)

    /// Classifies an exception thrown by the underlying driver.
    let rec ofExn (e: exn) : BrahmaputraError =
        match e with
        | :? AggregateException as a when a.InnerExceptions.Count = 1 -> ofExn a.InnerExceptions.[0]
        | :? Brahmaputra.ServerException as s ->
            BrahmaputraError.Server
                { Code = s.Code
                  Error = s.Error
                  Context = s.Context
                  Message = s.Message }
        | :? Brahmaputra.BufferFullException as b -> BrahmaputraError.BufferFull b.Message
        | :? Brahmaputra.NoOffsetForPartitionException as n ->
            BrahmaputraError.NoOffsetForPartition
                { Topic = n.Partition.Topic
                  Partition = n.Partition.Partition }
        | :? Brahmaputra.BrahmaputraException as b ->
            match b.InnerException with
            | :? SocketException
            | :? IOException -> BrahmaputraError.Connection b.Message
            | _ when isConnectionMessage b.Message -> BrahmaputraError.Connection b.Message
            | _ -> BrahmaputraError.Client b.Message
        | :? TimeoutException as t -> BrahmaputraError.Timeout t.Message
        | :? ObjectDisposedException as d -> BrahmaputraError.Disposed d.Message
        | :? OperationCanceledException -> BrahmaputraError.Cancelled
        | :? IOException
        | :? SocketException -> BrahmaputraError.Connection e.Message
        | :? ArgumentException as a -> BrahmaputraError.InvalidArgument a.Message
        | :? InvalidOperationException as i -> BrahmaputraError.InvalidState i.Message
        | other -> BrahmaputraError.Unexpected other

    /// A one-line description of an error.
    let message (error: BrahmaputraError) = error.Message

/// Runs driver calls and turns their exceptions into `Result`s.
module internal Attempt =
    let run (f: unit -> 'T) : Result<'T, BrahmaputraError> =
        try
            Ok(f ())
        with e ->
            Error(BrahmaputraError.ofExn e)

    let runTask (f: unit -> Task<'T>) : Task<Result<'T, BrahmaputraError>> =
        task {
            try
                let! value = f ()
                return Ok value
            with e ->
                return Error(BrahmaputraError.ofExn e)
        }

    let runTaskUnit (f: unit -> Task) : Task<Result<unit, BrahmaputraError>> =
        task {
            try
                do! f ()
                return Ok()
            with e ->
                return Error(BrahmaputraError.ofExn e)
        }

    /// An Async that passes its own cancellation token to the driver.
    let runAsync (f: CancellationToken -> Task<'T>) : Async<Result<'T, BrahmaputraError>> =
        async {
            let! token = Async.CancellationToken
            return! runTask (fun () -> f token) |> Async.AwaitTask
        }

    let runAsyncUnit (f: CancellationToken -> Task) : Async<Result<unit, BrahmaputraError>> =
        async {
            let! token = Async.CancellationToken
            return! runTaskUnit (fun () -> f token) |> Async.AwaitTask
        }

/// Conversions between the F# records and the driver's classes.
module internal Convert =
    let bytesOrNull (value: byte[] option) : byte[] = Option.toObj value

    let header (h: Brahmaputra.RecordHeader) : Header = { Key = h.Key; Value = Option.ofObj h.Value }

    let toCsHeader (h: Header) = Brahmaputra.RecordHeader(h.Key, bytesOrNull h.Value)

    let topicPartition (tp: Brahmaputra.TopicPartition) : TopicPartition =
        { Topic = tp.Topic
          Partition = tp.Partition }

    let toCsTopicPartition (tp: TopicPartition) =
        Brahmaputra.TopicPartition(tp.Topic, tp.Partition)

    let consumerRecord (r: Brahmaputra.ConsumeResult) : ConsumerRecord =
        { Topic = r.Topic
          Partition = r.Partition
          Offset = r.Offset
          Key = Option.ofObj r.Key
          Value = Option.ofObj r.Value
          Timestamp = r.Timestamp
          Headers = r.Headers |> Seq.map header |> List.ofSeq }

    let consumerRecords (records: Collections.Generic.IReadOnlyList<Brahmaputra.ConsumeResult>) =
        records |> Seq.map consumerRecord |> List.ofSeq

    let recordMetadata (m: Brahmaputra.RecordMetadata) : RecordMetadata =
        { Topic = m.Topic
          Partition = m.Partition
          Offset = if m.Offset < 0L then None else Some m.Offset
          Timestamp = m.Timestamp }

    let toCsRecord (r: ProducerRecord) =
        Brahmaputra.ProducerRecord(
            r.Topic,
            bytesOrNull r.Value,
            bytesOrNull r.Key,
            Partition = Option.toNullable r.Partition,
            Headers = (r.Headers |> List.map toCsHeader |> Array.ofList),
            Timestamp = Option.toNullable r.Timestamp
        )

    let acks =
        function
        | Acks.None -> Brahmaputra.Acks.None
        | Acks.Leader -> Brahmaputra.Acks.Leader
        | Acks.All -> Brahmaputra.Acks.All

    let compression =
        function
        | Compression.None -> Brahmaputra.CompressionType.None
        | Compression.Gzip -> Brahmaputra.CompressionType.Gzip
        | Compression.Lz4 -> Brahmaputra.CompressionType.Lz4
        | Compression.Zstd -> Brahmaputra.CompressionType.Zstd
        | Compression.Snappy -> Brahmaputra.CompressionType.Snappy

    let isolation =
        function
        | IsolationLevel.ReadUncommitted -> Brahmaputra.Wire.ReadUncommitted
        | IsolationLevel.ReadCommitted -> Brahmaputra.Wire.ReadCommitted

    let offsetReset =
        function
        | AutoOffsetReset.Earliest -> Brahmaputra.AutoOffsetReset.Earliest
        | AutoOffsetReset.Latest -> Brahmaputra.AutoOffsetReset.Latest
        | AutoOffsetReset.None -> Brahmaputra.AutoOffsetReset.None

    let assignor =
        function
        | Assignor.Range -> Brahmaputra.PartitionAssignmentStrategy.Range
        | Assignor.RoundRobin -> Brahmaputra.PartitionAssignmentStrategy.RoundRobin
        | Assignor.Sticky -> Brahmaputra.PartitionAssignmentStrategy.Sticky

    let offsetSpec =
        function
        | OffsetSpec.Earliest -> Brahmaputra.Wire.Earliest
        | OffsetSpec.Latest -> Brahmaputra.Wire.Latest
        | OffsetSpec.AtTimestamp ts -> ts

    let producerConfig (c: ProducerConfig) =
        Brahmaputra.ProducerConfig(
            BootstrapServers = c.BootstrapServers,
            ClientId = c.ClientId,
            Acks = acks c.Acks,
            BatchSize = c.BatchSize,
            LingerMs = c.LingerMs,
            CompressionType = compression c.Compression,
            RequestTimeoutMs = c.RequestTimeoutMs,
            Retries = c.Retries,
            RetryBackoffMs = c.RetryBackoffMs,
            DeliveryTimeoutMs = c.DeliveryTimeoutMs,
            BufferMemory = c.BufferMemory,
            MaxBlockMs = c.MaxBlockMs,
            ConnectTimeoutMs = c.ConnectTimeoutMs
        )

    let consumerConfig (c: ConsumerConfig) =
        Brahmaputra.ConsumerConfig(
            BootstrapServers = c.BootstrapServers,
            ClientId = c.ClientId,
            FetchMaxBytes = c.FetchMaxBytes,
            FetchMinBytes = c.FetchMinBytes,
            FetchMaxWaitMs = c.FetchMaxWaitMs,
            ClientRack = Option.defaultValue "" c.ClientRack,
            IsolationLevel = isolation c.IsolationLevel,
            RequestTimeoutMs = c.RequestTimeoutMs,
            ConnectTimeoutMs = c.ConnectTimeoutMs
        )

    let groupConfig (c: GroupConsumerConfig) =
        Brahmaputra.GroupConsumerConfig(
            BootstrapServers = c.BootstrapServers,
            ClientId = c.ClientId,
            GroupId = c.GroupId,
            GroupInstanceId = Option.toObj c.GroupInstanceId,
            SessionTimeoutMs = c.SessionTimeoutMs,
            RebalanceTimeoutMs = c.RebalanceTimeoutMs,
            MaxPollIntervalMs = c.MaxPollIntervalMs,
            MaxPollRecords = c.MaxPollRecords,
            EnableAutoCommit = c.EnableAutoCommit,
            AutoCommitIntervalMs = c.AutoCommitIntervalMs,
            AutoOffsetReset = offsetReset c.AutoOffsetReset,
            PartitionAssignmentStrategy = assignor c.Assignor,
            FetchMaxBytes = c.FetchMaxBytes,
            FetchMinBytes = c.FetchMinBytes,
            FetchMaxWaitMs = c.FetchMaxWaitMs,
            ClientRack = Option.defaultValue "" c.ClientRack,
            IsolationLevel = isolation c.IsolationLevel,
            RequestTimeoutMs = c.RequestTimeoutMs,
            ConnectTimeoutMs = c.ConnectTimeoutMs
        )

    let metadata (m: Brahmaputra.ClusterMetadata) : ClusterMetadata =
        { Brokers =
            m.Brokers
            |> Seq.map (fun b ->
                { NodeId = b.NodeId
                  Host = b.Host
                  Port = b.Port
                  Rack = if String.IsNullOrEmpty b.Rack then None else Some b.Rack })
            |> List.ofSeq
          ControllerId = m.ControllerId
          Topics =
            m.Topics
            |> Seq.map (fun kv ->
                let partitions =
                    kv.Value.Partitions
                    |> Seq.map (fun p ->
                        { Partition = p.Partition
                          Leader = p.Leader
                          Replicas = List.ofSeq p.Replicas
                          Isr = List.ofSeq p.Isr
                          LeaderEpoch = p.LeaderEpoch })
                    |> Seq.sortBy (fun p -> p.Partition)
                    |> List.ofSeq

                kv.Key, partitions)
            |> Map.ofSeq }

    let apiVersions (versions: Collections.Generic.IReadOnlyList<Brahmaputra.ApiVersionRange>, brokerVersion: string) =
        { Ranges =
            versions
            |> Seq.map (fun v ->
                { ApiKey = v.ApiKey
                  MinVersion = v.MinVersion
                  MaxVersion = v.MaxVersion })
            |> List.ofSeq
          BrokerVersion = brokerVersion }
