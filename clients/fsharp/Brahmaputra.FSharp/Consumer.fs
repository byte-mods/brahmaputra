namespace Brahmaputra.FSharp

open System

/// Reads partitions directly, with no group coordination.
[<Sealed>]
type Consumer internal (inner: Brahmaputra.Consumer, config: ConsumerConfig) =
    /// The driver object underneath.
    member _.Underlying = inner

    /// The configuration it was created with.
    member _.Config = config

    interface IDisposable with
        member _.Dispose() = inner.Dispose()

/// Partition-consumer operations, in `op` / `opAsync` / `opTask` shapes.
[<RequireQualifiedAccess>]
module Consumer =
    /// Connects to the cluster. Throws if no bootstrap server is reachable;
    /// use `tryCreate` for a `Result`.
    let create (config: ConsumerConfig) : Consumer =
        new Consumer(new Brahmaputra.Consumer(Convert.consumerConfig config), config)

    /// `create`, returning a `Result`.
    let tryCreate (config: ConsumerConfig) : Result<Consumer, BrahmaputraError> =
        Attempt.run (fun () -> create config)

    /// A topic's partition ids in ascending order (auto-creates the topic).
    let partitions (topic: string) (consumer: Consumer) : Result<int list, BrahmaputraError> =
        Attempt.run (fun () -> consumer.Underlying.Partitions(topic) |> List.ofSeq)

    /// `partitions` as an Async.
    let partitionsAsync (topic: string) (consumer: Consumer) : Async<Result<int list, BrahmaputraError>> =
        async {
            let! result = Attempt.runAsync (fun token -> consumer.Underlying.PartitionsAsync(topic, token))
            return result |> Result.map List.ofSeq
        }

    /// Resolves earliest, latest, or a timestamp to an offset.
    let listOffsets (topic: string) (partition: int) (spec: OffsetSpec) (consumer: Consumer) =
        Attempt.run (fun () -> consumer.Underlying.ListOffsets(topic, partition, Convert.offsetSpec spec))

    /// `listOffsets` as an Async.
    let listOffsetsAsync (topic: string) (partition: int) (spec: OffsetSpec) (consumer: Consumer) =
        Attempt.runAsync (fun token ->
            consumer.Underlying.ListOffsetsAsync(topic, partition, Convert.offsetSpec spec, token))

    /// `listOffsets` as a Task.
    let listOffsetsTask (topic: string) (partition: int) (spec: OffsetSpec) (consumer: Consumer) =
        Attempt.runTask (fun () -> consumer.Underlying.ListOffsetsAsync(topic, partition, Convert.offsetSpec spec))

    /// Reads one partition from an offset, long-polling up to `maxWaitMs`
    /// (capped at `fetch.max.wait.ms`) when there is nothing yet.
    let fetchVerbose (topic: string) (partition: int) (offset: int64) (maxWaitMs: int) (consumer: Consumer) =
        Attempt.run (fun () ->
            let result = consumer.Underlying.FetchVerbose(topic, partition, offset, maxWaitMs)

            { Records = Convert.consumerRecords result.Records
              HighWatermark = result.HighWatermark })

    /// `fetchVerbose` as an Async.
    let fetchVerboseAsync (topic: string) (partition: int) (offset: int64) (maxWaitMs: int) (consumer: Consumer) =
        async {
            let! result =
                Attempt.runAsync (fun token ->
                    consumer.Underlying.FetchVerboseAsync(topic, partition, offset, maxWaitMs, token))

            return
                result
                |> Result.map (fun r ->
                    { Records = Convert.consumerRecords r.Records
                      HighWatermark = r.HighWatermark })
        }

    /// `fetchVerbose` as a Task.
    let fetchVerboseTask (topic: string) (partition: int) (offset: int64) (maxWaitMs: int) (consumer: Consumer) =
        task {
            let! result =
                Attempt.runTask (fun () -> consumer.Underlying.FetchVerboseAsync(topic, partition, offset, maxWaitMs))

            return
                result
                |> Result.map (fun r ->
                    { Records = Convert.consumerRecords r.Records
                      HighWatermark = r.HighWatermark })
        }

    /// Reads one partition from an offset; the records only.
    let fetch (topic: string) (partition: int) (offset: int64) (maxWaitMs: int) (consumer: Consumer) =
        fetchVerbose topic partition offset maxWaitMs consumer
        |> Result.map (fun r -> r.Records)

    /// `fetch` as an Async.
    let fetchAsync (topic: string) (partition: int) (offset: int64) (maxWaitMs: int) (consumer: Consumer) =
        async {
            let! result = fetchVerboseAsync topic partition offset maxWaitMs consumer
            return result |> Result.map (fun r -> r.Records)
        }

    /// `fetch` as a Task.
    let fetchTask (topic: string) (partition: int) (offset: int64) (maxWaitMs: int) (consumer: Consumer) =
        task {
            let! result = fetchVerboseTask topic partition offset maxWaitMs consumer
            return result |> Result.map (fun r -> r.Records)
        }

    /// Asks the bootstrap broker which API versions it speaks.
    let apiVersions (consumer: Consumer) : Result<ApiVersions, BrahmaputraError> =
        Attempt.run (fun () -> consumer.Underlying.Router.Seed().ApiVersions().ToTuple() |> Convert.apiVersions)

    /// Fresh cluster metadata for every topic.
    let metadata (consumer: Consumer) : Result<ClusterMetadata, BrahmaputraError> =
        Attempt.run (fun () -> consumer.Underlying.Router.Metadata(null, true) |> Convert.metadata)

    /// `metadata` as an Async.
    let metadataAsync (consumer: Consumer) : Async<Result<ClusterMetadata, BrahmaputraError>> =
        async {
            let! result = Attempt.runAsync (fun token -> consumer.Underlying.Router.MetadataAsync(null, true, token))
            return result |> Result.map Convert.metadata
        }

    /// Closes connections.
    let close (consumer: Consumer) = (consumer :> IDisposable).Dispose()
