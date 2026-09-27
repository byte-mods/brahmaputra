namespace Brahmaputra.FSharp

open System
open System.Threading.Tasks

/// A consumer-group member. Not thread-safe (like Kafka's consumer): poll and
/// commit from one thread or one async flow. Heartbeats run in the background.
/// Dispose commits (when auto commit is on) and sends LeaveGroup.
[<Sealed>]
type GroupConsumer internal (inner: Brahmaputra.GroupConsumer, config: GroupConsumerConfig) =
    /// The driver object underneath.
    member _.Underlying = inner

    /// The configuration it was created with.
    member _.Config = config

    interface IDisposable with
        member _.Dispose() = inner.Dispose()

    interface IAsyncDisposable with
        member _.DisposeAsync() = inner.DisposeAsync()

/// Consumer-group operations, in `op` / `opAsync` / `opTask` shapes.
[<RequireQualifiedAccess>]
module GroupConsumer =
    /// Connects and starts heartbeating. Throws if no bootstrap server is
    /// reachable or `GroupId` is empty; use `tryCreate` for a `Result`.
    let create (config: GroupConsumerConfig) : GroupConsumer =
        new GroupConsumer(new Brahmaputra.GroupConsumer(Convert.groupConfig config), config)

    /// `create`, returning a `Result`.
    let tryCreate (config: GroupConsumerConfig) : Result<GroupConsumer, BrahmaputraError> =
        Attempt.run (fun () -> create config)

    /// Sets the topics to share; takes effect on the next poll.
    let subscribe (topics: string list) (consumer: GroupConsumer) : unit = consumer.Underlying.Subscribe(topics)

    /// Returns up to `max.poll.records` records, joining or rejoining the
    /// group first if needed; an empty list when nothing arrives in time.
    let poll (timeout: TimeSpan) (consumer: GroupConsumer) : Result<ConsumerRecord list, BrahmaputraError> =
        Attempt.run (fun () -> consumer.Underlying.Poll(timeout) |> Convert.consumerRecords)

    /// `poll` as an Async.
    let pollAsync (timeout: TimeSpan) (consumer: GroupConsumer) : Async<Result<ConsumerRecord list, BrahmaputraError>> =
        async {
            let! result = Attempt.runAsync (fun token -> consumer.Underlying.PollAsync(timeout, token))
            return result |> Result.map Convert.consumerRecords
        }

    /// `poll` as a Task.
    let pollTask (timeout: TimeSpan) (consumer: GroupConsumer) : Task<Result<ConsumerRecord list, BrahmaputraError>> =
        task {
            let! result = Attempt.runTask (fun () -> consumer.Underlying.PollAsync(timeout))
            return result |> Result.map Convert.consumerRecords
        }

    /// Commits the positions of everything delivered so far. At-least-once:
    /// call it after processing. A fenced (stale-generation) member gets
    /// `BrahmaputraError.Server` and rejoins on the next poll.
    let commit (consumer: GroupConsumer) : Result<unit, BrahmaputraError> =
        Attempt.run (fun () -> consumer.Underlying.Commit())

    /// `commit` as an Async.
    let commitAsync (consumer: GroupConsumer) : Async<Result<unit, BrahmaputraError>> =
        Attempt.runAsyncUnit (fun token -> consumer.Underlying.CommitAsync(token))

    /// `commit` as a Task.
    let commitTask (consumer: GroupConsumer) : Task<Result<unit, BrahmaputraError>> =
        Attempt.runTaskUnit (fun () -> consumer.Underlying.CommitAsync())

    let private toMap (committed: Collections.Generic.Dictionary<Brahmaputra.TopicPartition, int64>) =
        committed
        |> Seq.map (fun kv -> Convert.topicPartition kv.Key, kv.Value)
        |> Map.ofSeq

    /// The group's committed offsets for the given partitions.
    let committedFor (partitions: TopicPartition list) (consumer: GroupConsumer) =
        Attempt.run (fun () ->
            let wanted = partitions |> List.map Convert.toCsTopicPartition |> ResizeArray
            consumer.Underlying.Committed(wanted) |> toMap)

    /// The group's committed offsets for every partition it holds.
    let committed (consumer: GroupConsumer) : Result<Map<TopicPartition, int64>, BrahmaputraError> =
        committedFor [] consumer

    /// `committed` as an Async.
    let committedAsync (consumer: GroupConsumer) : Async<Result<Map<TopicPartition, int64>, BrahmaputraError>> =
        async {
            let! result = Attempt.runAsync (fun token -> consumer.Underlying.CommittedAsync(null, token))
            return result |> Result.map toMap
        }

    /// The partitions currently assigned to this member.
    let assignment (consumer: GroupConsumer) : TopicPartition list =
        consumer.Underlying.Assignment |> Seq.map Convert.topicPartition |> List.ofSeq

    /// This member's id; `None` before the first join.
    let memberId (consumer: GroupConsumer) : string option =
        match consumer.Underlying.MemberId with
        | "" -> None
        | id -> Some id

    /// The generation this member last joined.
    let generation (consumer: GroupConsumer) : int = consumer.Underlying.Generation

    /// Commits (when auto commit is on), leaves the group and closes. Best
    /// effort: never fails.
    let close (consumer: GroupConsumer) : unit = (consumer :> IDisposable).Dispose()

    /// `close` as an Async.
    let closeAsync (consumer: GroupConsumer) : Async<unit> =
        async { do! consumer.Underlying.DisposeAsync().AsTask() |> Async.AwaitTask }
