namespace Brahmaputra.FSharp

open System
open System.Threading.Tasks

/// A batching producer. Thread-safe: share one. Dispose (or `Producer.close`)
/// flushes before releasing connections.
[<Sealed>]
type Producer internal (inner: Brahmaputra.Producer, config: ProducerConfig) =
    /// The driver object underneath.
    member _.Underlying = inner

    /// The configuration it was created with.
    member _.Config = config

    interface IDisposable with
        member _.Dispose() = inner.Dispose()

    interface IAsyncDisposable with
        member _.DisposeAsync() = inner.DisposeAsync()

/// The pending acknowledgement of one buffered record.
[<Sealed>]
type Delivery internal (task: Task<Brahmaputra.RecordMetadata>) =
    /// The driver's delivery task.
    member _.Underlying = task

    /// True once the record was acknowledged or failed.
    member _.IsCompleted = task.IsCompleted

/// Waits on deliveries.
[<RequireQualifiedAccess>]
module Delivery =
    /// Blocks until the record is acknowledged.
    let wait (delivery: Delivery) : Result<RecordMetadata, BrahmaputraError> =
        Attempt.run (fun () -> delivery.Underlying.GetAwaiter().GetResult() |> Convert.recordMetadata)

    /// Waits as a Task.
    let waitTask (delivery: Delivery) : Task<Result<RecordMetadata, BrahmaputraError>> =
        task {
            let! result = Attempt.runTask (fun () -> delivery.Underlying)
            return result |> Result.map Convert.recordMetadata
        }

    /// Waits as an Async; cancelling the Async stops the wait, not the delivery.
    let waitAsync (delivery: Delivery) : Async<Result<RecordMetadata, BrahmaputraError>> =
        async {
            let! result = Attempt.runAsync (fun token -> delivery.Underlying.WaitAsync(token))
            return result |> Result.map Convert.recordMetadata
        }

    /// The outcome if it is already known.
    let tryResult (delivery: Delivery) : Result<RecordMetadata, BrahmaputraError> option =
        if delivery.IsCompleted then Some(wait delivery) else None

    /// Waits for every delivery; the first failure, in list order, wins.
    let waitAll (deliveries: Delivery list) : Result<RecordMetadata list, BrahmaputraError> =
        let rec loop acc =
            function
            | [] -> Ok(List.rev acc)
            | d :: rest ->
                match wait d with
                | Ok m -> loop (m :: acc) rest
                | Error e -> Error e

        loop [] deliveries

/// Producer operations. Every call comes in three shapes: `op` (blocking,
/// returns `Result`), `opAsync` (`Async<Result<_>>`, cancelled through the
/// Async's token) and `opTask` (`Task<Result<_>>`). None of them throw for a
/// broker or network failure.
[<RequireQualifiedAccess>]
module Producer =
    /// Connects and starts the background sender. Throws if no bootstrap
    /// server is reachable; use `tryCreate` for a `Result`.
    let create (config: ProducerConfig) : Producer =
        new Producer(new Brahmaputra.Producer(Convert.producerConfig config), config)

    /// `create`, returning a `Result`.
    let tryCreate (config: ProducerConfig) : Result<Producer, BrahmaputraError> =
        Attempt.run (fun () -> create config)

    /// Buffers a record and returns its pending delivery. Blocks only while
    /// the buffer is full, for at most `max.block.ms`, then returns
    /// `BrahmaputraError.BufferFull`. A failed delivery nobody waits on is
    /// reported by the next `flush` or `close`.
    let send (record: ProducerRecord) (producer: Producer) : Result<Delivery, BrahmaputraError> =
        Attempt.run (fun () -> Delivery(producer.Underlying.Send(Convert.toCsRecord record)))

    /// `send`, waiting asynchronously for buffer space.
    let sendAsync (record: ProducerRecord) (producer: Producer) : Async<Result<Delivery, BrahmaputraError>> =
        async {
            let! result =
                Attempt.runAsync (fun token -> producer.Underlying.SendAsync(Convert.toCsRecord record, token))

            return result |> Result.map Delivery
        }

    /// `send` as a Task.
    let sendTask (record: ProducerRecord) (producer: Producer) : Task<Result<Delivery, BrahmaputraError>> =
        task {
            let! result = Attempt.runTask (fun () -> producer.Underlying.SendAsync(Convert.toCsRecord record))
            return result |> Result.map Delivery
        }

    /// Sends one record and waits for its acknowledgement: one round trip per record.
    let produce (record: ProducerRecord) (producer: Producer) : Result<RecordMetadata, BrahmaputraError> =
        Attempt.run (fun () -> producer.Underlying.Produce(Convert.toCsRecord record) |> Convert.recordMetadata)

    /// `produce` as an Async.
    let produceAsync (record: ProducerRecord) (producer: Producer) : Async<Result<RecordMetadata, BrahmaputraError>> =
        async {
            let! result =
                Attempt.runAsync (fun token -> producer.Underlying.ProduceAsync(Convert.toCsRecord record, token))

            return result |> Result.map Convert.recordMetadata
        }

    /// `produce` as a Task.
    let produceTask (record: ProducerRecord) (producer: Producer) : Task<Result<RecordMetadata, BrahmaputraError>> =
        task {
            let! result = Attempt.runTask (fun () -> producer.Underlying.ProduceAsync(Convert.toCsRecord record))
            return result |> Result.map Convert.recordMetadata
        }

    /// Sends everything buffered and waits for it. Returns the first delivery
    /// failure, including one from an earlier background (linger) send.
    let flush (producer: Producer) : Result<unit, BrahmaputraError> =
        Attempt.run (fun () -> producer.Underlying.Flush())

    /// `flush` as an Async.
    let flushAsync (producer: Producer) : Async<Result<unit, BrahmaputraError>> =
        Attempt.runAsyncUnit (fun token -> producer.Underlying.FlushAsync(token))

    /// `flush` as a Task.
    let flushTask (producer: Producer) : Task<Result<unit, BrahmaputraError>> =
        Attempt.runTaskUnit (fun () -> producer.Underlying.FlushAsync())

    /// Flushes, stops the sender and closes connections, then reports any
    /// failure not yet reported. Resources are released either way.
    let close (producer: Producer) : Result<unit, BrahmaputraError> =
        Attempt.run (fun () -> producer.Underlying.Close())

    /// `close` as an Async.
    let closeAsync (producer: Producer) : Async<Result<unit, BrahmaputraError>> =
        Attempt.runAsyncUnit (fun _ -> producer.Underlying.CloseAsync())

    /// `close` as a Task.
    let closeTask (producer: Producer) : Task<Result<unit, BrahmaputraError>> =
        Attempt.runTaskUnit (fun () -> producer.Underlying.CloseAsync())

    /// A topic's partition ids in ascending order (auto-creates the topic).
    let partitions (topic: string) (producer: Producer) : Result<int list, BrahmaputraError> =
        Attempt.run (fun () -> producer.Underlying.Router.Partitions(topic) |> List.ofSeq)

    /// `partitions` as an Async.
    let partitionsAsync (topic: string) (producer: Producer) : Async<Result<int list, BrahmaputraError>> =
        async {
            let! result = Attempt.runAsync (fun token -> producer.Underlying.Router.PartitionsAsync(topic, token))
            return result |> Result.map List.ofSeq
        }

    /// Fresh cluster metadata for every topic.
    let metadata (producer: Producer) : Result<ClusterMetadata, BrahmaputraError> =
        Attempt.run (fun () -> producer.Underlying.Router.Metadata(null, true) |> Convert.metadata)
