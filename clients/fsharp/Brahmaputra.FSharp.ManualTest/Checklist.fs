// Checks beyond the Go suite: every item of the client feature checklist that
// Program.fs's sections do not already exercise, each through the F# API
// (batch.size, linger.ms, partitioners, timestamps, send-and-wait, codec
// registration, retries and request/delivery timeouts through a
// fault-injecting proxy, bounds-checked decoding, fetch limits, the high
// watermark, offsets by timestamp, metadata, multi-topic groups, auto-commit,
// heartbeats and eviction, static membership, LeaveGroup and rebalances).
module Brahmaputra.FSharp.ManualTest.Checklist

open System
open System.Buffers.Binary
open System.Diagnostics
open System.IO
open System.Net
open System.Net.Sockets
open System.Text
open System.Threading
open Brahmaputra.FSharp

/// lz4 in the broker's format (little-endian uncompressed length, then a raw
/// LZ4 block). It compresses by emitting one literal run (valid LZ4 any
/// decoder reads) and decodes full LZ4, matches included, so it reads what
/// the broker's lz4 writes too.
module Lz4 =
    let literals (payload: byte[]) : byte[] =
        let size = payload.Length
        use output = new MemoryStream(size + size / 255 + 16)
        let prefix = Array.zeroCreate<byte> 4
        BinaryPrimitives.WriteInt32LittleEndian(Span<byte>(prefix), size)
        output.Write(prefix, 0, 4)
        output.WriteByte(byte ((min size 15) <<< 4))

        if size >= 15 then
            let mutable rest = size - 15

            while rest >= 255 do
                output.WriteByte 255uy
                rest <- rest - 255

            output.WriteByte(byte rest)

        output.Write(payload, 0, size)
        output.ToArray()

    let decode (data: byte[]) : byte[] =
        try
            let size = BinaryPrimitives.ReadInt32LittleEndian(ReadOnlySpan<byte>(data))

            if size < 0 || size > 256 * 1024 * 1024 then
                raise (Brahmaputra.BrahmaputraException(sprintf "lz4 size %d" size))

            let output = Array.zeroCreate<byte> size
            let mutable input = 4
            let mutable at = 0

            let length start =
                let mutable total = start

                if start = 15 then
                    let mutable more = 255

                    while more = 255 do
                        more <- int data.[input]
                        input <- input + 1
                        total <- total + more

                total

            let mutable finished = false

            while not finished && input < data.Length do
                let token = int data.[input]
                input <- input + 1
                let literalCount = length (token >>> 4)
                Array.blit data input output at literalCount
                input <- input + literalCount
                at <- at + literalCount

                if input >= data.Length then
                    finished <- true
                else
                    let distance = int data.[input] ||| (int data.[input + 1] <<< 8)
                    input <- input + 2
                    let matched = length (token &&& 15) + 4

                    if distance = 0 || distance > at then
                        raise (Brahmaputra.BrahmaputraException "lz4 match before the output")

                    for _ in 1..matched do
                        output.[at] <- output.[at - distance]
                        at <- at + 1

            if at <> size then
                raise (Brahmaputra.BrahmaputraException(sprintf "lz4 decoded %d of %d" at size))

            output
        with
        | :? IndexOutOfRangeException
        | :? ArgumentException -> raise (Brahmaputra.BrahmaputraException "truncated lz4 block")

/// Sits between a client and the broker, forwarding frames one request at a
/// time, and can answer a produce with a retriable error or a fetch with a
/// corrupt batch. It records the acks and timeout of every produce it sees.
/// Its fake answers are built with the .NET driver's frame codec.
[<Sealed>]
type FaultProxy(target: string) =
    let colon = target.LastIndexOf ':'
    let host = target.Substring(0, colon)
    let port = int (target.Substring(colon + 1))
    let listener = new TcpListener(IPAddress.Loopback, 0)
    let live = ResizeArray<TcpClient>()
    let gate = obj ()
    let mutable failuresLeft = 0
    let mutable produces = 0
    let mutable corruptFetch = 0
    let mutable lastAcks = Int32.MinValue
    let mutable lastTimeoutMs = Int32.MinValue

    let takeFailure () =
        lock gate (fun () ->
            if failuresLeft = 0 then
                false
            else
                if failuresLeft > 0 then
                    failuresLeft <- failuresLeft - 1

                true)

    let readFrame (stream: Stream) =
        let prefix = Array.zeroCreate<byte> 4
        stream.ReadExactly(prefix, 0, 4)
        let frame = Array.zeroCreate<byte> (BinaryPrimitives.ReadInt32BigEndian(ReadOnlySpan<byte>(prefix)))
        stream.ReadExactly(frame, 0, frame.Length)
        frame

    let writeFrame (stream: Stream) (payload: byte[]) =
        let framed = Array.zeroCreate<byte> (4 + payload.Length)
        BinaryPrimitives.WriteInt32BigEndian(Span<byte>(framed), payload.Length)
        Array.blit payload 0 framed 4 payload.Length
        stream.Write(framed, 0, framed.Length)
        stream.Flush()

    let serve (client: TcpClient) (upstream: TcpClient) =
        try
            try
                let down = client.GetStream()
                let up = upstream.GetStream()

                while true do
                    let frame = readFrame down
                    let apiKey: Brahmaputra.ApiKey =
                        LanguagePrimitives.EnumOfValue(BinaryPrimitives.ReadInt16BigEndian(ReadOnlySpan<byte>(frame)))
                    let correlation = BinaryPrimitives.ReadInt32BigEndian(ReadOnlySpan<byte>(frame, 4, 4))
                    let clientLen = int (BinaryPrimitives.ReadInt16BigEndian(ReadOnlySpan<byte>(frame, 8, 2)))
                    let body = frame.[10 + max clientLen 0 ..]
                    let mutable reply: byte[] option = None
                    let mutable oneway = false

                    if apiKey = Brahmaputra.ApiKey.Produce then
                        let r = Brahmaputra.BodyReader(body)
                        let topic = r.String()
                        let partition = r.Int32()
                        let acks = r.Int32()
                        lastTimeoutMs <- r.Int32()
                        lastAcks <- acks
                        lock gate (fun () -> produces <- produces + 1)
                        oneway <- acks = 0

                        if takeFailure () then
                            let w = Brahmaputra.BodyWriter()
                            w.String topic
                            w.Int32 partition
                            w.Int32(int Brahmaputra.ErrorCode.NotEnoughReplicas)
                            w.Int64 -1L
                            w.Int64 -1L
                            reply <- Some(w.ToArray())
                    elif apiKey = Brahmaputra.ApiKey.Fetch && corruptFetch <> 0 then
                        let r = Brahmaputra.BodyReader(body)
                        let topic = r.String()
                        let partition = r.Int32()
                        // A batch whose batch_length is negative (mode 1) or runs
                        // far past the bytes that follow (mode 2).
                        let batch = Array.zeroCreate<byte> 61
                        BinaryPrimitives.WriteInt32BigEndian(Span<byte>(batch, 8, 4), (if corruptFetch = 1 then -1 else 1_000_000))
                        let w = Brahmaputra.BodyWriter()
                        w.String topic
                        w.Int32 partition
                        w.Int32 0
                        w.Int64 1L
                        w.Int64 1L
                        w.Int64(int64 batch.Length)
                        w.Int32 -1
                        w.Raw(ReadOnlySpan<byte>(batch))
                        reply <- Some(w.ToArray())

                    match reply with
                    | Some answer ->
                        let framed = Brahmaputra.Frame.Encode(apiKey, correlation, "", answer)
                        down.Write(framed, 0, framed.Length)
                        down.Flush()
                    | None ->
                        writeFrame up frame

                        if not oneway then
                            writeFrame down (readFrame up)
            with _ ->
                () // either side went away
        finally
            client.Dispose()
            upstream.Dispose()

    do
        listener.Start()

        let accept () =
            let mutable running = true

            while running do
                try
                    let client = listener.AcceptTcpClient()
                    let upstream = new TcpClient()

                    try
                        upstream.Connect(host, port)
                        lock live (fun () ->
                            live.Add client
                            live.Add upstream)

                        Thread((fun () -> serve client upstream), IsBackground = true).Start()
                    with _ ->
                        client.Dispose()
                        upstream.Dispose()
                with _ ->
                    running <- false

        Thread(accept, IsBackground = true).Start()

    member _.Address = sprintf "127.0.0.1:%d" (listener.LocalEndpoint :?> IPEndPoint).Port

    /// Fails the next `count` produces (-1: every one) and resets the counters.
    member _.FailProduces(count: int) =
        lock gate (fun () ->
            failuresLeft <- count
            produces <- 0)

    member _.Produces = lock gate (fun () -> produces)

    member _.LastAcks = lastAcks

    member _.LastTimeoutMs = lastTimeoutMs

    member _.CorruptFetch
        with get () = corruptFetch
        and set (mode: int) = corruptFetch <- mode

    interface IDisposable with
        member _.Dispose() =
            listener.Stop()

            lock live (fun () ->
                for c in live do
                    c.Dispose()

                live.Clear())

let private b (text: string) = Encoding.UTF8.GetBytes text

let private unique (prefix: string) =
    sprintf "%s-%d" prefix (DateTime.UtcNow.Ticks % 1_000_000_000L)

let private nowMs () =
    DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

let private get (result: Result<'T, BrahmaputraError>) : 'T =
    match result with
    | Ok value -> value
    | Error e -> failwithf "setup failed: %s" e.Message

let private errText (result: Result<'T, BrahmaputraError>) =
    match result with
    | Ok _ -> "ok"
    | Error e -> e.Message

let private count (result: Result<ConsumerRecord list, BrahmaputraError>) =
    match result with
    | Ok records -> records.Length
    | Error _ -> -1

let private pollQuietly (member': GroupConsumer) (ms: int) : ConsumerRecord list =
    match GroupConsumer.poll (TimeSpan.FromMilliseconds(float ms)) member' with
    | Ok records -> records
    | Error _ -> []

let private drain (member': GroupConsumer) want (timeoutMs: int64) =
    let clock = Stopwatch.StartNew()
    let mutable got = 0

    while got < want && clock.ElapsedMilliseconds < timeoutMs do
        got <- got + (pollQuietly member' 300).Length

    got

let private awaitAssignment (member': GroupConsumer) (timeoutMs: int64) =
    let clock = Stopwatch.StartNew()

    while List.isEmpty (GroupConsumer.assignment member') && clock.ElapsedMilliseconds < timeoutMs do
        pollQuietly member' 200 |> ignore

let private memberText (member': GroupConsumer) =
    GroupConsumer.memberId member' |> Option.defaultValue ""

/// Runs every checklist section, reporting through the suite's own `check`.
let run (check: string -> bool -> string -> unit) (section: string -> unit) (address: string) =
    let unbatched = { ProducerConfig.create address with LingerMs = 0 }
    let consumerConf = ConsumerConfig.create address

    let groupConf groupId =
        { GroupConsumerConfig.create address groupId with EnableAutoCommit = false }

    let sendTo topic partition (value: byte[]) producer =
        producer
        |> Producer.send (ProducerRecord.create topic value |> ProducerRecord.withPartition partition)
        |> get
        |> ignore

    section "producer: batch.size and linger.ms"

    do
        let batchTopic = unique "fsharp-batchsize"
        let value = Array.create 200 (byte 'b')
        // Only batch.size can send anything during this check.
        use producer =
            Producer.create
                { ProducerConfig.create address with
                    LingerMs = 60_000
                    BatchSize = 1024 }

        use consumer = Consumer.create consumerConf
        Producer.partitions batchTopic producer |> get |> ignore

        for _ in 1..8 do
            sendTo batchTopic 0 value producer

        Thread.Sleep 500
        let early = Consumer.fetch batchTopic 0 0L 0 consumer |> count
        check "a batch that reaches batch.size is sent before linger.ms" (early >= 1 && early < 8) (sprintf "%d of 8 sent before any flush" early)
        Producer.flush producer |> get
        let after = Consumer.fetch batchTopic 0 0L 500 consumer |> count
        check "flush sends the partial batch that is left" (after = 8) (sprintf "got %d" after)

    do
        let lingerTopic = unique "fsharp-linger"
        use producer = Producer.create { ProducerConfig.create address with LingerMs = 500 }
        use consumer = Consumer.create consumerConf
        Producer.partitions lingerTopic producer |> get |> ignore
        sendTo lingerTopic 0 (b "lingering") producer
        let immediate = Consumer.fetch lingerTopic 0 0L 0 consumer |> count
        Thread.Sleep 1500
        let later = Consumer.fetch lingerTopic 0 0L 0 consumer |> count
        check "linger.ms holds a record back, then sends it without a flush" (immediate = 0 && later = 1) (sprintf "immediately %d, after linger %d" immediate later)

    section "producer: partitioners"

    do
        let rrTopic = unique "fsharp-rr"
        let pinTopic = unique "fsharp-pinned"
        use producer = Producer.create unbatched
        let partitions = Producer.partitions rrTopic producer |> get

        for i in 0 .. partitions.Length * 2 - 1 do
            producer |> Producer.send (ProducerRecord.create rrTopic (b (sprintf "rr%d" i))) |> get |> ignore

        Producer.partitions pinTopic producer |> get |> ignore
        sendTo pinTopic (List.last partitions) (b "pinned") producer
        Producer.flush producer |> get
        use consumer = Consumer.create consumerConf
        let counts = partitions |> List.map (fun p -> p, Consumer.fetch rrTopic p 0L 0 consumer |> count)
        let pinned = partitions |> List.map (fun p -> p, Consumer.fetch pinTopic p 0L 0 consumer |> count)
        check "a null key round-robins across every partition" (counts |> List.forall (fun (_, c) -> c = 2)) (sprintf "%A" counts)
        check "an explicit partition is honoured" (snd (List.last pinned) = 1 && List.sumBy snd pinned = 1) (sprintf "%A" pinned)

    section "producer: record timestamps and send-and-wait"

    do
        let timeTopic = unique "fsharp-timestamps"
        let syncTopic = unique "fsharp-sync"
        let baseTime = nowMs () - 60_000L
        let beforeSend = nowMs ()
        use producer = Producer.create unbatched
        // produce waits for each, so every record is a batch of its own.
        for i in 0..2 do
            producer
            |> Producer.produce (
                ProducerRecord.ofString timeTopic (sprintf "t%d" i)
                |> ProducerRecord.withPartition 0
                |> ProducerRecord.withTimestamp (baseTime + int64 i * 1000L)
            )
            |> get
            |> ignore

        producer |> Producer.produce (ProducerRecord.ofString timeTopic "now" |> ProducerRecord.withPartition 0) |> get |> ignore

        let offsets =
            [ for i in 0..1 ->
                  (producer
                   |> Producer.produce (ProducerRecord.ofString syncTopic (sprintf "s%d" i) |> ProducerRecord.withPartition 0)
                   |> get)
                      .Offset ]

        use consumer = Consumer.create consumerConf
        let got = Consumer.fetch timeTopic 0 0L 500 consumer |> get |> Array.ofList
        check "an explicit record timestamp round-trips exactly" (got.Length = 4 && [ 0..2 ] |> List.forall (fun i -> got.[i].Timestamp = baseTime + int64 i * 1000L)) (sprintf "%A" (got |> Array.map (fun r -> r.Timestamp)))
        check "a record without one is stamped with the wall clock" (got.Length = 4 && got.[3].Timestamp >= beforeSend - 1000L && got.[3].Timestamp <= nowMs () + 1000L) ""
        check "send-and-wait returns each record's offset" (offsets = [ Some 0L; Some 1L ]) (sprintf "%A" offsets)
        let atHalf = Consumer.listOffsets timeTopic 0 (OffsetSpec.AtTimestamp(baseTime + 500L)) consumer
        let atLast = Consumer.listOffsets timeTopic 0 (OffsetSpec.AtTimestamp(baseTime + 2000L)) consumer
        check "list offsets by timestamp finds the first record at or after it" (atHalf = Ok 1L && atLast = Ok 2L) (sprintf "%A, %A" atHalf atLast)

    section "producer: codec registration"

    do
        let compressed = ref 0
        let decompressed = ref 0

        Codecs.register
            Compression.Lz4
            (fun payload ->
                Interlocked.Increment(&compressed.contents) |> ignore
                Lz4.literals payload)
            (fun data ->
                Interlocked.Increment(&decompressed.contents) |> ignore
                Lz4.decode data)
        |> get

        let lz4Topic = unique "fsharp-lz4"
        let sent = [ for i in 0..9 -> b (sprintf "lz4 record %d%s" i (String(' ', 40))) ]

        do
            use producer = Producer.create { unbatched with Compression = Compression.Lz4 }

            sent
            |> List.iteri (fun i value ->
                producer
                |> Producer.send (ProducerRecord.create lz4Topic value |> ProducerRecord.withPartition 0 |> ProducerRecord.withStringKey (sprintf "k%d" i))
                |> get
                |> ignore)

            Producer.close producer |> get

        use consumer = Consumer.create consumerConf
        let got = Consumer.fetch lz4Topic 0 0L 500 consumer |> get
        let same = got.Length = sent.Length && List.forall2 (fun (r: ConsumerRecord) (v: byte[]) -> r.Value = Some v) got sent
        check "a registered codec (lz4) compresses sends and decodes fetches" (same && compressed.Value >= 1 && decompressed.Value >= 1) (sprintf "%d records, %d compressed, %d decompressed" got.Length compressed.Value decompressed.Value)

        let refused =
            use producer = Producer.create { unbatched with Compression = Compression.Zstd }
            producer |> Producer.send (ProducerRecord.ofString (unique "fsharp-zstd") "x" |> ProducerRecord.withPartition 0) |> ignore
            Producer.close producer

        check "an unregistered codec is refused, not sent uncompressed" ((errText refused).Contains "not registered") (errText refused)

    section "producer: retries, request.timeout.ms and delivery.timeout.ms"

    do
        use proxy = new FaultProxy(address)
        let retryTopic = unique "fsharp-retry"

        let settings =
            { ProducerConfig.create proxy.Address with
                LingerMs = 0
                Acks = Acks.All
                RequestTimeoutMs = 1234
                Retries = 3
                RetryBackoffMs = 150 }

        let sendThrough config value =
            use producer = Producer.create config
            producer |> Producer.produce (ProducerRecord.ofString retryTopic value |> ProducerRecord.withPartition 0)

        proxy.FailProduces 2
        let clock = Stopwatch.StartNew()
        let retried = sendThrough settings "retried"
        let elapsed = clock.ElapsedMilliseconds
        check "request.timeout.ms and acks travel with every produce" (proxy.LastTimeoutMs = 1234 && proxy.LastAcks = -1) (sprintf "timeout=%d acks=%d" proxy.LastTimeoutMs proxy.LastAcks)
        check "a retriable error is retried after retry.backoff.ms" (Result.isOk retried && proxy.Produces = 3 && elapsed >= 300L) (sprintf "attempts=%d elapsed=%d result=%s" proxy.Produces elapsed (errText retried))

        do
            use consumer = Consumer.create consumerConf
            let stored = Consumer.fetch retryTopic 0 0L 500 consumer |> count
            check "the retried record is stored exactly once" (stored = 1) (sprintf "stored %d" stored)

        proxy.FailProduces -1
        let bounded = sendThrough { settings with Retries = 2 } "never"
        check "retries bounds the attempts: the error surfaces after retries + 1" (Result.isError bounded && proxy.Produces = 3) (sprintf "attempts=%d result=%s" proxy.Produces (errText bounded))

        proxy.FailProduces -1
        clock.Restart()

        let late =
            sendThrough
                { settings with
                    Retries = 1_000_000
                    RetryBackoffMs = 50
                    DeliveryTimeoutMs = 500 }
                "late"

        let lateElapsed = clock.ElapsedMilliseconds
        check "delivery.timeout.ms bounds the time spent retrying" (Result.isError late && lateElapsed >= 450L && lateElapsed < 3000L) (sprintf "elapsed=%d attempts=%d result=%s" lateElapsed proxy.Produces (errText late))

        proxy.FailProduces 0

        for mode in 1..2 do
            proxy.CorruptFetch <- mode

            let fetched =
                use consumer = Consumer.create (ConsumerConfig.create proxy.Address)
                Consumer.fetch retryTopic 0 0L 100 consumer

            let rejected =
                match fetched with
                | Error(BrahmaputraError.Client _) -> true
                | _ -> false

            check (if mode = 1 then "a negative length on the wire is an error" else "a length past the end of the data is an error") rejected (errText fetched)

        proxy.CorruptFetch <- 0

    section "consumer: fetch limits, high watermark and metadata"

    do
        let fetchTopic = unique "fsharp-fetch"
        let value = Array.create 1000 (byte 'f')

        do
            use producer = Producer.create unbatched

            for _ in 1..10 do
                producer |> Producer.produce (ProducerRecord.create fetchTopic value |> ProducerRecord.withPartition 0) |> get |> ignore

        do
            use consumer = Consumer.create { consumerConf with FetchMaxBytes = 2500 }
            let got = Consumer.fetch fetchTopic 0 0L 500 consumer |> count
            check "fetch.max.bytes caps what one fetch returns" (got >= 1 && got < 10) (sprintf "got %d of 10" got)

        do
            use consumer = Consumer.create { consumerConf with MaxPollRecords = 4 }
            let got = Consumer.fetch fetchTopic 0 0L 500 consumer |> count
            check "max.poll.records caps one fetch" (got = 4) (sprintf "got %d" got)
            let next = Consumer.fetch fetchTopic 0 4L 500 consumer |> get
            check "the records a cap held back come on the next fetch" (next.Length = 4 && next.Head.Offset = 4L) (sprintf "got %d" next.Length)

        use waiting =
            Consumer.create
                { consumerConf with
                    FetchMinBytes = 1_000_000
                    FetchMaxWaitMs = 600 }

        use eager = Consumer.create consumerConf
        let clock = Stopwatch.StartNew()
        let waitedFor = Consumer.fetch fetchTopic 0 0L 600 waiting |> count
        let waited = clock.ElapsedMilliseconds
        clock.Restart()
        let eagerGot = Consumer.fetch fetchTopic 0 0L 600 eager |> count
        let quick = clock.ElapsedMilliseconds
        check "fetch.min.bytes holds a fetch open until fetch.max.wait.ms" (waited >= 450L && quick < 400L && waitedFor = 10 && eagerGot = 10) (sprintf "waited %dms, eager %dms" waited quick)

        let result = Consumer.fetchVerbose fetchTopic 0 0L 500 eager |> get
        check "the high watermark is reported" (result.HighWatermark = 10L) (string result.HighWatermark)

        let metadata = Consumer.metadata eager |> get
        let brokerIds = metadata.Brokers |> List.map (fun broker -> broker.NodeId) |> Set.ofList
        let partitions = metadata.Topics |> Map.tryFind fetchTopic |> Option.defaultValue []
        check "metadata names a live leader for every partition" (not partitions.IsEmpty && partitions |> List.forall (fun p -> brokerIds.Contains p.Leader)) (sprintf "%d partitions" partitions.Length)

    section "consumer group: several topics, auto-commit and max.poll.records"

    do
        let topicA = unique "fsharp-multi-a"
        let topicB = unique "fsharp-multi-b"

        do
            use producer = Producer.create unbatched

            for i in 0..5 do
                producer |> Producer.send (ProducerRecord.ofString topicA (sprintf "a%d" i)) |> get |> ignore
                producer |> Producer.send (ProducerRecord.ofString topicB (sprintf "b%d" i)) |> get |> ignore

            Producer.flush producer |> get

        let member' =
            GroupConsumer.create
                { groupConf (unique "fsharp-multi") with
                    EnableAutoCommit = true
                    AutoCommitIntervalMs = 200
                    MaxPollRecords = 5 }

        GroupConsumer.subscribe [ topicA; topicB ] member'
        let mutable seen: ConsumerRecord list = []
        let mutable largest = 0
        let clock = Stopwatch.StartNew()

        while seen.Length < 12 && clock.ElapsedMilliseconds < 20_000L do
            let batch = pollQuietly member' 500
            largest <- max largest batch.Length
            seen <- seen @ batch

        let topics = seen |> List.map (fun r -> r.Topic) |> Set.ofList
        check "one member consumes every subscribed topic" (seen.Length = 12 && topics.Count = 2) (sprintf "%d records from %d topics" seen.Length topics.Count)
        check "max.poll.records caps each poll" (largest >= 1 && largest <= 5) (sprintf "largest poll %d" largest)
        // Nothing calls commit: these polls are what auto-commit rides on.
        clock.Restart()

        while clock.ElapsedMilliseconds < 1000L do
            pollQuietly member' 100 |> ignore

        let total = GroupConsumer.committed member' |> get |> Map.toList |> List.sumBy snd
        check "auto.commit.interval.ms commits delivered positions without commit" (total = 12L) (sprintf "committed %d" total)
        GroupConsumer.close member'

    section "consumer group: heartbeats, session timeout and rejoin"

    do
        let hbTopic = unique "fsharp-heartbeat"

        do
            use producer = Producer.create unbatched

            for i in 0..3 do
                producer |> Producer.send (ProducerRecord.ofString hbTopic (sprintf "h%d" i)) |> get |> ignore

            Producer.flush producer |> get

        let steady =
            GroupConsumer.create
                { groupConf (unique "fsharp-hb") with
                    SessionTimeoutMs = 1500
                    HeartbeatIntervalMs = 300 }

        GroupConsumer.subscribe [ hbTopic ] steady
        let got = drain steady 4 15_000L
        let memberBefore = memberText steady
        Thread.Sleep 3500 // over twice the session timeout, with no poll
        let committed = GroupConsumer.commit steady
        check "heartbeats keep an idle member in its group past session.timeout.ms" (got = 4 && Result.isOk committed && memberText steady = memberBefore) (sprintf "got=%d commit=%s" got (errText committed))
        GroupConsumer.close steady

        let quiet =
            GroupConsumer.create
                { groupConf (unique "fsharp-evicted") with
                    SessionTimeoutMs = 1000
                    HeartbeatIntervalMs = 20_000 } // effectively never, within this check

        GroupConsumer.subscribe [ hbTopic ] quiet
        let quietGot = drain quiet 4 15_000L
        let evicted = memberText quiet
        Thread.Sleep 2500

        let fenced =
            match GroupConsumer.commit quiet with
            | Error(BrahmaputraError.Server s) -> s.Error = ErrorCode.UnknownMemberId
            | _ -> false

        check "a member that stops heartbeating is evicted after session.timeout.ms" (quietGot = 4 && fenced) (sprintf "got=%d" quietGot)
        // Only the join is checked: with no heartbeats this member is evicted
        // again one session timeout after it rejoins.
        let rejoined = GroupConsumer.poll (TimeSpan.FromSeconds 1.0) quiet
        check "an evicted member rejoins as a new member" (Result.isOk rejoined && memberText quiet <> "" && memberText quiet <> evicted) (sprintf "%s -> %s poll=%s" evicted (memberText quiet) (errText rejoined))
        GroupConsumer.close quiet

    section "consumer group: static membership, LeaveGroup and rebalances"

    do
        let staticTopic = unique "fsharp-static"

        let partitions =
            use producer = Producer.create unbatched
            let partitions = Producer.partitions staticTopic producer |> get

            for i in 0..3 do
                producer |> Producer.send (ProducerRecord.ofString staticTopic (sprintf "st%d" i)) |> get |> ignore

            Producer.flush producer |> get
            partitions

        let fixedConf =
            { groupConf (unique "fsharp-static-grp") with
                HeartbeatIntervalMs = 300
                GroupInstanceId = Some(unique "fsharp-instance") }

        let first = GroupConsumer.create fixedConf
        GroupConsumer.subscribe [ staticTopic ] first
        awaitAssignment first 15_000L
        let firstMember = memberText first
        let firstGeneration = GroupConsumer.generation first
        let returning = GroupConsumer.create fixedConf
        GroupConsumer.subscribe [ staticTopic ] returning
        awaitAssignment returning 15_000L
        check "a returning group.instance.id reclaims its member id without a rebalance" (firstMember <> "" && memberText returning = firstMember && GroupConsumer.generation returning = firstGeneration) (sprintf "%s/%d -> %s/%d" firstMember firstGeneration (memberText returning) (GroupConsumer.generation returning))
        GroupConsumer.close returning
        GroupConsumer.close first

        // LeaveGroup: with a 30 s session and a 10 s rebalance timeout, a
        // successor could only get the partitions quickly if the first member
        // told the coordinator it left.
        let leaving =
            { groupConf (unique "fsharp-leave-grp") with
                SessionTimeoutMs = 30_000
                RebalanceTimeoutMs = 10_000 }

        let departing = GroupConsumer.create leaving
        GroupConsumer.subscribe [ staticTopic ] departing
        awaitAssignment departing 15_000L
        GroupConsumer.close departing
        let clock = Stopwatch.StartNew()
        let successor = GroupConsumer.create leaving
        GroupConsumer.subscribe [ staticTopic ] successor
        awaitAssignment successor 15_000L
        let took = clock.ElapsedMilliseconds
        let held = (GroupConsumer.assignment successor).Length
        check "close sends LeaveGroup, so a successor is not kept waiting" (held = partitions.Length && took < 6000L) (sprintf "%d partitions after %dms" held took)
        GroupConsumer.close successor

        // Two members: the second's join makes the coordinator fence the first's
        // generation; its heartbeat learns that, it rejoins, and the partitions split.
        let sharing = { groupConf (unique "fsharp-share-grp") with HeartbeatIntervalMs = 200 }
        let one = GroupConsumer.create sharing
        GroupConsumer.subscribe [ staticTopic ] one
        awaitAssignment one 15_000L
        let before = GroupConsumer.generation one
        let two = GroupConsumer.create sharing
        GroupConsumer.subscribe [ staticTopic ] two
        use stop = new ManualResetEventSlim()

        let other =
            Thread(
                (fun () ->
                    while not stop.IsSet do
                        pollQuietly two 200 |> ignore),
                IsBackground = true
            )

        other.Start()
        let mutable split = false
        clock.Restart()

        while not split && clock.ElapsedMilliseconds < 20_000L do
            pollQuietly one 200 |> ignore
            let mine = GroupConsumer.assignment one
            let theirs = GroupConsumer.assignment two

            split <-
                not mine.IsEmpty
                && not theirs.IsEmpty
                && (Set.ofList (mine @ theirs)).Count = partitions.Length
                && mine.Length + theirs.Length = partitions.Length

        stop.Set()
        other.Join(10_000) |> ignore
        check "a second member rebalances the group and the partitions split between them" split (sprintf "%A / %A" (GroupConsumer.assignment one) (GroupConsumer.assignment two))
        check "the generation advances when the group rebalances" (GroupConsumer.generation one > before) (sprintf "%d -> %d" before (GroupConsumer.generation one))
        GroupConsumer.close two
        GroupConsumer.close one
