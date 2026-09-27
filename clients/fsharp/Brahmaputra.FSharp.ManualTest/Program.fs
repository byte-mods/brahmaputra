// End-to-end suite for the F# API against a live broker.
//
//   brahmaputra-server --data-dir ./data --default-partitions 4
//   dotnet run --project Brahmaputra.FSharp.ManualTest -- 127.0.0.1 9092
//
// A port of the Go suite (clients/go/cmd/manualtest), check for check, driven
// entirely through the F# API. Every check asserts a property of the system,
// not that a function ran. A setup failure aborts with exit code 2; any failed
// check makes the run exit 1.
module Brahmaputra.FSharp.ManualTest.Program

open System
open System.Diagnostics
open System.Text
open System.Threading
open System.Threading.Tasks
open Brahmaputra.FSharp
open Brahmaputra.FSharp.ManualTest.Proxy

let mutable private passed = 0
let mutable private failed = 0

let private check (name: string) (ok: bool) (detail: string) =
    if ok then
        passed <- passed + 1
        printfn "  ok   %s" name
    else
        failed <- failed + 1

        if detail.Length > 0 then
            printfn "  FAIL %s: %s" name detail
        else
            printfn "  FAIL %s" name

let private section (title: string) = printfn "\n%s" title

let private unique (prefix: string) =
    sprintf "%s-%d" prefix (DateTime.UtcNow.Ticks % 1_000_000_000L)

let private b (text: string) = Encoding.UTF8.GetBytes text

let private str (bytes: byte[] option) =
    match bytes with
    | None -> "<null>"
    | Some x -> Encoding.UTF8.GetString x

let private nowMs () =
    DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

/// A setup step that must succeed; its failure aborts the run (exit 2).
exception SetupFailed of string

let private orFail (result: Result<'T, BrahmaputraError>) : 'T =
    match result with
    | Ok value -> value
    | Error e -> raise (SetupFailed e.Message)

let private errText (result: Result<'T, BrahmaputraError>) =
    match result with
    | Ok _ -> "ok"
    | Error e -> e.Message

let private producerConf address =
    { ProducerConfig.create address with LingerMs = 0 }

let private consumerConf address = ConsumerConfig.create address

let private groupConf address groupId =
    { GroupConsumerConfig.create address groupId with EnableAutoCommit = false }

/// Buffers a record on an explicit partition; the delivery is reported by flush.
let private sendTo topic partition (value: byte[] option) (key: byte[] option) headers producer =
    let record =
        { ProducerRecord.create topic [||] with
            Partition = Some partition
            Value = value
            Key = key
            Headers = headers }

    producer |> Producer.send record |> orFail |> ignore

let private sendKeyless topic (value: byte[]) producer =
    producer |> Producer.send (ProducerRecord.create topic value) |> orFail |> ignore

/// Reads a partition from offset 0 until an empty fetch or `want` records.
let private fetchAll consumer topic partition want =
    let rec loop offset acc count =
        if count >= want then
            acc
        else
            match Consumer.fetch topic partition offset 500 consumer |> orFail with
            | [] -> acc
            | batch ->
                let last = List.last batch
                loop (last.Offset + 1L) (acc @ batch) (count + batch.Length)

    loop 0L [] 0

/// Polls until `enough` holds or the time runs out; poll errors are
/// swallowed (they are not what these loops check) when `tolerate` is set.
let private pollUntil (consumer: GroupConsumer) (window: TimeSpan) (pollFor: TimeSpan) tolerate enough =
    let clock = Stopwatch.StartNew()
    let mutable got: ConsumerRecord list = []

    while not (enough got) && clock.Elapsed < window do
        match GroupConsumer.poll pollFor consumer with
        | Ok records -> got <- got @ records
        | Error _ when tolerate -> ()
        | Error e -> raise (SetupFailed e.Message)

    got

// ---------------------------------------------------------------------------

let private connectionAndMetadata address =
    section "connection and metadata"
    use consumer = Consumer.create (consumerConf address)

    match Consumer.apiVersions consumer with
    | Ok versions ->
        check "ApiVersions answers" (versions.Ranges.Length > 0) (sprintf "%d ranges" versions.Ranges.Length)
        check "broker reports a version" (versions.BrokerVersion.Length > 0) versions.BrokerVersion
    | Error e ->
        check "ApiVersions answers" false e.Message
        check "broker reports a version" false e.Message

    match Consumer.metadata consumer with
    | Ok metadata ->
        check "metadata lists brokers" (metadata.Brokers.Length >= 1) (sprintf "%d brokers" metadata.Brokers.Length)
    | Error e -> check "metadata lists brokers" false e.Message

let private roundTrip address topic =
    section "produce and consume round trip"
    let payloads = [ for i in 0..49 -> b (sprintf "record-%d" i) ]

    do
        use producer = Producer.create (producerConf address)

        for payload in payloads do
            sendTo topic 0 (Some payload) None [] producer

        producer |> Producer.flush |> orFail

    use consumer = Consumer.create (consumerConf address)
    let got = Consumer.fetch topic 0 0L 500 consumer |> orFail
    check "every record comes back" (got.Length = payloads.Length) (sprintf "got %d" got.Length)

    let identical =
        got.Length = payloads.Length
        && List.forall2
            (fun (i, r: ConsumerRecord) (p: byte[]) -> r.Value = Some p && r.Offset = int64 i)
            (List.indexed got)
            payloads

    check "values byte-identical and offsets contiguous" identical ""

let private compressionCodecs address =
    section "compression codecs"
    // Only none and gzip ship with the driver; lz4/zstd/snappy are opt-in via
    // Codecs.register.
    for codec, name in [ Compression.None, "none"; Compression.Gzip, "gzip" ] do
        let codecTopic = unique ("fsharp-" + name)
        let body = b (String.replicate 40 "the same line over and over. ")

        do
            use producer = Producer.create { producerConf address with Compression = codec }

            for i in 0..19 do
                sendTo codecTopic 0 (Some(Array.append body [| byte '0' + byte (i % 10) |])) None [] producer

            producer |> Producer.flush |> orFail

        use consumer = Consumer.create (consumerConf address)
        let got = Consumer.fetch codecTopic 0 0L 500 consumer |> orFail

        let startsWithBody =
            match got with
            | first :: _ ->
                match first.Value with
                | Some v -> v.Length > body.Length && v.[0 .. body.Length - 1] = body
                | None -> false
            | [] -> false

        check (name + ": round trips") (got.Length = 20 && startsWithBody) (sprintf "got %d records" got.Length)

let private keysAndOrdering address =
    section "keys, partitioning and ordering"
    let keyTopic = unique "fsharp-keys"

    let partitions =
        use producer = Producer.create (producerConf address)
        let partitions = producer |> Producer.partitions keyTopic |> orFail

        for i in 0..29 do
            let record =
                ProducerRecord.ofString keyTopic (sprintf "v%d" i) |> ProducerRecord.withStringKey "user-7"

            producer |> Producer.send record |> orFail |> ignore

        producer |> Producer.flush |> orFail
        partitions

    let target = Partitioner.partitionForKey (b "user-7") partitions
    use consumer = Consumer.create (consumerConf address)
    let onTarget = Consumer.fetch keyTopic target 0L 500 consumer |> orFail

    check
        "a key pins every record to one partition"
        (onTarget.Length = 30)
        (sprintf "partition %d holds %d of 30" target onTarget.Length)

    let ordered =
        onTarget.Length = 30
        && onTarget |> List.mapi (fun i r -> str r.Value = sprintf "v%d" i) |> List.forall id

    check "per-key order is preserved" ordered ""

    let strays =
        partitions
        |> List.filter ((<>) target)
        |> List.sumBy (fun p -> (Consumer.fetch keyTopic p 0L 200 consumer |> orFail).Length)

    check "no keyed record landed elsewhere" (strays = 0) (sprintf "%d strays" strays)

let private murmur2 () =
    section "murmur2 agrees with the broker's partitioner"
    let empty = Partitioner.murmur2 [||]
    check "murmur2(\"\") is stable" (empty = 275646681u) (string empty)
    check "murmur2 is deterministic" (Partitioner.murmur2 (b "user-7") = Partitioner.murmur2 (b "user-7")) ""
    check "different keys hash differently" (Partitioner.murmur2 (b "user-7") <> Partitioner.murmur2 (b "user-8")) ""

let private headersAndTimestamps address =
    section "record headers and timestamps"
    let headerTopic = unique "fsharp-headers"
    let before = nowMs () - 1000L

    do
        use producer = Producer.create (producerConf address)

        sendTo
            headerTopic
            0
            (Some(b "annotated"))
            None
            [ Header.create "trace-id" (b "abc-123")
              Header.create "content-type" (b "application/json")
              Header.nullValue "tombstone-reason" ]
            producer

        sendTo headerTopic 0 (Some(b "plain")) None [] producer
        producer |> Producer.flush |> orFail

    let after = nowMs () + 1000L
    use consumer = Consumer.create (consumerConf address)
    let got = Consumer.fetch headerTopic 0 0L 500 consumer |> orFail
    check "both records arrive" (got.Length = 2) (sprintf "got %d" got.Length)

    match got with
    | [ annotated; plain ] ->
        check "headers survive the round trip" (annotated.Headers.Length = 3) (sprintf "%d headers" annotated.Headers.Length)
        check "header values are exact" (str (ConsumerRecord.headerValue "trace-id" annotated) = "abc-123") ""

        check
            "a null header value stays null"
            (annotated.Headers.Length = 3 && annotated.Headers.[2].Value = None)
            ""

        check
            "a record with no headers gains none from its batch"
            (plain.Headers.IsEmpty)
            (sprintf "%d headers" plain.Headers.Length)

        let inWindow =
            got |> List.forall (fun r -> r.Timestamp >= before && r.Timestamp <= after)

        check
            "timestamps are real wall-clock values"
            inWindow
            (sprintf "%d,%d outside %d..%d" annotated.Timestamp plain.Timestamp before after)
    | _ -> ()

let private tombstones address =
    section "tombstones"
    let tombTopic = unique "fsharp-tombstones"

    do
        use producer = Producer.create (producerConf address)
        sendTo tombTopic 0 (Some(b "set")) (Some(b "k1")) [] producer
        sendTo tombTopic 0 (Some [||]) (Some(b "k2")) [] producer
        // A None value is a deletion, and must stay distinguishable from the
        // empty value above all the way through the round trip.
        producer
        |> Producer.send (ProducerRecord.tombstone tombTopic (b "k3") |> ProducerRecord.withPartition 0)
        |> orFail
        |> ignore

        producer |> Producer.flush |> orFail

    use consumer = Consumer.create (consumerConf address)
    let got = Consumer.fetch tombTopic 0 0L 500 consumer |> orFail
    check "all three records arrive" (got.Length = 3) (sprintf "got %d" got.Length)

    match got with
    | [ set; empty; tomb ] ->
        check "an ordinary value round-trips" (str set.Value = "set") ""
        check "an empty value is empty, not null" (empty.Value = Some [||]) (str empty.Value)
        check "a tombstone arrives as a null value" (tomb.Value.IsNone) (str tomb.Value)
    | _ -> ()

let private offsets address topic =
    section "offsets"
    use consumer = Consumer.create (consumerConf address)
    let earliest = consumer |> Consumer.listOffsets topic 0 OffsetSpec.Earliest |> orFail
    let latest = consumer |> Consumer.listOffsets topic 0 OffsetSpec.Latest |> orFail
    check "earliest is 0 on a fresh topic" (earliest = 0L) (string earliest)
    check "latest equals the record count" (latest = 50L) (string latest)

let private acks address =
    section "acks"

    for acks, number in [ Acks.None, 0; Acks.Leader, 1; Acks.All, -1 ] do
        let acksTopic = unique (sprintf "fsharp-acks%d" number)

        do
            use producer = Producer.create { producerConf address with Acks = acks }
            sendTo acksTopic 0 (Some(b "durable")) None [] producer
            producer |> Producer.flush |> orFail

        Thread.Sleep 400
        use consumer = Consumer.create (consumerConf address)
        let got = Consumer.fetch acksTopic 0 0L 500 consumer |> orFail
        check (sprintf "acks=%d stores the record" number) (got.Length = 1) (sprintf "got %d" got.Length)

let private groupAssignment address =
    section "consumer group: assignment, commit, resume"
    let groupTopic = unique "fsharp-group"
    let groupId = unique "fsharp-billing"

    do
        use producer = Producer.create (producerConf address)

        for i in 0..39 do
            sendKeyless groupTopic (b (sprintf "g%d" i)) producer

        producer |> Producer.flush |> orFail

    do
        use consumer = GroupConsumer.create (groupConf address groupId)
        consumer |> GroupConsumer.subscribe [ groupTopic ]

        let seen =
            pollUntil consumer (TimeSpan.FromSeconds 30.) (TimeSpan.FromMilliseconds 500.) false (fun got ->
                got.Length >= 40)

        check "the group consumes every record" (seen.Length = 40) (sprintf "got %d" seen.Length)

        let distinct =
            seen |> List.distinctBy (fun r -> r.Partition, r.Offset) |> List.length

        check "no record is delivered twice" (distinct = seen.Length) ""

        consumer |> GroupConsumer.commit |> orFail
        let total = consumer |> GroupConsumer.committed |> orFail |> Map.values |> Seq.sum
        check "commit records a position" (total = 40L) (string total)

    // A second consumer in the same group must resume, not replay.
    use rejoined = GroupConsumer.create (groupConf address groupId)
    rejoined |> GroupConsumer.subscribe [ groupTopic ]

    let replayed =
        pollUntil rejoined (TimeSpan.FromSeconds 5.) (TimeSpan.FromMilliseconds 300.) true (fun _ -> false)

    check
        "a rejoining group resumes from its commit"
        (replayed.IsEmpty)
        (sprintf "replayed %d records it had already committed" replayed.Length)

let private autoOffsetReset address =
    section "auto.offset.reset"
    let resetTopic = unique "fsharp-reset"

    do
        use producer = Producer.create (producerConf address)

        for i in 0..9 do
            sendKeyless resetTopic (b (sprintf "r%d" i)) producer

        producer |> Producer.flush |> orFail

    do
        use consumer =
            GroupConsumer.create
                { groupConf address (unique "fsharp-latest") with
                    AutoOffsetReset = AutoOffsetReset.Latest }

        consumer |> GroupConsumer.subscribe [ resetTopic ]

        let skipped =
            pollUntil consumer (TimeSpan.FromSeconds 4.) (TimeSpan.FromMilliseconds 300.) true (fun _ -> false)

        check
            "latest skips records produced before the group existed"
            (skipped.IsEmpty)
            (sprintf "saw %d" skipped.Length)

    use strict =
        GroupConsumer.create
            { groupConf address (unique "fsharp-none") with
                AutoOffsetReset = AutoOffsetReset.None }

    strict |> GroupConsumer.subscribe [ resetTopic ]
    let clock = Stopwatch.StartNew()
    let mutable raised = false

    while clock.Elapsed < TimeSpan.FromSeconds 5. && not raised do
        match GroupConsumer.poll (TimeSpan.FromMilliseconds 300.) strict with
        | Error(BrahmaputraError.NoOffsetForPartition _) -> raised <- true
        | _ -> () // anything else is not the refusal we want

    check "none refuses to guess a position" raised ""

let private assignors address =
    section "assignors"

    for assignor, name in [ Assignor.Range, "range"; Assignor.RoundRobin, "roundrobin"; Assignor.Sticky, "sticky" ] do
        let assignorTopic = unique ("fsharp-" + name)

        do
            use producer = Producer.create (producerConf address)

            for i in 0..19 do
                sendKeyless assignorTopic (b (sprintf "a%d" i)) producer

            producer |> Producer.flush |> orFail

        use consumer =
            GroupConsumer.create { groupConf address (unique ("fsharp-grp-" + name)) with Assignor = assignor }

        consumer |> GroupConsumer.subscribe [ assignorTopic ]

        let collected =
            pollUntil consumer (TimeSpan.FromSeconds 20.) (TimeSpan.FromMilliseconds 500.) true (fun got ->
                got.Length >= 20)

        check (name + ": consumes every record") (collected.Length = 20) (sprintf "got %d" collected.Length)

let private boundedBuffer address =
    section "bounded client buffer"
    let bufferTopic = unique "fsharp-buffer"

    use producer =
        Producer.create
            { producerConf address with
                LingerMs = 10_000 // never flush on time during this check
                BufferMemory = 2048L
                MaxBlockMs = 300 }

    let record =
        ProducerRecord.create bufferTopic (Array.zeroCreate 256) |> ProducerRecord.withPartition 0

    let mutable blocked = false
    let mutable i = 0

    while i < 500 && not blocked do
        match producer |> Producer.send record with
        | Error(BrahmaputraError.BufferFull message) -> blocked <- message.Contains "buffer full"
        | _ -> ()

        i <- i + 1

    check "a full buffer blocks and then reports" blocked ""

let private wireEdgeCases address =
    section "wire edge cases"
    let edgeTopic = unique "fsharp-edge"
    let large = Array.init (1 <<< 20) (fun i -> byte (i * 7))
    let unicodeKey = b "ключ-✓-🔑"
    let unicodeValue = b "значение — 数据 — 🚀"

    do
        let producer = Producer.create (producerConf address)
        sendTo edgeTopic 0 (Some large) None [] producer
        sendTo edgeTopic 0 (Some unicodeValue) (Some unicodeKey) [ Header.create "ünïcødé-🏷" (b "✓") ] producer
        // An empty key and an empty header value are values, not nulls.
        sendTo
            edgeTopic
            0
            (Some(b "empty-key"))
            (Some [||])
            [ Header.create "empty" [||]; Header.nullValue "null" ]
            producer

        sendTo edgeTopic 0 (Some(b "null-key")) None [] producer
        producer |> Producer.close |> orFail

    use consumer = Consumer.create (consumerConf address)
    let got = fetchAll consumer edgeTopic 0 4
    check "edge records all arrive" (got.Length = 4) (sprintf "got %d" got.Length)

    match got with
    | [ big; uni; empty; nullKey ] ->
        check
            "a 1 MiB value round-trips byte-identical"
            (big.Value = Some large)
            (sprintf "%d bytes" (big.Value |> Option.map Array.length |> Option.defaultValue -1))

        check
            "unicode key, value and header key round-trip"
            (uni.Key = Some unicodeKey
             && uni.Value = Some unicodeValue
             && uni.Headers.Length = 1
             && uni.Headers.[0].Key = "ünïcødé-🏷")
            ""

        check "an empty key stays empty, not null" (empty.Key = Some [||]) (str empty.Key)

        check
            "an empty header value stays empty, not null"
            (match empty.Headers with
             | [ { Value = Some [||] }; { Value = None } ] -> true
             | _ -> false)
            (empty.Headers |> List.map (fun h -> h.Key + "=" + str h.Value) |> String.concat ",")

        check "a null key stays null" (nullKey.Key.IsNone) (str nullKey.Key)
    | _ -> ()

let private orderingUnderLinger address =
    section "ordering under linger flushes"
    let orderTopic = unique "fsharp-order"
    let total = 5000

    do
        let producer =
            Producer.create
                { producerConf address with
                    LingerMs = 1
                    BatchSize = 256 }

        for i in 0 .. total - 1 do
            sendTo orderTopic 0 (Some(b (string i))) None [] producer

        producer |> Producer.close |> orFail

    use consumer = Consumer.create (consumerConf address)

    let values =
        fetchAll consumer orderTopic 0 total
        |> List.map (fun r -> int (str r.Value))

    let inversions =
        values |> List.pairwise |> List.filter (fun (a, c) -> c < a) |> List.length

    check "every record of a partition arrives" (values.Length = total) (sprintf "got %d" values.Length)
    check "a partition's records keep send order" (inversions = 0) (sprintf "%d inversions" inversions)

let private backgroundFailures address =
    section "background flush failures are reported"
    let producer = Producer.create { producerConf address with LingerMs = 20 }
    // Partition 999 does not exist, so the background send fails. The delivery
    // is deliberately not awaited: flush must report it.
    let record =
        ProducerRecord.ofString (unique "fsharp-bgfail") "lost" |> ProducerRecord.withPartition 999

    let sendResult = producer |> Producer.send record
    Thread.Sleep 300
    let flushResult = producer |> Producer.flush

    check
        "a failed linger flush surfaces on the next Flush"
        (Result.isOk sendResult && Result.isError flushResult)
        (sprintf "send=%s flush=%s" (errText sendResult) (errText flushResult))

    let closing = Task.Run(fun () -> producer |> Producer.close |> ignore)
    check "Close returns after a failed flush" (closing.Wait(TimeSpan.FromSeconds 5.)) "hung"

let private connectionFailures address =
    section "connection failures"

    do
        // A broker that accepts and never answers must cost an error, not a
        // thread blocked forever.
        use silent = new SilentBroker()

        use conn =
            Connection.connect silent.Address "fsharp-test" (TimeSpan.FromSeconds 1.) (TimeSpan.FromMilliseconds 300.)
            |> orFail

        let started = Stopwatch.StartNew()
        let result = Connection.apiVersions conn

        check
            "a request to an unresponsive broker times out"
            (Result.isError result && started.Elapsed < TimeSpan.FromSeconds 3.)
            (match result with
             | Ok _ -> "answered"
             | Error e -> e.Message)

        check "a timed-out connection is not reused" (Connection.isBroken conn) ""

    // A connection the broker drops is redialled, not kept forever.
    use proxy = new TcpProxy(address)
    let dropTopic = unique "fsharp-drop"

    do
        use producer = Producer.create (producerConf proxy.Address)

        let sendAndWait value =
            let record =
                ProducerRecord.ofString dropTopic value |> ProducerRecord.withPartition 0

            producer |> Producer.send record |> Result.bind Delivery.wait

        sendAndWait "before" |> orFail |> ignore
        proxy.DropAll()

        let rec retry attempt last =
            if attempt >= 3 then
                last
            else
                match sendAndWait "after" with
                | Ok _ -> Ok()
                | Error e -> retry (attempt + 1) (Error e)

        let recovered = retry 0 (Error(BrahmaputraError.Client "not attempted"))

        check
            "a producer recovers after its connection drops"
            (Result.isOk recovered)
            (match recovered with
             | Ok _ -> ""
             | Error e -> e.Message)

    use consumer = Consumer.create (consumerConf proxy.Address)
    Consumer.fetch dropTopic 0 0L 100 consumer |> ignore
    proxy.DropAll()

    let rec retry attempt last =
        if attempt >= 3 then
            last
        else
            match Consumer.fetch dropTopic 0 0L 100 consumer with
            | Ok records -> Ok records
            | Error e -> retry (attempt + 1) (Error e)

    let fetched = retry 0 (Error(BrahmaputraError.Client "not attempted"))

    check
        "a consumer recovers after its connection drops"
        (match fetched with
         | Ok records -> records.Length >= 1
         | Error _ -> false)
        (match fetched with
         | Ok records -> sprintf "%d records" records.Length
         | Error e -> e.Message)

let private maxPollInterval address =
    section "consumer group: max.poll.interval and rejoin"
    let slowTopic = unique "fsharp-slow"
    let producer = Producer.create (producerConf address)

    for i in 0..9 do
        sendKeyless slowTopic (b (sprintf "s%d" i)) producer

    producer |> Producer.flush |> orFail

    use consumer =
        GroupConsumer.create { groupConf address (unique "fsharp-slow-grp") with MaxPollIntervalMs = 1500 }

    consumer |> GroupConsumer.subscribe [ slowTopic ]

    let pollBatch () =
        let clock = Stopwatch.StartNew()
        let mutable got: ConsumerRecord list = []
        let mutable error = None

        while got.Length < 10 && error.IsNone && clock.Elapsed < TimeSpan.FromSeconds 15. do
            match GroupConsumer.poll (TimeSpan.FromMilliseconds 300.) consumer with
            | Ok records -> got <- got @ records
            | Error e -> error <- Some e

        got, error

    let first, _ = pollBatch ()
    consumer |> GroupConsumer.commit |> orFail
    // Stall past max.poll.interval.ms: the member leaves the group.
    Thread.Sleep 2500

    for i in 10..19 do
        sendKeyless slowTopic (b (sprintf "s%d" i)) producer

    producer |> Producer.close |> orFail
    let second, pollError = pollBatch ()

    check
        "a member that stalled rejoins on its next poll"
        (first.Length = 10 && second.Length = 10 && pollError.IsNone)
        (sprintf
            "first=%d second=%d err=%s"
            first.Length
            second.Length
            (pollError |> Option.map (fun e -> e.Message) |> Option.defaultValue "none"))

let private timeInsidePoll address =
    section "consumer group: time inside poll does not count against max.poll.interval"
    let joinTopic = unique "fsharp-inpoll"
    let producer = Producer.create (producerConf address)
    producer |> Producer.partitions joinTopic |> orFail |> ignore

    // Far shorter than the first poll below, which spends ~1s joining (the
    // broker's initial rebalance delay) and then waits for data.
    use consumer =
        GroupConsumer.create { groupConf address (unique "fsharp-inpoll-grp") with MaxPollIntervalMs = 600 }

    consumer |> GroupConsumer.subscribe [ joinTopic ]

    let sender =
        Task.Run(fun () ->
            Thread.Sleep 2000

            for i in 0..9 do
                producer |> Producer.send (ProducerRecord.ofString joinTopic (sprintf "j%d" i)) |> ignore)

    // One long poll: it joins, then waits for the records above.
    let got = consumer |> GroupConsumer.poll (TimeSpan.FromSeconds 4.)
    // Committed straight away, before another poll could quietly rejoin: this
    // fails if the member left the group mid-poll.
    let committed = consumer |> GroupConsumer.commit

    let count =
        match got with
        | Ok records -> records.Length
        | Error _ -> 0

    check
        "a member is still in its group after a long poll"
        (Result.isOk got && count > 0 && Result.isOk committed)
        (sprintf "got=%d poll=%s commit=%s" count (errText got) (errText committed))

    sender.Wait()
    producer |> Producer.close |> orFail

let private run address =
    connectionAndMetadata address
    let topic = unique "fsharp-roundtrip"
    roundTrip address topic
    compressionCodecs address
    keysAndOrdering address
    murmur2 ()
    headersAndTimestamps address
    tombstones address
    offsets address topic
    acks address
    groupAssignment address
    autoOffsetReset address
    assignors address
    boundedBuffer address
    wireEdgeCases address
    orderingUnderLinger address
    backgroundFailures address
    connectionFailures address
    maxPollInterval address
    timeInsidePoll address
    Checklist.run check section address

[<EntryPoint>]
let main args =
    let host = if args.Length > 0 then args.[0] else "127.0.0.1"
    let port = if args.Length > 1 then args.[1] else "9092"
    // Also accept a single host:port argument, as the Go suite does.
    let address =
        if args.Length = 1 && host.Contains ':' then host else sprintf "%s:%s" host port

    try
        run address
        printfn "\n%d passed, %d failed" passed failed
        if failed > 0 then 1 else 0
    with
    | SetupFailed message ->
        printfn "  FATAL setup: %s" message
        2
    | e ->
        printfn "  FATAL %s: %s" (e.GetType().Name) e.Message
        2
