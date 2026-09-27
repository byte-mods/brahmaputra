// Cross-language conformance test for the BitPacker F# target.
module Test

open System
open System.IO
open Bench

let mutable passed = 0
let mutable failed = 0

let check (name: string) (ok: bool) (detail: string) =
    if ok then
        passed <- passed + 1
        printfn "  ok   %s" name
    else
        failed <- failed + 1
        printfn "  FAIL %s%s" name (if detail = "" then "" else sprintf " (%s)" detail)

let checkEq name (got: 'T) (want: 'T) = check name (got = want) (sprintf "got %A, want %A" got want)

let benchWorld () : WorldState =
    let sword = { Item.Default with Id = 1; Name = "Excalibur"; Value = 9999; Weight = 15; Rarity = "Legendary" }
    let hero =
        { Character.Default with
            Name = "TestHero"; Level = 99; Hp = 1000; Mp = 500; IsAlive = true
            Position = { X = 10; Y = -20; Z = 30 }; Skills = [ 1; 2; 3; 100 ]; Inventory = [ sword ] }
    let guild = { Name = "TestGuild"; Description = "A test guild for cross-language"; Members = [ hero ] }
    let potion = { Item.Default with Id = 2; Name = "HealthPotion"; Value = 50; Weight = 1; Rarity = "Common" }
    { WorldId = 42; Seed = "cross_lang_test"; Guilds = [ guild ]; LootTable = [ potion ] }

let verifyBench (label: string) (w: WorldState) =
    checkEq (label + " world_id") w.WorldId 42
    checkEq (label + " seed") w.Seed "cross_lang_test"
    checkEq (label + " guilds length") w.Guilds.Length 1
    for g in List.truncate 1 w.Guilds do
        checkEq (label + " guild name") g.Name "TestGuild"
        checkEq (label + " guild description") g.Description "A test guild for cross-language"
        checkEq (label + " members length") g.Members.Length 1
        for h in List.truncate 1 g.Members do
            checkEq (label + " hero name") h.Name "TestHero"
            checkEq (label + " hero level") h.Level 99
            checkEq (label + " hero hp") h.Hp 1000
            checkEq (label + " hero mp") h.Mp 500
            checkEq (label + " hero is_alive") h.IsAlive true
            checkEq (label + " position") h.Position { X = 10; Y = -20; Z = 30 }
            checkEq (label + " skills") h.Skills [ 1; 2; 3; 100 ]
            checkEq (label + " inventory length") h.Inventory.Length 1
            for s in List.truncate 1 h.Inventory do
                checkEq (label + " sword name") s.Name "Excalibur"
                checkEq (label + " sword value") s.Value 9999
                checkEq (label + " sword rarity") s.Rarity "Legendary"
    checkEq (label + " loot length") w.LootTable.Length 1
    for p in List.truncate 1 w.LootTable do
        checkEq (label + " potion name") p.Name "HealthPotion"
        checkEq (label + " potion rarity") p.Rarity "Common"

let canonicalEdge () : Edge.Edge =
    { IMin = Int32.MinValue; IMax = Int32.MaxValue; IZero = 0; INeg = -1
      LMin = Int64.MinValue; LMax = Int64.MaxValue; LNeg = -300L
      F = -1.25f; D = 1234.5625; DNeg = -0.5
      Yes = true; No = false
      Empty = ""; Unicode = "héllo wörld ✓ 日本 \U0001F680"
      Ints = [ 0; -1; 1; -64; 64; Int32.MinValue; Int32.MaxValue ]
      Longs = [ 0L; -1L; Int64.MaxValue; Int64.MinValue; 4294967296L ]
      Floats = [ 0.0f; 0.5f; -2.25f ]
      Doubles = [ 0.0; 3.5; -1000000.25 ]
      Bools = [ true; false; true ]
      Strings = [ ""; "a"; "日本語" ]
      NoInts = []
      Inner = { Big = 1099511627776L; Label = "inner" }
      Inners = [ { Big = -1L; Label = "" }; { Big = 0L; Label = "x" } ]
      NoInners = [] }

/// "" when Decode returns Error, else what went wrong.
let rejects (data: byte[]) =
    try
        match Edge.Edge.Decode data with
        | Error _ -> ""
        | Ok _ -> "no error"
    with e -> sprintf "threw %s" (e.GetType().Name)

[<EntryPoint>]
let main argv =
    let dir = if argv.Length > 0 then argv.[0] else ".."

    printfn "bench_complex"
    let benchRef = File.ReadAllBytes(Path.Combine(dir, "test_data.bin"))
    let benchEnc = (benchWorld ()).Encode()
    File.WriteAllBytes(Path.Combine(dir, "test_data_fsharp.bin"), benchEnc)
    checkEq "bench encode == test_data.bin" benchEnc benchRef
    match WorldState.Decode benchRef with
    | Error e -> check "bench decode test_data.bin" false e
    | Ok w ->
        verifyBench "bench decode" w
        checkEq "bench decode == canonical value" w (benchWorld ())
        checkEq "bench re-encode == test_data.bin" (w.Encode()) benchRef
    match WorldState.Decode benchEnc with
    | Error e -> check "bench roundtrip" false e
    | Ok w -> verifyBench "bench roundtrip" w

    printfn "edge"
    let ref = File.ReadAllBytes(Path.Combine(dir, "edge", "edge_ref.bin"))
    let want = canonicalEdge ()
    checkEq "edge encode == edge_ref.bin" (want.Encode()) ref
    match Edge.Edge.Decode ref with
    | Error e -> check "edge decode edge_ref.bin" false e
    | Ok d ->
        checkEq "edge field i_min" d.IMin Int32.MinValue
        checkEq "edge field i_max" d.IMax Int32.MaxValue
        checkEq "edge field i_zero" d.IZero 0
        checkEq "edge field i_neg" d.INeg -1
        checkEq "edge field l_min" d.LMin Int64.MinValue
        checkEq "edge field l_max" d.LMax Int64.MaxValue
        checkEq "edge field l_neg" d.LNeg -300L
        checkEq "edge field f" d.F -1.25f
        checkEq "edge field d" d.D 1234.5625
        checkEq "edge field d_neg" d.DNeg -0.5
        checkEq "edge field yes" d.Yes true
        checkEq "edge field no" d.No false
        checkEq "edge field empty" d.Empty ""
        checkEq "edge field unicode" d.Unicode want.Unicode
        checkEq "edge field ints" d.Ints want.Ints
        checkEq "edge field longs" d.Longs want.Longs
        checkEq "edge field floats" d.Floats want.Floats
        checkEq "edge field doubles" d.Doubles want.Doubles
        checkEq "edge field bools" d.Bools want.Bools
        checkEq "edge field strings" d.Strings want.Strings
        checkEq "edge field no_ints" d.NoInts []
        checkEq "edge field inner" d.Inner want.Inner
        checkEq "edge field inners" d.Inners want.Inners
        checkEq "edge field no_inners" d.NoInners []
        checkEq "edge decode == canonical value" d want
        checkEq "edge re-encode == edge_ref.bin" (d.Encode()) ref

    let bad = Array.copy ref
    bad.[5] <- byte '9' // "2.1.0" -> "2.1.9"
    let badResult = rejects bad
    check "edge wrong version rejected" (badResult = "") badResult

    let truncFail =
        seq { 0 .. ref.Length - 1 }
        |> Seq.tryPick (fun n ->
            match rejects (Array.sub ref 0 n) with
            | "" -> None
            | r -> Some(sprintf "prefix %d: %s" n r))
        |> Option.defaultValue ""
    check (sprintf "edge every truncation (%d prefixes) rejected" ref.Length) (truncFail = "") truncFail

    // float fields are scaled in single precision, like the Go and Java
    // targets: 1.0005f * 10000f = 10005 in float32 (10004 in
    // float64).
    match Edge.Edge.Decode({ Edge.Edge.Default with F = 1.0005f; Floats = [ 1.0013f ] }.Encode()) with
    | Error e -> check "float round trip" false e
    | Ok back ->
        checkEq "float field scaled in float32" back.F (float32 10005L / 10000.0f)
        checkEq "float[] element scaled in float32" back.Floats [ float32 10013L / 10000.0f ]

    let bom : Edge.Inner = { Big = 7L; Label = "\uFEFF\uFEFFbom" }
    checkEq "string with leading U+FEFF round-trips" (Edge.Inner.Decode(bom.Encode()) |> Result.map (fun i -> i.Label)) (Ok bom.Label)
    let overlong = [| 10uy; 50uy; 46uy; 49uy; 46uy; 48uy; 0uy; 4uy; 0xC0uy; 0x80uy |]
    let ovr = Edge.Inner.Decode overlong
    check "invalid UTF-8 rejected" (Result.isError ovr) (sprintf "%A" ovr)

    // edge_float32_ref.bin: f and floats whose x10000 is inexact in float32.
    let f32ref = File.ReadAllBytes(Path.Combine(dir, "edge", "edge_float32_ref.bin"))
    let f32want = { canonicalEdge () with F = 0.29f; Floats = [ 0.7f; 16777.217f; -0.29f ] }
    checkEq "float32 fixture: encode == edge_float32_ref.bin" (f32want.Encode()) f32ref
    match Edge.Edge.Decode f32ref with
    | Error e -> check "float32 fixture: decode" false e
    | Ok f32dec ->
        checkEq "float32 fixture: decoded f == 0.29f" f32dec.F 0.29f
        checkEq "float32 fixture: decoded floats" f32dec.Floats [ 0.7f; 16777.217f; -0.29f ]
        checkEq "float32 fixture: decode == variant" f32dec f32want

    checkEq "trailing bytes ignored" (Edge.Edge.Decode(Array.append ref [| 0xFFuy; 0uy; 0x7Fuy |])) (Ok want)

    let encodeRejects name (e: Edge.Edge) =
        let r =
            try
                e.Encode() |> ignore
                "no error"
            with
            | :? ArgumentException -> ""
            | x -> sprintf "wrong error: %s" (x.GetType().Name)
        check name (r = "") r
    let d0 = Edge.Edge.Default
    encodeRejects "NaN float rejected on encode" { d0 with F = Single.NaN }
    encodeRejects "infinite float rejected on encode" { d0 with Floats = [ Single.PositiveInfinity ] }
    encodeRejects "out-of-range float rejected on encode" { d0 with F = 1e15f }
    encodeRejects "NaN double rejected on encode" { d0 with D = Double.NaN }
    encodeRejects "infinite double rejected on encode" { d0 with DNeg = Double.NegativeInfinity }
    encodeRejects "out-of-range double rejected on encode" { d0 with Doubles = [ 1e300 ] }

    let decodeRejects name (data: byte[]) (f: byte[] -> bool) =
        let r =
            try (if f data then "" else "no error")
            with x -> sprintf "threw %s" (x.GetType().Name)
        check name (r = "") r
    let edgeFails d = Result.isError (Edge.Edge.Decode d)
    let innerFails d = Result.isError (Edge.Inner.Decode d)
    let bytesOf (xs: int list) = xs |> List.map byte |> Array.ofList
    let dflt = d0.Encode() // ints count at offset 20
    let withCount count = Array.concat [ dflt.[.. 19]; bytesOf count; dflt.[21 ..] ]
    decodeRejects "negative array length rejected" (withCount [ 0x01 ]) edgeFails
    decodeRejects "oversized array length rejected" (withCount [ 0xFE; 0xFF; 0xFF; 0xFF; 0x0F ]) edgeFails
    let innerHead = bytesOf [ 10; 50; 46; 49; 46; 48; 0 ]
    decodeRejects "negative string length rejected" (Array.append innerHead (bytesOf [ 0x01; 0x61 ])) innerFails
    decodeRejects "oversized string length rejected"
        (Array.append innerHead (bytesOf [ 0x80; 0x80; 0x80; 0x80; 0x80; 0x80; 0x80; 0x80; 0x40; 0x61 ])) innerFails
    decodeRejects "varint longer than 10 bytes rejected"
        (Array.append innerHead.[.. 5] (bytesOf [ 0xFF; 0xFF; 0xFF; 0xFF; 0xFF; 0xFF; 0xFF; 0xFF; 0xFF; 0xFF; 0x01; 0x00 ])) innerFails

    printfn "fsharp: %d passed, %d failed" passed failed
    if failed > 0 then 1 else 0
