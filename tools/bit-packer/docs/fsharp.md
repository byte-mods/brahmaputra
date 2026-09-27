# BitPacker for F#

Target names: `fsharp` or `fs`. Tested with the .NET 8 SDK (F# 8). The
generated code uses only the .NET base library and FSharp.Core, and builds
with `TreatWarningsAsErrors` at warning level 5.

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang fsharp --package my.game --out ./generated
# -> generated/fsharp/game.fs   (with --lang fs: generated/fs/game.fs)
```

One file per schema, in namespace `--package` with every segment PascalCased
(`my.game` -> `My.Game`; the default `generated` -> `Generated`). Add it to
your project's `<Compile>` list. The file also declares `BitPacker.Version`
and internal helper types, so **give each schema its own namespace**.

## API

For every schema class `C`, a record (all records of a schema form one
recursive `type ... and ...` group, so classes may reference each other in
any order):

```fsharp
type C =
    { Field: T; ... }                         // one field per schema field
    static member Default : C                 // every field at its zero value
    member this.Encode() : byte[]             // version prefix + fields; ArgumentException for a
                                              // NaN/infinite/out-of-range float or double
    static member Decode(data: byte[]) : Result<C, string>
```

`Decode` never throws for bad input: it returns `Error reason` on a wrong
version prefix, truncated input, a varint longer than 64 bits, an `int`
outside 32 bits, a negative or oversized length or count, invalid UTF-8, or
nesting deeper than 512 classes.

Field names become PascalCase (`is_alive` -> `IsAlive`); a name that clashes
with a generated member (`Encode`, `Decode`, `Default`, `ToString`, ...) gets a
trailing `_`. Two fields mapping to one name, and a cycle of non-array class
fields, are generation errors. A class with no fields becomes a single-case
union (`type Empty = | Empty`), since F# records need at least one field.

```fsharp
open My.Game

let w = { WorldState.Default with WorldId = 42; Seed = "abc"
                                  LootTable = [ { Item.Default with Id = 2; Name = "Potion" } ] }
let bytes = w.Encode()

match WorldState.Decode bytes with
| Ok back -> assert (back = w)          // structural equality, lists compare by content
| Error err -> eprintfn "bad message: %s" err
```

When two records share field names, F# infers the last one declared; annotate
(`let i : Inner = { Big = 1L; Label = "" }`) or use `{ Inner.Default with ... }`.

## Type mapping

| Schema | F# | Notes |
|---|---|---|
| `int` | `int32` | |
| `long` | `int64` | |
| `float` | `float32` | fixed point, product in float32 (see below) |
| `double` | `float` | fixed point, product in float64 |
| `bool` | `bool` | any non-zero byte decodes as `true` |
| `string` | `string` | UTF-8; lone surrogates encode as U+FFFD; decoding rejects invalid UTF-8 |
| `T[]` | `T list` | immutable; decoding pre-sizes an array and converts |
| class `C` | `C` | zero value `C.Default` |

## Float fixed point

`float`/`double` travel as `trunc(v * 10000)` in an int64. For `float32`
fields the encoder multiplies in single precision (`v * 10000.0f`, a float32
operation on .NET Core's SSE code generation), exactly like the Java target's
`(long)(v * 10000.0f)` and the Go target's float32 `int64(v * 10000.0)`. **Encoding rejects** a `float`/`double` that is NaN, infinite, or whose scaled value does not fit in an int64 (edge/README.md: never saturate or wrap): `Encode()` raises `ArgumentException`. The range check is
explicit, before the `int64` conversion, because a bare `int64 x` of an
out-of-range value differs between .NET versions and CPUs.
Decoding is `float32 n / 10000.0f` and `float n / 10000.0`. Example: `1.0005f`
encodes as 10005 (a float64 product would give 10004); the conformance test
checks this.

## Caveats

- `Decode` ignores bytes after the root value, like the other targets.
- Array counts and string lengths are checked against the remaining input
  before anything is allocated; an array of a class with no fields therefore
  cannot hold more elements than there are bytes left.

## Conformance test

`cross_lang_test/fsharp/run.sh` builds `FSharpTest.fsproj` (the two generated
files plus `Test.fs`) with `dotnet build` and runs it. FSharp.Core comes from
the SDK's offline library pack, so no network is needed.

Besides the bench and edge fixtures the test covers `edge/edge_float32_ref.bin`
(float values whose x10000 is inexact in float32), trailing bytes after the
root value, NaN/infinite/out-of-range floats and doubles on encode, invalid
UTF-8, negative and oversized lengths/counts, and over-long varints on decode.
