# BitPacker for Crystal

Target names: `crystal` or `cr`. Tested with Crystal 1.11. The generated code
uses only the standard library.

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang crystal --out ./generated
# -> generated/crystal/game.cr   (with --lang cr: generated/cr/game.cr)
```

One file per schema. Everything lives in a module named after the schema file
(`bench_complex.buff` -> `BenchComplex`, `edge.buff` -> `Edge`); pass
`--package my_proto` to name it `MyProto` instead.

## API

For every schema class the module has a `class` with:

| Member | Meaning |
|---|---|
| `property field : T` | one per schema field, same snake_case name |
| `.new(*, field = default, ...)` | keyword-only constructor; every argument is optional (0, `""`, `false`, `[] of T`, `Nested.new`) |
| `#encode : Bytes` | complete message: schema version prefix, then the fields |
| `.decode(data : Bytes) : self` | raises `DecodeError` on a wrong version, truncated or malformed input |
| `.decode?(data : Bytes) : self?` | same, but returns `nil` instead of raising |
| `==` / `hash` | structural (`def_equals_and_hash` over all fields) |

`encode_to` / `decode_from` and the `BPWriter` / `BPReader` classes are
`:nodoc:` internals used for nested classes.

`DecodeError < Exception` is the only exception `decode` raises (from any
input). Error choice: raising is the primary API because a decoded message is
normally expected to be valid; `decode?` is there for the "try" style.

```crystal
require "./generated/crystal/game"

hero = Game::Character.new(name: "Ayla", level: 12, position: Game::Vec3.new(x: 1, y: 2, z: 3))
bytes = hero.encode                        # Bytes, e.g. File.write("hero.bin", bytes)

begin
  back = Game::Character.decode(bytes)
rescue ex : Game::DecodeError
  STDERR.puts "bad message: #{ex.message}"
end

maybe = Game::Character.decode?(io.to_slice) # IO::Memory -> Bytes
```

## Type mapping

| Schema | Crystal | Notes |
|---|---|---|
| `int` | `Int32` | zigzag varint; `Int32::MIN`/`MAX` round-trip |
| `long` | `Int64` | zigzag varint, full 64-bit range |
| `float` | `Float32` | fixed point, see below |
| `double` | `Float64` | fixed point, see below |
| `bool` | `Bool` | decoder treats any non-zero byte as `true` |
| `string` | `String` | UTF-8; decode rejects invalid UTF-8 |
| `T[]` | `Array(T)` | |
| class `C` | `C` (a `class`, reference type) | |

## Floats (fixed point)

`float` and `double` go on the wire as `trunc(v * 10000)` in an `Int64`.
For `float` the product `v * 10000_f32` is computed in **Float32**, exactly as
the Go (`int64(v * 10000.0)` with `v float32`) and Java (`(long)(v * 10000.0f)`)
targets do, and decoding is `n.to_f32 / 10000_f32`, also in Float32, like
Go/Java. This matters: for `0.7_f32` the Float32 product is `7000.0` (wire
`7000`) while the Float64 product is `6999.99988…` (wire `6999`). The test
(`cross_lang_test/crystal`) checks six such values against numbers produced by
the Go target.

`double` uses `v * 10000.0` in Float64. For both, NaN encodes as 0 and values
whose product is outside the Int64 range saturate to `Int64::MIN`/`MAX` (the
Java behaviour; Go's is implementation-defined). Precision is 4 decimal digits
by design.

## Caveats

- Field names: a field that is a Crystal keyword or clobbers a method the
  class relies on (`end`, `type`, `hash`, `encode`, `class`, ...) gets a
  trailing underscore (`type_`, `hash_`). A class name gets its first letter
  upper-cased. Two fields mapping to the same name is a generation error.
- Classes are reference types; `encode` walks them recursively, so a cyclic
  object graph would not terminate. A schema class that contains itself
  (directly or indirectly, not through an array) cannot be default-constructed
  and is not supported.
- Decoding rejects an array count larger than the remaining input (each
  element takes at least one byte). The only thing this excludes is an array of
  a class with no fields longer than the rest of the message.
- Trailing bytes after a complete message are ignored.
- Varints longer than 10 bytes, and `int` varints above 32 bits, are rejected.
- Arrays longer than `Int32::MAX` raise `ArgumentError` on encode.
