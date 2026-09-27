# BitPacker for Nim

Target name: `nim`. Tested with Nim 1.6.14 (default GC, also `-d:release`).
The generated module imports only `std/unicode`.

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang nim --out ./generated
# -> generated/nim/game.nim
```

One module per schema, named after the schema file (non-identifier characters
become `_`).

## API

For every schema class `C` the module exports:

| Symbol | Meaning |
|---|---|
| `C* = object` with `field*: T` | value type; fields are camelCase (`is_alive` -> `isAlive`; Nim is style-insensitive, so `is_alive` also works) |
| `proc encode*(o: C): seq[byte]` | complete message: schema version prefix, then the fields |
| `proc decode*(T: typedesc[C], data: openArray[byte]): C {.raises: [DecodeError].}` | `C.decode(bytes)` |
| `proc decode*(T: typedesc[C], data: string): C {.raises: [DecodeError].}` | same, for a message held in a `string` |

plus `DecodeError* = object of CatchableError` (the only exception `decode`
raises, for any input) and `const SchemaVersion*`. Errors are exceptions rather
than a Result object because Nim's standard library has no Result type and the
generated code is dependency-free; wrap `decode` in `try` for the "maybe" style.

```nim
import generated/nim/game

let hero = Character(name: "Ayla", level: 12, position: Vec3(x: 1, y: 2, z: 3))
let bytes = hero.encode()            # seq[byte]

try:
  let back = Character.decode(bytes)
  assert back == hero                # objects compare structurally
except game.DecodeError as e:
  echo "bad message: ", e.msg
```

If you import two generated modules, `DecodeError`/`SchemaVersion` exist in
both: qualify them (`game.DecodeError`). `encode`/`decode` overload on the
type and need no qualification.

## Type mapping

| Schema | Nim | Notes |
|---|---|---|
| `int` | `int32` | zigzag varint, computed on `uint32` (no overflow defects) |
| `long` | `int64` | zigzag varint, full range |
| `float` | `float32` | fixed point, see below |
| `double` | `float64` | fixed point, see below |
| `bool` | `bool` | decoder treats any non-zero byte as `true` |
| `string` | `string` | UTF-8; decode rejects invalid UTF-8 |
| `T[]` | `seq[T]` | |
| class `C` | `C` (`object`, value type) | |

## Floats (fixed point)

`float` and `double` go on the wire as `trunc(v * 10000)` in an `int64`.
For `float` the product is the **float32** product, exactly as the Go
(`int64(v * 10000.0)` with `v float32`) and Java (`(long)(v * 10000.0f)`)
targets compute it. The generated code writes it as
`float32(float64(v) * 10000.0)`: the product of two float32 values is exact in
float64, so rounding it once to float32 is bit-for-bit IEEE single-precision
multiplication, independent of how the C compiler treats float temporaries.
Decoding is `float32(n) / 10000` in single precision, like Go/Java (done as a
float64 quotient rounded once to float32, which is also exact since 53 >= 2*24+2).

Example of why it matters: `0.7'f32` encodes as `7000` (float64 math would give
`6999`). `cross_lang_test/nim` checks six such values against the Go target.

`double` uses `v * 10000.0` in float64. NaN encodes as 0; products outside the
int64 range saturate to `low(int64)`/`high(int64)` (Java's behaviour).

## Caveats

- Keyword field names are backtick-quoted (`` o.`type` ``). A field named
  `encode`/`decode` becomes `encodeField`/`decodeField` so it does not hide the
  procs. Two schema fields that are the same Nim identifier (e.g. `a_b` and
  `aB`) is a generation error.
- Objects are value types, so a class containing itself (other than through an
  array) is not supported.
- The decoder reads through a raw pointer into the input with explicit bounds
  checks on every read; it never reads past `data.len`. Array counts larger
  than the remaining input are rejected (each element takes at least one byte),
  which also excludes arrays of field-less classes longer than the rest of the
  message.
- Trailing bytes after a complete message are ignored. Varints longer than 10
  bytes, and `int` varints above 32 bits, are rejected.
- An empty-class `bpRead` emits a harmless `XCannotRaiseY` hint.
