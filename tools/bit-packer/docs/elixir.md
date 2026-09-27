# BitPacker for Elixir

Target names: `elixir` or `ex`. Tested with Elixir 1.14 on OTP 25. The
generated code uses only the Elixir standard library (`Bitwise`, `String`,
`IO`) and OTP.

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang elixir --out ./generated
# -> generated/elixir/game.ex   (with --lang ex: generated/ex/game.ex)
```

One file per schema. Drop it into a Mix project's `lib/` (or compile it with
`elixirc`). Modules are namespaced by the schema file name in PascalCase
(`bench_complex.buff` -> `BenchComplex.*`, `edge.buff` -> `Edge.*`);
`--package` is ignored.

## API

For every schema class there is a module `Namespace.Class` (e.g.
`BenchComplex.WorldState`) with:

| Member | Meaning |
|---|---|
| `defstruct` | one key per schema field, same snake_case name, with a zero default |
| `@type t` | the struct type, with a typespec per field |
| `encode(t()) :: binary()` | complete message: version prefix, then the fields |
| `decode(binary()) :: {:ok, t()} \| {:error, reason}` | never raises; bytes after the message are ignored |

`encode_body/1` and `decode_body/1` (`@doc false`) are internals used for
nested classes, and `Namespace.BitPacker` (`@moduledoc false`) is the shared
wire runtime.

Error reasons are terms such as `:truncated`, `:varint_too_long`,
`{:negative_length, n}`, `:invalid_utf8`,
`{:version_mismatch, expected, got}`, and `:badarg` for a non-binary input.

`encode/1` raises `ArgumentError` for a value the wire cannot carry: a
non-integer in an `int`/`long` field, a non-boolean in a `bool` field, a
non-binary string, a non-list array, or a `float`/`double` whose ×10000 does
not fit an int64 (or, for `float`, is outside the float32 range). A struct of
another module raises `FunctionClauseError`.

```elixir
alias Game.{Character, Vec3}

hero = %Character{name: "Ayla", level: 12, position: %Vec3{x: 1, y: 2, z: 3}}
bin = Character.encode(hero)

case Character.decode(bin) do
  {:ok, %Character{level: level}} -> level
  {:error, reason} -> raise "bad message: #{inspect(reason)}"
end
```

## Type mapping

| Schema | Elixir | Default | Notes |
|---|---|---|---|
| `int` | `integer()` (spec `-2_147_483_648..2_147_483_647`) | `0` | any integer is accepted and wrapped to 32 bits (two's complement): `1 <<< 32` encodes as `0` |
| `long` | `integer()` | `0` | wrapped to 64 bits the same way; decodes to the full signed range |
| `float` | `float()` | `0.0` | fixed point, see below; integers are accepted |
| `double` | `float()` | `0.0` | fixed point, see below; integers are accepted |
| `bool` | `boolean()` | `false` | decode: any non-zero byte is `true` |
| `string` | `String.t()` | `""` | decode rejects invalid UTF-8 (`String.valid?/1`) |
| `T[]` | `[T]` | `[]` | |
| class | `Namespace.Class.t() \| nil` | `nil` | `nil` encodes as the default struct |

Nested-struct defaults are `nil` rather than `%Class{}` so modules do not
depend on each other at compile time (and mutually recursive classes work).

## Caveats

- **Integers are bignums.** Encoders mask to 32/64 bits before zigzag;
  decoders take the low 32 (or 64) bits of the varint.
- **Fixed point.** `float` and `double` are sent as `trunc(v × 10000)` as a
  `long`. Elixir floats are doubles, so for a `float` field the generated
  code rounds the value to float32 and rounds the product to float32 again
  (via `<<x::float-32>>`), which is exactly the single-precision
  `v * 10000.0f` of the C/C++/Java/C# targets (`0.29` goes on the wire as 2900,
  not 2899). A decoded `float` is `float32(n) / 10000` rounded to float32, so
  it equals the float32 value of what was sent (`0.29` comes back as
  `0.28999999165534973`). `double` fields use double precision throughout.
- Decoding checks string lengths and array counts against the bytes left
  before reading (arrays of a field-less class excepted, since those
  elements take no bytes). Decoded strings are copied so they do not keep the
  input binary alive.
