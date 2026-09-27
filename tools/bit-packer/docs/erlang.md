# BitPacker for Erlang

Target names: `erlang` or `erl`. Tested with Erlang/OTP 25. The generated
code uses only OTP (`erlang`, `lists`, `binary`, `unicode`).

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang erlang --out ./generated
# -> generated/erlang/game.hrl  generated/erlang/game.erl
#    (with --lang erl the directory is generated/erl/)
```

One header and one module per schema, both named after the schema file in
snake_case (`bench_complex.buff` -> `bench_complex.hrl` / module
`bench_complex`). `--package` is ignored. Compile with the header on the
include path: `erlc -I generated/erlang generated/erlang/game.erl`.

## API

`game.hrl` has one record per schema class, named in snake_case
(`WorldState` -> `#world_state{}`), with the schema's field names and types
in the record definition, plus `?GAME_VERSION` (the schema version as a
binary). Field names that are Erlang reserved words are quoted
(`'end'`, `'when'`, `'of'`...).

`game.erl` exports, for every class `Class` (snake_case `class`):

| Function | Meaning |
|---|---|
| `encode_class(#class{}) -> binary()` | complete message: version prefix, then the fields |
| `decode_class(binary()) -> {ok, #class{}} \| {error, Reason}` | never raises; bytes after the message are ignored |
| `version() -> binary()` | the schema version |

Error reasons are terms such as `truncated`, `varint_too_long`,
`{negative_length, N}`, `invalid_utf8` and
`{version_mismatch, Expected, Got}`; a non-binary argument gives
`{error, badarg}`.

`encode_class/1` raises `error({bitpacker, Reason})` for a value the wire
cannot carry: a non-integer in an `int`/`long` field, a non-boolean in a
`bool` field, a string that is neither a binary nor valid chardata, or a
`float`/`double` whose ×10000 does not fit an int64 (or, for `float`, is
outside the float32 range). It raises `function_clause` for a record of the
wrong class.

```erlang
-include("game.hrl").

Hero = #character{name = <<"Ayla">>, level = 12, position = #vec3{x = 1, y = 2, z = 3}},
Bin = game:encode_character(Hero),
case game:decode_character(Bin) of
    {ok, #character{level = L}} -> L;
    {error, Reason} -> error(Reason)
end.
```

## Type mapping

| Schema | Erlang | Record default | Notes |
|---|---|---|---|
| `int` | `integer()` (spec `-2147483648..2147483647`) | `0` | any integer is accepted and wrapped to 32 bits (two's complement), like a C cast: `1 bsl 32` encodes as `0` |
| `long` | `integer()` | `0` | wrapped to 64 bits the same way; decodes to the full signed range |
| `float` | `float()` | `0.0` | fixed point, see below; integers are accepted |
| `double` | `float()` | `0.0` | fixed point, see below; integers are accepted |
| `bool` | `boolean()` | `false` | decode: any non-zero byte is `true` |
| `string` | `binary()` (UTF-8) | `<<>>` | encode also accepts chardata (lists); decode validates UTF-8 |
| `T[]` | `[T]` | `[]` | |
| class | `#class{}` | `undefined` | `undefined` encodes as a default record |

Records are defined in dependency order; a field whose class is not yet
defined (mutually recursive classes) gets the type `tuple()` in the record
spec.

## Caveats

- **Integers are bignums.** Encoders mask to 32/64 bits before zigzag, so an
  out-of-range integer silently wraps rather than producing a 10-byte varint
  for an `int`. Decoders take the low 32 (or 64) bits of the varint.
- **Fixed point.** `float` and `double` are sent as `trunc(v × 10000)` as a
  `long`. Erlang floats are doubles, so for a `float` field the generated code
  rounds the value to float32 first and rounds the product to float32 again
  (via `<<X:32/float>>`). That is exactly the single-precision
  `v * 10000.0f` the C/C++/Java/C# targets compute: for example `0.29` goes on
  the wire as 2900, not the 2899 a double product would give. Decoding a
  `float` returns `float32(n) / 10000` rounded to float32 (as a double), so
  float fields compare equal to their float32 value, not the original double.
  `double` fields multiply and divide in double precision.
- Decoding never allocates from a length it has not checked: string lengths
  and array counts are compared with the bytes left first (except arrays of a
  class that can encode to zero bytes, i.e. one with no fields).
- Decoded strings are copied (`binary:copy/1`) so they do not keep the whole
  input binary alive.
