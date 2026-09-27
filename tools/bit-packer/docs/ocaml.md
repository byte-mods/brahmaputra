# BitPacker for OCaml

Target names: `ocaml` or `ml`. Tested with OCaml 4.14 (needs >= 4.14 for
`String.is_valid_utf_8`). The generated code uses only the standard library;
no ocamlfind packages are needed.

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang ocaml --out ./generated
# -> generated/ocaml/game.ml   (with --lang ml: generated/ml/game.ml)
```

One compilation unit per schema, named after the schema file in snake_case
(`bench_complex.buff` -> `bench_complex.ml`, module `Bench_complex`).
`--package` is ignored. With dune, put the file in a library or executable
directory; by hand: `ocamlfind ocamlopt game.ml main.ml -o main`.

## API

For every schema class `Class` (snake_case `class`), the unit defines:

| Value | Meaning |
|---|---|
| `type class = { field : t; ... }` | immutable record, schema field names |
| `encode_class : class -> string` | complete message: version prefix, then the fields |
| `decode_class : string -> (class, string) result` | `Error msg` on any bad input; never raises; bytes after the message are ignored |
| `decode_class_exn : string -> class` | same, but raises `Decode_error msg` |
| `default_class : unit -> class` | every field `0`, `false`, `""`, `[]` or the nested default |

plus a submodule per class, named after the schema class (`WorldState`,
`Vec3`), that re-exports the record with its labels in scope:

```ocaml
module Character : sig
  type t = character = { name : string; level : int32; ... }
  val encode : t -> string
  val decode : string -> (t, string) result
  val decode_exn : string -> t
  val default : unit -> t
end
```

`version : string` and `exception Decode_error of string` are also defined.
Encoded messages are `string`s (immutable bytes); use `Bytes.of_string` /
`Bytes.unsafe_to_string` at I/O boundaries as needed.

**Errors.** `decode_*` returns a `result`; it is the recommended entry point.
`decode_*_exn` raises only `Decode_error`. `encode_*` raises
`Invalid_argument` for a NaN, an infinity, or a `float`/`double` whose ×10000
does not fit an `int64`; it cannot fail otherwise.

```ocaml
open Game

let hero = { (Character.default ()) with
             Character.name = "Ayla"; level = 12l;
             position = { Vec3.x = 1l; y = 2l; z = 3l } }

let () =
  let bytes = Character.encode hero in
  match Character.decode bytes with
  | Ok c -> Printf.printf "level %ld\n" c.level
  | Error msg -> prerr_endline ("bad message: " ^ msg)
```

Labels are shared between records of the same unit when two classes have a
field of the same name (`name` in `Item` and `Character`). Write records
through the class submodule (`{ Item.id = 1l; name = ...}`) or with a type
annotation, and OCaml's type-directed disambiguation picks the right one.
Labels, type names and value names that are OCaml keywords (or `t`,
`string`, `version`, ...) get a trailing underscore: `end` -> `end_`,
`type` -> `type_`.

## Type mapping

| Schema | OCaml | Notes |
|---|---|---|
| `int` | `int32` | zigzag varint; `Int32.min_int`/`max_int` round-trip |
| `long` | `int64` | zigzag varint, full 64-bit range (not the 63-bit native `int`) |
| `float` | `float` | fixed point; ×10000 computed in single precision, see below |
| `double` | `float` | fixed point, double precision |
| `bool` | `bool` | decode: any non-zero byte is `true` |
| `string` | `string` | raw UTF-8 bytes; decode rejects invalid UTF-8 |
| `T[]` | `T list` | |
| class | the record type | mutually recursive classes become one `type ... and ...` group |
| class with no fields | `unit` | |

## Caveats

- **Native `int` is 63-bit**, so `int`/`long` fields use `int32`/`int64`,
  whose arithmetic wraps exactly like the other targets.
- **All OCaml floats are doubles.** The float32 targets (C, C++, Java, C#)
  encode a `float` field as `(int64_t)(v * 10000.0f)`: a single-precision
  product. The OCaml encoder reproduces it by rounding the value to float32
  and rounding the product to float32 again
  (`Int32.float_of_bits (Int32.bits_of_float x)`); the product of two float32
  values is exact in a double, so this equals the single-precision multiply.
  It matters: `0.29` goes on the wire as 2900, where a double product would
  give 2899. Decoding a `float` field returns `float32(n) / 10000` rounded to
  float32 (the int64 -> float32 conversion is correctly rounded even above
  2^53), so the result equals the float32 value, e.g. `0.29` decodes to
  `0.28999999165534973`. `double` fields use plain double arithmetic.
- Decoding checks string lengths and array counts against the bytes left
  before reading (arrays of a field-less class excepted), so a bogus length
  cannot trigger a huge allocation.
