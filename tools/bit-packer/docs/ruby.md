# BitPacker for Ruby

Target names: `ruby` or `rb`. Tested with Ruby 3.3 (`ruby -w` clean);
needs Ruby 3.0+. Standard library only.

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang ruby --out ./generated
# -> generated/ruby/game.rb   (with --lang rb: generated/rb/game.rb)
```

One file per schema. Everything lives in a module named after the schema
file in CamelCase (`bench_complex.buff` -> `BenchComplex`, `edge.buff` ->
`Edge`); pass `--package my_proto` to name it `MyProto` instead.

## API

For every schema class the module has a class with:

| Member | Meaning |
|---|---|
| `attr_accessor :field, ...` | one per schema field, same snake_case name |
| `.new(field: default, ...)` | keyword initializer; every argument is optional (`0`, `0.0`, `false`, `''`, `[]`, `Nested.new`) |
| `#encode` | complete message (version prefix + fields) as a binary (`ASCII-8BIT`) `String` |
| `.decode(data)` | parses a message; raises `Module::DecodeError` on a wrong version, truncated or malformed input |
| `#to_h`, `==`, `eql?`, `hash` | structural, over all fields |
| `FIELDS` | the field names, in schema order |

`#encode_to(buf)` / `.decode_from(reader, depth)` and the `Wire` / `Reader`
helpers are internals used for nested classes. `SCHEMA_VERSION` is the
schema's version string.

`DecodeError < StandardError` is the only error `decode` raises for bad
input, whatever the input bytes are.

```ruby
require_relative 'generated/ruby/game'

hero = Game::Character.new(
  name: 'Ayla', level: 12,
  position: Game::Vec3.new(x: 1, y: 2, z: 3),
  skills: [1, 2, 3],
  inventory: [Game::Item.new(id: 1, name: 'Excalibur')]
)
bytes = hero.encode               # => "\n1.0.0..." (binary String)

begin
  back = Game::Character.decode(bytes)
  back.name                       # => "Ayla" (UTF-8)
  back == hero                    # => true
rescue Game::DecodeError => e
  warn "bad message: #{e.message}"
end
```

## Type mapping

| Schema | Ruby | Notes |
|---|---|---|
| `int` | `Integer` | wrapped to signed 32 bits on encode |
| `long` | `Integer` | wrapped to signed 64 bits on encode |
| `float` | `Float` | fixed point, single precision: `trunc(f32(f32(v) * 10000))`; decodes to a float32-representable value |
| `double` | `Float` | fixed point, double precision: `trunc(v * 10000)` |
| `bool` | `true` / `false` | any truthy value encodes as 1 |
| `string` | `String` (UTF-8) | |
| `T[]` | `Array` | `nil` encodes as empty |
| class `C` | `C` | `nil` encodes as `C.new` |

## Caveats

* Ruby integers are bignums, so encoding does the fixed-width arithmetic
  itself: an `int` is masked to 32 bits and a `long` to 64 bits before
  zigzag, exactly like the fixed-width targets (`2**31` encodes as
  `-2**31`, `2**64 - 1` as `-1`). Decoded values are always in range.
  A `Float` given for an integer field is truncated (`to_int`).
* Strings are written as UTF-8: a `UTF-8` or binary string is written as its
  bytes (`String#b`); a string in another encoding is transcoded first.
  Decoded strings are `force_encoding(UTF_8)` and must be valid UTF-8, or
  `decode` raises `DecodeError`.
* `float` fields are single precision on the wire, like the C/C++/Java/C#/Go
  targets. Ruby only has double `Float`, so the code emulates float32 with
  `[x].pack('f').unpack1('f')` at each step: encode is
  `f32(f32(v) * 10000.0)` then truncate (a product of two float32 values is
  exact in a double, so rounding it once equals a float32 multiply), and
  decode is `f32(f32(n) / 10000.0)`. So `0.29` encodes as 2900 (a double
  multiply would give 2899) and decodes to `0.28999999165534973`, the
  float32 nearest 0.29. `double` fields use plain `Float` arithmetic
  (`n / 10000.0` on decode). NaN encodes as 0 and values beyond the `long`
  range saturate.
* Decoding is bounds-checked: varints over 10 bytes, negative lengths,
  lengths past the end and array counts the remaining input cannot hold are
  rejected before any allocation; nesting deeper than 256 raises.
* Field names that would shadow a method the class relies on (`class`,
  `hash`, `encode`, `to_h`, ...) get a trailing `_`; a field name with a
  leading capital is lower-cased. Keyword field names such as `end` work
  (`obj.end`, `new(end: 1)`).
