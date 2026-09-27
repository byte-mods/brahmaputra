# BitPacker for Lua

Target name: `lua`. Tested with Lua 5.4; also runs on 5.3. Pure Lua, no C
modules. Needs Lua's native 64-bit integers (the default build), which the
module asserts at load time; Lua 5.1/5.2 and LuaJIT are not supported (no
integer subtype, no bitwise operators).

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang lua --out ./generated
# -> generated/lua/game.lua
```

One module per schema, named after the schema file:

```lua
package.path = "generated/lua/?.lua;" .. package.path
local Game = require("game")
```

## API

The module table holds `SCHEMA_VERSION` and one class per schema class. A
class is a metatable (`__index` = itself); instances are plain tables whose
keys are the schema field names.

| Member | Meaning |
|---|---|
| `Game.C.new{ field = value, ... }` | every field optional (`0`, `0.0`, `false`, `""`, `{}`, `Game.Nested.new()`); an unknown key raises |
| `obj.field` | direct field access |
| `obj:encode()` | complete message (version prefix + fields) as a Lua string |
| `Game.C.decode(bytes)` | returns the value, or `nil, errmsg` on a wrong version, truncated or malformed input; never raises |
| `Game.C.FIELDS` | field names in schema order |

Decode error messages start with `"bitpacker: "` (e.g.
`"bitpacker: truncated input"`, `"bitpacker: version mismatch: ..."`).
`encode` raises (`error`) for a value the wire cannot carry: a non-integer
number in an `int`/`long` field, or a non-string in a `string` field.
`encode_to` is an internal used for nested classes.

```lua
local hero = Game.Character.new {
  name = "Ayla", level = 12,
  position = Game.Vec3.new { x = 1, y = 2, z = 3 },
  skills = { 1, 2, 3 },
  inventory = { Game.Item.new { id = 1, name = "Excalibur" } },
}
local bytes = hero:encode()

local back, err = Game.Character.decode(bytes)
if not back then error(err) end
print(back.name, #back.skills)
```

Nested values may also be plain tables (`position = { x = 1, y = 2, z = 3 }`):
the encoder only reads fields, it never calls methods on them.

## Type mapping

| Schema | Lua | Notes |
|---|---|---|
| `int` | integer | wrapped to signed 32 bits on encode; integral floats (`3.0`) accepted |
| `long` | integer | full signed 64-bit range (`math.mininteger` .. `math.maxinteger`) |
| `float` | float | fixed point, single precision: `trunc(f32(f32(v) * 10000))`; decodes to a float32-representable value |
| `double` | float | fixed point, double precision: `trunc(v * 10000)` |
| `bool` | boolean | `new` normalises with `not not`; the encoder treats any truthy value as true |
| `string` | string | bytes, unchanged |
| `T[]` | sequence (1-based) | `nil` encodes as empty |
| class `C` | table with metatable `Game.C` | `nil` encodes as `C.new()` |

## Caveats

* Zigzag uses Lua 5.4's 64-bit integer operators; note `>>` is a *logical*
  shift in Lua, so the encoder uses `(v << 1) ~ -(v >> 63)` and the decoder
  `(u >> 1) ~ -(u & 1)`. Varint bytes are accumulated as unsigned bit
  patterns, so `-9223372036854775808` round-trips as an integer, never a
  float. Beware writing that literal in Lua source: `-9223372036854775808`
  parses as a float; use `math.mininteger`.
* `float` fields are single precision on the wire, like the C/C++/Java/C#/Go
  targets. Lua floats are doubles, so the code emulates float32 with
  `string.unpack("f", string.pack("f", x))` at each step: encode is
  `f32(f32(v) * 10000.0)` then truncate (a product of two float32 values is
  exact in a double, so rounding it once equals a float32 multiply), and
  decode is `f32(f32(n) / 10000.0)`. So `0.29` encodes as 2900 (a double
  multiply would give 2899) and decodes to `0.28999999165534973`, the float32
  nearest 0.29. `double` fields use plain float arithmetic (`n / 10000` on
  decode). NaN encodes as 0 and values beyond the `long` range saturate.
* Strings are byte strings: no UTF-8 validation on either side (use
  `utf8.len` if you need it).
* Decoding is bounds-checked: varints over 10 bytes, negative lengths,
  lengths past the end and array counts the remaining input cannot hold are
  rejected before looping; nesting deeper than 256 is an error.
* A field named like a class method (`encode`, `encode_to`) shadows it on
  instances; call `Game.C.encode(obj)` instead. Fields that are Lua keywords
  work through `obj["end"]`.
* Encoding builds a table of string pieces and `table.concat`s it once, so it
  is linear in the message size.
