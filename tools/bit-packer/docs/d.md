# BitPacker for D

Target name: `d`. Tested with LDC 1.36 (D frontend 2.106), `-w -dip1000`.
Only Phobos is used, and the whole API is `@safe`.

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang d --out ./generated
# -> generated/d/game.d   (module game;)
ldc2 app.d generated/d/game.d
```

One module per schema, named after the schema file (non-identifier characters
become `_`; a keyword gets a trailing `_`).

## API

For every schema class `C` the module has a `struct C` with:

| Member | Meaning |
|---|---|
| fields | camelCase (`is_alive` -> `isAlive`), default-initialised (0, `null` arrays/strings, `false`) |
| `ubyte[] encode() const @safe` | complete message (version prefix + fields), built with `Appender!(ubyte[])` |
| `static C decode(const(ubyte)[] data) @safe` | throws `BitPackerException` on wrong version, truncated or malformed input |
| `void encodeTo(ref Appender!(ubyte[]) w) const @safe` | fields only, no prefix (for embedding) |
| `static C decodeFrom(ref BitPackerReader r) @safe` | fields only |

plus `class BitPackerException : Exception` (the only thing `decode` throws for
any input), `enum string schemaVersion`, and `struct BitPackerReader`.

Errors are exceptions: that is the idiomatic D way for malformed input, keeps
`decode` returning the value directly, and works in `@safe` code.

```d
import game;

Character hero = {name: "Ayla", level: 12, position: Vec3(1, 2, 3)};
ubyte[] bytes = hero.encode();

try
{
    auto back = Character.decode(bytes);
    assert(back == hero);           // structs compare field by field
}
catch (BitPackerException e)
    stderr.writeln("bad message: ", e.msg);
```

Importing two generated modules makes `BitPackerException`, `schemaVersion`
and `BitPackerReader` ambiguous; qualify them (`game.BitPackerException`).

## Type mapping

| Schema | D | Notes |
|---|---|---|
| `int` | `int` | zigzag varint (D integer arithmetic wraps, so it is well defined) |
| `long` | `long` | zigzag varint, full range |
| `float` | `float` | fixed point, see below |
| `double` | `double` | fixed point, see below |
| `bool` | `bool` | decoder treats any non-zero byte as `true` |
| `string` | `string` | UTF-8; decode rejects invalid UTF-8 (`std.utf.validate`) |
| `T[]` | `T[]` | decoded arrays are freshly allocated |
| class `C` | `struct C` (value type) | |

## Floats (fixed point)

`float` and `double` go on the wire as `trunc(v * 10000)` in a `long`.
For `float` the product must be the **float32** product, as the Go
(`int64(v * 10000.0)` with `v float32`) and Java (`(long)(v * 10000.0f)`)
targets compute it. D allows the compiler to evaluate `float` expressions at
higher precision, so the generated code writes
`cast(float)(cast(double) v * 10000.0)`: a float x float product is exact in
double, so a single rounding to float is bit-for-bit IEEE single multiplication
on any backend. Decoding is `cast(float) n / 10000` in single precision like
Go/Java (computed as a double quotient rounded once to float, exact because
53 >= 2*24+2).

Example: `0.7f` encodes as `7000`; double math would give `6999`.
`cross_lang_test/d` checks six such values against the Go target.

`double` uses `v * 10000.0`. NaN encodes as 0; out-of-range products saturate
to `long.min`/`long.max` (Java's behaviour).

## Caveats

- Field names that are D keywords, or that would shadow names the struct body
  uses (`string`, `encode`, `decode`, `init`, ...) get a trailing `_`
  (`version_`, `string_`). Colliding mapped names are a generation error.
- Structs are value types, so a class containing itself (other than through an
  array) is not supported.
- Array counts larger than the remaining input are rejected before allocating
  (each element takes at least one byte); this also excludes arrays of
  field-less classes longer than the rest of the message.
- Trailing bytes after a complete message are ignored. Varints longer than 10
  bytes, and `int` varints above 32 bits, are rejected.
- Encoding an array longer than `int.max` is an `assert` failure.
