# BitPacker for Scala

Target name: `scala`. The generated code uses only the Scala standard library
and `java.nio`. It compiles warning-free (`-Xlint -Werror`) with Scala 2.13
(tested with 2.13.18, the version the conformance test uses) and Scala 3
(checked with 3.3.6 LTS).

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang scala --package com.example.game --out ./generated
# -> generated/scala/game.scala
```

One file per schema, in package `--package` (default `generated`). The file
also declares `object BitPacker` and package-private helper classes, so
**give each schema its own package**.

## API

For every schema class `C`:

```scala
final case class C(field: T = zeroValue, ...) {   // one parameter per field
  def encode(): Array[Byte]                        // version prefix + fields; throws
                                                   // IllegalArgumentException for a NaN/infinite/
                                                   // out-of-range float or double
}
object C {
  def decode(data: Array[Byte]): Either[String, C]
}
```

plus `BitPacker.Version`, the schema version string.

`decode` never throws for bad input: it returns `Left(reason)` on a wrong
version prefix, truncated input, a varint longer than 64 bits, an `int`
outside 32 bits, a negative or oversized length or count, invalid UTF-8, or
nesting deeper than 512 classes.

Field names become lowerCamelCase (`is_alive` -> `isAlive`). Keywords are
back-quoted (`` `type` ``); a name that clashes with a case-class member
(`copy`, `hashCode`, `productArity`, `encode`, ...) gets a trailing `_`. Two
fields mapping to one name, and a cycle of non-array class fields, are
generation errors.

```scala
import com.example.game._

val w = WorldState(worldId = 42, seed = "abc", lootTable = Vector(Item(id = 2, name = "Potion")))
val bytes: Array[Byte] = w.encode()

WorldState.decode(bytes) match {
  case Right(back) => assert(back == w)
  case Left(err)   => println(s"bad message: $err")
}
```

## Type mapping

| Schema | Scala | Notes |
|---|---|---|
| `int` | `Int` | |
| `long` | `Long` | |
| `float` | `Float` | fixed point, product in float32 (see below) |
| `double` | `Double` | fixed point, product in float64 |
| `bool` | `Boolean` | any non-zero byte decodes as `true` |
| `string` | `String` | UTF-8; unpaired surrogates encode as `?` (JDK behaviour); decoding rejects malformed UTF-8 |
| `T[]` | `Vector[T]` | immutable, so case-class equality compares contents |
| class `C` | `C` | default value `C()` |

## Float fixed point

`float`/`double` travel as `trunc(v * 10000)` in an int64. For `float` the
encoder computes `v * 10000.0f`: a float32 multiplication, the same product
the Java target's `(long)(v * 10000.0f)` and the Go target's float32
`int64(v * 10000.0)` truncate. **Encoding rejects** a `float`/`double` that is NaN, infinite, or whose scaled value does not fit in an int64 (edge/README.md: never saturate or wrap): `encode()` has no error
channel (it returns `Array[Byte]`), so it throws `IllegalArgumentException`;
otherwise the product is truncated toward zero with `toLong`.
`double` uses `(v * 10000.0).toLong`. Decoding is `n.toFloat / 10000.0f` and
`n.toDouble / 10000.0`. Example: `1.0005f` encodes as 10005 (a float64 product
would give 10004); the conformance test checks this.

## Caveats

- `decode` ignores bytes after the root value, like the other targets.
- Case classes with more than 22 fields are fine for encode/decode, but
  Scala 2 does not generate `unapply`/`tupled` for them.
- Array counts and string lengths are checked against the remaining input
  before anything is allocated; an array of a class with no fields therefore
  cannot hold more elements than there are bytes left.

## Conformance test

`cross_lang_test/scala/run.sh` downloads `scala-compiler`, `scala-library`
and `scala-reflect` 2.13.18 from Maven Central (falling back to
repo.maven.apache.org, SHA-1 verified) into a gitignored `.cache/`, compiles
with `-deprecation -feature -Xlint:_ -Werror`, and runs the test on the JVM.
The apt `scala` (2.11) is not used.

Besides the bench and edge fixtures the test covers `edge/edge_float32_ref.bin`
(float values whose x10000 is inexact in float32), trailing bytes after the
root value, NaN/infinite/out-of-range floats and doubles on encode, invalid
UTF-8, negative and oversized lengths/counts, and over-long varints on decode.
