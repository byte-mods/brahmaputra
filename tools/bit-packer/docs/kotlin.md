# BitPacker for Kotlin

Target names: `kotlin` or `kt`. Kotlin/JVM, tested with Kotlin 2.2 on JDK 21
and compiled with `-Werror`. The generated code uses only the Kotlin standard
library and `java.nio` (for strict UTF-8 decoding), so it also works on
Android.

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang kotlin --package com.example.game --out ./generated
# -> generated/kotlin/game.kt   (with --lang kt: generated/kt/game.kt)
```

One file per schema, in package `--package` (default `generated`). The file
also declares `VERSION`, `BitPackerException` and two internal helper
classes, so **give each schema its own package**; two schemas in one package
would declare them twice.

## API

For every schema class `C`:

```kotlin
public data class C(
    val field: T = zeroValue,   // one per schema field, all with defaults
    ...
) {
    fun encode(): ByteArray                    // version prefix + fields; IllegalArgumentException
                                               // for a NaN/infinite/out-of-range float or double
    companion object {
        @JvmStatic @Throws(BitPackerException::class)
        fun decode(data: ByteArray): C
    }
}
```

plus `const val VERSION: String` and `class BitPackerException : Exception`.

**Error handling: `decode` throws.** It throws `BitPackerException` (and
nothing else) on a wrong version prefix, truncated input, a varint longer
than 64 bits, an `int` outside 32 bits, a negative or oversized length or
count, invalid UTF-8, or nesting deeper than 512 classes. Throwing was chosen
over `Result` because it is what Java callers expect (`@Throws` makes it a
checked exception there) and because Kotlin's `Result` is awkward as a public
return type; wrap the call in `runCatching { C.decode(bytes) }` when you want
a `Result<C>`.

Field names become lowerCamelCase (`is_alive` -> `isAlive`); Kotlin keywords
are escaped with backticks (`` `in` ``). Two fields mapping to one name, and a
cycle of non-array class fields, are generation errors. A class with no
fields becomes a plain class with value equality (a data class needs a
property).

```kotlin
import com.example.game.*

val w = WorldState(worldId = 42, seed = "abc", lootTable = listOf(Item(id = 2, name = "Potion")))
val bytes: ByteArray = w.encode()

val back = try { WorldState.decode(bytes) } catch (e: BitPackerException) { null }
check(back == w)                                // data-class equality, lists compare by content

val result: Result<WorldState> = runCatching { WorldState.decode(bytes) }
```

## Type mapping

| Schema | Kotlin | Notes |
|---|---|---|
| `int` | `Int` | |
| `long` | `Long` | |
| `float` | `Float` | fixed point, product in float32 (see below) |
| `double` | `Double` | fixed point, product in float64 |
| `bool` | `Boolean` | any non-zero byte decodes as `true` |
| `string` | `String` | UTF-8; unpaired surrogates encode as `?` (JDK behaviour); decoding rejects malformed UTF-8 |
| `T[]` | `List<T>` | decoded as an `ArrayList`; a `List` keeps data-class `equals` meaningful (arrays compare by identity) |
| class `C` | `C` | default value `C()` |

## Float fixed point

`float`/`double` travel as `trunc(v * 10000)` in an int64. For `float` the
encoder computes `v * 10000.0f`: a float32 multiplication, byte-for-byte the
product the Java target's `(long)(v * 10000.0f)` and the Go target's
`int64(v * 10000.0)` (on a `float32`) truncate. **Encoding rejects** a `float`/`double` that is NaN, infinite, or whose scaled value does not fit in an int64 (edge/README.md: never saturate or wrap): `encode()`
throws `IllegalArgumentException`; otherwise the product is truncated toward
zero with `toLong()`. `double` uses `(v * 10000.0).toLong()`. Decoding is
`n.toFloat() / 10000.0f` and `n.toDouble() / 10000.0`. Example: `1.0005f`
encodes as 10005 (a float64 product would give 10004); the conformance test
checks this.

## Caveats

- `decode` ignores bytes after the root value, like the other targets.
- Array counts and string lengths are checked against the remaining input
  before anything is allocated. Consequently an array of a class with no
  fields cannot hold more elements than there are bytes left.

## Conformance test

`cross_lang_test/kotlin/run.sh` downloads `kotlin-compiler-embeddable` 2.2.20
and its runtime dependencies from Maven Central (falling back to
repo.maven.apache.org, SHA-1 verified) into a gitignored `.cache/` on first
use, compiles the generated code and `Test.kt` with `-Werror`, and runs the
test on the JVM. The apt `kotlinc` (1.3) is not used.

Besides the bench and edge fixtures the test covers `edge/edge_float32_ref.bin`
(float values whose x10000 is inexact in float32), trailing bytes after the
root value, NaN/infinite/out-of-range floats and doubles on encode, invalid
UTF-8, negative and oversized lengths/counts, and over-long varints on decode.
