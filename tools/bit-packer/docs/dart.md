# BitPacker for Dart

Target name: `dart`. Tested with Dart 3.13 on the Dart VM; the generated code
is clean under `dart analyze --fatal-infos` and uses only `dart:core`,
`dart:convert` and `dart:typed_data` (no pub packages).

**Native platforms only (Dart VM, AOT, Flutter on Android/iOS/desktop).** A
`long` is a Dart `int`, which is a 64-bit two's-complement integer there. When
compiled to JavaScript (dart2js, DDC, Flutter web) `int` is a JS double: values
beyond 2^53 lose precision, the 64-bit literals and shifts in the generated
code do not behave, and `long` fields are wrong. Use the TypeScript target
for the web.

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang dart --out ./generated
# -> generated/dart/game.dart
```

One library per schema, named after the schema file. `--package` is ignored;
when you use several schemas in one file, import them with prefixes
(`import 'game.dart' as game;`), since each declares `bitPackerVersion` and
`BitPackerException`.

## API

For every schema class `C`:

```dart
class C {
  final T field;                         // one per schema field
  const C({this.field = zeroValue, ...}); // named, all optional
  Uint8List encode();                    // version prefix + fields; ArgumentError for a
                                         // NaN/infinite/out-of-range float or double
  static C decode(Uint8List data);       // throws BitPackerException
  // value-based operator == and hashCode (lists compare element-wise), toString
}
```

plus `const String bitPackerVersion` and
`class BitPackerException implements Exception`, thrown by `decode` on a
wrong version prefix, truncated input, a varint longer than 64 bits, an `int`
outside 32 bits, a negative or oversized length or count, invalid UTF-8, or
nesting deeper than 512 classes.

Field names become lowerCamelCase (`is_alive` -> `isAlive`). Reserved words,
built-in type names and members every object has (`hashCode`, `toString`,
`encode`, ...) get a trailing `_` (`type` -> `type_`). Two fields mapping to
one name, and a cycle of non-array class fields, are generation errors.

```dart
import 'generated/dart/game.dart';

const w = WorldState(worldId: 42, seed: 'abc', lootTable: [Item(id: 2, name: 'Potion')]);
final bytes = w.encode();

try {
  final back = WorldState.decode(bytes);
  assert(back == w);
} on BitPackerException catch (e) {
  print('bad message: ${e.message}');
}
```

Decoded lists are fixed-length (`growable: false`).

## Type mapping

| Schema | Dart | Notes |
|---|---|---|
| `int` | `int` | encoded as `v.toSigned(32)`: wraps to 32 bits |
| `long` | `int` | 64-bit on native platforms only (see above) |
| `float` | `double` | fixed point, product in float32 (see below); decodes to a float32-representable value |
| `double` | `double` | fixed point, product in float64 |
| `bool` | `bool` | any non-zero byte decodes as `true` |
| `string` | `String` | UTF-8; lone surrogates encode as U+FFFD; decoding rejects invalid UTF-8 and keeps leading U+FEFF characters (which `utf8.decode` alone would drop) |
| `T[]` | `List<T>` | default `const []` |
| class `C` | `C` | default `const C()` |

## Float fixed point

Dart has no float32 arithmetic, so a `float` field is rounded through a
one-element `Float32List`: the encoder computes
`toF32(toF32(v) * 10000.0)`. The product of two float32 values is exact in a
double, so rounding it once to float32 gives exactly the IEEE float32
multiplication that the Go (`int64(v * 10000.0)` on a `float32`) and Java
(`(long)(v * 10000.0f)`) targets perform. **Encoding rejects** a `float`/`double` that is NaN, infinite, or whose scaled value does not fit in an int64 (edge/README.md: never saturate or wrap): `encode()` throws an
`ArgumentError`. Otherwise the product is truncated toward zero. Decoding is `toF32(toF32(n) / 10000.0)`. Example:
`1.0005` encodes as 10005 (a float64 product would give 10004); the
conformance test checks this. For |n| above 2^53 the int64 is rounded to a
double before float32, which can differ from Java's direct long-to-float
rounding in the last bit; such values are far outside any sensible float
field.

## Caveats

- `decode` ignores bytes after the root value, like the other targets.
- Array counts and string lengths are checked against the remaining input
  before anything is allocated; an array of a class with no fields therefore
  cannot hold more elements than there are bytes left.

## Conformance test

`cross_lang_test/dart/run.sh` generates both test schemas, runs
`dart analyze --fatal-infos` over them and the test, then `dart run test.dart`.

Besides the bench and edge fixtures the test covers `edge/edge_float32_ref.bin`
(float values whose x10000 is inexact in float32), trailing bytes after the
root value, NaN/infinite/out-of-range floats and doubles on encode, invalid
UTF-8, negative and oversized lengths/counts, and over-long varints on decode.
