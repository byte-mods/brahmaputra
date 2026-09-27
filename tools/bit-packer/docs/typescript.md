# BitPacker for TypeScript

Target names: `typescript` or `ts`. The generated file is plain TypeScript
with **no imports and no Node or DOM APIs** (UTF-8 is encoded and validated
by hand, bytes are `Uint8Array`, never `Buffer`), so the same file works in
browsers, Node, Deno and Bun. It needs an ES2020 target (it uses `bigint`).

It type-checks under `strict` plus `noUncheckedIndexedAccess`,
`exactOptionalPropertyTypes`, `noUnusedLocals`, `noUnusedParameters`,
`noImplicitOverride`, `isolatedModules` and `erasableSyntaxOnly` (so Node's
built-in type stripping can run it directly), with `lib: ["ES2020"]` only.

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang typescript --out ./generated
# -> generated/typescript/game.ts   (with --lang ts: generated/ts/game.ts)
```

One self-contained module per schema, named after the schema file.

## API

For every schema class `C` the module exports an interface and a class:

```ts
export interface CFields { /* one property per field */ }

export class C implements CFields {
  constructor(init?: Partial<CFields>);   // missing fields get zero values
  static from(value: CFields): C;         // value itself if already a C
  encode(): Uint8Array;                   // version prefix + fields
  static encode(value: CFields): Uint8Array; // encode any plain object of that shape
  // both encodes throw RangeError for a NaN/infinite/out-of-range float or double
  static decode(data: Uint8Array): C;     // throws BitPackerError
}
```

plus `export const VERSION` (the schema version string) and
`export class BitPackerError extends Error`, thrown by `decode` on a wrong
version prefix, truncated input, a varint longer than 64 bits, an `int`
outside 32 bits, a negative or oversized length/count, invalid UTF-8, or
nesting deeper than 512 classes.

Field names are converted to lowerCamelCase (`is_alive` -> `isAlive`,
`loot_table` -> `lootTable`). A name that would clash with `constructor`,
`encode`, `__proto__`, `prototype` or an `Object.prototype` member
(`toString`, `valueOf`, ...) gets a trailing `_`. Two fields that map to the
same name (`is_x` and `isX`) are a generation error, as is a cycle of
non-array class fields (such a value would be infinite).

```ts
import { WorldState, Item, BitPackerError } from "./generated/typescript/bench_complex.js";

const w = new WorldState({
  worldId: 42,
  seed: "abc",
  lootTable: [{ id: 2, name: "HealthPotion", value: 50, weight: 1, rarity: "Common" }], // plain objects are fine
});
const bytes: Uint8Array = w.encode();

try {
  const back = WorldState.decode(bytes);
  console.log(back.lootTable[0] instanceof Item); // true: decode builds class instances
} catch (e) {
  if (e instanceof BitPackerError) console.error("bad message:", e.message);
  else throw e;
}
```

The constructor converts nested plain objects into class instances, and stores
scalar arrays by reference (it does not copy them).

## Type mapping

| Schema | TypeScript | Notes |
|---|---|---|
| `int` | `number` | encoded as `v \| 0`: non-integers truncate, out-of-range values wrap to 32 bits |
| `long` | `bigint` | encoded as `BigInt.asIntN(64, v)`: wraps to 64 bits. Decodes to the exact value, including -2^63 |
| `float` | `number` | fixed point, computed in **float32**, see below. Decodes to a float32-representable `number` |
| `double` | `number` | fixed point, computed in float64 |
| `bool` | `boolean` | any non-zero byte decodes as `true` |
| `string` | `string` | UTF-8. Lone surrogates encode as U+FFFD (like `TextEncoder`); decoding rejects invalid UTF-8 and keeps a leading U+FEFF |
| `T[]` | `T[]` | |
| class `C` | `C` (in `CFields`: `CFields`) | |

## Float fixed point

`float` and `double` go on the wire as `trunc(v * 10000)` in an int64. For a
`float` field the multiplication happens in **single precision**, exactly as
the Go (`int64(v * 10000.0)` with `v float32`) and Java
(`(long)(v * 10000.0f)`) targets do: the encoder computes
`Math.fround(Math.fround(v) * 10000)`. The product of two float32 values is
exact in a double, so rounding it once with `Math.fround` is the IEEE float32
multiplication, bit for bit. Example: `1.0005` as a float32 times 10000 is
10005 in float32 but 10004.99... (so 10004) in float64; this target writes
10005 like Go and Java. The conformance test checks this.

**Encoding rejects** a `float`/`double` that is NaN, infinite, or whose scaled value does not fit in an int64 (edge/README.md: never saturate or wrap): `encode()` throws a
`RangeError` naming the field kind and value. Decoding a `float` returns
`Math.fround(Math.fround(n) / 10000)`, Java's `(float) n / 10000.0f`. (For
|n| above 2^53 the int64 is rounded to a double before float32, which can
differ from Java's direct long-to-float rounding in the last bit; such values
are far outside any sensible float field.)

## Caveats

- `decode` ignores bytes after the root value, like the other targets.
- `decode` accepts `Buffer` too (it is a `Uint8Array`); `encode` always
  returns a plain `Uint8Array`.
- Array lengths are checked against the remaining input before any element is
  read, so a bogus count cannot trigger a huge allocation. Consequently an
  array of a class with no fields cannot hold more elements than there are
  bytes left in the message.

## Conformance test

`cross_lang_test/typescript/run.sh` generates both test schemas, compiles
them and the test with `tsc` (from PATH when it is 5.8 or newer, else it
installs `typescript@5` from npm into a gitignored `.cache/`), and runs it
under Node.

Besides the bench and edge fixtures the test covers `edge/edge_float32_ref.bin`
(float values whose x10000 is inexact in float32), trailing bytes after the
root value, NaN/infinite/out-of-range floats and doubles on encode, invalid
UTF-8, negative and oversized lengths/counts, and over-long varints on decode.
