# Edge-case conformance

`bench_complex.buff` (the original cross-language test) only exercises
small `int`s, `bool`, `string` and arrays. `edge.buff` covers every type
BitPacker has, at the values encoders get wrong. `edge_ref.bin` is the
canonical `Edge` value, written by `make_ref.py`: a hand-written encoder
that shares no code with any generator. So a generator bug cannot pass by
agreeing with itself.

Every target must:

1. build the canonical value below with its generated types, encode it,
   and produce **exactly** the bytes of `edge_ref.bin`;
2. decode `edge_ref.bin` and get back every field below exactly;
3. round-trip the decoded value to the same bytes;
4. reject a wrong version prefix and a truncated buffer with an error,
   not a crash, hang or partial value;
5. encode the float32 variant (below) to exactly `edge_float32_ref.bin`
   and decode it back to the same float32 values.

## Wire rules

| Type | Encoding |
|---|---|
| `int` | zigzag (32-bit), then unsigned LEB128 varint |
| `long` | zigzag (64-bit), then unsigned LEB128 varint |
| `float` | **fixed point**, computed in **single precision**: round `v` to float32, multiply by `10000` in float32 (the float32 product), truncate toward zero to an `int64`, send as `long`. Decoders return `float32(n) / 10000` rounded to float32. Lossy by design |
| `double` | **fixed point** in double precision: `trunc(v × 10000)` (float64 product) as an `int64`, sent as `long`; decoders return `n / 10000` as a float64 |
| `bool` | one byte, `0` or `1` (decoders treat any non-zero as true) |
| `string` | UTF-8 byte length as a `long`, then the bytes |
| `T[]` | element count as an `int`, then each element |
| class | its fields in schema order, no framing |
| message | the schema `version` as a `string`, then the root class |

Further rules every target follows:

- **Encoders reject** a `float`/`double` that is NaN, ±infinity, or whose
  scaled value falls outside the `int64` range (an error or exception; they
  never saturate or wrap). Targets whose encode API has no error channel
  document how they signal it (Go panics with `ErrFloatRange`).
- **Decoders reject** invalid UTF-8 in a `string`, a wrong version, a
  truncated buffer, a VarInt longer than 10 bytes (or wider than 64 bits),
  and a negative length or one larger than the bytes left.
- **Trailing bytes** after the root class are ignored by `decode`. The
  Brahmaputra protocol relies on this: raw record batches follow the
  encoded struct in the same frame.

## The canonical value

```
Edge {
  i_min  = -2147483648        i_max = 2147483647   i_zero = 0   i_neg = -1
  l_min  = -9223372036854775808
  l_max  =  9223372036854775807                    l_neg  = -300
  f      = -1.25              d     = 1234.5625    d_neg  = -0.5
  yes    = true               no    = false
  empty  = ""
  unicode = "héllo wörld ✓ 日本 🚀"   (U+00E9, U+00F6, U+2713, U+65E5 U+672C, U+1F680)
  ints    = [0, -1, 1, -64, 64, -2147483648, 2147483647]
  longs   = [0, -1, 9223372036854775807, -9223372036854775808, 4294967296]
  floats  = [0.0, 0.5, -2.25]
  doubles = [0.0, 3.5, -1000000.25]
  bools   = [true, false, true]
  strings = ["", "a", "日本語"]
  no_ints = []
  inner   = Inner { big = 1099511627776, label = "inner" }
  inners  = [Inner { big = -1, label = "" }, Inner { big = 0, label = "x" }]
  no_inners = []
}
```

Every `float`/`double` here times 10000 is an exact integer in both
`float32` and `float64`, so no target can legitimately round differently.

## The float32 variant

`edge_float32_ref.bin` is the canonical value with `f = 0.29` and
`floats = [0.7, 16777.217, -0.29]`, values whose ×10000 is **not** exact.
In float32 they scale to `2900, 7000, 167772160, -2900`; a target that
multiplies a `float` field in double precision sends `2899, 6999,
167772167, -2899` and fails. Decoding the fixture must give back the
float32 nearest each literal (e.g. `0.29f`). Both fixtures are written by
`make_ref.py`.
A target that loses the high bit of `l_min`, or that computes the ×10000
in a narrower type than its field, fails here rather than in production.
