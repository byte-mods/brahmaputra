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
   not a crash, hang or partial value.

## Wire rules

| Type | Encoding |
|---|---|
| `int` | zigzag (32-bit), then unsigned LEB128 varint |
| `long` | zigzag (64-bit), then unsigned LEB128 varint |
| `float`, `double` | **fixed point**: `trunc(v × 10000)` as an `int64`, then as `long`. Lossy by design; decoders return `n / 10000` |
| `bool` | one byte, `0` or `1` (decoders treat any non-zero as true) |
| `string` | UTF-8 byte length as a `long`, then the bytes |
| `T[]` | element count as an `int`, then each element |
| class | its fields in schema order, no framing |
| message | the schema `version` as a `string`, then the root class |

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
A target that loses the high bit of `l_min`, or that computes the ×10000
in a narrower type than its field, fails here rather than in production.
