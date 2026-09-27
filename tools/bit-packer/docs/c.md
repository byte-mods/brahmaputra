# BitPacker for C

Target name: `c`. C11, standard library only. The generated code is clean
under `gcc`/`clang` with `-std=c11 -Wall -Wextra -Wpedantic -Werror`, and the
conformance test also runs under `-fsanitize=address,undefined`. The header
can be included from C++ as well (`extern "C"`, no C++ keywords as member
names).

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang c --out ./generated
# -> generated/c/game.h, generated/c/game.c
cc -std=c11 -O2 -I generated/c app.c generated/c/game.c
```

One header and one source file per schema, named after the schema file.
Every runtime helper in `game.c` is `static`, so several generated schemas can
be linked into the same program; the shared types in the header (`bp_status`,
`bp_str`, `bp_buffer`) are behind a common include guard, so their headers can
be included together too.

## API

For every schema class `C`:

```c
typedef struct C C;
void      C_init(C *o);                                   /* zero = every default */
void      C_free(C *o);                                   /* frees what decode allocated, re-zeroes */
bp_status C_encode(const C *o, bp_buffer *out);           /* appends version + fields */
bp_status C_decode(C *out, const uint8_t *data, size_t len);
```

plus `#define GAME_SCHEMA_VERSION "1.0.0"` and the shared types:

| Type | Meaning |
|---|---|
| `bp_status` | `BP_OK` (0) or an error: `BP_ERR_TRUNCATED`, `BP_ERR_VERSION`, `BP_ERR_OVERFLOW` (varint over 10 bytes / 64 bits), `BP_ERR_LENGTH` (negative length or count), `BP_ERR_NOMEM`, `BP_ERR_DEPTH` (nesting over `BP_MAX_DEPTH` = 256), `BP_ERR_INVALID` (encode: NULL pointer with a non-zero length, or an array over `INT32_MAX`). `bp_status_str()` names them. |
| `bp_str` | `{ char *data; size_t len; }`. Decoded strings are heap-allocated, never NULL, NUL-terminated (`data[len] == 0`), and may contain embedded NULs. For encoding `data` may be NULL when `len` is 0. |
| `bp_buffer` | `{ uint8_t *data; size_t len, cap; }` growable output. Start from `{0}`; encoders append; `bp_buffer_free()` releases it. Reset `len = 0` to reuse the allocation. |

Arrays are a pointer plus an explicit length member: a field `int[] skills`
becomes `int32_t *skills; size_t skills_len;`. A nested class field is
embedded by value (`Vec3 position;`), so it can never be NULL; an array of a
class is `Item *inventory; size_t inventory_len;`.

Ownership: **the caller owns everything**.

* `C_decode` zeroes `*out`, allocates every string and array inside it, and
  returns `BP_OK`; call `C_free(out)` when done. On any error it has already
  freed what it allocated and `*out` is zeroed, so there is nothing to clean
  up and never a partial value.
* `C_encode` only reads `*o`. Build values to encode from any memory you like
  (stack arrays, string literals via `BP_STR_LIT("...")` or
  `bp_str_from(const char *)`). Such a value is not owned by BitPacker: do not
  pass it to `C_free`.
* On error `C_encode` restores `out->len`, so a failed encode leaves no
  partial message in the buffer.

```c
#include "game.h"

int32_t skills[] = {1, 2, 3};
Item sword;
Item_init(&sword);
sword.id = 1;
sword.name = BP_STR_LIT("Excalibur");

Character hero;
Character_init(&hero);
hero.name = BP_STR_LIT("Ayla");
hero.level = 12;
hero.position.x = 1;
hero.skills = skills;
hero.skills_len = 3;
hero.inventory = &sword;
hero.inventory_len = 1;

bp_buffer buf = {0};
if (Character_encode(&hero, &buf) != BP_OK) { /* only NOMEM / INVALID */ }

Character back;
bp_status st = Character_decode(&back, buf.data, buf.len);
if (st != BP_OK) {
    fprintf(stderr, "decode: %s\n", bp_status_str(st));
} else {
    printf("%s has %zu skills\n", back.name.data, back.skills_len);
    Character_free(&back);
}
bp_buffer_free(&buf);
```

## Type mapping

| Schema | C | Wire |
|---|---|---|
| `int` | `int32_t` | zigzag-32 varint |
| `long` | `int64_t` | zigzag-64 varint |
| `float` | `float` | `trunc(float32(v * 10000))` as a long (single precision) |
| `double` | `double` | `trunc(v * 10000)` as a long (double precision) |
| `bool` | `bool` | one byte |
| `string` | `bp_str` | long length + bytes |
| `T[]` | `T *f; size_t f_len;` | int count + elements |
| class `C` | `C` (by value) | fields in order |

## Caveats

* `float` and `double` are fixed-point on the wire (4 decimal places, by
  design). A `float` field is single precision throughout, like the C++,
  Java, C# and Go targets: encode is `(int64_t)(float)(v * 10000.0f)` and
  decode is `(float)n / 10000.0f`. The explicit `(float)` cast forces the
  product to be rounded to float even on a compiler that evaluates float
  expressions in wider precision (`FLT_EVAL_METHOD` != 0, e.g. x87); on
  x86-64/SSE and AArch64 (`FLT_EVAL_METHOD` 0) it is a no-op. Do not build
  the generated code with `-ffast-math`. So `0.29f` encodes as 2900, not the
  2899 a double multiply would give, and 16777.217f as 167772160. A `double`
  field uses `double` arithmetic: `(int64_t)(v * 10000.0)` and
  `n / 10000.0`. In both, truncation is toward zero, NaN encodes as 0 and
  values beyond the `int64` range saturate (no undefined behaviour).
* Strings are bytes: the encoder does not validate UTF-8 and the decoder does
  not either. Use `data`+`len`, not `strlen`, if embedded NULs are possible.
* Decoding is bounds-checked everywhere. An array count is rejected before
  allocation when the remaining input cannot hold that many elements (each
  element needs at least one byte, or the class's minimum size), so a bogus
  count cannot trigger a huge allocation.
* A varint wider than 32 bits in an `int` field keeps its low 32 bits, like
  the Go, Rust and Java targets.
* Member names that are C or C++ keywords get a trailing `_` (`class` ->
  `class_`); names starting with `bp_` too. A schema whose `foo[]` and
  `foo_len` fields would collide is rejected at generation time. A class that
  contains itself by value (not through an array) is rejected too, as it has
  infinite size.
* Not thread-hostile: there is no global state; distinct values and buffers
  can be used from different threads.
