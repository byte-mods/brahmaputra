# BitPacker for Perl

Target names: `perl` or `pl`. Tested with Perl 5.38 (`-w` clean). Core
modules only (`Carp`, `Config`). Needs a perl built with 64-bit integers
(`perl -V:ivsize` prints 8, the default on every 64-bit platform); the module
dies at load time otherwise.

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang perl --out ./generated
# -> generated/perl/Game.pm   (with --lang pl: generated/pl/Game.pm)
perl -Igenerated/perl app.pl
```

One module per schema, named after the schema file in CamelCase
(`bench_complex.buff` -> `BenchComplex.pm`, package `BenchComplex`); pass
`--package my_proto` to name it `MyProto` instead. Each schema class `C` is
the package `Game::C`, all in that one file, so a single `use Game;` loads
everything.

## API

Objects are blessed hashes whose keys are the schema field names.

| Member | Meaning |
|---|---|
| `Game::C->new(field => value, ...)` | every field optional (`0`, `0.0`, false, `''`, `[]`, `Game::Nested->new`); an unknown field name croaks |
| `$obj->{field}` / `$obj->field` / `$obj->field($new)` | hash access or accessor (getter/setter) |
| `$obj->encode` | complete message (version prefix + fields) as a byte string |
| `Game::C->decode($bytes)` | returns a `Game::C`; dies on a wrong version, truncated or malformed input |
| `Game::C->fields` | the field names in schema order |
| `$Game::SCHEMA_VERSION` | the schema's version string |

Decode errors are plain `die` strings starting with `"Game: "` and ending in a
newline (e.g. `"Game: truncated input\n"`), so catch them with `eval` (or
`try` in 5.34+). `encode_to` / `decode_from` / `_enc` / `_dec` and the `_put_*`
/ `_get_*` subs in the top package are internals.

```perl
use Game;

my $hero = Game::Character->new(
    name      => 'Ayla',
    level     => 12,
    position  => Game::Vec3->new(x => 1, y => 2, z => 3),
    skills    => [1, 2, 3],
    inventory => [Game::Item->new(id => 1, name => 'Excalibur')],
);
my $bytes = $hero->encode;

my $back = eval { Game::Character->decode($bytes) }
    or die "bad message: $@";
print $back->name, " has ", scalar @{ $back->skills }, " skills\n";
```

## Type mapping

| Schema | Perl | Notes |
|---|---|---|
| `int` | integer (IV) | wrapped to signed 32 bits on encode |
| `long` | integer (IV) | full signed 64-bit range; a UV >= 2**63 wraps to negative |
| `float` | number (NV) | fixed point, single precision: `trunc(f32(f32(v) * 10000))`; decodes to a float32-representable value |
| `double` | number (NV) | fixed point, double precision: `trunc(v * 10000)` |
| `bool` | `!!1` / `!!0` | any true value encodes as 1; decodes to perl's core booleans |
| `string` | character string | UTF-8 on the wire |
| `T[]` | array ref | `undef` encodes as empty |
| class `C` | `Game::C` object | `undef` encodes as `Game::C->new`; a plain hash ref also works |

## Caveats

* Zigzag is done with Perl's unsigned bit operators (the module does **not**
  `use integer`, which would make `>>` arithmetic and `~` signed): a long `v`
  encodes as `(~v << 1) | 1` when negative, `v << 1` otherwise, on 64-bit
  UVs; decoding computes `-(u >> 1) - 1` so `-9223372036854775808` comes back
  as an exact IV, never an NV.
* Strings are Perl character strings: `encode` UTF-8-encodes a copy (so a
  byte string holding already-encoded UTF-8 would be encoded twice; decode
  your input first), and `decode` returns decoded character strings. Invalid
  UTF-8 on the wire is a decode error. `decode` itself needs bytes: a string
  with characters above `0xFF` dies.
* `float` fields are single precision on the wire, like the C/C++/Java/C#/Go
  targets. Perl NVs are doubles, so the code emulates float32 with
  `unpack('f', pack('f', $x))` at each step: encode is
  `f32(f32($v) * 10000)` then `int` (a product of two float32 values is
  exact in a double, so rounding it once equals a float32 multiply), and
  decode is `f32(f32($n) / 10000)`. So `0.29` encodes as 2900 (a double
  multiply would give 2899) and decodes to `0.28999999165534973`, the float32
  nearest 0.29. `double` fields use NV arithmetic (`$n / 10000` on decode).
  NaN encodes as 0 and values beyond the `long` range saturate.
* Decoding is bounds-checked: varints over 10 bytes, negative lengths,
  lengths past the end and array counts the remaining input cannot hold are
  rejected before looping; nesting deeper than `$Game::MAX_DEPTH` (256) dies.
* No accessor is generated for a field named like a method the class needs
  (`new`, `encode`, `decode`, `fields`, `isa`, `can`, `DESTROY`, ...); use
  `$obj->{name}` for those. Accessors are installed by name at load time, so
  fields such as `y`, `s` or `q` (Perl quote operators) work fine.
