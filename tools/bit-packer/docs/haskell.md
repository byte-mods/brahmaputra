# BitPacker for Haskell

Target names: `haskell` or `hs`. Tested with GHC 9.4. The generated module
depends only on `base`, `bytestring` and `text`, all of which ship with GHC
(checked with `ghc-pkg list`: `bytestring-0.11`, `text-2.0`).

## Generate

```sh
go build -o bitpacker ./cmd/bitpacker
./bitpacker --file game.buff --lang haskell --out ./generated
# -> generated/haskell/Game.hs   (with --lang hs: generated/hs/Game.hs)
```

One module per schema, named after the schema file in PascalCase
(`bench_complex.buff` -> `module BenchComplex` in `BenchComplex.hs`).
`--package` is ignored. Add the directory to your source path
(`ghc -igenerated/haskell`, or `hs-source-dirs` in Cabal, with
`build-depends: base, bytestring, text`).

## API

For every schema class `Class`:

| Export | Meaning |
|---|---|
| `data Class = Class { classField :: !T, ... }` | strict record; `deriving (Eq, Show)` |
| `encodeClass :: Class -> ByteString` | complete message (strict `Data.ByteString`): version prefix, then the fields |
| `decodeClass :: ByteString -> Either String Class` | `Left` with a message on any bad input; bytes after the message are ignored |
| `defaultClass :: Class` | every field `0`, `False`, empty text/list or the nested default |

Also exported: `schemaVersion :: Text`, and the class `BitPacked` (abstract;
every generated record is an instance) with the polymorphic
`encodeMessage :: BitPacked a => a -> ByteString` and
`decodeMessage :: BitPacked a => ByteString -> Either String a`.

Record fields are prefixed with the class name in lowerCamelCase so that
fields with the same schema name in different classes do not clash:
`WorldState.world_id` -> `worldStateWorldId`, `Character.is_alive` ->
`characterIsAlive`.

Encoding goes through `Data.ByteString.Builder`; decoding is a small
bounds-checked parser over the strict input (`Either String`), so it never
throws for any input.

```haskell
import qualified Data.Text as T
import Game

hero :: Character
hero = defaultCharacter
  { characterName = T.pack "Ayla"
  , characterLevel = 12
  , characterPosition = Vec3 1 2 3
  }

main :: IO ()
main = do
  let bytes = encodeCharacter hero
  case decodeCharacter bytes of
    Left err -> putStrLn ("bad message: " ++ err)
    Right c  -> print (characterLevel c)
```

## Type mapping

| Schema | Haskell | Notes |
|---|---|---|
| `int` | `Int32` | zigzag varint; wraps like every fixed-width target |
| `long` | `Int64` | zigzag varint, full 64-bit range |
| `float` | `Float` | fixed point; the ×10000 is a `Float` (single-precision) product |
| `double` | `Double` | fixed point |
| `bool` | `Bool` | decode: any non-zero byte is `True` |
| `string` | `Data.Text.Text` | UTF-8 on the wire (`encodeUtf8` / `decodeUtf8'`); invalid UTF-8 is a `Left` |
| `T[]` | `[T]` | |
| class | the record type | recursive classes are fine |

## Caveats

- **Fixed point.** `float`/`double` are sent as `trunc(v × 10000)` as a
  `long`; decoders return `fromIntegral n / 10000` in the field's own type.
  Because `float` fields are `Float`, the product is computed in single
  precision exactly like the C/C++/Java/C# targets (`0.29` goes on the wire as
  2900, not 2899).
- **Encoding a NaN, an infinity, or a value whose ×10000 does not fit an
  `Int64` calls `error`** (an `ErrorCall` exception when the resulting
  `ByteString` is forced), since `encodeClass` is pure and total otherwise.
  Validate such values first if they can occur.
- Decoding checks string lengths and array counts against the bytes left
  before reading (arrays of a field-less class excepted); decoded `Text`
  values do not share memory with the input.
