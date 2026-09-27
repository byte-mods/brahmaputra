-- Cross-language conformance test for the BitPacker Haskell target.
-- Run through run.sh, which generates the modules and compiles this file.
module Main (main) where

import Control.Exception (ErrorCall, SomeException, evaluate, try)
import Data.Bits (xor)
import qualified Data.ByteString as B
import Data.IORef
import Data.Either (isLeft)
import Data.List (isInfixOf)
import qualified Data.Text as T
import System.Environment (getArgs)
import System.Exit (exitWith, ExitCode (..))
import System.FilePath (takeDirectory, (</>))

import qualified BenchComplex as BC
import qualified Edge as E

type Counter = IORef (Int, Int)

check :: Counter -> String -> Bool -> String -> IO ()
check ref name ok detail
  | ok = do
      modifyIORef' ref (\(p, f) -> (p + 1, f))
      putStrLn ("  ok   " ++ name)
  | otherwise = do
      modifyIORef' ref (\(p, f) -> (p, f + 1))
      putStrLn ("  FAIL " ++ name ++ " (" ++ detail ++ ")")

eq :: (Eq a, Show a) => Counter -> String -> a -> a -> IO ()
eq ref name got want = check ref name (got == want) ("got " ++ show got ++ ", want " ++ show want)

world :: BC.WorldState
world = BC.WorldState
  { BC.worldStateWorldId = 42
  , BC.worldStateSeed = T.pack "cross_lang_test"
  , BC.worldStateGuilds =
      [ BC.Guild
          { BC.guildName = T.pack "TestGuild"
          , BC.guildDescription = T.pack "A test guild for cross-language"
          , BC.guildMembers =
              [ BC.Character
                  { BC.characterName = T.pack "TestHero"
                  , BC.characterLevel = 99
                  , BC.characterHp = 1000
                  , BC.characterMp = 500
                  , BC.characterIsAlive = True
                  , BC.characterPosition = BC.Vec3 10 (-20) 30
                  , BC.characterSkills = [1, 2, 3, 100]
                  , BC.characterInventory = [BC.Item 1 (T.pack "Excalibur") 9999 15 (T.pack "Legendary")]
                  }
              ]
          }
      ]
  , BC.worldStateLootTable = [BC.Item 2 (T.pack "HealthPotion") 50 1 (T.pack "Common")]
  }

canonical :: E.Edge
canonical = E.defaultEdge
  { E.edgeIMin = -2147483648, E.edgeIMax = 2147483647, E.edgeIZero = 0, E.edgeINeg = -1
  , E.edgeLMin = -9223372036854775808, E.edgeLMax = 9223372036854775807, E.edgeLNeg = -300
  , E.edgeF = -1.25, E.edgeD = 1234.5625, E.edgeDNeg = -0.5
  , E.edgeYes = True, E.edgeNo = False
  , E.edgeEmpty = T.empty
  , E.edgeUnicode = T.pack "h\x00E9llo w\x00F6rld \x2713 \x65E5\x672C \x1F680"
  , E.edgeInts = [0, -1, 1, -64, 64, -2147483648, 2147483647]
  , E.edgeLongs = [0, -1, 9223372036854775807, -9223372036854775808, 4294967296]
  , E.edgeFloats = [0.0, 0.5, -2.25]
  , E.edgeDoubles = [0.0, 3.5, -1000000.25]
  , E.edgeBools = [True, False, True]
  , E.edgeStrings = [T.empty, T.pack "a", T.pack "\x65E5\x672C\x8A9E"]
  , E.edgeNoInts = []
  , E.edgeInner = E.Inner 1099511627776 (T.pack "inner")
  , E.edgeInners = [E.Inner (-1) T.empty, E.Inner 0 (T.pack "x")]
  , E.edgeNoInners = []
  }

uv :: Integer -> [Integer]
uv n | n < 128 = [n]
     | otherwise = (n `mod` 128 + 128) : uv (n `div` 128)

isLeftSafe :: Either String a -> IO Bool
isLeftSafe r = do
  x <- try (evaluate (isLeft r)) :: IO (Either SomeException Bool)
  return (either (const False) id x)

main :: IO ()
main = do
  [here] <- getArgs
  let parent = takeDirectory here
  ref <- newIORef (0, 0)

  -- bench
  benchRef <- B.readFile (parent </> "test_data.bin")
  let enc = BC.encodeWorldState world
  B.writeFile (parent </> "test_data_haskell.bin") enc
  check ref "bench: encode == test_data.bin" (enc == benchRef)
    (show (B.length enc) ++ " vs " ++ show (B.length benchRef) ++ " bytes")
  case BC.decodeWorldState benchRef of
    Left err -> check ref "bench: decode test_data.bin" False err
    Right w -> do
      check ref "bench: decode test_data.bin" True ""
      eq ref "bench: world_id" (BC.worldStateWorldId w) 42
      eq ref "bench: seed" (BC.worldStateSeed w) (T.pack "cross_lang_test")
      eq ref "bench: guilds length" (length (BC.worldStateGuilds w)) 1
      let g = head (BC.worldStateGuilds w)
      eq ref "bench: guild name" (BC.guildName g) (T.pack "TestGuild")
      eq ref "bench: guild description" (BC.guildDescription g) (T.pack "A test guild for cross-language")
      eq ref "bench: members length" (length (BC.guildMembers g)) 1
      let h = head (BC.guildMembers g)
      eq ref "bench: hero name" (BC.characterName h) (T.pack "TestHero")
      eq ref "bench: hero level" (BC.characterLevel h) 99
      eq ref "bench: hero hp" (BC.characterHp h) 1000
      eq ref "bench: hero mp" (BC.characterMp h) 500
      eq ref "bench: hero is_alive" (BC.characterIsAlive h) True
      eq ref "bench: position" (BC.characterPosition h) (BC.Vec3 10 (-20) 30)
      eq ref "bench: skills" (BC.characterSkills h) [1, 2, 3, 100]
      eq ref "bench: inventory length" (length (BC.characterInventory h)) 1
      let s = head (BC.characterInventory h)
      eq ref "bench: sword name" (BC.itemName s) (T.pack "Excalibur")
      eq ref "bench: sword value" (BC.itemValue s) 9999
      eq ref "bench: sword rarity" (BC.itemRarity s) (T.pack "Legendary")
      eq ref "bench: loot length" (length (BC.worldStateLootTable w)) 1
      let p = head (BC.worldStateLootTable w)
      eq ref "bench: potion name" (BC.itemName p) (T.pack "HealthPotion")
      eq ref "bench: potion rarity" (BC.itemRarity p) (T.pack "Common")
      eq ref "bench: re-encode decoded == test_data.bin" (BC.encodeWorldState w) benchRef
  eq ref "bench: round-trip" (BC.decodeWorldState enc) (Right world)

  -- edge
  edgeRef <- B.readFile (parent </> "edge" </> "edge_ref.bin")
  let eenc = E.encodeEdge canonical
  check ref "edge: encode == edge_ref.bin" (eenc == edgeRef)
    (show (B.length eenc) ++ " vs " ++ show (B.length edgeRef) ++ " bytes")
  case E.decodeEdge edgeRef of
    Left err -> check ref "edge: decode edge_ref.bin" False err
    Right d -> do
      check ref "edge: decode edge_ref.bin" True ""
      let c = canonical
          f :: (Eq a, Show a) => String -> (E.Edge -> a) -> IO ()
          f name sel = eq ref ("edge: field " ++ name) (sel d) (sel c)
      f "i_min" E.edgeIMin
      f "i_max" E.edgeIMax
      f "i_zero" E.edgeIZero
      f "i_neg" E.edgeINeg
      f "l_min" E.edgeLMin
      f "l_max" E.edgeLMax
      f "l_neg" E.edgeLNeg
      f "f" E.edgeF
      f "d" E.edgeD
      f "d_neg" E.edgeDNeg
      f "yes" E.edgeYes
      f "no" E.edgeNo
      f "empty" E.edgeEmpty
      f "unicode" E.edgeUnicode
      f "ints" E.edgeInts
      f "longs" E.edgeLongs
      f "floats" E.edgeFloats
      f "doubles" E.edgeDoubles
      f "bools" E.edgeBools
      f "strings" E.edgeStrings
      f "no_ints" E.edgeNoInts
      f "inner" E.edgeInner
      f "inners" E.edgeInners
      f "no_inners" E.edgeNoInners
      eq ref "edge: decoded == canonical" d c
      eq ref "edge: re-encode decoded == edge_ref.bin" (E.encodeEdge d) edgeRef
  let bad = B.take 5 edgeRef <> B.singleton (B.index edgeRef 5 `xor` 1) <> B.drop 6 edgeRef
  case E.decodeEdge bad of
    Left err -> check ref "edge: wrong version rejected" ("version" `isInfixOf` err) err
    Right _ -> check ref "edge: wrong version rejected" False "decoded"
  accepted <- fmap concat $ mapM (\l -> do
      rejected <- isLeftSafe (E.decodeEdge (B.take l edgeRef))
      return [l | not rejected]) [0 .. B.length edgeRef - 1]
  check ref ("edge: all " ++ show (B.length edgeRef) ++ " truncations rejected") (null accepted)
    ("accepted prefixes " ++ show accepted)

  -- extras
  garbage <- isLeftSafe (E.decodeEdge (B.pack [255, 255, 255]))
  check ref "extra: garbage input is an error" garbage ""
  let huge = B.pack ([10] ++ map (fromIntegral . fromEnum) "1.0.0" ++ [84, 0] ++ map fromIntegral (uv 4000000000))
  hugeRejected <- isLeftSafe (BC.decodeWorldState huge)
  check ref "extra: huge array count is an error" hugeRejected ""
  r <- try (evaluate (E.encodeEdge canonical { E.edgeF = 1.0e30 })) :: IO (Either ErrorCall B.ByteString)
  check ref "extra: out-of-range float raises" (isLeft r) ""

  -- float32(0.29) * 10000 is 2899.9999... in double but 2900 in single
  -- precision, which is what the float32 targets put on the wire.
  eq ref "extra: float field x10000 in single precision (0.29)"
    (E.edgeF <$> E.decodeEdge (E.encodeEdge canonical { E.edgeF = 0.29 })) (Right 0.29)

  (p, f) <- readIORef ref
  putStrLn ("haskell: " ++ show p ++ " passed, " ++ show f ++ " failed")
  exitWith (if f == 0 then ExitSuccess else ExitFailure 1)
