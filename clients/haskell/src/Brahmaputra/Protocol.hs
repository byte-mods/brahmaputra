{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | The wire protocol: three encodings that share one connection and do
-- not agree with each other.
--
-- * The frame header is fixed big-endian: an @int32@ length prefix, then
--   api key, api version, correlation id and an @int16@-prefixed client id.
-- * A request/response body is BitPacker: every integer is a zigzag
--   varint, every string and array a varint count followed by its
--   contents, and the whole body starts with the schema version string.
-- * A record batch is neither: fixed big-endian header fields and plain
--   (non-zigzag) varints inside each record, because the broker stamps
--   offsets into it in place and validates its CRC without decoding it.
--
-- Every wire value is an explicit 'Int16' / 'Int32' / 'Int64' / 'Word64';
-- 'Int' only ever counts things in memory.
module Brahmaputra.Protocol
  ( -- * Versions
    schemaVersion
  , apiVersion
    -- * API keys
  , apiProduce, apiFetch, apiListOffsets, apiMetadata, apiJoinGroup
  , apiSyncGroup, apiHeartbeat, apiOffsetCommit, apiOffsetFetch
  , apiApiVersions, apiLeaveGroup
    -- * Error codes
  , errNone, errUnknownTopicOrPartition, errOffsetOutOfRange
  , errInvalidRequest, errUnsupportedVersion, errInternal
  , errNotLeaderOrFollower, errFencedLeaderEpoch, errUnknownLeaderEpoch
  , errNotEnoughReplicas, errUnknownMemberId, errRebalanceInProgress
  , errNotCoordinator, errIllegalGeneration, errCoordinatorLoadInProgress
  , errorName
  , retriable
    -- * Errors
  , BrahmaputraError (..)
  , isServerError
    -- * Shared types
  , TopicPartition (..)
  , Header (..)
  , IsolationLevel (..)
  , isolationLevelWire
    -- * BitPacker writer
  , BodyWriter
  , buildBody
  , wInt32, wInt64, wBool, wString, wStringArray, wRaw
    -- * BitPacker reader
  , Reader
  , runReader
  , readBody
  , decodeBody
  , peekErrorCode
  , rUvarint, rInt32, rInt64, rBool, rString, rStringArray, rBytes, rRest
  , rTake, rArray
  , failReader
    -- * Frames
  , encodeFrame
  , decodeFramePayload
    -- * Checksums and hashing
  , crc32c
  , murmur2
  , partitionForKey
    -- * Compression
  , Compression (..)
  , compressionCode
  , parseCompression
  , registerCodec
  , compress
  , decompress
    -- * Record batches
  , Record (..)
  , DecodedBatch (..)
  , encodeRecordBatch
  , decodeRecordBatches
  ) where

import Control.Exception (Exception (..), SomeException, evaluate, throwIO, try)
import Control.Monad (replicateM, when)
import Data.Array.Unboxed (UArray, listArray, (!))
import Data.Bits (complement, shiftL, shiftR, xor, (.&.), (.|.))
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Lazy as BL
import qualified Codec.Compression.GZip as GZip
import Data.Int (Int16, Int32, Int64)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.List (foldl')
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Word (Word16, Word32, Word64, Word8)
import System.IO.Unsafe (unsafePerformIO)

-- ---------------------------------------------------------------------------
-- Versions and keys
-- ---------------------------------------------------------------------------

-- | The BitPacker schema version every body carries first.
schemaVersion :: Text
schemaVersion = "1.0.0"

-- | The wire version this client speaks. The broker requires an exact match.
-- Version 4 added tombstones (null values) and @client.rack@ in Fetch.
apiVersion :: Int16
apiVersion = 4

apiProduce, apiFetch, apiListOffsets, apiMetadata, apiJoinGroup,
  apiSyncGroup, apiHeartbeat, apiOffsetCommit, apiOffsetFetch,
  apiApiVersions, apiLeaveGroup :: Int16
apiProduce = 0
apiFetch = 1
apiListOffsets = 2
apiMetadata = 3
apiJoinGroup = 7
apiSyncGroup = 8
apiHeartbeat = 9
apiOffsetCommit = 10
apiOffsetFetch = 11
apiApiVersions = 14
apiLeaveGroup = 18

errNone, errUnknownTopicOrPartition, errOffsetOutOfRange, errInvalidRequest,
  errUnsupportedVersion, errInternal, errNotLeaderOrFollower,
  errFencedLeaderEpoch, errUnknownLeaderEpoch, errNotEnoughReplicas,
  errUnknownMemberId, errRebalanceInProgress, errNotCoordinator,
  errIllegalGeneration, errCoordinatorLoadInProgress :: Int32
errNone = 0
errUnknownTopicOrPartition = 1
errOffsetOutOfRange = 2
errInvalidRequest = 3
errUnsupportedVersion = 4
errInternal = 5
errNotLeaderOrFollower = 6
errFencedLeaderEpoch = 8
errUnknownLeaderEpoch = 9
errNotEnoughReplicas = 10
errUnknownMemberId = 13
errRebalanceInProgress = 14
errNotCoordinator = 15
errIllegalGeneration = 16
errCoordinatorLoadInProgress = 17

-- | The broker's name for an error code.
errorName :: Int32 -> String
errorName code = case code of
  0 -> "NONE"
  1 -> "UNKNOWN_TOPIC_OR_PARTITION"
  2 -> "OFFSET_OUT_OF_RANGE"
  3 -> "INVALID_REQUEST"
  4 -> "UNSUPPORTED_VERSION"
  5 -> "INTERNAL"
  6 -> "NOT_LEADER_OR_FOLLOWER"
  7 -> "FENCED_BROKER_EPOCH"
  8 -> "FENCED_LEADER_EPOCH"
  9 -> "UNKNOWN_LEADER_EPOCH"
  10 -> "NOT_ENOUGH_REPLICAS"
  11 -> "FENCED_PRODUCER_EPOCH"
  12 -> "OUT_OF_ORDER_SEQUENCE"
  13 -> "UNKNOWN_MEMBER_ID"
  14 -> "REBALANCE_IN_PROGRESS"
  15 -> "NOT_COORDINATOR"
  16 -> "ILLEGAL_GENERATION"
  17 -> "COORDINATOR_LOAD_IN_PROGRESS"
  18 -> "SASL_AUTHENTICATION_FAILED"
  19 -> "AUTHORIZATION_FAILED"
  _ -> "UNKNOWN"

-- | Whether a code means "this send did not happen": every one of these
-- is returned strictly before the broker appends, so a retry cannot
-- duplicate a record.
retriable :: Int32 -> Bool
retriable code =
  code `elem` [ errNotLeaderOrFollower, errFencedLeaderEpoch, errUnknownLeaderEpoch
              , errNotEnoughReplicas, errCoordinatorLoadInProgress, errInternal ]

-- ---------------------------------------------------------------------------
-- Errors
-- ---------------------------------------------------------------------------

-- | Every failure this library reports is one of these, thrown in 'IO'.
data BrahmaputraError
  = ServerError !Int32 String
    -- ^ A non-zero error code from the broker, with what was being done.
  | ProtocolError String
    -- ^ Bytes that do not decode: a truncated body, a bad CRC, a schema
    -- version mismatch.
  | ConnectionError String
    -- ^ An I/O failure, a request timeout or a correlation mismatch. The
    -- connection is closed and marked broken; the router redials it.
  | BufferFull String
    -- ^ @buffer.memory@ stayed full for longer than @max.block.ms@.
  | NoOffsetForPartition TopicPartition
    -- ^ @auto.offset.reset=none@ and there is no position to resume from.
  | ClientError String
    -- ^ Misuse or configuration: no leader, unknown codec, not subscribed.
  deriving (Eq)

instance Show BrahmaputraError where
  show (ServerError code ctx)
    | null ctx = "broker returned " ++ errorName code ++ "[" ++ show code ++ "]"
    | otherwise = "broker returned " ++ errorName code ++ "[" ++ show code ++ "] (" ++ ctx ++ ")"
  show (ProtocolError msg) = "protocol error: " ++ msg
  show (ConnectionError msg) = "connection error: " ++ msg
  show (BufferFull msg) = "producer buffer full: " ++ msg
  show (NoOffsetForPartition tp) =
    "no committed offset for partition " ++ T.unpack (tpTopic tp) ++ "-" ++ show (tpPartition tp)
  show (ClientError msg) = msg

instance Exception BrahmaputraError

-- | The broker error code, if this is a 'ServerError'.
isServerError :: BrahmaputraError -> Maybe Int32
isServerError (ServerError code _) = Just code
isServerError _ = Nothing

-- ---------------------------------------------------------------------------
-- Shared types
-- ---------------------------------------------------------------------------

-- | One partition of one topic. Ordered by topic, then by partition as an
-- integer (never as a string), which is what the sticky assignor relies on.
data TopicPartition = TopicPartition
  { tpTopic :: !Text
  , tpPartition :: !Int32
  } deriving (Eq, Ord, Show)

-- | A record header. The value may be 'Nothing', which is distinct from an
-- empty value.
data Header = Header
  { headerKey :: !Text
  , headerValue :: !(Maybe ByteString)
  } deriving (Eq, Show)

-- | @isolation.level@.
data IsolationLevel = ReadUncommitted | ReadCommitted
  deriving (Eq, Show)

isolationLevelWire :: IsolationLevel -> Int32
isolationLevelWire ReadUncommitted = 0
isolationLevelWire ReadCommitted = 1

-- ---------------------------------------------------------------------------
-- BitPacker writer
-- ---------------------------------------------------------------------------

-- | A body under construction. Every integer is zigzag-varint encoded,
-- which is why this cannot share code with the record-batch encoder.
type BodyWriter = B.Builder

-- | Assemble a body, schema version first.
buildBody :: [BodyWriter] -> ByteString
buildBody parts = BL.toStrict (B.toLazyByteString (wString schemaVersion <> mconcat parts))

uvarint :: Word64 -> B.Builder
uvarint v
  | v < 0x80 = B.word8 (fromIntegral v)
  | otherwise = B.word8 (fromIntegral (v .&. 0x7f) .|. 0x80) <> uvarint (v `shiftR` 7)

wInt32 :: Int32 -> BodyWriter
wInt32 n = uvarint (fromIntegral (fromIntegral ((n `shiftL` 1) `xor` (n `shiftR` 31)) :: Word32))

wInt64 :: Int64 -> BodyWriter
wInt64 n = uvarint (fromIntegral ((n `shiftL` 1) `xor` (n `shiftR` 63)) :: Word64)

wBool :: Bool -> BodyWriter
wBool b = B.word8 (if b then 1 else 0)

wString :: Text -> BodyWriter
wString t = let bytes = TE.encodeUtf8 t
            in wInt32 (fromIntegral (BS.length bytes)) <> B.byteString bytes

wStringArray :: [Text] -> BodyWriter
wStringArray xs = wInt32 (fromIntegral (length xs)) <> foldMap wString xs

wRaw :: ByteString -> BodyWriter
wRaw = B.byteString

-- ---------------------------------------------------------------------------
-- BitPacker reader
-- ---------------------------------------------------------------------------

-- | A decoder over a strict 'ByteString'. Every read is bounds-checked and
-- fails the whole decode rather than returning a plausible-looking default.
newtype Reader a = Reader { unReader :: ByteString -> Either String (a, ByteString) }

instance Functor Reader where
  fmap f (Reader g) = Reader $ \bs -> case g bs of
    Left e -> Left e
    Right (a, rest) -> Right (f a, rest)

instance Applicative Reader where
  pure a = Reader $ \bs -> Right (a, bs)
  Reader f <*> Reader g = Reader $ \bs -> case f bs of
    Left e -> Left e
    Right (h, rest) -> case g rest of
      Left e -> Left e
      Right (a, rest') -> Right (h a, rest')

instance Monad Reader where
  Reader g >>= k = Reader $ \bs -> case g bs of
    Left e -> Left e
    Right (a, rest) -> unReader (k a) rest

failReader :: String -> Reader a
failReader msg = Reader $ \_ -> Left msg

-- | Run a reader over raw bytes (no schema version).
runReader :: Reader a -> ByteString -> Either String (a, ByteString)
runReader = unReader

-- | Decode a response body, verifying the schema version first. A
-- mismatch means broker and client disagree about the message shapes, so
-- failing loudly beats decoding garbage.
decodeBody :: Reader a -> ByteString -> Either String a
decodeBody body bytes = fst <$> unReader withVersion bytes
  where
    withVersion = do
      version <- rString
      when (version /= schemaVersion) $
        failReader ("schema version mismatch: broker speaks " ++ show version
                    ++ ", this client speaks " ++ show schemaVersion)
      body

-- | 'decodeBody' in 'IO', throwing 'ProtocolError'.
readBody :: ByteString -> Reader a -> IO a
readBody bytes body = either (throwIO . ProtocolError) pure (decodeBody body bytes)

-- | A response's leading error code, which every group response starts
-- with. A body that does not decode reports 'errNone', leaving the real
-- decoder to fail on it.
peekErrorCode :: ByteString -> Int32
peekErrorCode bytes = either (const errNone) id (decodeBody rInt32 bytes)

rUvarint :: Reader Word64
rUvarint = Reader (go 0 0)
  where
    go :: Word64 -> Int -> ByteString -> Either String (Word64, ByteString)
    go acc shift bs = case BS.uncons bs of
      Nothing -> Left "truncated varint"
      Just (b, rest) ->
        let acc' = acc .|. (fromIntegral (b .&. 0x7f) `shiftL` shift)
        in if b .&. 0x80 == 0
             then Right (acc', rest)
             else if shift + 7 > 63
                    then Left "varint overflows 64 bits"
                    else go acc' (shift + 7) rest

rInt32 :: Reader Int32
rInt32 = do
  v <- rUvarint
  let half = fromIntegral (v `shiftR` 1) :: Int32
  pure (half `xor` negate (fromIntegral (v .&. 1)))

rInt64 :: Reader Int64
rInt64 = do
  v <- rUvarint
  let half = fromIntegral (v `shiftR` 1) :: Int64
  pure (half `xor` negate (fromIntegral (v .&. 1)))

rBool :: Reader Bool
rBool = Reader $ \bs -> case BS.uncons bs of
  Nothing -> Left "truncated bool"
  Just (b, rest) -> Right (b /= 0, rest)

-- | Take exactly @n@ bytes. The length is compared unsigned so a huge
-- claimed length cannot wrap negative and slip past the check.
rTake :: Word64 -> Reader ByteString
rTake n = Reader $ \bs ->
  if n > fromIntegral (BS.length bs)
    then Left ("truncated: wanted " ++ show n ++ " bytes, " ++ show (BS.length bs) ++ " left")
    else Right (BS.splitAt (fromIntegral n) bs)

rBytes :: Reader ByteString
rBytes = do
  len <- rInt32
  when (len < 0) $ failReader "negative length"
  rTake (fromIntegral len)

rString :: Reader Text
rString = do
  bytes <- rBytes
  case TE.decodeUtf8' bytes of
    Left _ -> failReader "string is not valid UTF-8"
    Right t -> pure t

-- | A count-prefixed array. The count is validated against the bytes left
-- (every element is at least one byte), so a corrupt count cannot ask for
-- gigabytes.
rArray :: Reader a -> Reader [a]
rArray element = do
  count <- rInt32
  left <- Reader $ \bs -> Right (BS.length bs, bs)
  when (count < 0) $ failReader "negative array count"
  when (fromIntegral count > left) $ failReader "array count exceeds the body"
  replicateM (fromIntegral count) element

rStringArray :: Reader [Text]
rStringArray = rArray rString

rRest :: Reader ByteString
rRest = Reader $ \bs -> Right (bs, BS.empty)

-- ---------------------------------------------------------------------------
-- Frames
-- ---------------------------------------------------------------------------

-- | One complete frame, length prefix included. The header is fixed
-- big-endian because the broker must read it before it knows which body
-- decoder to use.
encodeFrame :: Int16 -> Int32 -> Text -> ByteString -> B.Builder
encodeFrame key correlationId clientId body =
  B.int32BE payloadLen
    <> B.int16BE key
    <> B.int16BE apiVersion
    <> B.int32BE correlationId
    <> B.int16BE (fromIntegral (BS.length cid))
    <> B.byteString cid
    <> B.byteString body
  where
    cid = TE.encodeUtf8 clientId
    payloadLen = fromIntegral (8 + 2 + BS.length cid + BS.length body) :: Int32

-- | Split a response frame payload into correlation id and body.
decodeFramePayload :: ByteString -> Either String (Int32, ByteString)
decodeFramePayload payload
  | BS.length payload < 10 = Left "frame payload shorter than its header"
  | otherwise =
      let correlationId = fromIntegral (be32 payload 4) :: Int32
          clientLen = fromIntegral (be16 payload 8) :: Int16
          offset = 10 + (if clientLen >= 0 then fromIntegral clientLen else 0)
      in if offset > BS.length payload
           then Left "frame client id runs past the payload"
           else Right (correlationId, BS.drop offset payload)

be16 :: ByteString -> Int -> Word16
be16 bs i = (fromIntegral (BS.index bs i) `shiftL` 8) .|. fromIntegral (BS.index bs (i + 1))

be32 :: ByteString -> Int -> Word32
be32 bs i = foldl' (\acc k -> (acc `shiftL` 8) .|. fromIntegral (BS.index bs (i + k))) 0 [0 .. 3]

be64 :: ByteString -> Int -> Word64
be64 bs i = foldl' (\acc k -> (acc `shiftL` 8) .|. fromIntegral (BS.index bs (i + k))) 0 [0 .. 7]

-- ---------------------------------------------------------------------------
-- CRC32C and murmur2
-- ---------------------------------------------------------------------------

crcTable :: UArray Word8 Word32
crcTable = listArray (0, 255) [entry (fromIntegral i) | i <- [0 .. 255 :: Int]]
  where
    entry :: Word32 -> Word32
    entry c0 = iterate step c0 !! 8
    step c = if c .&. 1 /= 0 then 0x82F63B78 `xor` (c `shiftR` 1) else c `shiftR` 1

-- | The Castagnoli CRC record batches carry — not zlib's CRC32.
crc32c :: ByteString -> Word32
crc32c = complement . BS.foldl' step 0xFFFFFFFF
  where
    step c b = (crcTable ! (fromIntegral c `xor` b)) `xor` (c `shiftR` 8)

-- | Kafka's 32-bit murmur2, transcribed rather than imported so a key lands
-- on the same partition from every driver. @murmur2 "" == 275646681@.
murmur2 :: ByteString -> Word32
murmur2 bytes = finish (tailMix (foldl' chunk h0 [0 .. chunks - 1]))
  where
    m = 0x5bd1e995 :: Word32
    len = BS.length bytes
    h0 = 0x9747b28c `xor` fromIntegral len :: Word32
    chunks = len `div` 4
    byte i = fromIntegral (BS.index bytes i) :: Word32
    chunk h i =
      let o = i * 4
          k0 = byte o .|. (byte (o + 1) `shiftL` 8) .|. (byte (o + 2) `shiftL` 16)
               .|. (byte (o + 3) `shiftL` 24)
          k1 = k0 * m
          k2 = k1 `xor` (k1 `shiftR` 24)
          k3 = k2 * m
      in (h * m) `xor` k3
    t = chunks * 4
    tailMix h = case len - t of
      3 -> (h `xor` (byte (t + 2) `shiftL` 16) `xor` (byte (t + 1) `shiftL` 8) `xor` byte t) * m
      2 -> (h `xor` (byte (t + 1) `shiftL` 8) `xor` byte t) * m
      1 -> (h `xor` byte t) * m
      _ -> h
    finish h =
      let h1 = h `xor` (h `shiftR` 13)
          h2 = h1 * m
      in h2 `xor` (h2 `shiftR` 15)

-- | @murmur2(key) % partitions@, Kafka's default partitioner. The list must
-- be non-empty and in ascending order.
partitionForKey :: ByteString -> [Int32] -> Int32
partitionForKey key partitions =
  partitions !! (fromIntegral (murmur2 key .&. 0x7fffffff) `mod` length partitions)

-- ---------------------------------------------------------------------------
-- Compression
-- ---------------------------------------------------------------------------

-- | @compression.type@. Only 'NoCompression' and 'Gzip' are built in; the
-- others work once registered with 'registerCodec'.
data Compression = NoCompression | Lz4 | Zstd | Snappy | Gzip
  deriving (Eq, Ord, Show, Enum, Bounded)

compressionCode :: Compression -> Word16
compressionCode c = case c of
  NoCompression -> 0
  Lz4 -> 1
  Zstd -> 2
  Snappy -> 3
  Gzip -> 4

codecFromCode :: Word16 -> Maybe Compression
codecFromCode code = lookup code [(compressionCode c, c) | c <- [minBound .. maxBound]]

-- | Kafka's spelling: none, gzip, lz4, zstd, snappy.
parseCompression :: String -> Either String Compression
parseCompression name = case name of
  "none" -> Right NoCompression
  "gzip" -> Right Gzip
  "lz4" -> Right Lz4
  "zstd" -> Right Zstd
  "snappy" -> Right Snappy
  _ -> Left ("unknown compression " ++ show name ++ " (none, gzip, lz4, zstd, snappy)")

type Codec = (ByteString -> IO ByteString, ByteString -> IO ByteString)

codecRegistry :: IORef (Map.Map Compression Codec)
codecRegistry = unsafePerformIO (newIORef Map.empty)
{-# NOINLINE codecRegistry #-}

-- | Plug in a codec this package does not carry, so an application that
-- wants lz4 or zstd pays for that dependency and one that does not, does
-- not. The broker's lz4 is @lz4_flex::compress_prepend_size@: a
-- little-endian @u32@ of the uncompressed length, then a raw LZ4 block.
registerCodec :: Compression
              -> (ByteString -> IO ByteString)  -- ^ compress
              -> (ByteString -> IO ByteString)  -- ^ decompress
              -> IO ()
registerCodec codec c d = atomicModifyIORef' codecRegistry (\m -> (Map.insert codec (c, d) m, ()))

maxDecompressedBytes :: Int64
maxDecompressedBytes = 256 * 1024 * 1024

compress :: Compression -> ByteString -> IO ByteString
compress NoCompression payload = pure payload
compress Gzip payload = evaluate (BL.toStrict (GZip.compress (BL.fromStrict payload)))
compress codec payload = do
  registry <- readIORef codecRegistry
  case Map.lookup codec registry of
    Just (c, _) -> c payload
    Nothing -> throwIO (ClientError (show codec ++ " compression is not registered; call registerCodec or use NoCompression/Gzip"))

decompress :: Compression -> ByteString -> IO ByteString
decompress NoCompression payload = pure payload
decompress Gzip payload = do
  -- Capped, so a hostile batch cannot name gigabytes of output.
  result <- try (evaluate (BL.toStrict (BL.take (maxDecompressedBytes + 1)
                                        (GZip.decompress (BL.fromStrict payload)))))
  case result of
    Left (e :: SomeException) -> throwIO (ProtocolError ("gzip: " ++ displayException e))
    Right out
      | fromIntegral (BS.length out) > maxDecompressedBytes ->
          throwIO (ProtocolError "gzip batch decompresses past the 256 MiB cap")
      | otherwise -> pure out
decompress codec payload = do
  registry <- readIORef codecRegistry
  case Map.lookup codec registry of
    Just (_, d) -> d payload
    Nothing -> throwIO (ClientError (show codec ++ " decompression is not registered; call registerCodec"))

-- ---------------------------------------------------------------------------
-- Record batches
-- ---------------------------------------------------------------------------

-- | One record inside a batch. A 'Nothing' value is a tombstone, distinct
-- from an empty value; a 'Nothing' key is distinct from an empty key.
data Record = Record
  { recordKey :: !(Maybe ByteString)
  , recordValue :: !(Maybe ByteString)
  , recordTimestampDelta :: !Int64
    -- ^ Milliseconds relative to the batch's max timestamp (<= 0).
  , recordHeaders :: ![Header]
  } deriving (Eq, Show)

data DecodedBatch = DecodedBatch
  { batchBaseOffset :: !Int64
  , batchMaxTimestamp :: !Int64
  , batchRecords :: ![Record]
  } deriving (Show)

batchHeaderLen, minBatchLength, producerExtensionLen :: Int
batchHeaderLen = 12
minBatchLength = 4 + 1 + 4 + 2 + 4 + 8
producerExtensionLen = 8 + 2 + 4

compressionMask, headersBit, nullValueBit :: Word16
compressionMask = 0x0007
headersBit = 0x0008
nullValueBit = 0x0040

strict :: B.Builder -> ByteString
strict = BL.toStrict . B.toLazyByteString

zigzag64 :: Int64 -> Word64
zigzag64 n = fromIntegral ((n `shiftL` 1) `xor` (n `shiftR` 63))

lenPlusOne :: Maybe ByteString -> B.Builder
lenPlusOne Nothing = uvarint 0
lenPlusOne (Just b) = uvarint (fromIntegral (BS.length b) + 1) <> B.byteString b

-- | Encode one batch exactly as the broker stores it. The broker never
-- re-encodes it: it stamps base offset and leader epoch in place (both sit
-- before the CRC) and writes these bytes to disk.
encodeRecordBatch :: Compression -> Int64 -> [Record] -> IO ByteString
encodeRecordBatch codec maxTimestamp records = do
  compressed <- compress codec payload
  let attributes = (compressionCode codec .&. compressionMask)
                   .|. (if hasHeaders then headersBit else 0)
                   .|. (if hasNulls then nullValueBit else 0)
      lastDelta = fromIntegral (max 0 (length records - 1)) :: Int32
      afterCrc = strict (B.word16BE attributes <> B.int32BE lastDelta
                         <> B.int64BE maxTimestamp <> B.byteString compressed)
      batchLength = fromIntegral (minBatchLength + BS.length compressed) :: Int32
  pure $ strict $
    B.int64BE 0 <> B.int32BE batchLength <> B.int32BE 0 <> B.word8 1
      <> B.word32BE (crc32c afterCrc) <> B.byteString afterCrc
  where
    hasHeaders = any (not . null . recordHeaders) records
    -- A Nothing value is a tombstone and needs the widened length
    -- encoding; an empty value is an ordinary record and must not trigger it.
    hasNulls = any ((== Nothing) . recordValue) records
    payload = strict (foldMap framed records)
    framed r = let bytes = strict (encodeRecord r)
               in uvarint (fromIntegral (BS.length bytes)) <> B.byteString bytes
    encodeRecord r =
      lenPlusOne (recordKey r)
        <> (if hasNulls
              then lenPlusOne (recordValue r)
              else let v = maybe BS.empty id (recordValue r)
                   in uvarint (fromIntegral (BS.length v)) <> B.byteString v)
        <> uvarint (zigzag64 (recordTimestampDelta r))
        <> (if hasHeaders then encodeHeaders (recordHeaders r) else mempty)
    encodeHeaders hs = uvarint (fromIntegral (length hs)) <> foldMap encodeHeader hs
    encodeHeader (Header k v) =
      let kb = TE.encodeUtf8 k
      in uvarint (fromIntegral (BS.length kb)) <> B.byteString kb <> lenPlusOne v

-- | Decode every batch in a fetch response's record bytes.
decodeRecordBatches :: ByteString -> IO [DecodedBatch]
decodeRecordBatches raw
  | BS.null raw = pure []
  | otherwise = do
      (batch, rest) <- decodeOne raw
      (batch :) <$> decodeRecordBatches rest

decodeOne :: ByteString -> IO (DecodedBatch, ByteString)
decodeOne bytes = do
  when (BS.length bytes < batchHeaderLen) $ bad "truncated batch header"
  let baseOffset = fromIntegral (be64 bytes 0) :: Int64
      batchLength = fromIntegral (fromIntegral (be32 bytes 8) :: Int32) :: Int
  -- Covers a negative length too.
  when (batchLength < minBatchLength) $ bad ("batch_length " ++ show batchLength ++ " too small")
  when (BS.length bytes - batchHeaderLen < batchLength) $ bad "truncated batch body"
  let body = BS.take batchLength (BS.drop batchHeaderLen bytes)
      rest = BS.drop (batchHeaderLen + batchLength) bytes
      magic = BS.index body 4
  when (magic /= 1 && magic /= 2) $ bad ("unsupported magic " ++ show magic)
  let stored = be32 body 5
      computed = crc32c (BS.drop 9 body)
  when (stored /= computed) $ bad ("crc mismatch: stored " ++ show stored ++ ", computed " ++ show computed)
  let attributes = be16 body 9
      maxTimestamp = fromIntegral (be64 body 15) :: Int64
      recordsAt = 23 + (if magic == 2 then producerExtensionLen else 0)
  when (recordsAt > BS.length body) $ bad "truncated batch producer extension"
  codec <- maybe (bad ("unknown compression " ++ show (attributes .&. compressionMask))) pure
             (codecFromCode (attributes .&. compressionMask))
  payload <- decompress codec (BS.drop recordsAt body)
  case decodeRecords (attributes .&. headersBit /= 0) (attributes .&. nullValueBit /= 0) payload of
    Left e -> bad e
    Right records -> pure (DecodedBatch baseOffset maxTimestamp records, rest)
  where
    bad :: String -> IO a
    bad = throwIO . ProtocolError

decodeRecords :: Bool -> Bool -> ByteString -> Either String [Record]
decodeRecords hasHeaders hasNulls = go []
  where
    go acc bs
      | BS.null bs = Right (reverse acc)
      | otherwise = do
          (recBytes, rest) <- runReader (rUvarint >>= rTake) bs
          (record, trailing) <- runReader recordReader recBytes
          if BS.null trailing
            then go (record : acc) rest
            else Left "trailing bytes in record"
    recordReader = do
      key <- nullable
      value <- if hasNulls then nullable else Just <$> (rUvarint >>= rTake)
      delta <- rUvarint
      let ts = fromIntegral (delta `shiftR` 1) `xor` negate (fromIntegral (delta .&. 1)) :: Int64
      headers <- if hasHeaders then headersReader else pure []
      pure (Record key value ts headers)
    -- Zero means null; otherwise length + 1. An empty value stays Just "".
    nullable = do
      n <- rUvarint
      if n == 0 then pure Nothing else Just <$> rTake (n - 1)
    headersReader = do
      count <- rUvarint
      left <- Reader $ \bs -> Right (BS.length bs, bs)
      when (count > fromIntegral left) $ failReader "record header count exceeds record"
      replicateM (fromIntegral count) $ do
        kb <- rUvarint >>= rTake
        k <- either (const (failReader "header key is not valid UTF-8")) pure (TE.decodeUtf8' kb)
        Header k <$> nullable
