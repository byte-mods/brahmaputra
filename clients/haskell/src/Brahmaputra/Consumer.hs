{-# LANGUAGE OverloadedStrings #-}
-- | A partition consumer with no group coordination.
module Brahmaputra.Consumer
  ( -- * Configuration
    ConsumerConfig (..)
  , defaultConsumerConfig
    -- * Records
  , ConsumerRecord (..)
  , recordHeader
    -- * Consumer
  , Consumer
  , newConsumer
  , closeConsumer
  , withConsumer
  , consumerRouter
  , partitions
  , OffsetSpec (..)
  , listOffsets
  , fetch
  , fetchVerbose
  ) where

import Control.Exception (bracket, throwIO)
import Control.Monad (when)
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Data.Int (Int32, Int64)
import Data.Text (Text)
import qualified Data.Text as T

import Brahmaputra.Connection
import Brahmaputra.Protocol

-- | Named as Kafka names its consumer settings.
data ConsumerConfig = ConsumerConfig
  { ccClientId :: !Text                -- ^ @client.id@
  , ccFetchMaxBytes :: !Int32          -- ^ @fetch.max.bytes@ (8 MiB)
  , ccFetchMinBytes :: !Int32          -- ^ @fetch.min.bytes@ (1)
  , ccFetchMaxWaitMs :: !Int32         -- ^ @fetch.max.wait.ms@: long-poll ceiling (500)
  , ccRack :: !Text                    -- ^ @client.rack@, empty for none
  , ccIsolationLevel :: !IsolationLevel -- ^ @isolation.level@
  , ccMaxPollRecords :: !Int           -- ^ @max.poll.records@ (used by groups)
  , ccDialTimeoutMs :: !Int            -- ^ TCP connect timeout (30 s)
  , ccSocketTimeoutMs :: !Int          -- ^ client-side round-trip bound per request (120 s)
  } deriving (Show)

defaultConsumerConfig :: ConsumerConfig
defaultConsumerConfig = ConsumerConfig
  { ccClientId = "brahmaputra-haskell"
  , ccFetchMaxBytes = 8 * 1024 * 1024
  , ccFetchMinBytes = 1
  , ccFetchMaxWaitMs = 500
  , ccRack = ""
  , ccIsolationLevel = ReadUncommitted
  , ccMaxPollRecords = 500
  , ccDialTimeoutMs = 30000
  , ccSocketTimeoutMs = defaultRequestTimeoutMs
  }

-- | One record delivered to the application.
data ConsumerRecord = ConsumerRecord
  { crTopic :: !Text
  , crPartition :: !Int32
  , crOffset :: !Int64
  , crKey :: !(Maybe ByteString)
  , crValue :: !(Maybe ByteString)   -- ^ 'Nothing' is a tombstone
  , crTimestamp :: !Int64            -- ^ absolute unix milliseconds
  , crHeaders :: ![Header]
  } deriving (Eq, Show)

-- | The first value stored under a header key. 'Nothing' when absent;
-- @Just Nothing@ when present with a null value.
recordHeader :: Text -> ConsumerRecord -> Maybe (Maybe ByteString)
recordHeader key r = case [v | Header k v <- crHeaders r, k == key] of
  (v : _) -> Just v
  [] -> Nothing

data Consumer = Consumer
  { cConfig :: !ConsumerConfig
  , cRouter :: !Router
  }

newConsumer :: String -> ConsumerConfig -> IO Consumer
newConsumer address config =
  Consumer config <$> newRouter address (ccClientId config) (ccDialTimeoutMs config) (ccSocketTimeoutMs config)

closeConsumer :: Consumer -> IO ()
closeConsumer = closeRouter . cRouter

withConsumer :: String -> ConsumerConfig -> (Consumer -> IO a) -> IO a
withConsumer address config = bracket (newConsumer address config) closeConsumer

consumerRouter :: Consumer -> Router
consumerRouter = cRouter

partitions :: Consumer -> Text -> IO [Int32]
partitions c = routerPartitions (cRouter c)

-- | What 'listOffsets' resolves.
data OffsetSpec
  = Earliest              -- ^ the oldest retained offset
  | Latest                -- ^ the next offset to be written (the high watermark)
  | AtTimestamp !Int64    -- ^ the first offset at or after this unix-ms time
  deriving (Eq, Show)

offsetSpecWire :: OffsetSpec -> Int64
offsetSpecWire Earliest = -2
offsetSpecWire Latest = -1
offsetSpecWire (AtTimestamp t) = t

listOffsets :: Consumer -> Text -> Int32 -> OffsetSpec -> IO Int64
listOffsets c topic partition spec = do
  conn <- connFor (cRouter c) topic partition
  response <- request conn apiListOffsets
                (buildBody [wString topic, wInt32 partition, wInt64 (offsetSpecWire spec)])
  (code, offset) <- readBody response $ do
    _ <- rString
    _ <- rInt32
    code <- rInt32
    offset <- rInt64
    _ <- rInt64
    pure (code, offset)
  when (code /= errNone) $
    throwIO (ServerError code ("list_offsets " ++ T.unpack topic ++ "-" ++ show partition))
  pure offset

-- | Read one partition from @offset@, waiting up to @maxWaitMs@ (capped at
-- @fetch.max.wait.ms@) for data.
fetch :: Consumer -> Text -> Int32 -> Int64 -> Int32 -> IO [ConsumerRecord]
fetch c topic partition offset maxWaitMs = fst <$> fetchVerbose c topic partition offset maxWaitMs

-- | 'fetch', also returning the partition's high watermark.
fetchVerbose :: Consumer -> Text -> Int32 -> Int64 -> Int32 -> IO ([ConsumerRecord], Int64)
fetchVerbose c topic partition offset maxWaitMs = do
  let config = cConfig c
      body = buildBody
        [ wString topic, wInt32 partition, wInt64 offset, wInt32 (ccFetchMaxBytes config)
        , wInt32 (min maxWaitMs (ccFetchMaxWaitMs config)), wInt32 (ccFetchMinBytes config)
        , wInt32 (isolationLevelWire (ccIsolationLevel config)), wString (ccRack config) ]
  conn <- connFor (cRouter c) topic partition
  first <- fetchOnce conn body
  (code, highWatermark, batches) <-
    if fst3 first == errNotLeaderOrFollower
      then do
        _ <- refreshMetadata (cRouter c) topic
        conn' <- connFor (cRouter c) topic partition
        fetchOnce conn' body
      else pure first
  when (code /= errNone) $
    throwIO (ServerError code ("fetch " ++ T.unpack topic ++ "-" ++ show partition))
  let records =
        [ ConsumerRecord topic partition off (recordKey r) (recordValue r)
                         (batchMaxTimestamp b + recordTimestampDelta r) (recordHeaders r)
        | b <- batches
        , (i, r) <- zip [0 ..] (batchRecords b)
        , let off = batchBaseOffset b + i
        -- A batch can start before the requested offset.
        , off >= offset ]
  pure (records, highWatermark)
  where
    fst3 (a, _, _) = a

fetchOnce :: Conn -> ByteString -> IO (Int32, Int64, [DecodedBatch])
fetchOnce conn body = do
  response <- request conn apiFetch body
  (code, highWatermark, batchesLength, trailing) <- readBody response $ do
    _ <- rString            -- topic
    _ <- rInt32             -- partition
    code <- rInt32
    hw <- rInt64
    _ <- rInt64             -- last_stable_offset
    len <- rInt64
    -- Read even though unused: the batches trail the whole struct.
    _ <- rInt32             -- preferred_read_replica
    rest <- rRest
    pure (code, hw, len, rest)
  when (batchesLength < 0 || batchesLength > fromIntegral (BS.length trailing)) $
    throwIO (ProtocolError "fetch response claims more batch bytes than it carries")
  batches <- decodeRecordBatches (BS.take (fromIntegral batchesLength) trailing)
  pure (code, highWatermark, batches)
