{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | A batching producer.
--
-- Records are buffered per partition and each batch goes out as one
-- Produce request, either when it reaches @batch.size@ or when the linger
-- thread flushes it. A partition has at most one batch in flight, so the
-- log keeps send order whichever of the two flushes it.
module Brahmaputra.Producer
  ( -- * Configuration
    ProducerConfig (..)
  , defaultProducerConfig
  , Acks (..)
  , acksWire
    -- * Records
  , ProducerRecord (..)
  , producerRecord
    -- * Producer
  , Producer
  , newProducer
  , producerRouter
  , send
  , sendSync
  , flush
  , closeProducer
  , withProducer
  ) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Exception
import Control.Monad (forM_, void, when)
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Data.Int (Int32, Int64)
import Data.IORef
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import System.Timeout (timeout)

import Brahmaputra.Connection
import Brahmaputra.Protocol

-- | @acks@: 0 fire-and-forget, 1 leader append, all every in-sync replica.
data Acks = AcksNone | AcksLeader | AcksAll
  deriving (Eq, Show)

acksWire :: Acks -> Int32
acksWire AcksNone = 0
acksWire AcksLeader = 1
acksWire AcksAll = -1

-- | Named as Kafka names its producer settings.
data ProducerConfig = ProducerConfig
  { pcClientId :: !Text             -- ^ @client.id@
  , pcAcks :: !Acks                 -- ^ @acks@ (default 'AcksLeader')
  , pcBatchSize :: !Int             -- ^ @batch.size@ bytes per partition buffer (16 KiB)
  , pcLingerMs :: !Int              -- ^ @linger.ms@; 0 sends every record at once (default 5)
  , pcCompression :: !Compression   -- ^ @compression.type@
  , pcRequestTimeoutMs :: !Int32    -- ^ @request.timeout.ms@: broker-side wait for acks (30 s)
  , pcRetries :: !Int               -- ^ @retries@ of retriable broker errors (5)
  , pcRetryBackoffMs :: !Int        -- ^ @retry.backoff.ms@ (100)
  , pcDeliveryTimeoutMs :: !Int     -- ^ @delivery.timeout.ms@: caps a send and its retries (120 s)
  , pcBufferMemory :: !Int          -- ^ @buffer.memory@: unflushed bytes held client-side (32 MiB)
  , pcMaxBlockMs :: !Int            -- ^ @max.block.ms@: how long send blocks on a full buffer (60 s)
  , pcDialTimeoutMs :: !Int         -- ^ TCP connect timeout (30 s)
  , pcSocketTimeoutMs :: !Int       -- ^ client-side round-trip bound per request (120 s)
  } deriving (Show)

defaultProducerConfig :: ProducerConfig
defaultProducerConfig = ProducerConfig
  { pcClientId = "brahmaputra-haskell"
  , pcAcks = AcksLeader
  , pcBatchSize = 16 * 1024
  , pcLingerMs = 5
  , pcCompression = NoCompression
  , pcRequestTimeoutMs = 30000
  , pcRetries = 5
  , pcRetryBackoffMs = 100
  , pcDeliveryTimeoutMs = 120000
  , pcBufferMemory = 32 * 1024 * 1024
  , pcMaxBlockMs = 60000
  , pcDialTimeoutMs = 30000
  , pcSocketTimeoutMs = defaultRequestTimeoutMs
  }

-- | One record to send. Build with 'producerRecord' and record update
-- syntax: @(producerRecord "orders" (Just v)) { prKey = Just k }@.
data ProducerRecord = ProducerRecord
  { prTopic :: !Text
  , prPartition :: !(Maybe Int32)     -- ^ explicit partition, bypassing the partitioner
  , prKey :: !(Maybe ByteString)      -- ^ 'Nothing' is a null key, distinct from empty
  , prValue :: !(Maybe ByteString)    -- ^ 'Nothing' is a tombstone, distinct from empty
  , prHeaders :: ![Header]
  , prTimestamp :: !(Maybe Int64)     -- ^ unix ms; defaults to the send time
  } deriving (Eq, Show)

producerRecord :: Text -> Maybe ByteString -> ProducerRecord
producerRecord topic value = ProducerRecord topic Nothing Nothing value [] Nothing

data Pending = Pending !Record !Int64

data PartBuf = PartBuf
  { pbRecords :: ![Pending]   -- newest first
  , pbSize :: !Int
  }

data Producer = Producer
  { pConfig :: !ProducerConfig
  , pRouter :: !Router
  , pBuffers :: !(TVar (Map.Map TopicPartition PartBuf))
  , pBytes :: !(TVar Int)
  , pRoundRobin :: !(IORef Int)
  , pClosed :: !(TVar Bool)
  , pDone :: !(MVar ())
  -- | One lock per partition, held across a batch's round trip and its
  -- retries: without it the linger thread and a send that fills a batch
  -- could each take a batch for the same partition and race.
  , pSendLocks :: !(TVar (Map.Map TopicPartition (TMVar ())))
  -- | The first failure of a linger-driven flush. Those records have left
  -- the buffer, so this is their only trace; the next 'flush' or
  -- 'closeProducer' throws it.
  , pBackgroundErr :: !(TVar (Maybe SomeException))
  }

-- | Connect and start the linger thread. Needs the threaded RTS.
newProducer :: String -> ProducerConfig -> IO Producer
newProducer address config = do
  router <- newRouter address (pcClientId config) (pcDialTimeoutMs config) (pcSocketTimeoutMs config)
  p <- Producer config router
         <$> newTVarIO Map.empty <*> newTVarIO 0 <*> newIORef 0 <*> newTVarIO False
         <*> newEmptyMVar <*> newTVarIO Map.empty <*> newTVarIO Nothing
  if pcLingerMs config > 0
    then void (forkIO (lingerLoop p `finally` putMVar (pDone p) ()))
    else putMVar (pDone p) ()
  pure p

producerRouter :: Producer -> Router
producerRouter = pRouter

-- | 'newProducer' … 'closeProducer', exception-safe.
withProducer :: String -> ProducerConfig -> (Producer -> IO a) -> IO a
withProducer address config = bracket (newProducer address config) closeProducer

-- | Flush, stop the linger thread and release connections. The thread and
-- connections are released even when the final flush fails; that failure
-- is still thrown.
closeProducer :: Producer -> IO ()
closeProducer p = do
  flushed <- try (flush p)
  atomically (writeTVar (pClosed p) True)
  _ <- timeout 2000000 (readMVar (pDone p))
  closeRouter (pRouter p)
  either (\(e :: SomeException) -> throwIO e) pure flushed

-- | Buffer one record; 'flush' to await delivery. With batching the offset
-- is not known until the batch goes out; use 'sendSync' for one.
send :: Producer -> ProducerRecord -> IO ()
send p r = do
  partition <- maybe (choosePartition p (prTopic r) (prKey r)) pure (prPartition r)
  ts <- maybe nowMs pure (prTimestamp r)
  let record = Record (prKey r) (prValue r) 0 (prHeaders r)
      size = recordSize r
      slot = TopicPartition (prTopic r) partition
  reserve p size
  full <- atomically $ do
    bufs <- readTVar (pBuffers p)
    let PartBuf rs sz = Map.findWithDefault (PartBuf [] 0) slot bufs
        buf' = PartBuf (Pending record ts : rs) (sz + size)
    writeTVar (pBuffers p) (Map.insert slot buf' bufs)
    pure (pbSize buf' >= pcBatchSize (pConfig p))
  when (pcLingerMs (pConfig p) == 0 || full) $ void (flushPartition p slot)

-- | Send one record on its own and return its offset. A full round trip
-- per record — correct, and slow.
sendSync :: Producer -> ProducerRecord -> IO Int64
sendSync p r = do
  partition <- maybe (choosePartition p (prTopic r) (prKey r)) pure (prPartition r)
  ts <- maybe nowMs pure (prTimestamp r)
  produce p (TopicPartition (prTopic r) partition) [Pending (Record (prKey r) (prValue r) 0 (prHeaders r)) ts]

recordSize :: ProducerRecord -> Int
recordSize r =
  maybe 0 BS.length (prValue r) + maybe 0 BS.length (prKey r) + 16
    + sum [BS.length (TE.encodeUtf8 k) + maybe 0 BS.length v + 4 | Header k v <- prHeaders r]

-- | Send every buffered record and wait for acknowledgement. Also throws
-- the failure of any background (linger) flush since the last call.
flush :: Producer -> IO ()
flush p = do
  result <- try (flushAll p)
  background <- atomically (stateTVar (pBackgroundErr p) (\e -> (e, Nothing)))
  either (\(e :: SomeException) -> throwIO e) pure result
  maybe (pure ()) throwIO background

flushAll :: Producer -> IO ()
flushAll p = do
  slots <- Map.keys . Map.filter (not . null . pbRecords) <$> readTVarIO (pBuffers p)
  forM_ slots (flushPartition p)

choosePartition :: Producer -> Text -> Maybe ByteString -> IO Int32
choosePartition p topic key = do
  partitions <- routerPartitions (pRouter p) topic
  case key of
    Just k -> pure (partitionForKey k partitions)
    Nothing -> do
      i <- atomicModifyIORef' (pRoundRobin p) (\n -> (n + 1, n))
      pure (partitions !! (i `mod` length partitions))

-- | Block until @size@ more bytes may be buffered. This is what makes
-- @buffer.memory@ real: a producer faster than its broker is slowed here
-- rather than allowed to grow without limit.
reserve :: Producer -> Int -> IO ()
reserve p size
  | limit <= 0 || size >= limit =
      -- A record larger than the whole budget is admitted rather than
      -- waiting on a condition that can never hold.
      atomically (modifyTVar' (pBytes p) (+ size))
  | otherwise = do
      got <- timeout (max 0 (pcMaxBlockMs (pConfig p)) * 1000) $ atomically $ do
        used <- readTVar (pBytes p)
        if used + size > limit then retry else writeTVar (pBytes p) (used + size)
      case got of
        Just () -> pure ()
        Nothing -> do
          used <- readTVarIO (pBytes p)
          throwIO (BufferFull (show used ++ " of " ++ show limit ++ " bytes unflushed after max.block.ms="
                               ++ show (pcMaxBlockMs (pConfig p))))
  where
    limit = pcBufferMemory (pConfig p)

lingerLoop :: Producer -> IO ()
lingerLoop p = loop
  where
    loop = do
      stop <- timeout (pcLingerMs (pConfig p) * 1000) (atomically (readTVar (pClosed p) >>= check))
      case stop of
        Just () -> pure ()
        Nothing -> do
          -- A failed background flush must not kill this thread; the next
          -- explicit flush surfaces it to a caller who can act on it.
          result <- try (flushAll p)
          case result of
            Left (e :: SomeException)
              | Just (_ :: SomeAsyncException) <- fromException e -> throwIO e
              | otherwise -> atomically $ modifyTVar' (pBackgroundErr p) (maybe (Just e) Just)
            Right () -> pure ()
          loop

sendLock :: Producer -> TopicPartition -> IO (TMVar ())
sendLock p slot = atomically $ do
  locks <- readTVar (pSendLocks p)
  case Map.lookup slot locks of
    Just l -> pure l
    Nothing -> do
      l <- newTMVar ()
      writeTVar (pSendLocks p) (Map.insert slot l locks)
      pure l

flushPartition :: Producer -> TopicPartition -> IO Int64
flushPartition p slot = do
  lock <- sendLock p slot
  bracket_ (atomically (takeTMVar lock)) (atomically (putTMVar lock ())) $ do
    taken <- atomically $ do
      bufs <- readTVar (pBuffers p)
      case Map.lookup slot bufs of
        Nothing -> pure Nothing
        Just buf -> do
          writeTVar (pBuffers p) (Map.delete slot bufs)
          modifyTVar' (pBytes p) (\n -> max 0 (n - pbSize buf))
          pure (Just (reverse (pbRecords buf)))
    case taken of
      Nothing -> pure (-1)
      Just [] -> pure (-1)
      Just batch -> produce p slot batch

produce :: Producer -> TopicPartition -> [Pending] -> IO Int64
produce _ _ [] = pure (-1)
produce p (TopicPartition topic partition) batch = do
  -- One base timestamp per batch and a delta per record, so maxTimestamp
  -- is the newest record's time.
  let maxTs = maximum [ts | Pending _ ts <- batch]
      records = [r { recordTimestampDelta = ts - maxTs } | Pending r ts <- batch]
      config = pConfig p
  encoded <- encodeRecordBatch (pcCompression config) maxTs records
  let body = buildBody [ wString topic, wInt32 partition, wInt32 (acksWire (pcAcks config))
                       , wInt32 (pcRequestTimeoutMs config)
                       , wInt64 (fromIntegral (BS.length encoded)), wRaw encoded ]
      context = "produce to " ++ T.unpack topic ++ "-" ++ show partition
  if pcAcks config == AcksNone
    then do
      conn <- connFor (pRouter p) topic partition
      sendOneway conn apiProduce body
      pure (-1)
    else do
      start <- monoMs
      let deadline = start + fromIntegral (pcDeliveryTimeoutMs config)
          attempt left = do
            conn <- connFor (pRouter p) topic partition
            response <- request conn apiProduce body
            (code, baseOffset) <- readBody response $ do
              _ <- rString
              _ <- rInt32
              code <- rInt32
              base <- rInt64
              _ <- rInt64
              pure (code, base)
            now <- monoMs
            if code == errNone
              then pure baseOffset
              else if not (retriable code) || left <= 0 || now > deadline
                then throwIO (ServerError code context)
                else do
                  -- A stale route is the most common retriable cause.
                  when (code `elem` [errNotLeaderOrFollower, errFencedLeaderEpoch, errUnknownLeaderEpoch]) $
                    void (try (refreshMetadata (pRouter p) topic) :: IO (Either BrahmaputraError ClusterMetadata))
                  threadDelay (pcRetryBackoffMs config * 1000)
                  attempt (left - 1)
      attempt (pcRetries config)
