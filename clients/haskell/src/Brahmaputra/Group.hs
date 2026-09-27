{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | Consumer groups: join/sync/heartbeat with generation fencing,
-- assignment, commits, and the two liveness deadlines.
--
-- A 'GroupConsumer' is single-threaded by design, as Kafka's is: call
-- 'poll', 'commit' and 'closeGroupConsumer' from one thread. A background
-- heartbeat thread shares only the membership fields, through STM.
module Brahmaputra.Group
  ( -- * Configuration
    GroupConfig (..)
  , defaultGroupConfig
  , AutoOffsetReset (..)
  , Assignor (..)
    -- * Group consumer
  , GroupConsumer
  , newGroupConsumer
  , withGroupConsumer
  , subscribe
  , poll
  , commit
  , committed
  , assignment
  , closeGroupConsumer
  , groupConsumer
  , offsetsTopic
  ) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Exception
import Control.Monad (forM, forM_, unless, void, when)
import Data.ByteString (ByteString)
import Data.Foldable (toList)
import Data.Int (Int16, Int32, Int64)
import Data.IORef
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import qualified Data.Sequence as Seq
import Data.Sequence (Seq)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import System.Timeout (timeout)

import Brahmaputra.Assignor
import Brahmaputra.Connection
import Brahmaputra.Consumer
import Brahmaputra.Protocol

-- | The internal topic whose partition leaders coordinate groups.
offsetsTopic :: Text
offsetsTopic = "__consumer_offsets"

coordinatorAttempts, joinAttempts :: Int
coordinatorAttempts = 4
joinAttempts = 4

-- | @auto.offset.reset@: where to start when a partition has no valid
-- position (never committed, or fallen off the log).
data AutoOffsetReset
  = ResetEarliest   -- ^ reprocess history; never silently skip
  | ResetLatest     -- ^ skip what was missed; never reprocess
  | ResetNone       -- ^ refuse to guess: 'poll' throws 'NoOffsetForPartition'
  deriving (Eq, Show)

-- | Named as Kafka names its consumer-group settings.
data GroupConfig = GroupConfig
  { gcClientId :: !Text
  , gcSessionTimeoutMs :: !Int32        -- ^ @session.timeout.ms@ (10 s)
  , gcRebalanceTimeoutMs :: !Int32      -- ^ @rebalance.timeout.ms@ (3 s)
  , gcMaxPollIntervalMs :: !Int         -- ^ @max.poll.interval.ms@ (300 s)
  , gcAutoCommitIntervalMs :: !Int      -- ^ @auto.commit.interval.ms@; 0 disables auto-commit (5 s)
  , gcAutoOffsetReset :: !AutoOffsetReset -- ^ @auto.offset.reset@
  , gcAssignor :: !Assignor             -- ^ @partition.assignment.strategy@
  , gcGroupInstanceId :: !(Maybe Text)  -- ^ @group.instance.id@: static membership
  , gcMaxPollRecords :: !Int            -- ^ @max.poll.records@ (500)
  , gcFetchMaxBytes :: !Int32           -- ^ @fetch.max.bytes@ (8 MiB)
  , gcDialTimeoutMs :: !Int
  , gcSocketTimeoutMs :: !Int
  } deriving (Show)

defaultGroupConfig :: GroupConfig
defaultGroupConfig = GroupConfig
  { gcClientId = "brahmaputra-haskell"
  , gcSessionTimeoutMs = 10000
  , gcRebalanceTimeoutMs = 3000
  , gcMaxPollIntervalMs = 300000
  , gcAutoCommitIntervalMs = 5000
  , gcAutoOffsetReset = ResetEarliest
  , gcAssignor = RangeAssignor
  , gcGroupInstanceId = Nothing
  , gcMaxPollRecords = 500
  , gcFetchMaxBytes = 8 * 1024 * 1024
  , gcDialTimeoutMs = 30000
  , gcSocketTimeoutMs = defaultRequestTimeoutMs
  }

data GroupConsumer = GroupConsumer
  { gGroupId :: !Text
  , gConfig :: !GroupConfig
  , gConsumer :: !Consumer
  -- Owned by the polling thread.
  , gSubscribed :: !(IORef [Text])
  , gAssignment :: !(IORef [TopicPartition])
  -- | Next offset to deliver — what gets committed. Advances only over
  -- records handed to the caller.
  , gPositions :: !(IORef (Map.Map TopicPartition Int64))
  -- | Next offset to fetch; runs ahead of positions by what is buffered.
  , gFetchPositions :: !(IORef (Map.Map TopicPartition Int64))
  , gBuffered :: !(IORef (Seq ConsumerRecord))
  , gLastCommit :: !(IORef Int64)
  -- Shared with the heartbeat thread.
  , gMemberId :: !(TVar Text)
  , gGeneration :: !(TVar Int32)
  , gJoined :: !(TVar Bool)
  , gLastPoll :: !(TVar Int64)
  -- | True while 'poll' runs: max.poll.interval bounds the gap *between*
  -- polls, so a poll busy joining a slow rebalance must not count.
  , gInPoll :: !(TVar Bool)
  , gClosed :: !(TVar Bool)
  , gDone :: !(MVar ())
  }

-- | Connect and start heartbeating. Needs the threaded RTS.
newGroupConsumer :: String -> Text -> GroupConfig -> IO GroupConsumer
newGroupConsumer address groupId config = do
  consumer <- newConsumer address defaultConsumerConfig
    { ccClientId = gcClientId config
    , ccFetchMaxBytes = gcFetchMaxBytes config
    , ccMaxPollRecords = gcMaxPollRecords config
    , ccDialTimeoutMs = gcDialTimeoutMs config
    , ccSocketTimeoutMs = gcSocketTimeoutMs config
    }
  now <- monoMs
  g <- GroupConsumer groupId config consumer
         <$> newIORef [] <*> newIORef [] <*> newIORef Map.empty <*> newIORef Map.empty
         <*> newIORef Seq.empty <*> newIORef now
         <*> newTVarIO "" <*> newTVarIO (-1) <*> newTVarIO False <*> newTVarIO now
         <*> newTVarIO False <*> newTVarIO False <*> newEmptyMVar
  void (forkIO (heartbeatLoop g `finally` putMVar (gDone g) ()))
  pure g

withGroupConsumer :: String -> Text -> GroupConfig -> (GroupConsumer -> IO a) -> IO a
withGroupConsumer address groupId config =
  bracket (newGroupConsumer address groupId config) closeGroupConsumer

-- | The underlying partition consumer (for metadata and offsets).
groupConsumer :: GroupConsumer -> Consumer
groupConsumer = gConsumer

-- | Set the topics this member wants a share of; rejoins on the next poll.
subscribe :: GroupConsumer -> [Text] -> IO ()
subscribe g topics = do
  writeIORef (gSubscribed g) topics
  atomically (writeTVar (gJoined g) False)

-- | The partitions currently assigned to this member.
assignment :: GroupConsumer -> IO [TopicPartition]
assignment g = readIORef (gAssignment g)

-- | Commit, leave the group, then stop. Leaving is what lets the
-- coordinator reassign at once instead of waiting out the session timeout.
closeGroupConsumer :: GroupConsumer -> IO ()
closeGroupConsumer g = do
  atomically (writeTVar (gClosed g) True)
  joined <- readTVarIO (gJoined g)
  when joined $ void (try (commit g) :: IO (Either SomeException ()))
  memberId <- readTVarIO (gMemberId g)
  -- Best effort: failing here costs only the session timeout.
  unless (T.null memberId) $ void (try (leave g) :: IO (Either SomeException ()))
  _ <- timeout 2000000 (readMVar (gDone g))
  closeConsumer (gConsumer g)

-- | Up to @max.poll.records@ records, joining the group if needed. Waits
-- up to @timeoutMs@ for data.
poll :: GroupConsumer -> Int -> IO [ConsumerRecord]
poll g timeoutMs = do
  subscribed <- readIORef (gSubscribed g)
  when (null subscribed) $ throwIO (ClientError "subscribe to at least one topic before polling")
  -- Stamped on entry and on exit and never enforced in between: a poll
  -- that blocks is the consumer working normally.
  let stamp inPoll = do
        now <- monoMs
        atomically $ writeTVar (gLastPoll g) now >> writeTVar (gInPoll g) inPoll
  stamp True
  deadline <- (+ fromIntegral timeoutMs) <$> monoMs
  loop deadline `finally` stamp False
  where
    loop deadline = do
      -- Checked every sweep: a rebalance the heartbeat learns of mid-poll
      -- must stop this member fetching partitions it may no longer own.
      joined <- readTVarIO (gJoined g)
      unless joined (joinGroup g)
      buffered <- readIORef (gBuffered g)
      slots <- readIORef (gAssignment g)
      if not (Seq.null buffered)
        then takeBuffered g
        else if null slots
          then do
            now <- monoMs
            if now > deadline then pure [] else threadDelay 50000 >> loop deadline
          else do
            gotAny <- or <$> forM slots (fetchSlot deadline)
            maybeAutoCommit g
            buffered' <- readIORef (gBuffered g)
            now <- monoMs
            if not (Seq.null buffered') then takeBuffered g
            else if not gotAny && now > deadline then pure []
            else loop deadline

    fetchSlot deadline slot = do
      now <- monoMs
      let waitMs = fromIntegral (min 500 (max 0 (deadline - now))) :: Int32
      offset <- fromMaybe 0 . Map.lookup slot <$> readIORef (gFetchPositions g)
      result <- try (fetch (gConsumer g) (tpTopic slot) (tpPartition slot) offset waitMs)
      case result of
        Left (ServerError code _)
          | code == errOffsetOutOfRange -> do
              -- The committed offset fell off the log; restart where the
              -- policy says.
              reset <- resetOffset g slot
              modifyIORef' (gFetchPositions g) (Map.insert slot reset)
              modifyIORef' (gPositions g) (Map.insert slot reset)
              pure False
          | code == errNotLeaderOrFollower -> do
              void (try (refreshMetadata (consumerRouter (gConsumer g)) (tpTopic slot)) :: IO (Either SomeException ClusterMetadata))
              pure False
        Left e -> throwIO e
        Right [] -> pure False
        Right records -> do
          modifyIORef' (gFetchPositions g) (Map.insert slot (crOffset (last records) + 1))
          modifyIORef' (gBuffered g) (<> Seq.fromList records)
          pure True

takeBuffered :: GroupConsumer -> IO [ConsumerRecord]
takeBuffered g = do
  buffered <- readIORef (gBuffered g)
  let limit = gcMaxPollRecords (gConfig g)
      n = if limit <= 0 then Seq.length buffered else min limit (Seq.length buffered)
      (delivered, rest) = Seq.splitAt n buffered
  writeIORef (gBuffered g) rest
  -- The consumed position advances only over records actually handed to
  -- the caller; committing what was merely fetched would skip records.
  forM_ delivered $ \r ->
    modifyIORef' (gPositions g) (Map.insert (TopicPartition (crTopic r) (crPartition r)) (crOffset r + 1))
  pure (toList delivered)

-- | Commit the delivered positions. At-least-once: call it after
-- processing, not before.
commit :: GroupConsumer -> IO ()
commit g = do
  positions <- readIORef (gPositions g)
  unless (Map.null positions) $ do
    (memberId, generation) <- atomically ((,) <$> readTVar (gMemberId g) <*> readTVar (gGeneration g))
    response <- coordinatorRequest g apiOffsetCommit $ buildBody $
      [ wString (gGroupId g), wInt32 generation, wString memberId
      , wInt32 (fromIntegral (Map.size positions)) ]
      ++ [ wString t <> wInt32 p <> wInt64 o | (TopicPartition t p, o) <- Map.toAscList positions ]
    code <- readBody response rInt32
    when (code /= errNone) $ throwIO (ServerError code "offset_commit")
    writeIORef (gLastCommit g) =<< monoMs

-- | The group's committed offsets for these partitions; an empty list asks
-- for every partition the group holds.
committed :: GroupConsumer -> [TopicPartition] -> IO (Map.Map TopicPartition Int64)
committed g slots = do
  response <- coordinatorRequest g apiOffsetFetch $ buildBody $
    [wString (gGroupId g), wInt32 (fromIntegral (length slots))]
    ++ [wString t <> wInt32 p | TopicPartition t p <- slots]
  (code, entries) <- readBody response $ do
    code <- rInt32
    if code /= errNone then pure (code, []) else do
      entries <- rArray ((,) <$> (TopicPartition <$> rString <*> rInt32) <*> rInt64)
      pure (code, entries)
  when (code /= errNone) $ throwIO (ServerError code "offset_fetch")
  pure (Map.fromList entries)

maybeAutoCommit :: GroupConsumer -> IO ()
maybeAutoCommit g = do
  let interval = gcAutoCommitIntervalMs (gConfig g)
  positions <- readIORef (gPositions g)
  lastCommit <- readIORef (gLastCommit g)
  now <- monoMs
  when (interval > 0 && not (Map.null positions) && now - lastCommit >= fromIntegral interval) $
    -- A failed auto-commit is retried on the next poll; an explicit
    -- 'commit' is what a caller relies on.
    void (try (commit g) :: IO (Either BrahmaputraError ()))

resetOffset :: GroupConsumer -> TopicPartition -> IO Int64
resetOffset g slot = case gcAutoOffsetReset (gConfig g) of
  ResetEarliest -> listOffsets (gConsumer g) (tpTopic slot) (tpPartition slot) Earliest
  ResetLatest -> listOffsets (gConsumer g) (tpTopic slot) (tpPartition slot) Latest
  ResetNone -> throwIO (NoOffsetForPartition slot)

-- ---------------------------------------------------------------------------
-- Membership
-- ---------------------------------------------------------------------------

data JoinResponse = JoinResponse
  { jrGeneration :: !Int32
  , jrMemberId :: !Text
  , jrLeaderId :: !Text
  , jrMembers :: ![(Text, [Text], [TopicPartition])]
  }

joinGroup :: GroupConsumer -> IO ()
joinGroup g = attempt joinAttempts
  where
    config = gConfig g
    attempt :: Int -> IO ()
    attempt 0 = throwIO (ClientError ("consumer group failed to stabilise after "
                                      ++ show joinAttempts ++ " join attempts"))
    attempt left = do
      memberId0 <- readTVarIO (gMemberId g)
      subscribed <- readIORef (gSubscribed g)
      response <- coordinatorRequest g apiJoinGroup $ buildBody
        [ wString (gGroupId g), wInt32 (gcSessionTimeoutMs config), wInt32 (gcRebalanceTimeoutMs config)
        , wString memberId0, wStringArray subscribed, wString (fromMaybe "" (gcGroupInstanceId config)) ]
      (code, joined) <- readBody response $ do
        code <- rInt32
        if code /= errNone then pure (code, Nothing) else do
          jr <- JoinResponse <$> rInt32 <*> rString <*> rString
                  <*> rArray ((,,) <$> rString <*> rStringArray
                                   <*> rArray (TopicPartition <$> rString <*> rInt32))
          pure (code, Just jr)
      case joined of
        Nothing
          | code == errRebalanceInProgress -> threadDelay 100000 >> attempt (left - 1)
          | code == errUnknownMemberId -> do
              -- The coordinator dropped this member (session expiry, or
              -- removed while it waited): join again as a new one.
              atomically (writeTVar (gMemberId g) "")
              attempt (left - 1)
          | otherwise -> throwIO (ServerError code "join_group")
        Just jr -> do
          atomically $ do
            writeTVar (gMemberId g) (jrMemberId jr)
            writeTVar (gGeneration g) (jrGeneration jr)
          assignments <-
            if jrMemberId jr /= jrLeaderId jr then pure [] else do
              let topics = Map.keys (Map.fromList [(t, ()) | (_, ts, _) <- jrMembers jr, t <- ts])
              topicPartitions <- Map.fromList <$> forM topics (\t -> (,) t <$> partitions (gConsumer g) t)
              let members = [AssignorMember mid ts | (mid, ts, _) <- jrMembers jr]
                  previous = Map.fromList [(mid, held) | (mid, _, held) <- jrMembers jr]
              pure (Map.toAscList (assign (gcAssignor config) members topicPartitions previous))
          ok <- syncGroup g assignments
          if ok then atomically (writeTVar (gJoined g) True) else attempt (left - 1)

syncGroup :: GroupConsumer -> [(Text, [TopicPartition])] -> IO Bool
syncGroup g assignments = do
  (memberId, generation) <- atomically ((,) <$> readTVar (gMemberId g) <*> readTVar (gGeneration g))
  response <- coordinatorRequest g apiSyncGroup $ buildBody $
    [ wString (gGroupId g), wInt32 generation, wString memberId
    , wInt32 (fromIntegral (length assignments)) ]
    ++ [ wString mid <> wInt32 (fromIntegral (length slots)) <> foldMap (\(TopicPartition t p) -> wString t <> wInt32 p) slots
       | (mid, slots) <- assignments ]
  (code, mine) <- readBody response $ do
    code <- rInt32
    if code /= errNone then pure (code, []) else do
      mine <- rArray (TopicPartition <$> rString <*> rInt32)
      pure (code, mine)
  if code == errRebalanceInProgress || code == errIllegalGeneration
    then pure False
    else if code == errUnknownMemberId
      then atomically (writeTVar (gMemberId g) "") >> pure False
      else if code /= errNone
        then throwIO (ServerError code "sync_group")
        else applyAssignment g mine >> pure True

applyAssignment :: GroupConsumer -> [TopicPartition] -> IO ()
applyAssignment g slots = do
  writeIORef (gAssignment g) slots
  let owned = Map.fromList [(s, ()) | s <- slots]
  modifyIORef' (gPositions g) (`Map.intersection` owned)
  -- Buffered records were never delivered, so a new assignment drops them.
  writeIORef (gBuffered g) Seq.empty
  positions <- readIORef (gPositions g)
  let needed = [s | s <- slots, not (Map.member s positions)]
  unless (null needed) $ do
    known <- committed g needed
    forM_ needed $ \slot -> do
      offset <- case Map.lookup slot known of
        Just o | o >= 0 -> pure o
        _ -> resetOffset g slot
      modifyIORef' (gPositions g) (Map.insert slot offset)
  writeIORef (gFetchPositions g) =<< readIORef (gPositions g)

leave :: GroupConsumer -> IO ()
leave g = do
  memberId <- readTVarIO (gMemberId g)
  response <- coordinatorRequest g apiLeaveGroup (buildBody [wString (gGroupId g), wString memberId])
  code <- readBody response rInt32
  when (code /= errNone) $ throwIO (ServerError code "leave_group")
  atomically (writeTVar (gJoined g) False)

heartbeatLoop :: GroupConsumer -> IO ()
heartbeatLoop g = loop False
  where
    config = gConfig g
    maxPoll = gcMaxPollIntervalMs config
    -- Two independent deadlines, so wake often enough for the shorter.
    interval = max 1 (min (fromIntegral (gcSessionTimeoutMs config) `div` 3) (maxPoll `div` 3))
    loop leftForSlowPoll = do
      stop <- timeout (interval * 1000) (atomically (readTVar (gClosed g) >>= check))
      case stop of
        Just () -> pure ()
        Nothing -> do
          result <- try (tick leftForSlowPoll)
          case result of
            Right next -> loop next
            Left (e :: SomeException)
              | Just (_ :: SomeAsyncException) <- fromException e -> throwIO e
              | otherwise -> loop leftForSlowPoll   -- transient: retry next tick
    tick leftForSlowPoll = do
      now <- monoMs
      (memberId, generation, joined, lastPoll, inPoll) <- atomically $
        (,,,,) <$> readTVar (gMemberId g) <*> readTVar (gGeneration g) <*> readTVar (gJoined g)
               <*> readTVar (gLastPoll g) <*> readTVar (gInPoll g)
      if not joined || T.null memberId
        then pure leftForSlowPoll
        else if not inPoll && now - lastPoll >= fromIntegral maxPoll
          then do
            -- The application stopped consuming though the process lives;
            -- heartbeating on would hold its partitions from a consumer
            -- that could make progress.
            unless leftForSlowPoll $ do
              void (try (leave g) :: IO (Either SomeException ()))
              atomically (writeTVar (gJoined g) False)
            pure True
          else do
            response <- coordinatorRequest g apiHeartbeat $
              buildBody [wString (gGroupId g), wInt32 generation, wString memberId]
            code <- readBody response rInt32
            when (code `elem` [errRebalanceInProgress, errUnknownMemberId, errIllegalGeneration]) $
              -- Only if nothing changed since the snapshot: a reply for an
              -- old generation must not send a rejoined member round again.
              atomically $ do
                g' <- readTVar (gGeneration g)
                m' <- readTVar (gMemberId g)
                when (g' == generation && m' == memberId) $ writeTVar (gJoined g) False
            pure False

-- ---------------------------------------------------------------------------
-- Coordinator routing
-- ---------------------------------------------------------------------------

coordinatorPartition :: GroupConsumer -> IO Int32
coordinatorPartition g = do
  parts <- partitions (gConsumer g) offsetsTopic
  pure (fromIntegral (crc32c (TE.encodeUtf8 (gGroupId g)) `mod` fromIntegral (length parts)))

-- | Send to the group's coordinator, following moves and waiting out loads.
coordinatorRequest :: GroupConsumer -> Int16 -> ByteString -> IO ByteString
coordinatorRequest g key body = go coordinatorAttempts
  where
    router = consumerRouter (gConsumer g)
    go 0 = throwIO (ClientError ("group coordinator unavailable after " ++ show coordinatorAttempts ++ " attempts"))
    go left = do
      partition <- coordinatorPartition g
      conn <- connFor router offsetsTopic partition
      response <- request conn key body
      let code = peekErrorCode response
      if code == errCoordinatorLoadInProgress
        then threadDelay 100000 >> go (left - 1)
        else if code == errNotCoordinator || code == errNotLeaderOrFollower
          then do
            void (try (refreshMetadata router offsetsTopic) :: IO (Either BrahmaputraError ClusterMetadata))
            go (left - 1)
          else pure response
