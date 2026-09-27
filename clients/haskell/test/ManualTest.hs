{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | End-to-end suite for the Haskell driver against a live broker; a port
-- of clients/go/cmd/manualtest with the same sections and checks.
--
-- > ghc -threaded -O1 -isrc test/ManualTest.hs -o manualtest
-- > ./manualtest 127.0.0.1 9092
--
-- Every check asserts a property of the system, not that a function ran.
module Main (main) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad (forM, forM_, forever, unless, void, when)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import Data.ByteString (ByteString)
import Data.Int (Int32, Int64)
import Data.IORef
import Data.List (isInfixOf)
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as NSB
import System.Environment (getArgs)
import System.Exit (ExitCode (..), exitWith)
import System.IO (BufferMode (..), hSetBuffering, stdout)
import System.IO.Unsafe (unsafePerformIO)
import System.Timeout (timeout)

import Brahmaputra

-- ---------------------------------------------------------------------------
-- Harness
-- ---------------------------------------------------------------------------

counters :: IORef (Int, Int)
counters = unsafePerformIO (newIORef (0, 0))
{-# NOINLINE counters #-}

check :: String -> Bool -> String -> IO ()
check name ok detail
  | ok = do
      modifyIORef' counters (\(p, f) -> (p + 1, f))
      putStrLn ("  ok   " ++ name)
  | otherwise = do
      modifyIORef' counters (\(p, f) -> (p, f + 1))
      putStrLn ("  FAIL " ++ name ++ (if null detail then "" else ": " ++ detail))

section :: String -> IO ()
section title = putStrLn ("\n" ++ title)

uniqueCounter :: IORef Int
uniqueCounter = unsafePerformIO (newIORef 0)
{-# NOINLINE uniqueCounter #-}

unique :: String -> IO Text
unique prefix = do
  t <- getPOSIXTime
  n <- atomicModifyIORef' uniqueCounter (\c -> (c + 1, c))
  let nanos = (floor (t * 1000000000) :: Integer) `mod` 1000000000
  pure (T.pack (prefix ++ "-" ++ show nanos ++ show n))

-- | Abort the whole suite: the setup a check depends on failed.
must :: IO a -> IO a
must action = action `catch` \(e :: SomeException) -> case fromException e of
  Just (code :: ExitCode) -> throwIO code
  Nothing -> do
    putStrLn ("  FATAL " ++ show e)
    exitWith (ExitFailure 2)

nowMillis :: IO Int64
nowMillis = nowMs

elapsedSince :: Int64 -> IO Int64
elapsedSince start = subtract start <$> monoMs

bytes :: String -> ByteString
bytes = TE.encodeUtf8 . T.pack

sendTo :: Producer -> Text -> Int32 -> Maybe ByteString -> Maybe ByteString -> [Header] -> IO ()
sendTo p topic partition value key headers =
  send p (producerRecord topic value) { prPartition = Just partition, prKey = key, prHeaders = headers }

sendKeyed :: Producer -> Text -> Maybe ByteString -> Maybe ByteString -> IO ()
sendKeyed p topic value key = send p (producerRecord topic value) { prKey = key }

noLinger :: ProducerConfig
noLinger = defaultProducerConfig { pcLingerMs = 0 }

noAutoCommit :: GroupConfig
noAutoCommit = defaultGroupConfig { gcAutoCommitIntervalMs = 0 }

-- | Poll until @want@ records or the deadline.
pollUntil :: GroupConsumer -> Int -> Int -> Int -> IO [ConsumerRecord]
pollUntil consumer want deadlineMs pollMs = do
  start <- monoMs
  let go acc = do
        el <- elapsedSince start
        if length acc >= want || el >= fromIntegral deadlineMs
          then pure acc
          else do
            records <- must (poll consumer pollMs)
            go (acc ++ records)
  go []

-- | Poll for the whole window, ignoring errors, collecting what arrives.
pollFor :: GroupConsumer -> Int -> Int -> IO [ConsumerRecord]
pollFor consumer windowMs pollMs = do
  start <- monoMs
  let go acc = do
        el <- elapsedSince start
        if el >= fromIntegral windowMs then pure acc else do
          r <- try (poll consumer pollMs)
          case r of
            Left (_ :: BrahmaputraError) -> go acc
            Right records -> go (acc ++ records)
  go []

-- | Read a partition from offset 0 until @want@ records or an empty fetch.
drain :: Consumer -> Text -> Int32 -> Int -> IO [ConsumerRecord]
drain consumer topic partition want = go 0 []
  where
    go offset acc
      | length acc >= want = pure acc
      | otherwise = do
          r <- try (fetch consumer topic partition offset 500)
          case r of
            Left (_ :: BrahmaputraError) -> pure acc
            Right [] -> pure acc
            Right batch -> go (crOffset (last batch) + 1) (acc ++ batch)

-- ---------------------------------------------------------------------------
-- Main
-- ---------------------------------------------------------------------------

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  args <- getArgs
  let address = case args of
        [host, port] -> host ++ ":" ++ port
        [hostPort] -> hostPort
        _ -> "127.0.0.1:9092"

  section "connection and metadata"
  do
    consumer <- must (newConsumer address defaultConsumerConfig)
    seed <- routerSeed (consumerRouter consumer)
    versions <- try (apiVersions seed)
    case versions of
      Left (e :: BrahmaputraError) -> do
        check "ApiVersions answers" False (show e)
        check "broker reports a version" False (show e)
      Right (ranges, brokerVersion) -> do
        check "ApiVersions answers" (not (null ranges)) (show (length ranges) ++ " ranges")
        check "broker reports a version" (not (T.null brokerVersion)) (T.unpack brokerVersion)
    metadata <- must (routerMetadata (consumerRouter consumer) [] True)
    check "metadata lists brokers" (not (null (metaBrokers metadata)))
      (show (length (metaBrokers metadata)) ++ " brokers")
    closeConsumer consumer

  section "produce and consume round trip"
  topic <- unique "hs-roundtrip"
  let payloads = [bytes ("record-" ++ show i) | i <- [0 .. 49 :: Int]]
  do
    producer <- must (newProducer address noLinger)
    forM_ payloads $ \payload -> must (sendTo producer topic 0 (Just payload) Nothing [])
    must (flush producer)
    must (closeProducer producer)
  do
    consumer <- must (newConsumer address defaultConsumerConfig)
    got <- must (fetch consumer topic 0 0 500)
    check "every record comes back" (length got == length payloads) ("got " ++ show (length got))
    let identical = length got == length payloads
          && and [crValue r == Just p && crOffset r == i | (r, p, i) <- zip3 got payloads [0 ..]]
    check "values byte-identical and offsets contiguous" identical ""
    closeConsumer consumer

  section "compression codecs"
  -- Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
  -- registerCodec.
  forM_ [("none", NoCompression), ("gzip", Gzip)] $ \(name, codec) -> do
    codecTopic <- unique ("hs-" ++ name)
    let body = BS.concat (replicate 40 "the same line over and over. ")
    producer <- must (newProducer address noLinger { pcCompression = codec })
    forM_ [0 .. 19 :: Int] $ \i ->
      must (sendTo producer codecTopic 0 (Just (body <> BC.singleton (toEnum (48 + i `mod` 10)))) Nothing [])
    must (flush producer)
    must (closeProducer producer)
    consumer <- must (newConsumer address defaultConsumerConfig)
    got <- must (fetch consumer codecTopic 0 0 500)
    check (name ++ ": round trips")
      (length got == 20 && case got of
                             (r : _) -> maybe False (body `BS.isPrefixOf`) (crValue r)
                             [] -> False)
      ("got " ++ show (length got) ++ " records")
    closeConsumer consumer

  section "keys, partitioning and ordering"
  do
    keyTopic <- unique "hs-keys"
    producer <- must (newProducer address noLinger)
    parts <- must (routerPartitions (producerRouter producer) keyTopic)
    forM_ [0 .. 29 :: Int] $ \i ->
      must (sendKeyed producer keyTopic (Just (bytes ("v" ++ show i))) (Just "user-7"))
    must (flush producer)
    must (closeProducer producer)

    let target = partitionForKey "user-7" parts
    consumer <- must (newConsumer address defaultConsumerConfig)
    onTarget <- must (fetch consumer keyTopic target 0 500)
    check "a key pins every record to one partition" (length onTarget == 30)
      ("partition " ++ show target ++ " holds " ++ show (length onTarget) ++ " of 30")
    let ordered = length onTarget == 30
          && and [crValue r == Just (bytes ("v" ++ show i)) | (r, i) <- zip onTarget [0 :: Int ..]]
    check "per-key order is preserved" ordered ""
    strays <- sum <$> forM [p | p <- parts, p /= target]
                (\p -> length <$> must (fetch consumer keyTopic p 0 200))
    check "no keyed record landed elsewhere" (strays == 0) (show strays ++ " strays")
    closeConsumer consumer

  section "murmur2 agrees with the broker's partitioner"
  check "murmur2(\"\") is stable" (murmur2 "" == 275646681) (show (murmur2 ""))
  check "murmur2 is deterministic" (murmur2 "user-7" == murmur2 "user-7") ""
  check "different keys hash differently" (murmur2 "user-7" /= murmur2 "user-8") ""

  section "record headers and timestamps"
  do
    headerTopic <- unique "hs-headers"
    before <- subtract 1000 <$> nowMillis
    producer <- must (newProducer address noLinger)
    must (sendTo producer headerTopic 0 (Just "annotated") Nothing
            [ Header "trace-id" (Just "abc-123")
            , Header "content-type" (Just "application/json")
            , Header "tombstone-reason" Nothing ])
    must (sendTo producer headerTopic 0 (Just "plain") Nothing [])
    must (flush producer)
    must (closeProducer producer)
    after <- (+ 1000) <$> nowMillis

    consumer <- must (newConsumer address defaultConsumerConfig)
    got <- must (fetch consumer headerTopic 0 0 500)
    check "both records arrive" (length got == 2) ("got " ++ show (length got))
    case got of
      [annotated, plain] -> do
        check "headers survive the round trip" (length (crHeaders annotated) == 3)
          (show (length (crHeaders annotated)) ++ " headers")
        check "header values are exact" (recordHeader "trace-id" annotated == Just (Just "abc-123")) ""
        check "a null header value stays null"
          (length (crHeaders annotated) == 3 && headerValue (crHeaders annotated !! 2) == Nothing) ""
        check "a record with no headers gains none from its batch" (null (crHeaders plain))
          (show (length (crHeaders plain)) ++ " headers")
        let inWindow = all (\r -> crTimestamp r >= before && crTimestamp r <= after) got
        check "timestamps are real wall-clock values" inWindow
          (show (map crTimestamp got) ++ " outside " ++ show before ++ ".." ++ show after)
      _ -> pure ()
    closeConsumer consumer

  section "tombstones"
  do
    tombTopic <- unique "hs-tombstones"
    producer <- must (newProducer address noLinger)
    must (sendTo producer tombTopic 0 (Just "set") (Just "k1") [])
    must (sendTo producer tombTopic 0 (Just "") (Just "k2") [])
    -- A Nothing value is a deletion, and must stay distinguishable from
    -- the empty value above all the way through the round trip.
    must (sendTo producer tombTopic 0 Nothing (Just "k3") [])
    must (flush producer)
    must (closeProducer producer)
    consumer <- must (newConsumer address defaultConsumerConfig)
    got <- must (fetch consumer tombTopic 0 0 500)
    check "all three records arrive" (length got == 3) ("got " ++ show (length got))
    case got of
      [a, b, c] -> do
        check "an ordinary value round-trips" (crValue a == Just "set") ""
        check "an empty value is empty, not null" (crValue b == Just "") (show (crValue b))
        check "a tombstone arrives as a null value" (crValue c == Nothing) (show (crValue c))
      _ -> pure ()
    closeConsumer consumer

  section "offsets"
  do
    consumer <- must (newConsumer address defaultConsumerConfig)
    earliest <- must (listOffsets consumer topic 0 Earliest)
    latest <- must (listOffsets consumer topic 0 Latest)
    check "earliest is 0 on a fresh topic" (earliest == 0) (show earliest)
    check "latest equals the record count" (latest == 50) (show latest)
    closeConsumer consumer

  section "acks"
  forM_ [AcksNone, AcksLeader, AcksAll] $ \acks -> do
    let wire = acksWire acks
    acksTopic <- unique ("hs-acks" ++ show wire)
    producer <- must (newProducer address noLinger { pcAcks = acks })
    must (sendTo producer acksTopic 0 (Just "durable") Nothing [])
    must (flush producer)
    must (closeProducer producer)
    threadDelay 400000
    consumer <- must (newConsumer address defaultConsumerConfig)
    got <- must (fetch consumer acksTopic 0 0 500)
    check ("acks=" ++ show wire ++ " stores the record") (length got == 1) ("got " ++ show (length got))
    closeConsumer consumer

  section "consumer group: assignment, commit, resume"
  do
    groupTopic <- unique "hs-group"
    groupId <- unique "hs-billing"
    producer <- must (newProducer address noLinger)
    forM_ [0 .. 39 :: Int] $ \i -> must (sendKeyed producer groupTopic (Just (bytes ("g" ++ show i))) Nothing)
    must (flush producer)
    must (closeProducer producer)

    consumer <- must (newGroupConsumer address groupId noAutoCommit)
    subscribe consumer [groupTopic]
    seen <- pollUntil consumer 40 30000 500
    check "the group consumes every record" (length seen == 40) ("got " ++ show (length seen))
    let distinct = Set.fromList [(crPartition r, crOffset r) | r <- seen]
    check "no record is delivered twice" (Set.size distinct == length seen) ""
    must (commit consumer)
    offsets <- must (committed consumer [])
    let total = sum (Map.elems offsets)
    check "commit records a position" (total == 40) (show total)
    must (closeGroupConsumer consumer)

    -- A second consumer in the same group must resume, not replay.
    rejoined <- must (newGroupConsumer address groupId noAutoCommit)
    subscribe rejoined [groupTopic]
    replayed <- pollFor rejoined 5000 300
    check "a rejoining group resumes from its commit" (null replayed)
      ("replayed " ++ show (length replayed) ++ " records it had already committed")
    must (closeGroupConsumer rejoined)

  section "auto.offset.reset"
  do
    resetTopic <- unique "hs-reset"
    producer <- must (newProducer address noLinger)
    forM_ [0 .. 9 :: Int] $ \i -> must (sendKeyed producer resetTopic (Just (bytes ("r" ++ show i))) Nothing)
    must (flush producer)
    must (closeProducer producer)

    latestGroup <- unique "hs-latest"
    consumer <- must (newGroupConsumer address latestGroup noAutoCommit { gcAutoOffsetReset = ResetLatest })
    subscribe consumer [resetTopic]
    skipped <- pollFor consumer 4000 300
    check "latest skips records produced before the group existed" (null skipped)
      ("saw " ++ show (length skipped))
    must (closeGroupConsumer consumer)

    noneGroup <- unique "hs-none"
    strict <- must (newGroupConsumer address noneGroup noAutoCommit { gcAutoOffsetReset = ResetNone })
    subscribe strict [resetTopic]
    start <- monoMs
    let attempt = do
          el <- elapsedSince start
          if el >= 5000 then pure False else do
            r <- try (poll strict 300)
            case r of
              Left (NoOffsetForPartition _) -> pure True
              Left e -> pure ("no committed offset" `isInfixOf` show e)
              Right _ -> attempt
    raised <- attempt
    check "none refuses to guess a position" raised ""
    must (closeGroupConsumer strict)

  section "assignors"
  forM_ [RangeAssignor, RoundRobinAssignor, StickyAssignor] $ \assignor -> do
    let name = assignorName assignor
    assignorTopic <- unique ("hs-" ++ name)
    producer <- must (newProducer address noLinger)
    forM_ [0 .. 19 :: Int] $ \i -> must (sendKeyed producer assignorTopic (Just (bytes ("a" ++ show i))) Nothing)
    must (flush producer)
    must (closeProducer producer)
    groupId <- unique ("hs-grp-" ++ name)
    consumer <- must (newGroupConsumer address groupId noAutoCommit { gcAssignor = assignor })
    subscribe consumer [assignorTopic]
    start <- monoMs
    let collect acc = do
          el <- elapsedSince start
          if length acc >= 20 || el >= 20000 then pure acc else do
            r <- try (poll consumer 500)
            case r of
              Left (_ :: BrahmaputraError) -> collect acc
              Right records -> collect (acc ++ records)
    collected <- collect []
    check (name ++ ": consumes every record") (length collected == 20) ("got " ++ show (length collected))
    must (closeGroupConsumer consumer)

  section "bounded client buffer"
  do
    bufferTopic <- unique "hs-buffer"
    -- Never flushes on time during this check.
    producer <- must (newProducer address defaultProducerConfig
                        { pcLingerMs = 10000, pcBufferMemory = 2048, pcMaxBlockMs = 300 })
    let go :: Int -> IO Bool
        go i
          | i >= 500 = pure False
          | otherwise = do
              r <- try (sendTo producer bufferTopic 0 (Just (BS.replicate 256 120)) Nothing [])
              case r of
                Left (BufferFull _) -> pure True
                Left e -> pure ("buffer full" `isInfixOf` show e)
                Right () -> go (i + 1)
    blocked <- go 0
    check "a full buffer blocks and then reports" blocked ""
    void (try (closeProducer producer) :: IO (Either SomeException ()))

  section "wire edge cases"
  do
    edgeTopic <- unique "hs-edge"
    producer <- must (newProducer address noLinger)
    let large = BS.pack [fromIntegral (i * 7) | i <- [0 .. 1024 * 1024 - 1 :: Int]]
        unicodeKey = bytes "ключ-✓-🔑"
        unicodeValue = bytes "значение — 数据 — 🚀"
    must (sendTo producer edgeTopic 0 (Just large) Nothing [])
    must (sendTo producer edgeTopic 0 (Just unicodeValue) (Just unicodeKey) [Header "ünïcødé-🏷" (Just (bytes "✓"))])
    -- An empty key and an empty header value are values, not nulls.
    must (sendTo producer edgeTopic 0 (Just "empty-key") (Just "")
            [Header "empty" (Just ""), Header "null" Nothing])
    must (sendTo producer edgeTopic 0 (Just "null-key") Nothing [])
    must (closeProducer producer)

    consumer <- must (newConsumer address defaultConsumerConfig)
    got <- drain consumer edgeTopic 0 4
    check "edge records all arrive" (length got == 4) ("got " ++ show (length got))
    case got of
      [big, uni, emptyKey, nullKey] -> do
        check "a 1 MiB value round-trips byte-identical" (crValue big == Just large)
          (show (maybe 0 BS.length (crValue big)) ++ " bytes")
        check "unicode key, value and header key round-trip"
          (crKey uni == Just unicodeKey && crValue uni == Just unicodeValue
             && map headerKey (crHeaders uni) == ["ünïcødé-🏷"]) ""
        check "an empty key stays empty, not null" (crKey emptyKey == Just "") (show (crKey emptyKey))
        check "an empty header value stays empty, not null"
          (map headerValue (crHeaders emptyKey) == [Just "", Nothing]) (show (crHeaders emptyKey))
        check "a null key stays null" (crKey nullKey == Nothing) (show (crKey nullKey))
      _ -> pure ()
    closeConsumer consumer

  section "ordering under linger flushes"
  do
    orderTopic <- unique "hs-order"
    producer <- must (newProducer address defaultProducerConfig { pcLingerMs = 1, pcBatchSize = 256 })
    let total = 5000 :: Int
    forM_ [0 .. total - 1] $ \i -> must (sendTo producer orderTopic 0 (Just (bytes (show i))) Nothing [])
    must (closeProducer producer)
    consumer <- must (newConsumer address defaultConsumerConfig)
    records <- drain consumer orderTopic 0 total
    let values = [maybe (-1) (read . BC.unpack) (crValue r) :: Int | r <- records]
        inversions = length (filter id (zipWith (>) values (drop 1 values)))
    check "every record of a partition arrives" (length values == total) ("got " ++ show (length values))
    check "a partition's records keep send order" (inversions == 0) (show inversions ++ " inversions")
    closeConsumer consumer

  section "background flush failures are reported"
  do
    producer <- must (newProducer address defaultProducerConfig { pcLingerMs = 20 })
    failTopic <- unique "hs-bgfail"
    -- Partition 999 does not exist, so the linger thread's flush fails.
    sendResult <- try (sendTo producer failTopic 999 (Just "lost") Nothing [])
    threadDelay 300000
    flushResult <- try (flush producer)
    check "a failed linger flush surfaces on the next Flush"
      (either (\(_ :: SomeException) -> False) (const True) sendResult
         && either (\(_ :: SomeException) -> True) (const False) flushResult)
      ("send=" ++ show (either (Just . show) (const Nothing) sendResult)
         ++ " flush=" ++ show (either (\(e :: SomeException) -> Just (show e)) (const Nothing) flushResult))
    closed <- timeout 5000000 (try (closeProducer producer) :: IO (Either SomeException ()))
    check "Close returns after a failed flush" (maybe False (const True) closed) "hung"

  section "connection failures"
  do
    -- A broker that accepts and never answers must cost an error, not a
    -- thread blocked forever.
    silent <- listenLocal
    silentPort <- NS.socketPort silent
    _ <- forkIO $ acceptLoop silent $ \conn -> void $ forkIO $
      let discard = do
            chunk <- NSB.recv conn 65536
            unless (BS.null chunk) discard
      in discard `catch` \(_ :: SomeException) -> pure ()
    conn <- must (dial ("127.0.0.1:" ++ show silentPort) "hs-test" 1000)
    setRequestTimeout conn 300
    started <- monoMs
    requestResult <- try (apiVersions conn)
    took <- elapsedSince started
    check "a request to an unresponsive broker times out"
      (either (\(_ :: BrahmaputraError) -> True) (const False) requestResult && took < 3000)
      (either show (const "answered") requestResult)
    broken <- isBroken conn
    check "a timed-out connection is not reused" broken ""
    closeConn conn
    NS.close silent

    -- A connection the broker drops is redialled, not kept forever.
    proxy <- newProxy address
    dropTopic <- unique "hs-drop"
    producer <- must (newProducer (proxyAddress proxy) noLinger)
    must (sendTo producer dropTopic 0 (Just "before") Nothing [])
    dropAll proxy
    let retrySend :: Int -> Maybe String -> IO (Maybe String)
        retrySend 0 lastErr = pure lastErr
        retrySend n _ = do
          r <- try (sendTo producer dropTopic 0 (Just "after") Nothing [])
          case r of
            Right () -> pure Nothing
            Left (e :: BrahmaputraError) -> retrySend (n - 1) (Just (show e))
    recovered <- retrySend 3 (Just "not attempted")
    check "a producer recovers after its connection drops" (recovered == Nothing) (show recovered)
    void (try (closeProducer producer) :: IO (Either SomeException ()))

    consumer <- must (newConsumer (proxyAddress proxy) defaultConsumerConfig)
    _ <- must (fetch consumer dropTopic 0 0 100)
    dropAll proxy
    let retryFetch :: Int -> Either String [ConsumerRecord] -> IO (Either String [ConsumerRecord])
        retryFetch 0 lastResult = pure lastResult
        retryFetch n _ = do
          r <- try (fetch consumer dropTopic 0 0 100)
          case r of
            Right records -> pure (Right records)
            Left (e :: BrahmaputraError) -> retryFetch (n - 1) (Left (show e))
    fetched <- retryFetch 3 (Left "not attempted")
    check "a consumer recovers after its connection drops"
      (either (const False) (not . null) fetched) (either id (const "") fetched)
    closeConsumer consumer
    closeProxy proxy

  section "consumer group: max.poll.interval and rejoin"
  do
    slowTopic <- unique "hs-slow"
    producer <- must (newProducer address noLinger)
    forM_ [0 .. 9 :: Int] $ \i -> must (sendKeyed producer slowTopic (Just (bytes ("s" ++ show i))) Nothing)
    slowGroup <- unique "hs-slow-grp"
    consumer <- must (newGroupConsumer address slowGroup noAutoCommit { gcMaxPollIntervalMs = 1500 })
    subscribe consumer [slowTopic]
    first <- pollUntilError consumer 10 15000
    must (commit consumer)
    -- Stall past max.poll.interval.ms: the member leaves the group.
    threadDelay 2500000
    forM_ [10 .. 19 :: Int] $ \i -> must (sendKeyed producer slowTopic (Just (bytes ("s" ++ show i))) Nothing)
    must (closeProducer producer)
    (second, pollErr) <- pollUntilError consumer 10 15000
    check "a member that stalled rejoins on its next poll"
      (length (fst first) == 10 && length second == 10 && pollErr == Nothing)
      ("first=" ++ show (length (fst first)) ++ " second=" ++ show (length second) ++ " err=" ++ show pollErr)
    must (closeGroupConsumer consumer)

  section "consumer group: time inside poll does not count against max.poll.interval"
  do
    joinTopic <- unique "hs-inpoll"
    producer <- must (newProducer address noLinger)
    _ <- must (routerPartitions (producerRouter producer) joinTopic)
    inpollGroup <- unique "hs-inpoll-grp"
    -- Far shorter than the first poll below, which spends ~1s joining
    -- (the broker's initial rebalance delay) and then waits for data.
    consumer <- must (newGroupConsumer address inpollGroup noAutoCommit { gcMaxPollIntervalMs = 600 })
    subscribe consumer [joinTopic]
    _ <- forkIO $ do
      threadDelay 2000000
      forM_ [0 .. 9 :: Int] $ \i ->
        void (try (sendKeyed producer joinTopic (Just (bytes ("j" ++ show i))) Nothing) :: IO (Either SomeException ()))
    -- One long poll: it joins, then waits for the records above.
    pollResult <- try (poll consumer 4000)
    -- Committed straight away, before another poll could quietly rejoin:
    -- this fails if the member left the group mid-poll.
    commitResult <- try (commit consumer)
    let got = either (const []) id pollResult
    check "a member is still in its group after a long poll"
      (either (const False) (const True) pollResult && not (null got)
         && either (\(_ :: BrahmaputraError) -> False) (const True) commitResult)
      ("got=" ++ show (length got) ++ " poll=" ++ either (\(e :: BrahmaputraError) -> show e) (const "ok") pollResult
         ++ " commit=" ++ either show (const "ok") commitResult)
    must (closeGroupConsumer consumer)
    must (closeProducer producer)

  (passed, failed) <- readIORef counters
  putStrLn ("\n" ++ show passed ++ " passed, " ++ show failed ++ " failed")
  when (failed > 0) $ exitWith (ExitFailure 1)

-- | Poll until @want@ records or the deadline, stopping at the first error.
pollUntilError :: GroupConsumer -> Int -> Int -> IO ([ConsumerRecord], Maybe String)
pollUntilError consumer want deadlineMs = do
  start <- monoMs
  let go acc = do
        el <- elapsedSince start
        if length acc >= want || el >= fromIntegral deadlineMs then pure (acc, Nothing) else do
          r <- try (poll consumer 300)
          case r of
            Left (e :: BrahmaputraError) -> pure (acc, Just (show e))
            Right records -> go (acc ++ records)
  go []

-- ---------------------------------------------------------------------------
-- Sockets for the failure tests
-- ---------------------------------------------------------------------------

listenLocal :: IO NS.Socket
listenLocal = do
  sock <- NS.socket NS.AF_INET NS.Stream NS.defaultProtocol
  NS.setSocketOption sock NS.ReuseAddr 1
  NS.bind sock (NS.SockAddrInet 0 (NS.tupleToHostAddress (127, 0, 0, 1)))
  NS.listen sock 64
  pure sock

acceptLoop :: NS.Socket -> (NS.Socket -> IO ()) -> IO ()
acceptLoop listener handler =
  forever (NS.accept listener >>= handler . fst) `catch` \(_ :: SomeException) -> pure ()

-- | Forwards TCP to the broker and can sever every live connection, which
-- is how a broker restart or an idle timeout looks to a client.
data Proxy = Proxy
  { proxyAddress :: String
  , proxyListener :: NS.Socket
  , proxyLive :: MVar [NS.Socket]
  }

newProxy :: String -> IO Proxy
newProxy target = do
  listener <- listenLocal
  port <- NS.socketPort listener
  live <- newMVar []
  let (host, targetPort) = let (p, h) = break (== ':') (reverse target) in (reverse (drop 1 h), reverse p)
  _ <- forkIO $ acceptLoop listener $ \client -> do
    upstream <- try $ do
      addr : _ <- NS.getAddrInfo (Just NS.defaultHints { NS.addrSocketType = NS.Stream }) (Just host) (Just targetPort)
      s <- NS.socket (NS.addrFamily addr) NS.Stream NS.defaultProtocol
      NS.connect s (NS.addrAddress addr)
      pure s
    case upstream of
      Left (_ :: SomeException) -> NS.close client
      Right up -> do
        modifyMVar_ live (pure . ([client, up] ++))
        void (forkIO (pump client up))
        void (forkIO (pump up client))
  pure (Proxy ("127.0.0.1:" ++ show port) listener live)
  where
    pump from to = do
      let go = do
            chunk <- NSB.recv from 65536
            unless (BS.null chunk) (NSB.sendAll to chunk >> go)
      go `catch` (\(_ :: SomeException) -> pure ())
      sever to

sever :: NS.Socket -> IO ()
sever s = do
  NS.shutdown s NS.ShutdownBoth `catch` \(_ :: SomeException) -> pure ()
  NS.close s `catch` \(_ :: SomeException) -> pure ()

dropAll :: Proxy -> IO ()
dropAll proxy = do
  socks <- modifyMVar (proxyLive proxy) (\s -> pure ([], s))
  mapM_ sever socks
  threadDelay 50000

closeProxy :: Proxy -> IO ()
closeProxy proxy = do
  NS.close (proxyListener proxy) `catch` \(_ :: SomeException) -> pure ()
  dropAll proxy
