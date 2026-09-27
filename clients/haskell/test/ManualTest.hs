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
import Data.Bits (shiftL, shiftR, (.&.), (.|.))
import Data.IORef
import Data.List (sort)
import Data.Word (Word8)
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

  coverage address

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

-- ---------------------------------------------------------------------------
-- Coverage: one check per client feature the sections above do not
-- already exercise.
-- ---------------------------------------------------------------------------

isLeft' :: Either a b -> Bool
isLeft' = either (const True) (const False)

coverage :: String -> IO ()
coverage address = do
  section "producer settings"
  consumer <- must (newConsumer address defaultConsumerConfig)
  do
    -- batch.size: a full partition goes out at once although linger would
    -- hold it for a minute.
    topic <- unique "hs-batchsize"
    producer <- must (newProducer address defaultProducerConfig { pcLingerMs = 60000, pcBatchSize = 64 })
    forM_ [0 .. 2 :: Int] $ \i -> must (sendTo producer topic 0 (Just (bytes (replicate 100 'b' ++ show i))) Nothing [])
    got <- must (fetch consumer topic 0 0 1000)
    check "batch.size sends a full batch without waiting for linger" (length got == 3) ("got " ++ show (length got))
    void (try (closeProducer producer) :: IO (Either SomeException ()))
  do
    topic <- unique "hs-linger"
    producer <- must (newProducer address defaultProducerConfig { pcLingerMs = 50, pcBatchSize = 1048576 })
    must (sendTo producer topic 0 (Just "lingering") Nothing [])
    threadDelay 500000
    got <- must (fetch consumer topic 0 0 1000)
    check "linger.ms flushes a partial batch on its own" (length got == 1) ("got " ++ show (length got))
    void (try (closeProducer producer) :: IO (Either SomeException ()))
  do
    topic <- unique "hs-sync"
    producer <- must (newProducer address noLinger)
    let stamp = 1600000000000 :: Int64
        at ts v = (producerRecord topic (Just v)) { prPartition = Just 2, prTimestamp = Just ts }
    first <- must (sendSync producer (at stamp "one"))
    second <- must (sendSync producer (at (stamp + 1000) "two"))
    check "send_sync returns consecutive offsets" (first == 0 && second == 1) (show (first, second))
    got <- must (fetch consumer topic 2 0 1000)
    check "an explicit partition is honoured" (length got == 2) ("partition 2 holds " ++ show (length got))
    check "an explicit timestamp is stored exactly" (map crTimestamp got == [stamp, stamp + 1000])
      (show (map crTimestamp got))
    rrTopic <- unique "hs-roundrobin"
    parts <- must (routerPartitions (producerRouter producer) rrTopic)
    forM_ [1 .. 2 * length parts] $ \i -> must (sendKeyed producer rrTopic (Just (bytes ("rr" ++ show i))) Nothing)
    must (flush producer)
    counts <- forM parts $ \part -> length <$> must (fetch consumer rrTopic part 0 300)
    check "keyless records are spread round-robin" (all (== 2) counts) (show counts)
    must (closeProducer producer)
  do
    -- A codec the driver does not carry, registered by the application: a
    -- valid LZ4 block of literals only, which the broker accepts as-is.
    registerCodec Lz4 (pure . lz4Compress) lz4Decompress
    topic <- unique "hs-lz4"
    let body i = bytes (concat (replicate 20 "registered codec payload ") ++ show i)
    producer <- must (newProducer address noLinger { pcCompression = Lz4 })
    forM_ [0 .. 4 :: Int] $ \i -> must (sendTo producer topic 0 (Just (body i)) Nothing [])
    must (closeProducer producer)
    got <- must (fetch consumer topic 0 0 1000)
    check "a registered codec round-trips through the broker"
      (map crValue got == [Just (body i) | i <- [0 .. 4 :: Int]]) ("got " ++ show (length got))
  closeConsumer consumer

  section "retries against a broker that refuses"
  do
    (fakeAddress, produces, stopFake) <- fakeBroker
    producer <- must (newProducer fakeAddress noLinger
      { pcAcks = AcksAll, pcRequestTimeoutMs = 1234, pcRetries = 2, pcRetryBackoffMs = 150 })
    started <- monoMs
    result <- try (sendSync producer (producerRecord "retriable" (Just "x")) { prPartition = Just 0 })
    took <- elapsedSince started
    attempts <- readIORef produces
    check "request.timeout.ms and acks reach the broker"
      (not (null attempts) && all (\(_, acks, t) -> acks == -1 && t == 1234) attempts) (show attempts)
    check "a retriable error is retried `retries` times"
      (either (\(_ :: BrahmaputraError) -> True) (const False) result && length attempts == 3)
      (show (length attempts) ++ " attempts")
    check "retry.backoff.ms spaces the retries" (took >= 300) (show took ++ " ms")
    writeIORef produces []
    fatal <- try (sendSync producer (producerRecord "fatal" (Just "x")) { prPartition = Just 0 })
    fatalAttempts <- readIORef produces
    check "a non-retriable error is not retried"
      (either (\(_ :: BrahmaputraError) -> True) (const False) fatal && length fatalAttempts == 1)
      (show (length fatalAttempts) ++ " attempts")
    void (try (closeProducer producer) :: IO (Either SomeException ()))
    writeIORef produces []
    capped <- must (newProducer fakeAddress noLinger
      { pcRetries = 1000000, pcRetryBackoffMs = 50, pcDeliveryTimeoutMs = 400 })
    started' <- monoMs
    result' <- try (sendSync capped (producerRecord "retriable" (Just "x")) { prPartition = Just 0 })
    took' <- elapsedSince started'
    n <- length <$> readIORef produces
    check "delivery.timeout.ms caps the retries"
      (either (\(_ :: BrahmaputraError) -> True) (const False) result' && took' < 3000)
      (show took' ++ " ms, " ++ show n ++ " attempts")
    void (try (closeProducer capped) :: IO (Either SomeException ()))
    stopFake

  section "consumer settings"
  do
    topic <- unique "hs-fetchcfg"
    producer <- must (newProducer address noLinger)
    forM_ [0 .. 19 :: Int] $ \i -> must (sendTo producer topic 0 (Just (bytes (replicate 1000 'f' ++ show i))) Nothing [])
    must (closeProducer producer)
    c <- must (newConsumer address defaultConsumerConfig)
    (records, hw) <- must (fetchVerbose c topic 0 0 500)
    check "fetch reports the high watermark" (hw == 20) (show hw)
    check "a default fetch returns every record" (length records == 20) ("got " ++ show (length records))
    meta <- must (refreshMetadata (consumerRouter c) topic)
    let brokers = Set.fromList (map brokerNodeId (metaBrokers meta))
        infos = concat [topicPartitions t | t <- metaTopics meta, topicName t == topic]
    check "metadata names a live leader for every partition"
      (not (null infos) && all ((`Set.member` brokers) . piLeader) infos) (show infos)
    closeConsumer c
    small <- must (newConsumer address defaultConsumerConfig { ccFetchMaxBytes = 2500 })
    got <- must (fetch small topic 0 0 500)
    check "fetch.max.bytes caps a response" (not (null got) && length got < 20) ("got " ++ show (length got))
    closeConsumer small
    patient <- must (newConsumer address defaultConsumerConfig { ccFetchMinBytes = 10000000, ccFetchMaxWaitMs = 400 })
    started <- monoMs
    tailRecords <- must (fetch patient topic 0 19 400)
    waited <- elapsedSince started
    check "fetch.min.bytes holds a fetch for up to fetch.max.wait.ms"
      (length tailRecords == 1 && waited >= 300 && waited < 5000)
      (show waited ++ " ms, " ++ show (length tailRecords) ++ " records")
    closeConsumer patient
  do
    topic <- unique "hs-bytime"
    producer <- must (newProducer address noLinger)
    let base = 1700000000000 :: Int64
    forM_ [0 .. 2 :: Int64] $ \i ->
      must (send producer (producerRecord topic (Just (bytes ("t" ++ show i))))
              { prPartition = Just 0, prTimestamp = Just (base + i * 10000) })
    must (closeProducer producer)
    c <- must (newConsumer address defaultConsumerConfig)
    at <- must (listOffsets c topic 0 (AtTimestamp (base + 5000)))
    check "list offsets by timestamp finds the first record at or after it" (at == 1) (show at)
    closeConsumer c
  do
    topic <- unique "hs-maxpoll"
    producer <- must (newProducer address noLinger)
    forM_ [0 .. 9 :: Int] $ \i -> must (sendTo producer topic 0 (Just (bytes ("m" ++ show i))) Nothing [])
    must (closeProducer producer)
    groupId <- unique "hs-maxpoll-grp"
    g <- must (newGroupConsumer address groupId noAutoCommit { gcMaxPollRecords = 3 })
    subscribe g [topic]
    sizes <- pollSizes g 10 15000
    check "max.poll.records caps a poll" (sum sizes == 10 && all (<= 3) sizes) (show sizes)
    must (closeGroupConsumer g)

  section "consumer group settings"
  producer <- must (newProducer address noLinger)
  do
    t1 <- unique "hs-multi-a"
    t2 <- unique "hs-multi-b"
    forM_ [0 .. 4 :: Int] $ \i -> must (sendKeyed producer t1 (Just (bytes ("a" ++ show i))) Nothing)
    forM_ [0 .. 4 :: Int] $ \i -> must (sendKeyed producer t2 (Just (bytes ("b" ++ show i))) Nothing)
    groupId <- unique "hs-multi-grp"
    g <- must (newGroupConsumer address groupId noAutoCommit)
    subscribe g [t1, t2]
    got <- pollUntil g 10 15000 300
    let perTopic = Map.fromListWith (+) [(crTopic r, 1 :: Int) | r <- got]
    check "a group consumes every subscribed topic" (perTopic == Map.fromList [(t1, 5), (t2, 5)]) (show perTopic)
    must (closeGroupConsumer g)
  do
    topic <- unique "hs-autocommit"
    forM_ [0 .. 5 :: Int] $ \i -> must (sendTo producer topic 0 (Just (bytes ("c" ++ show i))) Nothing [])
    let slot = TopicPartition topic 0
    autoGroup <- unique "hs-auto-grp"
    g <- must (newGroupConsumer address autoGroup defaultGroupConfig { gcAutoCommitIntervalMs = 100 })
    subscribe g [topic]
    _ <- pollUntil g 6 15000 300
    threadDelay 200000
    _ <- try (poll g 300) :: IO (Either BrahmaputraError [ConsumerRecord])
    committedAuto <- must (committed g [slot])
    check "auto-commit records positions without an explicit commit"
      (Map.lookup slot committedAuto == Just 6) (show committedAuto)
    must (closeGroupConsumer g)
    manualGroup <- unique "hs-noauto-grp"
    g' <- must (newGroupConsumer address manualGroup noAutoCommit)
    subscribe g' [topic]
    _ <- pollUntil g' 6 15000 300
    threadDelay 200000
    _ <- try (poll g' 300) :: IO (Either BrahmaputraError [ConsumerRecord])
    committedManual <- must (committed g' [slot])
    check "disabled auto-commit commits nothing"
      (maybe True (< 0) (Map.lookup slot committedManual)) (show committedManual)
    must (closeGroupConsumer g')
  do
    -- Static membership: a second instance presenting the same
    -- group.instance.id takes over the first one's partitions at once,
    -- without a rebalance, while the first is still heartbeating.
    topic <- unique "hs-static"
    _ <- must (routerPartitions (producerRouter producer) topic)
    groupId <- unique "hs-static-grp"
    let config = noAutoCommit { gcGroupInstanceId = Just "instance-1" }
    first <- must (newGroupConsumer address groupId config)
    subscribe first [topic]
    _ <- try (poll first 2000) :: IO (Either BrahmaputraError [ConsumerRecord])
    firstAssignment <- assignment first
    second <- must (newGroupConsumer address groupId config)
    subscribe second [topic]
    started <- monoMs
    _ <- try (poll second 200) :: IO (Either BrahmaputraError [ConsumerRecord])
    took <- elapsedSince started
    secondAssignment <- assignment second
    check "a static member reclaims its partitions without a rebalance"
      (length firstAssignment == 4 && sort secondAssignment == sort firstAssignment && took < 2000)
      ("first=" ++ show firstAssignment ++ " second=" ++ show secondAssignment ++ " " ++ show took ++ " ms")
    void (try (closeGroupConsumer second) :: IO (Either SomeException ()))
    void (try (closeGroupConsumer first) :: IO (Either SomeException ()))
  do
    -- LeaveGroup on close: with a 30 s session and a 200 ms heartbeat, the
    -- survivor takes over within a heartbeat, not a session.
    topic <- unique "hs-leave"
    _ <- must (routerPartitions (producerRouter producer) topic)
    groupId <- unique "hs-leave-grp"
    let config = noAutoCommit { gcSessionTimeoutMs = 30000, gcHeartbeatIntervalMs = 200 }
    a <- must (newGroupConsumer address groupId config)
    b <- must (newGroupConsumer address groupId config)
    subscribe a [topic]
    subscribe b [topic]
    split <- settle [a, b] (do { as <- mapM assignment [a, b]; pure (map length as == [2, 2]) }) 20000
    must (closeGroupConsumer a)
    started <- monoMs
    tookOver <- settle [b] ((== 4) . length <$> assignment b) 15000
    took <- elapsedSince started
    check "closing a member hands its partitions over within a heartbeat"
      (split && tookOver && took < 5000)
      ("split=" ++ show split ++ " took_over=" ++ show tookOver ++ " " ++ show took ++ " ms")
    must (closeGroupConsumer b)
  do
    -- session.timeout.ms: a member that goes silent without leaving (its
    -- only route to the broker is a proxy that is shut) is evicted once its
    -- session lapses, and the survivor takes over.
    topic <- unique "hs-session"
    _ <- must (routerPartitions (producerRouter producer) topic)
    groupId <- unique "hs-session-grp"
    let config = noAutoCommit { gcSessionTimeoutMs = 2000, gcHeartbeatIntervalMs = 200 }
    proxy <- newProxy address
    a <- must (newGroupConsumer (proxyAddress proxy) groupId config)
    b <- must (newGroupConsumer address groupId config)
    subscribe a [topic]
    subscribe b [topic]
    split <- settle [a, b] (do { as <- mapM assignment [a, b]; pure (map length as == [2, 2]) }) 20000
    closeProxy proxy
    started <- monoMs
    tookOver <- settle [b] ((== 4) . length <$> assignment b) 20000
    took <- elapsedSince started
    check "a silent member is evicted after session.timeout.ms"
      (split && tookOver && took >= 1000 && took < 12000)
      ("split=" ++ show split ++ " took_over=" ++ show tookOver ++ " " ++ show took ++ " ms")
    must (closeGroupConsumer b)
    void (try (closeGroupConsumer a) :: IO (Either SomeException ()))
  do
    -- Generation fencing: a member whose generation moved on cannot commit.
    topic <- unique "hs-fence"
    forM_ [0 .. 3 :: Int] $ \i -> must (sendKeyed producer topic (Just (bytes ("f" ++ show i))) Nothing)
    groupId <- unique "hs-fence-grp"
    a <- must (newGroupConsumer address groupId noAutoCommit)
    subscribe a [topic]
    _ <- pollUntil a 4 10000 300
    b <- must (newGroupConsumer address groupId noAutoCommit)
    subscribe b [topic]
    _ <- try (poll b 500) :: IO (Either BrahmaputraError [ConsumerRecord])
    fenced <- try (commit a)
    check "a commit from a stale generation is refused"
      (either (\(_ :: BrahmaputraError) -> True) (const False) fenced) "commit succeeded"
    void (try (closeGroupConsumer b) :: IO (Either SomeException ()))
    void (try (closeGroupConsumer a) :: IO (Either SomeException ()))
  must (closeProducer producer)

  section "assignors (unit)"
  do
    let members = [AssignorMember "a" ["t"], AssignorMember "b" ["t"]]
        slots = map (TopicPartition "t")
        sticky = stickyAssign members (Map.fromList [("t", [0 .. 11])])
                   (Map.fromList [("a", slots [0 .. 11]), ("b", [])])
    check "sticky keeps partitions in numeric order"
      (Map.lookup "a" sticky == Just (slots [0 .. 5]) && Map.lookup "b" sticky == Just (slots [6 .. 11]))
      (show sticky)
    let held = Map.fromList [("a", slots [1, 3]), ("b", slots [0, 2])]
        kept = stickyAssign members (Map.fromList [("t", [0 .. 3])]) held
    check "sticky keeps what members already hold" (kept == held) (show kept)

  section "decoder bounds"
  do
    let negative = decodeBody rString (buildBody [wInt32 (-5)])
    check "a negative length is an error" (isLeft' negative) (show negative)
    let oversized = decodeBody rString (buildBody [wInt32 1000000, wRaw "short"])
    check "a length past the end of the data is an error" (isLeft' oversized) (show oversized)
    truncated <- try (decodeRecordBatches (BS.pack ([0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0xff, 0xff, 0xff] ++ replicate 4 0)))
    check "a batch longer than its bytes is an error"
      (either (\(_ :: BrahmaputraError) -> True) (const False) truncated) "decoded"

-- | Sizes of successive non-empty polls until @want@ records or the deadline.
pollSizes :: GroupConsumer -> Int -> Int -> IO [Int]
pollSizes g want deadlineMs = do
  start <- monoMs
  let go acc = do
        el <- elapsedSince start
        if sum acc >= want || el >= fromIntegral deadlineMs then pure acc else do
          r <- try (poll g 300)
          case r of
            Right records | not (null records) -> go (acc ++ [length records])
            Right _ -> go acc
            Left (_ :: BrahmaputraError) -> go acc
  go []

-- | Poll every consumer in parallel (a join blocks until every member has
-- rejoined) until @done@ holds or @ms@ pass.
settle :: [GroupConsumer] -> IO Bool -> Int -> IO Bool
settle groups done ms = do
  start <- monoMs
  let go = do
        vars <- forM groups $ \g -> do
          v <- newEmptyMVar
          _ <- forkIO ((void (try (poll g 200) :: IO (Either SomeException [ConsumerRecord]))) `finally` putMVar v ())
          pure v
        mapM_ takeMVar vars
        ok <- done
        el <- elapsedSince start
        if ok then pure True else if el >= fromIntegral ms then pure False else go
  go

-- | The broker's lz4 payload: a little-endian uncompressed length, then a
-- raw LZ4 block. This encoder writes literals only (valid, if uncompressed).
lz4Compress :: ByteString -> ByteString
lz4Compress bs = BS.concat [le32 n, token, bs]
  where
    n = BS.length bs
    token
      | n >= 15 = BS.pack (0xf0 : ext (n - 15))
      | otherwise = BS.singleton (fromIntegral (n `shiftL` 4))
    ext k = if k >= 255 then 255 : ext (k - 255) else [fromIntegral k]
    le32 v = BS.pack [fromIntegral (v `shiftR` s) | s <- [0, 8, 16, 24]]

lz4Decompress :: ByteString -> IO ByteString
lz4Decompress payload = pure (go (BS.drop 4 payload) BS.empty)
  where
    go block out
      | BS.null block = out
      | otherwise =
          let token = BS.head block
              (lits, rest) = len (fromIntegral (token `shiftR` 4)) (BS.tail block)
              out' = out <> BS.take lits rest
              rest' = BS.drop lits rest
          in if BS.null rest' then out' else
               let offset = fromIntegral (BS.index rest' 0) .|. (fromIntegral (BS.index rest' 1) `shiftL` 8)
                   (mlen, rest'') = len (fromIntegral (token .&. 15)) (BS.drop 2 rest')
               in go rest'' (copy out' offset (mlen + 4))
    len :: Int -> ByteString -> (Int, ByteString)
    len 15 bs = more 15 bs
    len n bs = (n, bs)
    more acc bs = let b = BS.head bs in
      if b == 255 then more (acc + 255) (BS.tail bs) else (acc + fromIntegral (b :: Word8), BS.tail bs)
    copy out _ 0 = out
    copy out offset k = copy (BS.snoc out (BS.index out (BS.length out - offset))) offset (k - 1 :: Int)

-- | A broker that answers Metadata with itself as the only broker and
-- refuses every produce: topic "fatal" with a non-retriable code, anything
-- else with NOT_ENOUGH_REPLICAS (retriable). Records (topic, acks,
-- timeout) for each produce it sees.
fakeBroker :: IO (String, IORef [(Text, Int32, Int32)], IO ())
fakeBroker = do
  listener <- listenLocal
  port <- NS.socketPort listener
  seen <- newIORef []
  let serve conn = do
        frame <- recvFrame conn
        case frame of
          Nothing -> NS.close conn
          Just payload -> do
            let apiKey = fromIntegral (BS.index payload 1) :: Int
                clen = fromIntegral (BS.index payload 8) * 256 + fromIntegral (BS.index payload 9)
                header = BS.take (10 + clen) payload
                req = BS.drop (10 + clen) payload
            body <- answer apiKey req
            let resp = header <> body
                n = BS.length resp
            NSB.sendAll conn (BS.pack [fromIntegral (n `shiftR` s) | s <- [24, 16, 8, 0]] <> resp)
            serve conn
      answer :: Int -> ByteString -> IO ByteString
      answer 3 req = do
        let topics = either (const []) id (decodeBody rStringArray req)
        pure $ buildBody $
          [ wInt32 0, wInt32 1, wInt32 0, wString "127.0.0.1", wInt32 (fromIntegral port), wString ""
          , wInt32 0, wInt32 (fromIntegral (length topics)) ]
          ++ concat [ [ wString t, wInt32 0, wInt32 1, wInt32 0, wInt32 0, wInt32 1, wInt32 0
                      , wInt32 1, wInt32 0, wInt32 0 ] | t <- topics ]
      answer 0 req = do
        let parsed = decodeBody ((,,,) <$> rString <*> rInt32 <*> rInt32 <*> rInt32) req
        case parsed of
          Left _ -> pure (buildBody [wInt32 35])
          Right (topic, partition, acks, t) -> do
            modifyIORef' seen (++ [(topic, acks, t)])
            let code = if topic == "fatal" then 87 else 10
            pure (buildBody [wString topic, wInt32 partition, wInt32 code, wInt64 (-1), wInt64 (-1)])
      answer _ _ = pure (buildBody [wInt32 35])
  _ <- forkIO $ acceptLoop listener $ \conn ->
    void (forkIO (serve conn `catch` \(_ :: SomeException) -> NS.close conn))
  pure ("127.0.0.1:" ++ show port, seen, NS.close listener)

recvExact :: NS.Socket -> Int -> IO (Maybe ByteString)
recvExact sock n = go n []
  where
    go 0 acc = pure (Just (BS.concat (reverse acc)))
    go left acc = do
      chunk <- NSB.recv sock left
      if BS.null chunk then pure Nothing else go (left - BS.length chunk) (chunk : acc)

recvFrame :: NS.Socket -> IO (Maybe ByteString)
recvFrame sock = do
  prefix <- recvExact sock 4
  case prefix of
    Nothing -> pure Nothing
    Just p -> recvExact sock (foldl (\acc b -> acc * 256 + fromIntegral b) 0 (BS.unpack p))
