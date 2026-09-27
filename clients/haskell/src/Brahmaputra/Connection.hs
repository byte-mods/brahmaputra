{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | Connections, metadata and leader routing.
module Brahmaputra.Connection
  ( -- * One connection
    Conn
  , connAddress
  , dial
  , request
  , sendOneway
  , setRequestTimeout
  , isBroken
  , closeConn
  , defaultRequestTimeoutMs
  , ApiVersionRange (..)
  , apiVersions
    -- * Metadata
  , BrokerInfo (..)
  , PartitionInfo (..)
  , TopicInfo (..)
  , ClusterMetadata (..)
  , partitionsOf
  , leaderOf
    -- * Routing
  , Router
  , newRouter
  , closeRouter
  , routerSeed
  , routerMetadata
  , refreshMetadata
  , routerPartitions
  , connFor
    -- * Clock
  , nowMs
  , monoMs
  ) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad (forM_, unless, when)
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Lazy as BL
import Data.Bits (shiftL, (.|.))
import Data.Int (Int16, Int32, Int64)
import Data.Word (Word32)
import Data.IORef
import Data.List (sort)
import qualified Data.Map.Strict as Map
import Data.Maybe (listToMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import GHC.Clock (getMonotonicTimeNSec)
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as NSB
import System.Timeout (timeout)

import Brahmaputra.Protocol

-- | Wall-clock unix milliseconds.
nowMs :: IO Int64
nowMs = (\t -> floor (t * 1000)) <$> getPOSIXTime

-- | Monotonic milliseconds, for deadlines.
monoMs :: IO Int64
monoMs = (\ns -> fromIntegral (ns `div` 1000000)) <$> getMonotonicTimeNSec

-- ---------------------------------------------------------------------------
-- Conn
-- ---------------------------------------------------------------------------

-- | One TCP connection to one broker.
--
-- An 'MVar' serialises request/response pairs, so there is at most one
-- request in flight per connection. Any I/O failure, timeout or
-- correlation mismatch leaves the byte stream at an unknown position, so
-- the connection is closed and marked broken rather than reused; the
-- 'Router' notices and redials.
data Conn = Conn
  { connSocket :: !NS.Socket
  , connAddress :: !String
  , connClientId :: !Text
  , connNext :: !(MVar Int32)
  , connTimeout :: !(IORef Int)
  , connBroken :: !(IORef Bool)
  }

instance Eq Conn where
  a == b = connBroken a == connBroken b

-- | Round-trip bound for one request: 120 s. It must exceed the longest
-- the broker may legitimately hold a request (a fetch long-poll, a
-- JoinGroup waiting out a rebalance); its job is to turn a wedged broker
-- into an error instead of a thread blocked forever.
defaultRequestTimeoutMs :: Int
defaultRequestTimeoutMs = 120000

splitAddress :: String -> IO (String, String)
splitAddress address =
  case break (== ':') (reverse address) of
    (revPort, ':' : revHost) | not (null revPort) ->
      pure (stripBrackets (reverse revHost), reverse revPort)
    _ -> throwIO (ClientError ("address must be host:port, got " ++ show address))
  where
    stripBrackets ('[' : rest) | not (null rest) && last rest == ']' = init rest
    stripBrackets h = h

-- | Open a connection to @host:port@, giving up after @dialTimeoutMs@.
dial :: String -> Text -> Int -> IO Conn
dial address clientId dialTimeoutMs = do
  (host, port) <- splitAddress address
  let hints = NS.defaultHints { NS.addrSocketType = NS.Stream }
      open = do
        addrs <- NS.getAddrInfo (Just hints) (Just host) (Just port)
        connectFirst addrs
  result <- try (bounded dialTimeoutMs open)
  case result of
    Left (e :: IOException) -> throwIO (ConnectionError ("dial " ++ address ++ ": " ++ displayException e))
    Right Nothing -> throwIO (ConnectionError ("dial " ++ address ++ ": timed out"))
    Right (Just sock) -> do
      -- Latency matters more than packet count for small responses.
      NS.setSocketOption sock NS.NoDelay 1 `catch` \(_ :: IOException) -> pure ()
      Conn sock address clientId <$> newMVar 0 <*> newIORef defaultRequestTimeoutMs <*> newIORef False
  where
    connectFirst [] = throwIO (userError "no addresses")
    connectFirst (a : rest) = do
      r <- try $ bracketOnError (NS.socket (NS.addrFamily a) NS.Stream NS.defaultProtocol) NS.close $ \s -> do
        NS.connect s (NS.addrAddress a)
        pure s
      case r of
        Right s -> pure s
        Left (e :: IOException) -> if null rest then throwIO e else connectFirst rest

bounded :: Int -> IO a -> IO (Maybe a)
bounded ms action
  | ms <= 0 = Just <$> action
  | otherwise = timeout (ms * 1000) action

-- | Change how long one round trip may take before the connection is
-- abandoned. Zero or negative disables the bound.
setRequestTimeout :: Conn -> Int -> IO ()
setRequestTimeout c ms = writeIORef (connTimeout c) ms

-- | Whether this connection failed and must not be reused.
isBroken :: Conn -> IO Bool
isBroken c = readIORef (connBroken c)

closeConn :: Conn -> IO ()
closeConn c = do
  writeIORef (connBroken c) True
  NS.close (connSocket c) `catch` \(_ :: IOException) -> pure ()

markBroken :: Conn -> IO ()
markBroken = closeConn

-- | Run one exchange on the socket, bounded by the request timeout. Any
-- failure closes the connection and is rethrown as 'ConnectionError'.
guarded :: Conn -> String -> IO a -> IO a
guarded c what action = do
  ms <- readIORef (connTimeout c)
  result <- try (bounded ms action)
  case result of
    Right (Just a) -> pure a
    Right Nothing -> do
      -- The response may still be on its way; reading on from here would
      -- pair it with the next request.
      markBroken c
      throwIO (ConnectionError (what ++ " to " ++ connAddress c ++ " timed out after " ++ show ms ++ " ms"))
    Left (e :: SomeException) -> do
      markBroken c
      case fromException e of
        Just (be :: BrahmaputraError) -> throwIO be
        Nothing -> case fromException e of
          Just (_ :: SomeAsyncException) -> throwIO e
          Nothing -> throwIO (ConnectionError (what ++ " to " ++ connAddress c ++ ": " ++ displayException e))

brokenError :: BrahmaputraError
brokenError = ConnectionError "connection is broken; the router will redial"

-- | Send one request and return the matching response body.
request :: Conn -> Int16 -> ByteString -> IO ByteString
request c key body = modifyMVar (connNext c) $ \n -> do
  broken <- readIORef (connBroken c)
  when broken $ throwIO brokenError
  let cid = n + 1
  payload <- guarded c "request" $ do
    NSB.sendAll (connSocket c) (BL.toStrict (B.toLazyByteString (encodeFrame key cid (connClientId c) body)))
    readFrame (connSocket c)
  case decodeFramePayload payload of
    Left e -> markBroken c >> throwIO (ProtocolError e)
    Right (got, responseBody) -> do
      when (got /= cid) $ do
        -- The stream has desynchronised; continuing would pair every later
        -- response with the wrong request.
        markBroken c
        throwIO (ConnectionError ("correlation id mismatch: expected " ++ show cid ++ ", got " ++ show got))
      pure (cid, responseBody)

-- | Send without awaiting a response (acks=0).
sendOneway :: Conn -> Int16 -> ByteString -> IO ()
sendOneway c key body = modifyMVar_ (connNext c) $ \n -> do
  broken <- readIORef (connBroken c)
  when broken $ throwIO brokenError
  let cid = n + 1
  guarded c "send" $
    NSB.sendAll (connSocket c) (BL.toStrict (B.toLazyByteString (encodeFrame key cid (connClientId c) body)))
  pure cid

readFrame :: NS.Socket -> IO ByteString
readFrame sock = do
  header <- recvExact sock 4
  let len = fromIntegral (BS.foldl' (\acc b -> (acc `shiftL` 8) .|. fromIntegral b) (0 :: Word32) header) :: Int32
  when (len < 0) $ throwIO (ProtocolError ("negative frame length " ++ show len))
  recvExact sock (fromIntegral len)

recvExact :: NS.Socket -> Int -> IO ByteString
recvExact sock total = go total []
  where
    go 0 acc = pure (BS.concat (reverse acc))
    go left acc = do
      chunk <- NSB.recv sock (min left (1024 * 1024))
      when (BS.null chunk) $ throwIO (ConnectionError "connection closed by broker")
      go (left - BS.length chunk) (chunk : acc)

-- | One entry of an ApiVersions response.
data ApiVersionRange = ApiVersionRange
  { avApiKey :: !Int32
  , avMinVersion :: !Int32
  , avMaxVersion :: !Int32
  } deriving (Eq, Show)

-- | Ask the broker what it speaks, and its software version.
apiVersions :: Conn -> IO ([ApiVersionRange], Text)
apiVersions c = do
  response <- request c apiApiVersions (buildBody [wString "brahmaputra-haskell", wString "0.1.0"])
  readBody response $ do
    code <- rInt32
    unless (code == errNone) $ failReader (show (ServerError code "api_versions"))
    ranges <- rArray (ApiVersionRange <$> rInt32 <*> rInt32 <*> rInt32)
    version <- rString
    pure (ranges, version)

-- ---------------------------------------------------------------------------
-- Metadata
-- ---------------------------------------------------------------------------

data BrokerInfo = BrokerInfo
  { brokerNodeId :: !Int32
  , brokerHost :: !Text
  , brokerPort :: !Int32
  , brokerRack :: !Text
  } deriving (Eq, Show)

data PartitionInfo = PartitionInfo
  { piPartition :: !Int32
  , piLeader :: !Int32
  , piReplicas :: ![Int32]
  , piIsr :: ![Int32]
  , piLeaderEpoch :: !Int32
  } deriving (Eq, Show)

data TopicInfo = TopicInfo
  { topicName :: !Text
  , topicPartitions :: ![PartitionInfo]
  } deriving (Eq, Show)

data ClusterMetadata = ClusterMetadata
  { metaBrokers :: ![BrokerInfo]
  , metaTopics :: ![TopicInfo]
  } deriving (Eq, Show)

-- | A topic's partition ids, ascending.
partitionsOf :: ClusterMetadata -> Text -> [Int32]
partitionsOf meta topic =
  maybe [] (sort . map piPartition . topicPartitions)
    (listToMaybe [t | t <- metaTopics meta, topicName t == topic])

-- | The broker id leading a partition, if known.
leaderOf :: ClusterMetadata -> Text -> Int32 -> Maybe Int32
leaderOf meta topic partition =
  listToMaybe [ piLeader p | t <- metaTopics meta, topicName t == topic
                           , p <- topicPartitions t, piPartition p == partition, piLeader p >= 0 ]

-- Field order is the schema's: error_code, brokers, controller_id, topics.
metadataReader :: Reader (Either BrahmaputraError ClusterMetadata)
metadataReader = do
  code <- rInt32
  if code /= errNone
    then pure (Left (ServerError code "metadata"))
    else do
      brokers <- rArray (BrokerInfo <$> rInt32 <*> rString <*> rInt32 <*> rString)
      _controller <- rInt32
      topics <- rArray $ do
        name <- rString
        topicError <- rInt32
        parts <- rArray (PartitionInfo <$> rInt32 <*> rInt32 <*> rArray rInt32 <*> rArray rInt32 <*> rInt32)
        pure (name, topicError, parts)
      pure $ case [ServerError e ("metadata for " ++ T.unpack n) | (n, e, _) <- topics, e /= errNone, e /= errUnknownTopicOrPartition] of
        (err : _) -> Left err
        [] -> Right (ClusterMetadata brokers [TopicInfo n ps | (n, _, ps) <- topics])

-- ---------------------------------------------------------------------------
-- Router
-- ---------------------------------------------------------------------------

data RouterState = RouterState
  { rsSeed :: !Conn
  , rsConns :: !(Map.Map Int32 Conn)
  , rsMeta :: !(Maybe ClusterMetadata)
  }

-- | Keeps a connection to every broker and routes by partition leader.
--
-- Metadata is cached and refreshed only when a request says the route was
-- stale. A connection that failed is replaced on its next use — the seed
-- included — so one dropped socket does not fail every later request.
data Router = Router
  { routerClientId :: !Text
  , routerDialTimeoutMs :: !Int
  , routerRequestTimeoutMs :: !Int
  , routerSeedAddress :: !String
  , routerState :: !(MVar RouterState)
  }

-- | @newRouter address clientId dialTimeoutMs requestTimeoutMs@.
newRouter :: String -> Text -> Int -> Int -> IO Router
newRouter address clientId dialMs requestMs = do
  seed <- dial address clientId dialMs
  setRequestTimeout seed requestMs
  Router clientId dialMs requestMs address <$> newMVar (RouterState seed Map.empty Nothing)

closeRouter :: Router -> IO ()
closeRouter router = modifyMVar_ (routerState router) $ \st -> do
  forM_ (Map.elems (rsConns st)) closeConn
  closeConn (rsSeed st)
  pure st { rsConns = Map.empty }

dialRouted :: Router -> String -> IO Conn
dialRouted router address = do
  c <- dial address (routerClientId router) (routerDialTimeoutMs router)
  setRequestTimeout c (routerRequestTimeoutMs router)
  pure c

-- The seed, redialled if it broke.
liveSeed :: Router -> RouterState -> IO (RouterState, Conn)
liveSeed router st = do
  broken <- isBroken (rsSeed st)
  if not broken
    then pure (st, rsSeed st)
    else do
      fresh <- dialRouted router (routerSeedAddress router)
      let old = rsSeed st
          conns = Map.map (\c -> if c == old then fresh else c) (rsConns st)
      pure (st { rsSeed = fresh, rsConns = conns }, fresh)

-- | The connection this router was opened with, redialled if it failed.
routerSeed :: Router -> IO Conn
routerSeed router = modifyMVar (routerState router) (liveSeed router)

-- | Cluster metadata for these topics (all topics when empty); cached
-- unless @refresh@.
routerMetadata :: Router -> [Text] -> Bool -> IO ClusterMetadata
routerMetadata router topics refresh = modifyMVar (routerState router) $ \st0 ->
  case rsMeta st0 of
    Just meta | not refresh -> pure (st0, meta)
    _ -> do
      (st, seed) <- liveSeed router st0
      response <- request seed apiMetadata (buildBody [wStringArray topics])
      decoded <- readBody response metadataReader
      meta <- either throwIO pure decoded
      pure (st { rsMeta = Just meta }, meta)

refreshMetadata :: Router -> Text -> IO ClusterMetadata
refreshMetadata router topic = routerMetadata router [topic] True

-- | A topic's partitions, ascending. A topic auto-created on first
-- reference is not in the cached image yet, so one refresh distinguishes
-- "new" from "absent".
routerPartitions :: Router -> Text -> IO [Int32]
routerPartitions router topic = do
  meta <- routerMetadata router [topic] False
  parts <- case partitionsOf meta topic of
    [] -> (`partitionsOf` topic) <$> refreshMetadata router topic
    ps -> pure ps
  when (null parts) $ throwIO (ClientError ("topic " ++ show topic ++ " has no partitions"))
  pure parts

-- | The connection to a partition's leader.
connFor :: Router -> Text -> Int32 -> IO Conn
connFor router topic partition = do
  meta0 <- routerMetadata router [topic] False
  (meta, leader) <- case leaderOf meta0 topic partition of
    Just l -> pure (meta0, l)
    Nothing -> do
      meta1 <- refreshMetadata router topic
      case leaderOf meta1 topic partition of
        Just l -> pure (meta1, l)
        Nothing -> throwIO (ClientError ("no leader for " ++ T.unpack topic ++ "-" ++ show partition))
  modifyMVar (routerState router) $ \st0 -> do
    cached <- case Map.lookup leader (rsConns st0) of
      Nothing -> pure Nothing
      Just c -> do
        broken <- isBroken c
        pure (if broken then Nothing else Just c)
    case cached of
      Just c -> pure (st0, c)
      Nothing -> do
        let st1 = st0 { rsConns = Map.delete leader (rsConns st0) }
        case [b | b <- metaBrokers meta, brokerNodeId b == leader] of
          [] -> throwIO (ClientError ("broker " ++ show leader ++ " is not in the metadata"))
          (b : _)
            -- A single-broker cluster advertises the address it was
            -- configured with, which may not be the one we dialled; reuse
            -- the seed rather than a second connection to ourselves.
            | length (metaBrokers meta) == 1 -> do
                (st2, seed) <- liveSeed router st1
                pure (st2 { rsConns = Map.insert leader seed (rsConns st2) }, seed)
            | otherwise -> do
                c <- dialRouted router (T.unpack (brokerHost b) ++ ":" ++ show (brokerPort b))
                pure (st1 { rsConns = Map.insert leader c (rsConns st1) }, c)
