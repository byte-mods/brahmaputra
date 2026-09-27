-- | Partition assignment strategies, run by the group leader. Pure.
--
-- Members computing an assignment independently must agree, so these
-- mirror the reference drivers exactly. Partitions are compared as
-- @(topic, partition-as-integer)@, never as strings.
module Brahmaputra.Assignor
  ( Assignor (..)
  , assignorName
  , AssignorMember (..)
  , assign
  , rangeAssign
  , roundRobinAssign
  , stickyAssign
  ) where

import Data.Int (Int32)
import Data.List (find, foldl', sort, sortOn)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.Text (Text)

import Brahmaputra.Protocol (TopicPartition (..))

-- | @partition.assignment.strategy@.
data Assignor
  = RangeAssignor        -- ^ contiguous ranges per topic
  | RoundRobinAssignor   -- ^ deal partitions round the members
  | StickyAssignor       -- ^ keep members on what they hold; move only what balance needs
  deriving (Eq, Show)

assignorName :: Assignor -> String
assignorName RangeAssignor = "range"
assignorName RoundRobinAssignor = "roundrobin"
assignorName StickyAssignor = "sticky"

data AssignorMember = AssignorMember
  { amId :: !Text
  , amTopics :: ![Text]
  } deriving (Eq, Show)

type Assignment = Map.Map Text [TopicPartition]

-- | Run a strategy. @previous@ is what each member held (sticky only).
assign :: Assignor -> [AssignorMember] -> Map.Map Text [Int32] -> Map.Map Text [TopicPartition] -> Assignment
assign RangeAssignor members tps _ = rangeAssign members tps
assign RoundRobinAssignor members tps _ = roundRobinAssign members tps
assign StickyAssignor members tps previous = stickyAssign members tps previous

emptyAssignment :: [AssignorMember] -> Assignment
emptyAssignment members = Map.fromList [(amId m, []) | m <- members]

subscribes :: AssignorMember -> Text -> Bool
subscribes m topic = topic `elem` amTopics m

-- | Each subscribed member gets a contiguous range per topic; the first
-- @partitions mod members@ take one extra.
rangeAssign :: [AssignorMember] -> Map.Map Text [Int32] -> Assignment
rangeAssign members topicPartitions = foldl' perTopic (emptyAssignment members) (Map.toAscList topicPartitions)
  where
    perTopic acc (topic, parts) =
      let subscribers = sort [amId m | m <- members, subscribes m topic]
          n = length subscribers
          (base, extra) = length parts `divMod` max 1 n
          counts = [base + (if i < extra then 1 else 0) | i <- [0 .. n - 1]]
          chunks = splitCounts counts parts
      in if n == 0 then acc
         else foldl' (\a (mid, ps) -> Map.insertWith (flip (++)) mid [TopicPartition topic p | p <- ps] a)
                     acc (zip subscribers chunks)
    splitCounts [] _ = []
    splitCounts (c : cs) xs = let (h, t) = splitAt c xs in h : splitCounts cs t

-- | Deal every partition round the circle of members sorted by id,
-- skipping members not subscribed to its topic.
roundRobinAssign :: [AssignorMember] -> Map.Map Text [Int32] -> Assignment
roundRobinAssign members topicPartitions
  | null circle = emptyAssignment members
  | otherwise = fst (foldl' place (emptyAssignment members, 0 :: Int) slots)
  where
    circle = sortOn amId members
    n = length circle
    slots = [TopicPartition t p | (t, ps) <- Map.toAscList topicPartitions, p <- ps]
    place (acc, cursor) slot = go cursor
      where
        go c
          | c - cursor >= n = (acc, c)   -- nobody subscribes to this topic
          | otherwise =
              let m = circle !! (c `mod` n)
              in if subscribes m (tpTopic slot)
                   then (Map.insertWith (flip (++)) (amId m) [slot] acc, c + 1)
                   else go (c + 1)

-- | Keep members on what they already hold and move only what balance
-- requires.
stickyAssign :: [AssignorMember] -> Map.Map Text [Int32] -> Map.Map Text [TopicPartition] -> Assignment
stickyAssign members topicPartitions previous
  | null members || null eligible = emptyAssignment members
  | otherwise = Map.map sort final
  where
    memberSubscribes mid topic = maybe False (`subscribes` topic) (find ((== mid) . amId) members)
    allSlots = [TopicPartition t p | (t, ps) <- Map.toAscList topicPartitions, p <- ps]
    -- The first previous holder (by member id) still subscribed to the topic.
    holderOf slot = find (\mid -> slot `elem` Map.findWithDefault [] mid previous
                                  && memberSubscribes mid (tpTopic slot))
                         (Map.keys previous)
    unassigned0 = [s | s <- allSlots, holderOf s == Nothing]
    claimed = Map.fromList [(s, h) | s <- allSlots, Just h <- [holderOf s]]
    eligible = sort [amId m | m <- members, any (`Map.member` topicPartitions) (amTopics m)]
    total = length allSlots
    (base, extra) = total `divMod` length eligible
    quota = Map.fromList [(mid, base + (if i < extra then 1 else 0)) | (i, mid) <- zip [0 :: Int ..] eligible]
    quotaOf mid = Map.findWithDefault 0 mid quota
    -- Claimed slots in (topic, partition) order: keep up to quota.
    (kept, overflow) = foldl' keep (Map.empty, []) (Map.toAscList claimed)
    keep (k, over) (slot, mid)
      | length (Map.findWithDefault [] mid k) < quotaOf mid = (Map.insertWith (flip (++)) mid [slot] k, over)
      | otherwise = (k, over ++ [slot])
    start = Map.mapWithKey (\mid held -> fromMaybe held (Map.lookup mid kept)) (emptyAssignment members)
    final = foldl' give start (sort (unassigned0 ++ overflow))
    give acc slot =
      let count mid = length (Map.findWithDefault [] mid acc)
          subscribed = [mid | mid <- eligible, memberSubscribes mid (tpTopic slot)]
          -- Quotas exhausted (uneven subscriptions): an unassigned
          -- partition is a stalled one, so fall back to any subscriber.
          taker = case [mid | mid <- subscribed, count mid < quotaOf mid] of
            (mid : _) -> Just mid
            [] -> case subscribed of
              (mid : _) -> Just mid
              [] -> Nothing
      in maybe acc (\mid -> Map.insertWith (flip (++)) mid [slot] acc) taker
