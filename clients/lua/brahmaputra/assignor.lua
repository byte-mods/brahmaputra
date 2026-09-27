-- Partition assignment strategies (`partition.assignment.strategy`).
--
-- The group leader computes the assignment client-side and hands it to the
-- coordinator in SyncGroup. Each strategy mirrors the Rust client's, so a
-- Lua leader and a Rust leader produce the same assignment for the same
-- group.
--
--   members          list of {id = string, topics = {string...}}
--   topicPartitions  topic -> ascending list of partition ids (integers)
--   previous         memberId -> list of {topic=, partition=} (sticky only)
--   result           memberId -> list of {topic=, partition=}
--
-- Partitions are always ordered by (topic, partition-as-integer), never by
-- a string form, so partition 10 sorts after partition 9.

local errors = require("brahmaputra.errors")

local M = {}

M.RANGE = "range"
M.ROUNDROBIN = "roundrobin"
--- Keeps members on the partitions they already hold; prefer it when
--- consumers carry per-partition state.
M.STICKY = "sticky"
M.ALL = { M.RANGE, M.ROUNDROBIN, M.STICKY }

--- (topic, partition) order, partition compared as an integer.
function M.less(a, b)
  if a.topic ~= b.topic then return a.topic < b.topic end
  return a.partition < b.partition
end

local function empty(members)
  local out = {}
  for _, m in ipairs(members) do out[m.id] = {} end
  return out
end

local function sortedTopics(topicPartitions)
  local topics = {}
  for t in pairs(topicPartitions) do topics[#topics + 1] = t end
  table.sort(topics)
  return topics
end

local function subscribes(member, topic)
  for _, t in ipairs(member.topics) do
    if t == topic then return true end
  end
  return false
end

--- Contiguous ranges per topic; the first (n % members) take one extra.
function M.range(members, topicPartitions)
  local assignment = empty(members)
  for _, topic in ipairs(sortedTopics(topicPartitions)) do
    local partitions = topicPartitions[topic]
    local subscribers = {}
    for _, m in ipairs(members) do
      if subscribes(m, topic) then subscribers[#subscribers + 1] = m.id end
    end
    if #subscribers > 0 then
      table.sort(subscribers)
      local base = #partitions // #subscribers
      local extra = #partitions % #subscribers
      local cursor = 1
      for index, memberId in ipairs(subscribers) do
        local take = base + ((index - 1) < extra and 1 or 0)
        for i = cursor, cursor + take - 1 do
          local list = assignment[memberId]
          list[#list + 1] = { topic = topic, partition = partitions[i] }
        end
        cursor = cursor + take
      end
    end
  end
  return assignment
end

--- Deal every partition around the circle of members sorted by id.
function M.roundRobin(members, topicPartitions)
  local assignment = empty(members)
  local circle = {}
  for i, m in ipairs(members) do circle[i] = m end
  table.sort(circle, function(a, b) return a.id < b.id end)
  if #circle == 0 then return assignment end
  local cursor = 0
  for _, topic in ipairs(sortedTopics(topicPartitions)) do
    for _, partition in ipairs(topicPartitions[topic]) do
      local start = cursor
      while true do
        local member = circle[cursor % #circle + 1]
        cursor = cursor + 1
        if subscribes(member, topic) then
          local list = assignment[member.id]
          list[#list + 1] = { topic = topic, partition = partition }
          break
        end
        if cursor - start >= #circle then break end -- nobody subscribes
      end
    end
  end
  return assignment
end

--- Keep members on what they hold; move only what balance requires.
function M.sticky(members, topicPartitions, previous)
  previous = previous or {}
  local assignment = empty(members)
  if #members == 0 then return assignment end
  local byId = {}
  for _, m in ipairs(members) do byId[m.id] = m end
  local function memberSubscribes(id, topic)
    return byId[id] ~= nil and subscribes(byId[id], topic)
  end

  local previousIds = {}
  for id in pairs(previous) do previousIds[#previousIds + 1] = id end
  table.sort(previousIds)

  -- Every partition that needs an owner, and who has a valid claim on it.
  local unassigned, claimed = {}, {}
  for _, topic in ipairs(sortedTopics(topicPartitions)) do
    for _, partition in ipairs(topicPartitions[topic]) do
      local tp = { topic = topic, partition = partition }
      local holder
      for _, id in ipairs(previousIds) do
        for _, h in ipairs(previous[id]) do
          if h.topic == topic and h.partition == partition and memberSubscribes(id, topic) then
            holder = id
            break
          end
        end
        if holder then break end
      end
      if holder then claimed[#claimed + 1] = { tp, holder } else unassigned[#unassigned + 1] = tp end
    end
  end

  -- Fair share among members subscribed to at least one live topic.
  local eligible = {}
  for _, m in ipairs(members) do
    for _, t in ipairs(m.topics) do
      if topicPartitions[t] then
        eligible[#eligible + 1] = m.id
        break
      end
    end
  end
  if #eligible == 0 then return assignment end
  table.sort(eligible)
  local total = 0
  for _, parts in pairs(topicPartitions) do total = total + #parts end
  local base, extra = total // #eligible, total % #eligible
  local quota = {}
  for index, id in ipairs(eligible) do quota[id] = base + ((index - 1) < extra and 1 or 0) end

  -- Honour claims up to quota; the overflow joins the pool.
  local kept = {}
  for _, c in ipairs(claimed) do
    local tp, id = c[1], c[2]
    kept[id] = kept[id] or {}
    if #kept[id] < (quota[id] or 0) then
      kept[id][#kept[id] + 1] = tp
    else
      unassigned[#unassigned + 1] = tp
    end
  end
  for id, held in pairs(kept) do
    if assignment[id] then assignment[id] = held end
  end

  table.sort(unassigned, M.less)
  for _, tp in ipairs(unassigned) do
    local taker
    for _, id in ipairs(eligible) do
      if memberSubscribes(id, tp.topic) and #assignment[id] < (quota[id] or 0) then
        taker = id
        break
      end
    end
    if taker == nil then
      -- Quotas exhausted (uneven subscriptions): an unassigned partition is
      -- a stalled one, so fall back to any subscriber.
      for _, id in ipairs(eligible) do
        if memberSubscribes(id, tp.topic) then
          taker = id
          break
        end
      end
    end
    if taker then
      local list = assignment[taker]
      list[#list + 1] = tp
    end
  end
  for _, held in pairs(assignment) do table.sort(held, M.less) end
  return assignment
end

function M.assign(strategy, members, topicPartitions, previous)
  if strategy == M.RANGE then return M.range(members, topicPartitions) end
  if strategy == M.ROUNDROBIN then return M.roundRobin(members, topicPartitions) end
  if strategy == M.STICKY then return M.sticky(members, topicPartitions, previous) end
  errors.raise("ConfigError", "unknown partition.assignment.strategy " .. tostring(strategy))
end

return M
