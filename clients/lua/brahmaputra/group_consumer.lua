-- A consumer that shares its topics' partitions with the rest of its group.
--
-- The group's coordinator is the leader of `__consumer_offsets` partition
-- `crc32c(group.id) % partitions`; every group request goes there.
--
-- Heartbeats run inside poll(). Lua has no threads, so there is no
-- background heartbeat thread as in the Java client: poll() heartbeats
-- every `heartbeat.interval.ms` (default session.timeout.ms / 3) while it
-- waits, and commit() heartbeats too. Processing between two polls must
-- therefore stay under `session.timeout.ms` or the coordinator evicts this
-- member; for longer work call consumer:heartbeat() from your loop.
--
-- `max.poll.interval.ms` bounds the time the *application* spends between
-- polls: it is stamped when poll() is entered and again when it returns,
-- and never enforced while inside poll(), so a slow join or a long wait
-- for records never counts. It is enforced at the next poll() (or
-- heartbeat()): if the gap exceeded it, this member leaves the group (as
-- the Java client's heartbeat thread would have), drops its uncommitted
-- positions and rejoins.
--
-- UNKNOWN_MEMBER_ID on join, sync or heartbeat clears the member id and
-- rejoins as a new member. A fencing heartbeat reply is ignored when the
-- member id or generation changed since that heartbeat was sent.
--
-- Run one GroupConsumer at a time per process: two members in one Lua
-- process cannot both answer a rebalance at once, because each blocks the
-- other.

local errors = require("brahmaputra.errors")
local protocol = require("brahmaputra.protocol")
local config = require("brahmaputra.config")
local hash = require("brahmaputra.hash")
local Consumer = require("brahmaputra.consumer")
local Assignor = require("brahmaputra.assignor")

local ApiKey = protocol.ApiKey
local ErrorCode = protocol.ErrorCode
local nowMs = config.nowMs

local OFFSETS_TOPIC = "__consumer_offsets"
local COORDINATOR_ATTEMPTS = 4
local JOIN_ATTEMPTS = 4

local GroupConsumer = {}
GroupConsumer.__index = GroupConsumer

function GroupConsumer.defaults()
  local d = {
    ["group.id"] = false,
    -- Kafka's default is 45 s; 10 s matches the Rust client
    ["session.timeout.ms"] = 10000,
    -- 0 means session.timeout.ms / 3
    ["heartbeat.interval.ms"] = 0,
    ["rebalance.timeout.ms"] = 3000,
    ["max.poll.interval.ms"] = 300000,
    ["enable.auto.commit"] = true,
    ["auto.commit.interval.ms"] = 5000,
    -- earliest, latest or none
    ["auto.offset.reset"] = "earliest",
    -- range, roundrobin or sticky
    ["partition.assignment.strategy"] = Assignor.RANGE,
    -- static membership (KIP-345); empty for a dynamic member
    ["group.instance.id"] = "",
  }
  for k, v in pairs(Consumer.defaults()) do d[k] = v end
  return d
end

local function tpKey(topic, partition) return topic .. "\0" .. partition end

function GroupConsumer.new(conf)
  local c = config.resolve(GroupConsumer.defaults(), conf, "group consumer")
  if type(c["group.id"]) ~= "string" or c["group.id"] == "" then
    errors.raise("ConfigError", "group consumer config needs group.id")
  end
  local reset = c["auto.offset.reset"]
  if reset ~= "earliest" and reset ~= "latest" and reset ~= "none" then
    errors.raise("ConfigError", "auto.offset.reset must be earliest, latest or none")
  end
  local strategy = c["partition.assignment.strategy"]
  if strategy ~= Assignor.RANGE and strategy ~= Assignor.ROUNDROBIN and strategy ~= Assignor.STICKY then
    errors.raise("ConfigError", "partition.assignment.strategy must be one of range, roundrobin, sticky")
  end
  for _, key in ipairs({ "session.timeout.ms", "heartbeat.interval.ms", "rebalance.timeout.ms",
    "max.poll.interval.ms", "auto.commit.interval.ms" }) do
    c[key] = config.int(c, key)
  end
  local consumerConf = {}
  for k in pairs(Consumer.defaults()) do consumerConf[k] = c[k] end
  local self = setmetatable({
    config = c,
    groupId = c["group.id"],
    consumerObj = Consumer.new(consumerConf),
    subscribed = {},
    memberIdValue = "",
    generationValue = -1,
    joined = false,
    assignmentList = {},
    positions = {},       -- key -> {tp, offset}: next offset to *deliver* (committed)
    fetchPositions = {},  -- key -> offset: next offset to *fetch*
    buffered = {},
    lastPollMs = nil,
    lastCommitMs = nowMs(),
    lastHeartbeatMs = 0,
    closed = false,
    inPoll = false,
  }, GroupConsumer)
  return self
end

function GroupConsumer:subscribe(topics)
  local seen, list = {}, {}
  for _, t in ipairs(topics) do
    if not seen[t] then
      seen[t] = true
      list[#list + 1] = t
    end
  end
  self.subscribed = list
  self.joined = false
end

--- The current assignment: list of {topic=, partition=}.
function GroupConsumer:assignment() return self.assignmentList end
function GroupConsumer:memberId() return self.memberIdValue end
function GroupConsumer:generation() return self.generationValue end
function GroupConsumer:consumer() return self.consumerObj end

--- Up to max.poll.records records, waiting up to timeoutMs for some to
--- arrive. Joins (or rejoins) the group, heartbeats and auto-commits as
--- needed while it waits.
function GroupConsumer:poll(timeoutMs)
  timeoutMs = math.tointeger(timeoutMs or 1000) or 1000
  if self.closed then errors.raise("BrahmaputraError", "group consumer is closed") end
  if #self.subscribed == 0 then
    errors.raise("BrahmaputraError", "subscribe to at least one topic before polling")
  end
  self:enforcePollInterval()
  -- Stamped on entry and again on return: the interval bounds how long the
  -- application goes without asking for records; a poll that spends its
  -- time joining or waiting is the consumer working normally.
  self.lastPollMs = nowMs()
  self.inPoll = true
  local ok, result = pcall(self.pollInner, self, self.lastPollMs + timeoutMs)
  self.inPoll = false
  self.lastPollMs = nowMs()
  if not ok then error(result, 0) end
  return result
end

function GroupConsumer:pollInner(deadline)
  while true do
    self:maybeHeartbeat()
    if not self.joined then self:join() end
    if #self.buffered > 0 then return self:takeBuffered() end
    if #self.assignmentList == 0 then
      if nowMs() >= deadline then return {} end
      config.sleepMs(math.min(50, math.max(1, deadline - nowMs())))
    else
      local gotAny = false
      for _, tp in ipairs(self.assignmentList) do
        if not self.joined then break end -- a heartbeat below saw a rebalance
        -- Once something is buffered, do not sit in a long poll on the
        -- remaining partitions.
        local wait = 0
        if not gotAny then
          wait = math.min(math.max(0, deadline - nowMs()), self:heartbeatIntervalMs(), 500)
        end
        local key = tpKey(tp.topic, tp.partition)
        local offset = self.fetchPositions[key] or 0
        local ok, records = pcall(self.consumerObj.fetch, self.consumerObj, tp.topic, tp.partition, offset, wait)
        if not ok then
          local err = records
          if errors.is(err, "ServerError") and err.code == ErrorCode.OFFSET_OUT_OF_RANGE then
            -- The position fell off the log (retention); restart where
            -- auto.offset.reset says.
            local reset = self:resetOffset(tp)
            self.fetchPositions[key] = reset
            self.positions[key] = { tp, reset }
            local kept = {}
            for _, r in ipairs(self.buffered) do
              if r.topic ~= tp.topic or r.partition ~= tp.partition then kept[#kept + 1] = r end
            end
            self.buffered = kept
          elseif errors.is(err, "ServerError") and protocol.isStaleRoute(err.code) then
            self.consumerObj:router():refresh(tp.topic)
          else
            error(err, 0)
          end
        elseif #records > 0 then
          gotAny = true
          self.fetchPositions[key] = records[#records].offset + 1
          for _, r in ipairs(records) do self.buffered[#self.buffered + 1] = r end
        end
        self:maybeHeartbeat()
      end
      self:maybeAutoCommit()
      if #self.buffered > 0 then return self:takeBuffered() end
      if nowMs() >= deadline then return {} end
    end
  end
end

local function sortedPositions(positions)
  local entries = {}
  for _, e in pairs(positions) do entries[#entries + 1] = e end
  table.sort(entries, function(a, b) return Assignor.less(a[1], b[1]) end)
  return entries
end

local function isFencing(code)
  return code == ErrorCode.REBALANCE_IN_PROGRESS or code == ErrorCode.UNKNOWN_MEMBER_ID
    or code == ErrorCode.ILLEGAL_GENERATION
end

--- Commit the positions of records poll() has returned (at-least-once: call
--- it after processing them).
function GroupConsumer:commit()
  local entries = sortedPositions(self.positions)
  if #entries == 0 then return end
  local w = protocol.Writer.body():string(self.groupId):int32(self.generationValue)
    :string(self.memberIdValue):int32(#entries)
  for _, e in ipairs(entries) do
    w:string(e[1].topic):int32(e[1].partition):int64(e[2])
  end
  local r = protocol.Reader.body(self:coordinatorRequest(ApiKey.OFFSET_COMMIT, w:bytes()))
  local code = r:int32()
  if code ~= ErrorCode.NONE then
    if isFencing(code) then
      -- Generation fencing: this member's view is stale, so its commit is
      -- refused. Rejoin on the next poll.
      if code == ErrorCode.UNKNOWN_MEMBER_ID then self.memberIdValue = "" end
      self.joined = false
    end
    protocol.serverError(code, "offset_commit")
  end
  self.lastCommitMs = nowMs()
  self:maybeHeartbeat()
end

--- The group's committed offsets for `partitions` ({{topic=, partition=}}),
--- or for every partition the group has committed when omitted. Returns a
--- list of {topic=, partition=, offset=}.
function GroupConsumer:committed(partitions)
  partitions = partitions or {}
  local w = protocol.Writer.body():string(self.groupId):int32(#partitions)
  for _, tp in ipairs(partitions) do w:string(tp.topic):int32(tp.partition) end
  local r = protocol.Reader.body(self:coordinatorRequest(ApiKey.OFFSET_FETCH, w:bytes()))
  local code = r:int32()
  if code ~= ErrorCode.NONE then protocol.serverError(code, "offset_fetch") end
  local out = {}
  for i = 1, r:count() do
    out[i] = { topic = r:string(), partition = r:int32(), offset = r:int64() }
  end
  return out
end

--- Heartbeat now. poll() does this itself; call it from a long processing
--- loop to stay in the group without polling. Returns false when the
--- coordinator reports a rebalance (the next poll() rejoins) or when
--- max.poll.interval.ms has already passed (the member has left).
function GroupConsumer:heartbeat()
  if not self:enforcePollInterval() then return false end
  if not self.joined or self.memberIdValue == "" then return false end
  local sentMember, sentGeneration = self.memberIdValue, self.generationValue
  local body = protocol.Writer.body():string(self.groupId):int32(sentGeneration):string(sentMember):bytes()
  local r = protocol.Reader.body(self:coordinatorRequest(ApiKey.HEARTBEAT, body))
  local code = r:int32()
  self.lastHeartbeatMs = nowMs()
  if code == ErrorCode.NONE then return true end
  if isFencing(code) then
    -- A reply about a membership we no longer hold says nothing about the
    -- current one.
    if self.memberIdValue ~= sentMember or self.generationValue ~= sentGeneration then
      return true
    end
    if code == ErrorCode.UNKNOWN_MEMBER_ID then
      self.memberIdValue = "" -- evicted: the next join gets a fresh id
    end
    self.joined = false
    return false
  end
  protocol.serverError(code, "heartbeat")
end

--- Commit, leave the group, and close connections. Leaving is what separates
--- a clean shutdown from a crash: without it the coordinator must wait out
--- session.timeout.ms before reassigning the partitions.
function GroupConsumer:close()
  if self.closed then return end
  self.closed = true
  if self.joined then
    -- A failed final commit shows up as the next member resuming from an
    -- older position, not as a crash on the shutdown path.
    pcall(self.commit, self)
  end
  if self.memberIdValue ~= "" then
    pcall(self.leave, self) -- best effort: failing costs the session timeout
  end
  self.consumerObj:close()
end

-- Last resort for a consumer that was never closed: leave the group so its
-- partitions move now rather than after session.timeout.ms.
function GroupConsumer:__gc()
  if not self.closed and self.consumerObj then pcall(self.close, self) end
end

function GroupConsumer:heartbeatIntervalMs()
  local interval = self.config["heartbeat.interval.ms"]
  if interval > 0 then return interval end
  return math.max(1, self.config["session.timeout.ms"] // 3)
end

function GroupConsumer:maybeHeartbeat()
  if self.joined and nowMs() - self.lastHeartbeatMs >= self:heartbeatIntervalMs() then
    self:heartbeat()
  end
end

-- Leave if the application went longer than max.poll.interval.ms between
-- polls. Returns false when it did. Never enforced from inside poll().
function GroupConsumer:enforcePollInterval()
  if self.inPoll or self.lastPollMs == nil or not self.joined then return true end
  if nowMs() - self.lastPollMs < self.config["max.poll.interval.ms"] then return true end
  pcall(self.leave, self) -- the coordinator evicts us after the session timeout anyway
  -- What was delivered but not committed is abandoned, exactly as when
  -- Kafka's heartbeat thread leaves on this deadline: another member may
  -- already own these partitions.
  self.positions = {}
  self.fetchPositions = {}
  self.buffered = {}
  self.assignmentList = {}
  self.memberIdValue = ""
  self.joined = false
  self.lastPollMs = nil
  return false
end

function GroupConsumer:takeBuffered()
  local limit = math.max(1, self.config["max.poll.records"])
  local delivered, rest = {}, {}
  for i, r in ipairs(self.buffered) do
    if i <= limit then delivered[#delivered + 1] = r else rest[#rest + 1] = r end
  end
  self.buffered = rest
  for _, r in ipairs(delivered) do
    -- The committed position advances only over records actually handed to
    -- the caller; committing what was merely fetched would skip records
    -- nobody processed.
    self.positions[tpKey(r.topic, r.partition)] = { { topic = r.topic, partition = r.partition }, r.offset + 1 }
  end
  return delivered
end

function GroupConsumer:maybeAutoCommit()
  if not self.config["enable.auto.commit"] or self.config["auto.commit.interval.ms"] <= 0 then return end
  if next(self.positions) == nil or nowMs() - self.lastCommitMs < self.config["auto.commit.interval.ms"] then
    return
  end
  local ok, err = pcall(self.commit, self)
  if not ok and not (errors.is(err, "ServerError") or errors.is(err, "ConnectionError")) then
    error(err, 0)
  end
  -- a refused auto-commit is retried on a later poll; an explicit commit()
  -- is what a caller relies on
end

function GroupConsumer:resetOffset(tp)
  local reset = self.config["auto.offset.reset"]
  if reset == "earliest" then
    return self.consumerObj:listOffsets(tp.topic, tp.partition, Consumer.EARLIEST)
  elseif reset == "latest" then
    return self.consumerObj:listOffsets(tp.topic, tp.partition, Consumer.LATEST)
  end
  errors.raise("NoOffsetForPartitionError", string.format(
    "no committed offset for %s-%d and auto.offset.reset=none", tp.topic, tp.partition))
end

function GroupConsumer:join()
  -- A member that rejoins after a rebalance commits what it has delivered
  -- first, while its generation may still be accepted.
  if self.generationValue >= 0 and next(self.positions) ~= nil and self.config["enable.auto.commit"] then
    pcall(self.commit, self)
  end
  local rebalanceTimeout = self.config["rebalance.timeout.ms"]
  for _ = 1, JOIN_ATTEMPTS do
    local body = protocol.Writer.body()
      :string(self.groupId)
      :int32(self.config["session.timeout.ms"])
      :int32(rebalanceTimeout)
      :string(self.memberIdValue)
      :stringArray(self.subscribed)
      :string(self.config["group.instance.id"])
      :bytes()
    local r = protocol.Reader.body(self:coordinatorRequest(ApiKey.JOIN_GROUP, body, rebalanceTimeout))
    local code = r:int32()
    if code == ErrorCode.REBALANCE_IN_PROGRESS then
      config.sleepMs(100)
    elseif code == ErrorCode.UNKNOWN_MEMBER_ID then
      self.memberIdValue = "" -- rejoin as a new member
    elseif code ~= ErrorCode.NONE then
      protocol.serverError(code, "join_group")
    else
      local generation = r:int32()
      local memberId = r:string()
      local leaderId = r:string()
      local members, previous = {}, {}
      for i = 1, r:count() do
        local id = r:string()
        local topics = r:stringArray()
        local held = {}
        for h = 1, r:count() do held[h] = { topic = r:string(), partition = r:int32() } end
        members[i] = { id = id, topics = topics }
        previous[id] = held
      end
      self.memberIdValue = memberId
      self.generationValue = generation
      self.lastHeartbeatMs = nowMs()
      local assignments = {}
      if memberId == leaderId then assignments = self:computeAssignments(members, previous) end
      if self:sync(assignments) then
        self.joined = true
        return
      end
    end
  end
  errors.raise("BrahmaputraError", "consumer group failed to stabilise after " .. JOIN_ATTEMPTS .. " join attempts")
end

function GroupConsumer:sync(assignments)
  local ids = {}
  for id in pairs(assignments) do ids[#ids + 1] = id end
  table.sort(ids)
  local w = protocol.Writer.body():string(self.groupId):int32(self.generationValue)
    :string(self.memberIdValue):int32(#ids)
  for _, id in ipairs(ids) do
    local parts = assignments[id]
    w:string(id):int32(#parts)
    for _, tp in ipairs(parts) do w:string(tp.topic):int32(tp.partition) end
  end
  local r = protocol.Reader.body(self:coordinatorRequest(ApiKey.SYNC_GROUP, w:bytes(),
    self.config["rebalance.timeout.ms"]))
  local code = r:int32()
  if code == ErrorCode.REBALANCE_IN_PROGRESS or code == ErrorCode.ILLEGAL_GENERATION then
    return false
  end
  if code == ErrorCode.UNKNOWN_MEMBER_ID then
    self.memberIdValue = "" -- evicted between join and sync: rejoin under a fresh id
    return false
  end
  if code ~= ErrorCode.NONE then protocol.serverError(code, "sync_group") end
  local assignment = {}
  for i = 1, r:count() do assignment[i] = { topic = r:string(), partition = r:int32() } end
  self:applyAssignment(assignment)
  return true
end

function GroupConsumer:applyAssignment(assignment)
  self.assignmentList = assignment
  local owned = {}
  for _, tp in ipairs(assignment) do owned[tpKey(tp.topic, tp.partition)] = true end
  for key in pairs(self.positions) do
    if not owned[key] then self.positions[key] = nil end
  end
  -- Buffered records sit ahead of the delivered position and were never
  -- handed out, so a new assignment simply drops them.
  self.buffered = {}

  local needed = {}
  for _, tp in ipairs(assignment) do
    if self.positions[tpKey(tp.topic, tp.partition)] == nil then needed[#needed + 1] = tp end
  end
  if #needed > 0 then
    local committed = {}
    for _, c in ipairs(self:committed(needed)) do committed[tpKey(c.topic, c.partition)] = c.offset end
    for _, tp in ipairs(needed) do
      local key = tpKey(tp.topic, tp.partition)
      local offset = committed[key] or -1
      if offset < 0 then offset = self:resetOffset(tp) end
      self.positions[key] = { tp, offset }
    end
  end
  self.fetchPositions = {}
  for key, e in pairs(self.positions) do self.fetchPositions[key] = e[2] end
end

function GroupConsumer:computeAssignments(members, previous)
  local topicPartitions = {}
  for _, m in ipairs(members) do
    for _, t in ipairs(m.topics) do
      if topicPartitions[t] == nil then topicPartitions[t] = self.consumerObj:partitions(t) end
    end
  end
  return Assignor.assign(self.config["partition.assignment.strategy"], members, topicPartitions, previous)
end

function GroupConsumer:leave()
  local body = protocol.Writer.body():string(self.groupId):string(self.memberIdValue):bytes()
  self.joined = false
  local r = protocol.Reader.body(self:coordinatorRequest(ApiKey.LEAVE_GROUP, body))
  local code = r:int32()
  if code ~= ErrorCode.NONE and code ~= ErrorCode.UNKNOWN_MEMBER_ID then
    protocol.serverError(code, "leave_group")
  end
end

function GroupConsumer:coordinatorPartition()
  local partitions = self.consumerObj:partitions(OFFSETS_TOPIC)
  return hash.crc32c(self.groupId) % #partitions
end

-- Every group response starts with an error code; read it without
-- consuming the body.
local function peekErrorCode(body)
  local ok, code = pcall(function() return protocol.Reader.body(body):int32() end)
  if ok then return code end
  return ErrorCode.NONE
end

-- Send to the group's coordinator, following moves and waiting out loads.
function GroupConsumer:coordinatorRequest(apiKey, body, extraWaitMs)
  local timeout = self.config["request.timeout.ms"] + (extraWaitMs or 0)
  local router = self.consumerObj:router()
  local lastError
  for _ = 1, COORDINATOR_ATTEMPTS do
    local ok, response = pcall(function()
      local partition = self:coordinatorPartition()
      return router:connectionFor(OFFSETS_TOPIC, partition):request(apiKey, body, timeout)
    end)
    if not ok then
      if not errors.is(response, "ConnectionError") then error(response, 0) end
      lastError = response
      config.sleepMs(100)
    else
      local code = peekErrorCode(response)
      if code == ErrorCode.COORDINATOR_LOAD_IN_PROGRESS then
        config.sleepMs(100)
      elseif code == ErrorCode.NOT_COORDINATOR or code == ErrorCode.NOT_LEADER_OR_FOLLOWER then
        router:refresh(OFFSETS_TOPIC)
      else
        return response
      end
    end
  end
  errors.raise("ConnectionError", "group coordinator unavailable after " .. COORDINATOR_ATTEMPTS ..
    " attempts" .. (lastError and (": " .. errors.message(lastError)) or ""), { cause = lastError })
end

return GroupConsumer
