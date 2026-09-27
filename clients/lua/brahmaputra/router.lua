-- Keeps one connection per broker and routes each request to its
-- partition's leader.
--
-- Metadata is cached and refreshed only when a request says the route was
-- stale (or a topic is not yet known), because refreshing per request would
-- put the control plane on the data path. A connection that broke (I/O
-- error, timeout, correlation mismatch) or that the peer closed is never
-- reused: the next request through the router redials, the seed included.

local errors = require("brahmaputra.errors")
local protocol = require("brahmaputra.protocol")
local config = require("brahmaputra.config")
local Connection = require("brahmaputra.connection")

local ApiKey = protocol.ApiKey
local ErrorCode = protocol.ErrorCode

local Router = {}
Router.__index = Router

--- opts: clientId, requestTimeoutMs, connectTimeoutMs.
function Router.new(bootstrapServers, opts)
  opts = opts or {}
  local self = setmetatable({
    bootstrap = config.parseBootstrap(bootstrapServers),
    clientId = opts.clientId or "brahmaputra-lua",
    requestTimeoutMs = opts.requestTimeoutMs or Connection.DEFAULT_REQUEST_TIMEOUT_MS,
    connectTimeoutMs = opts.connectTimeoutMs or 10000,
    seedConn = nil,
    connections = {},
    meta = nil,
  }, Router)
  self:seed()
  return self
end

function Router:dial(host, port)
  return Connection.open(host, port, {
    clientId = self.clientId,
    connectTimeoutMs = self.connectTimeoutMs,
    requestTimeoutMs = self.requestTimeoutMs,
  })
end

--- The bootstrap connection, redialled (trying each bootstrap server) if it
--- broke or the broker closed it.
function Router:seed()
  if self.seedConn and self.seedConn:isUsable() then return self.seedConn end
  local last
  for _, hp in ipairs(self.bootstrap) do
    local ok, conn = pcall(self.dial, self, hp[1], hp[2])
    if ok then
      self.seedConn = conn
      return conn
    end
    last = conn
  end
  error(last or errors.new("ConnectionError", "no bootstrap server reachable"), 0)
end

function Router:close()
  for _, conn in pairs(self.connections) do conn:close() end
  self.connections = {}
  if self.seedConn then self.seedConn:close() end
  self.seedConn = nil
end

local function decodeMetadata(r)
  -- Schema order: error_code, brokers, controller_id, topics. The leading
  -- code is request-level, distinct from the per-topic one.
  local requestError = r:int32()
  if requestError ~= ErrorCode.NONE then protocol.serverError(requestError, "metadata") end
  local brokers = {}
  for i = 1, r:count() do
    brokers[i] = { nodeId = r:int32(), host = r:string(), port = r:int32(), rack = r:string() }
  end
  local controllerId = r:int32()
  local topics = {}
  for _ = 1, r:count() do
    local name = r:string()
    local topicError = r:int32()
    local partitions = {}
    for p = 1, r:count() do
      local partition = r:int32()
      local leader = r:int32()
      local replicas, isr = {}, {}
      for i = 1, r:count() do replicas[i] = r:int32() end
      for i = 1, r:count() do isr[i] = r:int32() end
      partitions[p] = { partition = partition, leader = leader, replicas = replicas, isr = isr,
        leaderEpoch = r:int32() }
    end
    if topicError ~= ErrorCode.NONE and topicError ~= ErrorCode.UNKNOWN_TOPIC_OR_PARTITION then
      protocol.serverError(topicError, "metadata for " .. name)
    end
    topics[name] = partitions
  end
  return { brokers = brokers, controllerId = controllerId, topics = topics }
end

--- Cluster metadata: {brokers = {{nodeId, host, port, rack}}, controllerId,
--- topics = {name = {{partition, leader, replicas, isr, leaderEpoch}}}}.
--- Topics named are merged into the cached image; an empty list asks for
--- every topic.
function Router:metadata(topics, refresh)
  topics = topics or {}
  if not refresh and self.meta then
    local missing = false
    for _, t in ipairs(topics) do
      if self.meta.topics[t] == nil then missing = true end
    end
    if not missing then return self.meta end
  end
  local body = protocol.Writer.body():stringArray(topics):bytes()
  local ok, response = pcall(function() return self:seed():request(ApiKey.METADATA, body) end)
  if not ok then
    if not errors.is(response, "ConnectionError") then error(response, 0) end
    -- One redial: a dropped connection should not fail the caller.
    response = self:seed():request(ApiKey.METADATA, body)
  end
  local fresh = decodeMetadata(protocol.Reader.body(response))
  if self.meta and #topics > 0 then
    for name, parts in pairs(self.meta.topics) do
      if fresh.topics[name] == nil then fresh.topics[name] = parts end
    end
  end
  self.meta = fresh
  return fresh
end

function Router:refresh(topic)
  return self:metadata({ topic }, true)
end

--- The topic's partition ids, ascending.
function Router:partitions(topic)
  local meta = self:metadata({ topic })
  local infos = meta.topics[topic]
  if infos == nil or #infos == 0 then
    -- A topic auto-created on first use is not in the cached image yet; one
    -- refresh distinguishes "new" from "absent".
    meta = self:refresh(topic)
    infos = meta.topics[topic]
  end
  if infos == nil or #infos == 0 then
    errors.raise("BrahmaputraError", "topic " .. topic .. " has no partitions")
  end
  local ids = {}
  for i, info in ipairs(infos) do ids[i] = info.partition end
  table.sort(ids)
  return ids
end

local function leaderOf(meta, topic, partition)
  for _, info in ipairs(meta.topics[topic] or {}) do
    if info.partition == partition then return info.leader end
  end
  return -1
end

--- The connection to the leader of topic-partition.
function Router:connectionFor(topic, partition)
  local meta = self:metadata({ topic })
  local leader = leaderOf(meta, topic, partition)
  if leader < 0 then
    meta = self:refresh(topic)
    leader = leaderOf(meta, topic, partition)
  end
  if leader < 0 then
    errors.raise("BrahmaputraError", string.format("no leader for %s-%d", topic, partition))
  end
  return self:connectionTo(leader, meta)
end

function Router:connectionTo(nodeId, meta)
  local existing = self.connections[nodeId]
  if existing and existing:isUsable() then return existing end
  -- A single-broker cluster advertises the address it was configured with,
  -- which may not be the one we dialled (a proxy, a NAT); reuse the seed.
  if #meta.brokers == 1 then
    local conn = self:seed()
    self.connections[nodeId] = conn
    return conn
  end
  local broker
  for _, b in ipairs(meta.brokers) do
    if b.nodeId == nodeId then broker = b end
  end
  if broker == nil then
    errors.raise("BrahmaputraError", "broker " .. nodeId .. " is not in the metadata")
  end
  local conn = self:dial(broker.host, broker.port)
  self.connections[nodeId] = conn
  return conn
end

return Router
