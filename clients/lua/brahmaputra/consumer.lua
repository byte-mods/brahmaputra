-- Reads partitions directly, with no group coordination.
--
-- Consumed records are tables:
--   { topic, partition, offset, key, value, timestamp, headers }
-- where key/value are strings or nil (a nil value is a tombstone; "" is an
-- empty value) and headers is a list of {key=, value=} (value may be nil).

local errors = require("brahmaputra.errors")
local protocol = require("brahmaputra.protocol")
local config = require("brahmaputra.config")
local RecordBatch = require("brahmaputra.record_batch")
local Router = require("brahmaputra.router")

local ApiKey = protocol.ApiKey
local ErrorCode = protocol.ErrorCode

local Consumer = {}
Consumer.__index = Consumer

--- Sentinels for listOffsets(); any other value is a unix-ms timestamp.
Consumer.EARLIEST = -2
Consumer.LATEST = -1

function Consumer.defaults()
  return {
    ["bootstrap.servers"] = false,
    ["client.id"] = "brahmaputra-lua",
    ["fetch.max.bytes"] = 8 * 1024 * 1024,
    ["fetch.min.bytes"] = 1,
    ["fetch.max.wait.ms"] = 500,
    ["max.poll.records"] = 500,
    -- read_uncommitted or read_committed (stops at the last stable offset)
    ["isolation.level"] = "read_uncommitted",
    -- this consumer's failure domain; with it set the leader may point
    -- reads at a same-rack replica
    ["client.rack"] = "",
    -- socket round-trip bound for one request (a fetch adds its wait)
    ["request.timeout.ms"] = 30000,
    ["socket.connection.setup.timeout.ms"] = 10000,
  }
end

local intKeys = { "fetch.max.bytes", "fetch.min.bytes", "fetch.max.wait.ms", "max.poll.records",
  "request.timeout.ms", "socket.connection.setup.timeout.ms" }

function Consumer.new(conf)
  local c = config.resolve(Consumer.defaults(), conf, "consumer")
  for _, key in ipairs(intKeys) do c[key] = config.int(c, key) end
  local iso = c["isolation.level"]
  local isolation
  if iso == "read_uncommitted" or iso == 0 then
    isolation = protocol.READ_UNCOMMITTED
  elseif iso == "read_committed" or iso == 1 then
    isolation = protocol.READ_COMMITTED
  else
    errors.raise("ConfigError", "isolation.level must be read_uncommitted or read_committed")
  end
  local self = setmetatable({ config = c, isolation = isolation }, Consumer)
  self.routerObj = Router.new(c["bootstrap.servers"], {
    clientId = c["client.id"],
    requestTimeoutMs = c["request.timeout.ms"],
    connectTimeoutMs = c["socket.connection.setup.timeout.ms"],
  })
  return self
end

function Consumer:router() return self.routerObj end

function Consumer:close() self.routerObj:close() end

--- The topic's partition ids, ascending.
function Consumer:partitions(topic) return self.routerObj:partitions(topic) end

-- Send a read to the partition leader. Reads are idempotent, so a connection
-- that dropped (broker restart, idle timeout) is redialled and the request
-- sent once more before the error reaches the caller. A timeout is not
-- retried: the broker is slow, not gone.
function Consumer:requestLeader(topic, partition, apiKey, body, timeoutMs)
  local router = self.routerObj
  local ok, result = pcall(function()
    return router:connectionFor(topic, partition):request(apiKey, body, timeoutMs)
  end)
  if ok then return result end
  if not errors.is(result, "ConnectionError") or errors.is(result, "TimeoutError") then
    error(result, 0)
  end
  return router:connectionFor(topic, partition):request(apiKey, body, timeoutMs)
end

--- Resolve Consumer.EARLIEST, Consumer.LATEST or a unix-ms timestamp to an
--- offset (for a timestamp: the first offset at or after it).
function Consumer:listOffsets(topic, partition, timestamp)
  local body = protocol.Writer.body():string(topic):int32(partition):int64(timestamp):bytes()
  local r = protocol.Reader.body(self:requestLeader(topic, partition, ApiKey.LIST_OFFSETS, body))
  r:string() -- topic
  r:int32()  -- partition
  local code = r:int32()
  local offset = r:int64()
  r:int64()  -- timestamp
  if code ~= ErrorCode.NONE then
    protocol.serverError(code, string.format("list_offsets %s-%d", topic, partition))
  end
  return offset
end

--- The partition's high watermark: the offset the next record will get.
function Consumer:highWatermark(topic, partition)
  return self:listOffsets(topic, partition, Consumer.LATEST)
end

local function decodeFetch(body)
  local r = protocol.Reader.body(body)
  r:string() -- topic
  r:int32()  -- partition
  local code = r:int32()
  local highWatermark = r:int64()
  r:int64()  -- last_stable_offset
  local batchesLength = r:int64()
  -- Read even though unused: the batches trail the whole struct, so
  -- skipping a field would take them from the wrong offset.
  r:int32()  -- preferred_read_replica
  local trailing = r:rest()
  if batchesLength < 0 or batchesLength > #trailing then
    protocol.protoError("fetch response claims " .. batchesLength .. " batch bytes, carries " .. #trailing)
  end
  local raw = string.sub(trailing, 1, batchesLength)
  local batches = {}
  local pos = 1
  while pos <= #raw do
    local batch
    batch, pos = RecordBatch.decode(raw, pos)
    batches[#batches + 1] = batch
  end
  return code, highWatermark, batches
end

--- Records from `offset` on, waiting up to maxWaitMs (capped by
--- fetch.max.wait.ms) for fetch.min.bytes to accumulate.
function Consumer:fetch(topic, partition, offset, maxWaitMs)
  local records = self:fetchVerbose(topic, partition, offset, maxWaitMs)
  return records
end

--- Like fetch(), also returning the partition's high watermark.
function Consumer:fetchVerbose(topic, partition, offset, maxWaitMs)
  local configured = self.config["fetch.max.wait.ms"]
  local wait = math.max(0, math.min(math.tointeger(maxWaitMs) or configured, configured))
  local body = protocol.Writer.body()
    :string(topic):int32(partition):int64(offset)
    :int32(self.config["fetch.max.bytes"]):int32(wait):int32(self.config["fetch.min.bytes"])
    :int32(self.isolation):string(self.config["client.rack"]):bytes()
  -- The broker may hold the request for `wait` before answering.
  local timeout = self.config["request.timeout.ms"] + wait

  local code, highWatermark, batches = decodeFetch(self:requestLeader(topic, partition, ApiKey.FETCH, body, timeout))
  if code == ErrorCode.NOT_LEADER_OR_FOLLOWER then
    self.routerObj:refresh(topic)
    code, highWatermark, batches = decodeFetch(self:requestLeader(topic, partition, ApiKey.FETCH, body, timeout))
  end
  if code ~= ErrorCode.NONE then
    protocol.serverError(code, string.format("fetch %s-%d", topic, partition))
  end

  local out = {}
  for _, batch in ipairs(batches) do
    for index, rec in ipairs(batch.records) do
      local recordOffset = batch.baseOffset + index - 1
      -- A batch can start before the requested offset; skip what the
      -- caller has already seen.
      if recordOffset >= offset then
        out[#out + 1] = {
          topic = topic, partition = partition, offset = recordOffset,
          key = rec.key, value = rec.value,
          timestamp = batch.maxTimestamp + rec.timestampDelta,
          headers = rec.headers,
        }
      end
    end
  end
  return out, highWatermark
end

--- The value of the first header named `name` on a consumed record, or nil.
function Consumer.header(record, name)
  for _, h in ipairs(record.headers or {}) do
    if h.key == name then return h.value end
  end
  return nil
end

return Consumer
