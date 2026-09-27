-- A batching producer.
--
-- Lua has no threads, so there is no background sender: batching happens
-- in-process and batches go out from inside your calls. A partition's batch
-- is sent
--
--  * by send() when it reaches `batch.size` bytes, or at once when
--    `linger.ms` is 0;
--  * by send(), poll() or flush() once its oldest record has waited
--    `linger.ms` -- whichever you call first after the deadline;
--  * by flush() and close() unconditionally.
--
-- So a long-lived worker calls producer:poll(0) from its loop, and every
-- program calls flush() or close() before it ends (a __gc finalizer
-- flushes as a last resort and warns if it cannot).
--
-- Errors. A batch the call itself had to send -- its own record filled the
-- batch, linger.ms is 0, sendSync(), flush() -- raises from that call. A
-- batch sent only because its linger expired while you called send() or
-- poll() for something else is a *background* flush: its failure is not
-- raised from the unrelated call but held and raised by the next flush()
-- or close(), so it is never dropped. close() still releases its
-- connections when it raises. With `delivery.report.callback` set, every
-- outcome goes to the callback instead and nothing is held.
--
-- Ordering. A partition has at most one open batch and batches are sent
-- synchronously, one at a time, so at most one batch per partition is in
-- flight and a partition's records reach the broker in send order whichever
-- path sends them. sendSync() sends the partition's open batch first.
--
-- Retries: a batch the broker refuses with a retriable code (returned
-- before it appends, so no duplicate is possible) is retried up to
-- `retries` times, `retry.backoff.ms` apart, within `delivery.timeout.ms`
-- of its oldest record. A connection failure mid-request is retried too,
-- after a redial, as Kafka's non-idempotent producer does: the broker may
-- already have appended the batch, so that case is at-least-once.

local errors = require("brahmaputra.errors")
local protocol = require("brahmaputra.protocol")
local config = require("brahmaputra.config")
local hash = require("brahmaputra.hash")
local compression = require("brahmaputra.compression")
local RecordBatch = require("brahmaputra.record_batch")
local Router = require("brahmaputra.router")

local ApiKey = protocol.ApiKey
local ErrorCode = protocol.ErrorCode
local nowMs = config.nowMs

local Producer = {}
Producer.__index = Producer

--- Default configuration. Kafka names.
function Producer.defaults()
  return {
    ["bootstrap.servers"] = false,
    ["client.id"] = "brahmaputra-lua",
    -- 0 fire-and-forget, 1 leader append, -1 / "all" every in-sync replica
    ["acks"] = 1,
    ["batch.size"] = 16384,
    -- Kafka's default is 0; 5 because an unbatched producer is slow enough
    -- to look broken.
    ["linger.ms"] = 5,
    ["compression.type"] = "none",
    -- broker-side wait for acknowledgements; the socket round trip is
    -- bounded by this plus a 5 s allowance
    ["request.timeout.ms"] = 30000,
    ["retries"] = 5,
    ["retry.backoff.ms"] = 100,
    ["delivery.timeout.ms"] = 120000,
    ["buffer.memory"] = 32 * 1024 * 1024,
    ["max.block.ms"] = 60000,
    ["socket.connection.setup.timeout.ms"] = 10000,
  }
end

--- Create a producer. `conf` holds Kafka-style keys (see defaults());
--- `delivery.report.callback` = function(report) receives
--- {topic, partition, baseOffset, recordCount, error} for every batch.
function Producer.new(conf)
  local c = config.resolve(Producer.defaults(), conf, "producer", { ["delivery.report.callback"] = true })
  local acks = c["acks"]
  if acks == "all" then acks = -1 end
  acks = math.tointeger(tonumber(acks))
  if acks ~= 0 and acks ~= 1 and acks ~= -1 then
    errors.raise("ConfigError", 'acks must be 0, 1, -1 or "all", got ' .. tostring(c["acks"]))
  end
  local codec = compression.parse(c["compression.type"])
  if not compression.available(codec) then
    errors.raise("ConfigError", "compression.type " .. compression.name(codec) ..
      " is not available (install lua-zlib for gzip, or compression.register() a codec)")
  end
  for _, key in ipairs({ "batch.size", "linger.ms", "request.timeout.ms", "retries", "retry.backoff.ms",
    "delivery.timeout.ms", "buffer.memory", "max.block.ms", "socket.connection.setup.timeout.ms" }) do
    c[key] = config.int(c, key)
  end
  local self = setmetatable({
    config = c,
    acks = acks,
    codec = codec,
    slots = {},        -- name -> {topic, partition, records, bytes, firstMs}
    buffered = 0,
    roundRobin = 0,
    closed = false,
    pendingError = nil,
    pendingFailures = 0,
  }, Producer)
  self.routerObj = Router.new(c["bootstrap.servers"], {
    clientId = c["client.id"],
    requestTimeoutMs = c["request.timeout.ms"] + 5000,
    connectTimeoutMs = c["socket.connection.setup.timeout.ms"],
  })
  return self
end

function Producer:router() return self.routerObj end

--- Unflushed record bytes currently held client-side.
function Producer:bufferedBytes() return self.buffered end

function Producer:ensureOpen()
  if self.closed then errors.raise("BrahmaputraError", "producer is closed") end
end

local function estimate(value, key, headers)
  local size = #(value or "") + #(key or "") + 16
  for _, h in ipairs(headers or {}) do
    size = size + #h.key + #(h.value or "") + 4
  end
  return size
end

local function normHeaders(headers)
  local out = {}
  for i, h in ipairs(headers or {}) do
    if type(h.key) ~= "string" then
      errors.raise("ConfigError", "a header needs a string key")
    end
    if h.value ~= nil and type(h.value) ~= "string" then
      errors.raise("ConfigError", "a header value must be a string or nil")
    end
    out[i] = { key = h.key, value = h.value }
  end
  return out
end

function Producer:choosePartition(topic, key)
  local partitions = self.routerObj:partitions(topic)
  if key == nil then
    local p = partitions[self.roundRobin % #partitions + 1]
    self.roundRobin = self.roundRobin + 1
    return p
  end
  return partitions[hash.partitionIndex(key, #partitions) + 1]
end

local function checkArgs(topic, value, key)
  if type(topic) ~= "string" or topic == "" then errors.raise("ConfigError", "topic must be a non-empty string") end
  if value ~= nil and type(value) ~= "string" then errors.raise("ConfigError", "value must be a string or nil (tombstone)") end
  if key ~= nil and type(key) ~= "string" then errors.raise("ConfigError", "key must be a string or nil") end
end

local function makeRecord(value, key, opts)
  local ts = opts.timestamp
  if ts ~= nil then ts = protocol.toint(ts, "timestamp") else ts = config.wallMs() end
  return { key = key, value = value, headers = normHeaders(opts.headers), timestamp = ts, createdMs = nowMs() }
end

--- Buffer one record for delivery.
---
--- A nil `value` is a tombstone (deletes `key` on a compacted topic); "" is
--- an ordinary empty value. opts: partition (explicit partition), headers
--- ({{key=, value=}, ...}, a nil value is kept distinct from ""), timestamp
--- (unix ms; defaults to now). Without a partition a keyed record goes to
--- murmur2(key) % partitions and a keyless one round-robins.
function Producer:send(topic, value, key, opts)
  opts = opts or {}
  self:ensureOpen()
  checkArgs(topic, value, key)
  -- Deliver whatever has lingered long enough before adding more.
  self:sendExpired(true)

  local target = opts.partition and protocol.toint(opts.partition, "partition") or self:choosePartition(topic, key)
  local record = makeRecord(value, key, opts)
  local size = estimate(value, key, record.headers)
  self:reserve(size)

  local name = topic .. "\0" .. target
  local slot = self.slots[name]
  if slot == nil then
    slot = { topic = topic, partition = target, records = {}, bytes = 0, firstMs = nowMs() }
    self.slots[name] = slot
  end
  slot.records[#slot.records + 1] = record
  slot.bytes = slot.bytes + size

  if self.config["linger.ms"] <= 0 or slot.bytes >= self.config["batch.size"] then
    self:flushSlots({ name }, false)
  end
end

--- Send one record on its own and return its offset (-1 with acks=0). A
--- full round trip per record: correct, and slow.
function Producer:sendSync(topic, value, key, opts)
  opts = opts or {}
  self:ensureOpen()
  checkArgs(topic, value, key)
  local target = opts.partition and protocol.toint(opts.partition, "partition") or self:choosePartition(topic, key)
  -- Records already buffered for this partition were sent first, so they
  -- must reach the broker first.
  self:flushSlots({ topic .. "\0" .. target }, false)
  return self:produce(topic, target, { makeRecord(value, key, opts) })
end

function Producer:hasBuffered()
  return next(self.slots) ~= nil
end

function Producer:nextDueMs()
  local due = math.maxinteger
  local linger = self.config["linger.ms"]
  for _, slot in pairs(self.slots) do
    due = math.min(due, slot.firstMs + linger)
  end
  return due
end

--- Send every batch whose linger.ms has elapsed. With timeoutMs > 0, waits
--- up to that long for further batches to fall due and sends them too.
--- Returns the number of batches sent. Failures are held for flush().
function Producer:poll(timeoutMs)
  self:ensureOpen()
  local sent = self:sendExpired(true)
  timeoutMs = timeoutMs or 0
  if timeoutMs > 0 then
    local deadline = nowMs() + timeoutMs
    while self:hasBuffered() and nowMs() < deadline do
      local now = nowMs()
      config.sleepMs(math.max(1, math.min(self:nextDueMs() - now, deadline - now)))
      sent = sent + self:sendExpired(true)
    end
  end
  return sent
end

function Producer:takePendingError()
  local err, failures = self.pendingError, self.pendingFailures
  self.pendingError, self.pendingFailures = nil, 0
  if err ~= nil and failures > 1 then
    return errors.new("BrahmaputraError", string.format("%d background batches failed to deliver; first: %s",
      failures, errors.message(err)), { cause = err })
  end
  return err
end

--- Send every buffered record now and wait for the broker to acknowledge
--- them. Also raises any failure a background (linger) flush has held
--- since the last flush, so no failed batch goes unreported.
function Producer:flush()
  local names = {}
  for name in pairs(self.slots) do names[#names + 1] = name end
  table.sort(names)
  local ok, err = pcall(self.flushSlots, self, names, false)
  local held = self:takePendingError()
  if not ok then error(err, 0) end
  if held ~= nil then error(held, 0) end
end

--- Flush, then close every connection. Connections are released even when
--- the flush raises (the error is re-raised after).
function Producer:close()
  if self.closed then return end
  local ok, err = pcall(self.flush, self)
  self.closed = true
  self.routerObj:close()
  if not ok then error(err, 0) end
end

function Producer:__gc()
  if not self.closed and self.routerObj then
    local ok, err = pcall(self.close, self)
    if not ok then
      io.stderr:write("brahmaputra producer lost buffered records at collection: " .. errors.message(err) .. "\n")
    end
  end
end

-- Wait until `size` more bytes may be buffered.
--
-- This is what makes buffer.memory real: a producer faster than its broker
-- is held here instead of growing without limit. With no background sender,
-- the only thing that can drain the buffer while we wait is a batch whose
-- linger.ms falls due, so that is what the loop sends; if nothing frees
-- room within max.block.ms, the send fails with BufferFullError.
function Producer:reserve(size)
  local limit = self.config["buffer.memory"]
  if limit <= 0 or size >= limit then
    -- A record larger than the whole budget is admitted rather than waiting
    -- on a condition that can never hold.
    self.buffered = self.buffered + size
    return
  end
  local maxBlock = self.config["max.block.ms"]
  local deadline = nowMs() + maxBlock
  while self.buffered + size > limit do
    self:sendExpired(true)
    if self.buffered + size <= limit then break end
    local now = nowMs()
    if now >= deadline then
      errors.raise("BufferFullError", string.format(
        "producer buffer full: %d of %d bytes unflushed after max.block.ms=%d", self.buffered, limit, maxBlock))
    end
    config.sleepMs(math.max(1, math.min(20, deadline - now, self:nextDueMs() - now)))
  end
  self.buffered = self.buffered + size
end

function Producer:sendExpired(background)
  local now = nowMs()
  local linger = self.config["linger.ms"]
  local due = {}
  for name, slot in pairs(self.slots) do
    if now - slot.firstMs >= linger then due[#due + 1] = name end
  end
  if #due > 0 then
    table.sort(due)
    self:flushSlots(due, background)
  end
  return #due
end

-- Returns true when a callback consumed the report.
function Producer:report(report)
  local callback = self.config["delivery.report.callback"]
  if callback == nil then return false end
  callback(report)
  return true
end

-- Send the named slots. Every slot is attempted even if one fails; the first
-- failure is then raised (or held, when `background`), or each outcome goes
-- to the delivery callback.
function Producer:flushSlots(names, background)
  local firstError, failures = nil, 0
  for _, name in ipairs(names) do
    local slot = self.slots[name]
    if slot ~= nil then
      self.slots[name] = nil
      self.buffered = math.max(0, self.buffered - slot.bytes)
      local ok, result = pcall(self.produce, self, slot.topic, slot.partition, slot.records)
      if ok then
        self:report({ topic = slot.topic, partition = slot.partition, baseOffset = result,
          recordCount = #slot.records })
      else
        failures = failures + 1
        local err = errors.wrap(result)
        if not self:report({ topic = slot.topic, partition = slot.partition, baseOffset = -1,
              recordCount = #slot.records, error = err }) then
          firstError = firstError or err
        end
      end
    end
  end
  if firstError == nil then return end
  if background then
    self.pendingError = self.pendingError or firstError
    self.pendingFailures = self.pendingFailures + failures
    return
  end
  if failures > 1 then
    errors.raise("BrahmaputraError", string.format("%d batches failed to deliver; first: %s",
      failures, errors.message(firstError)), { cause = firstError })
  end
  error(firstError, 0)
end

-- Encode and deliver one batch; returns its base offset (-1 for acks=0).
function Producer:produce(topic, partition, buffered)
  -- One base timestamp per batch plus a delta per record; the base is the
  -- newest record's time, so max_timestamp truthfully answers "how recent".
  local maxTimestamp, oldestCreated = math.mininteger, math.maxinteger
  for _, item in ipairs(buffered) do
    if item.timestamp > maxTimestamp then maxTimestamp = item.timestamp end
    if item.createdMs < oldestCreated then oldestCreated = item.createdMs end
  end
  local records = {}
  for i, item in ipairs(buffered) do
    records[i] = { key = item.key, value = item.value, headers = item.headers,
      timestampDelta = item.timestamp - maxTimestamp }
  end
  local encoded = RecordBatch.encode(records, maxTimestamp, self.codec)
  local requestTimeout = self.config["request.timeout.ms"]
  local body = protocol.Writer.body()
    :string(topic):int32(partition):int32(self.acks):int32(requestTimeout)
    :int64(#encoded):raw(encoded):bytes()

  local deadline = oldestCreated + self.config["delivery.timeout.ms"]
  local attemptsLeft = self.config["retries"]
  local backoff = self.config["retry.backoff.ms"]
  local router = self.routerObj
  while true do
    if nowMs() >= deadline then
      errors.raise("TimeoutError", string.format("delivery.timeout.ms expired for %s-%d", topic, partition))
    end
    local ok, result = pcall(function()
      local conn = router:connectionFor(topic, partition)
      if self.acks == 0 then
        conn:sendOneway(ApiKey.PRODUCE, body)
        return false
      end
      return conn:request(ApiKey.PRODUCE, body, requestTimeout + 5000)
    end)
    if not ok then
      if not errors.is(result, "ConnectionError") or attemptsLeft <= 0 or nowMs() >= deadline then
        error(result, 0)
      end
      attemptsLeft = attemptsLeft - 1
      config.sleepMs(backoff)
    elseif result == false then
      return -1
    else
      local r = protocol.Reader.body(result)
      r:string() -- topic
      r:int32()  -- partition
      local code = r:int32()
      local baseOffset = r:int64()
      r:int64()  -- log_append_time_ms
      if code == ErrorCode.NONE then return baseOffset end
      if not protocol.isRetriable(code) or attemptsLeft <= 0 or nowMs() >= deadline then
        protocol.serverError(code, string.format("produce to %s-%d", topic, partition))
      end
      attemptsLeft = attemptsLeft - 1
      if protocol.isStaleRoute(code) then
        -- Resending to the same broker would repeat the error.
        router:refresh(topic)
      end
      config.sleepMs(backoff)
    end
  end
end

return Producer
