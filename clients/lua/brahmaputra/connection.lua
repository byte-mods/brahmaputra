-- One blocking TCP connection to one broker (LuaSocket).
--
-- Lua has no threads, so a request is written and its response read before
-- the call returns: at most one request is in flight per connection.
--
-- Every round trip is bounded by a timeout (DEFAULT_REQUEST_TIMEOUT_MS
-- unless set). Any I/O failure, timeout or correlation-id mismatch leaves
-- the byte stream at an unknown position -- a partial frame may have been
-- written, or a late response may still arrive -- so the connection is
-- closed and marked `broken` rather than reused. The Router notices and
-- redials on next use.

local socket = require("socket")
local errors = require("brahmaputra.errors")
local protocol = require("brahmaputra.protocol")
local config = require("brahmaputra.config")

local ApiKey = protocol.ApiKey
local ErrorCode = protocol.ErrorCode

local Connection = {}
Connection.__index = Connection

--- Bounds one request/response round trip. It must exceed the longest the
--- broker may legitimately hold a request (a fetch long-poll, an acks=all
--- wait, a JoinGroup waiting out a rebalance); its job is to turn a wedged
--- broker into an error instead of a process blocked forever.
Connection.DEFAULT_REQUEST_TIMEOUT_MS = 120000

local MAX_FRAME = 512 * 1024 * 1024

--- Open a connection. opts: clientId, connectTimeoutMs (10000),
--- requestTimeoutMs (DEFAULT_REQUEST_TIMEOUT_MS).
function Connection.open(host, port, opts)
  opts = opts or {}
  local sock, err
  if host:find(":", 1, true) and socket.tcp6 then sock, err = socket.tcp6() else sock, err = socket.tcp() end
  if not sock then
    errors.raise("ConnectionError", "socket: " .. tostring(err))
  end
  sock:settimeout((opts.connectTimeoutMs or 10000) / 1000)
  local ok, cerr = sock:connect(host, port)
  if not ok then
    sock:close()
    errors.raise("ConnectionError", string.format("connect to %s:%d failed: %s", host, port, tostring(cerr)))
  end
  -- Responses are small and latency matters more than packet count.
  sock:setoption("tcp-nodelay", true)
  return setmetatable({
    sock = sock,
    host = host,
    port = port,
    clientId = opts.clientId == nil and "brahmaputra-lua" or opts.clientId,
    requestTimeoutMs = opts.requestTimeoutMs or Connection.DEFAULT_REQUEST_TIMEOUT_MS,
    correlation = 0,
    broken = false,
  }, Connection)
end

function Connection:address() return self.host .. ":" .. self.port end

--- True once this connection failed or was closed. A broken connection is
--- never reused.
function Connection:isBroken() return self.broken end

--- Change how long one round trip may take (ms). Zero or negative disables
--- the bound.
function Connection:setRequestTimeout(ms) self.requestTimeoutMs = ms end

function Connection:close()
  self.broken = true
  if self.sock then
    self.sock:close()
    self.sock = nil
  end
end

-- Mark unusable and raise.
function Connection:fail(kind, message)
  self:close()
  errors.raise(kind, message)
end

--- A cheap liveness probe: an idle connection must have nothing to read. If
--- the socket is readable the peer closed it (or sent something unasked,
--- which desynchronises the stream), so it is closed and marked broken.
function Connection:isUsable()
  if self.broken or not self.sock then return false end
  local readable = socket.select({ self.sock }, nil, 0)
  if readable and #readable > 0 then
    self:close()
    return false
  end
  return true
end

function Connection:nextCorrelation()
  self.correlation = (self.correlation + 1) & 0x7fffffff
  return self.correlation
end

local function deadlineFor(timeoutMs)
  if timeoutMs == nil or timeoutMs <= 0 then return nil end
  return config.nowMs() + timeoutMs
end

function Connection:setTimeoutUntil(deadline)
  if deadline == nil then
    self.sock:settimeout(nil)
    return true
  end
  local remaining = deadline - config.nowMs()
  if remaining <= 0 then return false end
  self.sock:settimeout(remaining / 1000)
  return true
end

function Connection:write(frame, deadline)
  local sent = 0
  local total = #frame
  while sent < total do
    if not self:setTimeoutUntil(deadline) then
      self:fail("TimeoutError", string.format("write to %s timed out", self:address()))
    end
    local last, err, partial = self.sock:send(frame, sent + 1)
    if last then
      sent = last
    else
      sent = partial or sent
      if err ~= "timeout" then
        self:fail("ConnectionError", string.format("write to %s failed: %s", self:address(), tostring(err)))
      end
    end
  end
end

function Connection:readExact(n, deadline)
  local parts = {}
  local have = 0
  while have < n do
    if not self:setTimeoutUntil(deadline) then
      self:fail("TimeoutError", string.format("request to %s timed out", self:address()))
    end
    local data, err, partial = self.sock:receive(n - have)
    local chunk = data or partial
    if chunk and #chunk > 0 then
      parts[#parts + 1] = chunk
      have = have + #chunk
    end
    if not data and err ~= "timeout" then
      self:fail("ConnectionError", string.format("connection to %s %s", self:address(),
        err == "closed" and "closed by broker" or ("failed: " .. tostring(err))))
    end
  end
  return table.concat(parts)
end

--- Send one request and return the matching response body. `timeoutMs`
--- overrides the connection's round-trip timeout for this request.
function Connection:request(apiKey, body, timeoutMs)
  if self.broken then
    errors.raise("ConnectionError", "connection to " .. self:address() .. " is broken; the router will redial")
  end
  local correlationId = self:nextCorrelation()
  local deadline = deadlineFor(timeoutMs or self.requestTimeoutMs)
  self:write(protocol.encodeFrame(apiKey, correlationId, self.clientId, body), deadline)
  local prefix = self:readExact(4, deadline)
  local length = string.unpack(">i4", prefix)
  if length < 0 or length > MAX_FRAME then
    self:fail("ProtocolError", "implausible frame length " .. length)
  end
  local payload = self:readExact(length, deadline)
  local ok, got, responseBody = pcall(protocol.decodeFramePayload, payload)
  if not ok then
    self:close()
    error(got, 0)
  end
  if got ~= correlationId then
    -- A response for a request we are not waiting on means the stream has
    -- desynchronised; continuing would pair every later response with the
    -- wrong request.
    self:fail("ProtocolError", string.format("correlation id mismatch: expected %d, got %d", correlationId, got))
  end
  return responseBody
end

--- Send without awaiting a response (acks=0: the broker does not answer).
function Connection:sendOneway(apiKey, body)
  if self.broken then
    errors.raise("ConnectionError", "connection to " .. self:address() .. " is broken; the router will redial")
  end
  self:write(protocol.encodeFrame(apiKey, self:nextCorrelation(), self.clientId, body),
    deadlineFor(self.requestTimeoutMs))
end

--- Ask the broker what it speaks. Returns versions {{apiKey, minVersion,
--- maxVersion}, ...} and the broker's version string.
function Connection:apiVersions()
  local body = protocol.Writer.body():string("brahmaputra-lua"):string("0.1.0"):bytes()
  local r = protocol.Reader.body(self:request(ApiKey.API_VERSIONS, body))
  local code = r:int32()
  if code ~= ErrorCode.NONE then protocol.serverError(code, "api_versions") end
  local versions = {}
  for i = 1, r:count() do
    versions[i] = { apiKey = r:int32(), minVersion = r:int32(), maxVersion = r:int32() }
  end
  return versions, r:string()
end

return Connection
