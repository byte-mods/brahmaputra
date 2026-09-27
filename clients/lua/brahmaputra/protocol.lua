-- Wire constants, varints, the BitPacker body codec and the frame codec.
--
-- Three encodings share one connection and none agrees with the others:
--
--  * the frame header is fixed big-endian (int32 length, int16 api key,
--    int16 api version, int32 correlation id, int16-prefixed client id);
--  * the request/response body is BitPacker: every integer a zigzag
--    varint, every string/array a varint count then its contents, the whole
--    body prefixed with the schema version string "1.0.0" (Writer/Reader);
--  * a record batch is big-endian header fields plus plain varints
--    (see brahmaputra.record_batch).
--
-- Lua 5.4 integers are 64-bit two's complement and `>>` is a *logical*
-- shift, so an arithmetic shift is spelled `-(v >> 63)` where zigzag needs
-- the sign smeared across the word, and "is this uint64 >= 0x80" is
-- math.ult(0x7f, v) so values with the top bit set (negative as int64)
-- still loop.

local errors = require("brahmaputra.errors")

local M = {}

M.SCHEMA_VERSION = "1.0.0"
--- Wire version this client speaks. The broker requires an exact match.
M.API_VERSION = 4

M.READ_UNCOMMITTED = 0
M.READ_COMMITTED = 1

M.INT32_MIN = -0x80000000
M.INT32_MAX = 0x7fffffff
M.INT64_MIN = math.mininteger
M.INT64_MAX = math.maxinteger

M.ApiKey = {
  PRODUCE = 0, FETCH = 1, LIST_OFFSETS = 2, METADATA = 3, REPLICA_FETCH = 4,
  OFFSETS_FOR_LEADER_EPOCH = 5, INIT_PRODUCER_ID = 6, JOIN_GROUP = 7,
  SYNC_GROUP = 8, HEARTBEAT = 9, OFFSET_COMMIT = 10, OFFSET_FETCH = 11,
  LIST_GROUPS = 12, DESCRIBE_GROUP = 13, API_VERSIONS = 14, PRODUCE_MULTI = 15,
  FETCH_MULTI = 16, AUTHENTICATE = 17, LEAVE_GROUP = 18,
}

M.ErrorCode = {
  NONE = 0, UNKNOWN_TOPIC_OR_PARTITION = 1, OFFSET_OUT_OF_RANGE = 2,
  INVALID_REQUEST = 3, UNSUPPORTED_VERSION = 4, INTERNAL = 5,
  NOT_LEADER_OR_FOLLOWER = 6, FENCED_BROKER_EPOCH = 7, FENCED_LEADER_EPOCH = 8,
  UNKNOWN_LEADER_EPOCH = 9, NOT_ENOUGH_REPLICAS = 10, FENCED_PRODUCER_EPOCH = 11,
  OUT_OF_ORDER_SEQUENCE = 12, UNKNOWN_MEMBER_ID = 13, REBALANCE_IN_PROGRESS = 14,
  NOT_COORDINATOR = 15, ILLEGAL_GENERATION = 16, COORDINATOR_LOAD_IN_PROGRESS = 17,
  SASL_AUTHENTICATION_FAILED = 18, AUTHORIZATION_FAILED = 19,
}
local E = M.ErrorCode

local codeNames = {}
for name, code in pairs(E) do codeNames[code] = name end

function M.errorName(code)
  return codeNames[code] or "UNKNOWN"
end

-- Codes the broker only returns *before* it appends anything, so a retry
-- cannot duplicate a record.
local retriable = {
  [E.NOT_LEADER_OR_FOLLOWER] = true, [E.FENCED_LEADER_EPOCH] = true,
  [E.UNKNOWN_LEADER_EPOCH] = true, [E.NOT_ENOUGH_REPLICAS] = true,
  [E.COORDINATOR_LOAD_IN_PROGRESS] = true, [E.INTERNAL] = true,
}

function M.isRetriable(code) return retriable[code] == true end

--- True when the error means the cached leader route is stale.
function M.isStaleRoute(code)
  return code == E.NOT_LEADER_OR_FOLLOWER or code == E.FENCED_LEADER_EPOCH
    or code == E.UNKNOWN_LEADER_EPOCH
end

--- Raise a ServerError for a non-zero broker code.
function M.serverError(code, context)
  local msg = string.format("broker returned %s[%d]", M.errorName(code), code)
  if context and context ~= "" then msg = msg .. " (" .. context .. ")" end
  errors.raise("ServerError", msg, { code = code })
end

local function protoError(msg) errors.raise("ProtocolError", msg) end
M.protoError = protoError

---------------------------------------------------------------------------
-- Integers
---------------------------------------------------------------------------

--- Coerce a number to a Lua integer, rejecting fractions. Config values and
--- user offsets may arrive as floats (1e3); the wire needs integers.
function M.toint(v, what)
  local i = math.tointeger(v)
  if i == nil then
    errors.raise("ConfigError", string.format("%s must be an integer, got %s", what or "value", tostring(v)))
  end
  return i
end

function M.zigzag64(v)
  return (v << 1) ~ -(v >> 63)
end

function M.unzigzag64(u)
  return (u >> 1) ~ -(u & 1)
end

function M.zigzag32(v)
  if v < M.INT32_MIN or v > M.INT32_MAX then
    errors.raise("ConfigError", tostring(v) .. " does not fit an int32")
  end
  return ((v << 1) ~ -(v >> 63)) & 0xffffffff
end

function M.unzigzag32(u)
  u = u & 0xffffffff
  return (u >> 1) ~ -(u & 1)
end

--- Unsigned LEB128 of a 64-bit pattern, appended to array `out`.
function M.putUvarint(out, v)
  local n = #out
  while math.ult(0x7f, v) do
    n = n + 1
    out[n] = string.char((v & 0x7f) | 0x80)
    v = v >> 7
  end
  out[n + 1] = string.char(v)
end

function M.uvarint(v)
  local out = {}
  M.putUvarint(out, v)
  return table.concat(out)
end

--- Decode an unsigned varint from `data` at 1-based `pos`, bounded by `limit`
--- (inclusive last index; defaults to #data). Returns value, next pos.
function M.getUvarint(data, pos, limit)
  limit = limit or #data
  local result, shift = 0, 0
  while true do
    if pos > limit then protoError("truncated varint") end
    local b = string.byte(data, pos)
    pos = pos + 1
    if shift == 63 and b > 1 then protoError("varint overflows 64 bits") end
    result = result | ((b & 0x7f) << shift)
    if b < 0x80 then return result, pos end
    shift = shift + 7
    if shift > 63 then protoError("varint overflows 64 bits") end
  end
end

---------------------------------------------------------------------------
-- BitPacker body writer / reader
---------------------------------------------------------------------------

local Writer = {}
Writer.__index = Writer
M.Writer = Writer

--- A writer already carrying the schema version every body starts with.
function Writer.body()
  local w = setmetatable({ buf = {} }, Writer)
  return w:string(M.SCHEMA_VERSION)
end

function Writer.new() return setmetatable({ buf = {} }, Writer) end

function Writer:int32(v)
  M.putUvarint(self.buf, M.zigzag32(M.toint(v, "int32")))
  return self
end

function Writer:int64(v)
  M.putUvarint(self.buf, M.zigzag64(M.toint(v, "int64")))
  return self
end

function Writer:bool(v)
  self.buf[#self.buf + 1] = v and "\1" or "\0"
  return self
end

function Writer:string(s)
  self:int32(#s)
  self.buf[#self.buf + 1] = s
  return self
end

function Writer:stringArray(list)
  self:int32(#list)
  for _, s in ipairs(list) do self:string(s) end
  return self
end

function Writer:raw(bytes)
  self.buf[#self.buf + 1] = bytes
  return self
end

function Writer:bytes() return table.concat(self.buf) end

local Reader = {}
Reader.__index = Reader
M.Reader = Reader

function Reader.new(data) return setmetatable({ data = data, pos = 1 }, Reader) end

--- A reader positioned past the schema version, which is verified: decoding
--- garbage into plausible fields is worse than failing loudly.
function Reader.body(data)
  local r = Reader.new(data)
  local version = r:string()
  if version ~= M.SCHEMA_VERSION then
    protoError(string.format("schema version mismatch: broker speaks %s, this client speaks %s",
      version, M.SCHEMA_VERSION))
  end
  return r
end

function Reader:remaining() return #self.data - self.pos + 1 end

function Reader:uvarint()
  local v
  v, self.pos = M.getUvarint(self.data, self.pos)
  return v
end

function Reader:int32()
  local u = self:uvarint()
  if math.ult(0xffffffff, u) then protoError("int32 varint out of range") end
  return M.unzigzag32(u)
end

function Reader:int64() return M.unzigzag64(self:uvarint()) end

function Reader:bool()
  if self.pos > #self.data then protoError("truncated bool") end
  local b = string.byte(self.data, self.pos)
  self.pos = self.pos + 1
  return b ~= 0
end

function Reader:string()
  local n = self:int32()
  if n < 0 or n > self:remaining() then protoError("truncated string") end
  local s = string.sub(self.data, self.pos, self.pos + n - 1)
  self.pos = self.pos + n
  return s
end

--- An array length, bounded by the bytes left so garbage cannot allocate
--- gigabytes.
function Reader:count()
  local n = self:int32()
  if n < 0 or n > self:remaining() then protoError("implausible array count " .. n) end
  return n
end

function Reader:stringArray()
  local out = {}
  for i = 1, self:count() do out[i] = self:string() end
  return out
end

function Reader:rest()
  local s = string.sub(self.data, self.pos)
  self.pos = #self.data + 1
  return s
end

---------------------------------------------------------------------------
-- Frames
---------------------------------------------------------------------------

--- One complete frame, length prefix included. A nil client id is encoded
--- as length -1.
function M.encodeFrame(apiKey, correlationId, clientId, body)
  local client
  if clientId == nil then
    client = string.pack(">i2", -1)
  else
    client = string.pack(">s2", clientId)
  end
  local header = string.pack(">i2i2i4", apiKey, M.API_VERSION, correlationId) .. client
  return string.pack(">i4", #header + #body) .. header .. body
end

--- Split a frame payload (length prefix stripped) into correlation id, body.
function M.decodeFramePayload(payload)
  if #payload < 10 then protoError("frame payload shorter than its header") end
  local _, _, correlation, clientLen = string.unpack(">i2i2i4i2", payload)
  local offset = 11 + math.max(0, clientLen)
  if offset - 1 > #payload then protoError("frame client id runs past the payload") end
  return correlation, string.sub(payload, offset)
end

return M
