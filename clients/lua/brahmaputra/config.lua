-- Kafka-style configuration and the clock.
--
-- Configuration is a flat table of dotted keys, as the Java client and
-- librdkafka take them: { ["bootstrap.servers"] = "127.0.0.1:9092",
-- ["linger.ms"] = 5 }. Unknown keys are rejected rather than ignored,
-- because a misspelled `linger.ms` that silently falls back to the default
-- is the kind of mistake only found in production.

local socket = require("socket")
local errors = require("brahmaputra.errors")

local M = {}

--- Merge `given` over `defaults`, rejecting unknown keys and coercing
--- integral numbers to Lua integers (1e3 -> 1000).
function M.resolve(defaults, given, what, nullable)
  given = given or {}
  nullable = nullable or {}
  local unknown = {}
  for k in pairs(given) do
    if defaults[k] == nil and not nullable[k] then unknown[#unknown + 1] = tostring(k) end
  end
  if #unknown > 0 then
    table.sort(unknown)
    local known = {}
    for k in pairs(defaults) do known[#known + 1] = k end
    for k in pairs(nullable) do known[#known + 1] = k end
    table.sort(known)
    errors.raise("ConfigError", string.format("unknown %s config %s; known keys: %s",
      what, table.concat(unknown, ", "), table.concat(known, ", ")))
  end
  local config = {}
  for k, v in pairs(defaults) do config[k] = v end
  for k, v in pairs(given) do
    if type(v) == "number" and math.type(v) == "float" and math.tointeger(v) then
      v = math.tointeger(v)
    end
    config[k] = v
  end
  local servers = config["bootstrap.servers"]
  if type(servers) ~= "string" or servers == "" then
    errors.raise("ConfigError", what .. ' config needs bootstrap.servers ("host:port[,host:port]")')
  end
  return config
end

--- Integer config value, validated.
function M.int(config, key)
  local v = config[key]
  local i = math.tointeger(v)
  if i == nil then
    errors.raise("ConfigError", string.format("%s must be an integer, got %s", key, tostring(v)))
  end
  return i
end

--- Parse "host:port,host:port" into { {host, port}, ... }.
function M.parseBootstrap(servers)
  local out = {}
  for server in string.gmatch(servers, "[^,]+") do
    server = server:match("^%s*(.-)%s*$")
    if server ~= "" then
      local host, port = server:match("^%[(.+)%]:(%d+)$")
      if not host then host, port = server:match("^(.+):(%d+)$") end
      if not host then host, port = server, "9092" end
      out[#out + 1] = { host, math.tointeger(tonumber(port)) }
    end
  end
  if #out == 0 then errors.raise("ConfigError", "bootstrap.servers is empty") end
  return out
end

--- Milliseconds since the epoch, as an integer. Used both for deadlines and
--- record timestamps (LuaSocket exposes no monotonic clock).
function M.nowMs()
  return math.floor(socket.gettime() * 1000)
end

M.wallMs = M.nowMs

function M.sleepMs(ms)
  if ms > 0 then socket.sleep(ms / 1000) end
end

return M
