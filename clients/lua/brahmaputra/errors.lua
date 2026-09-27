-- Error values raised by the driver.
--
-- Every failure is raised with error() as a table carrying a `kind`, so a
-- caller can tell them apart after pcall without parsing messages:
--
--   local ok, err = pcall(producer.flush, producer)
--   if not ok and errors.is(err, "ServerError") then print(err.code) end
--
-- Kinds form a small hierarchy (errors.is follows it):
--
--   BrahmaputraError
--     ServerError                 broker returned a non-zero code (err.code)
--     ConnectionError             socket failed or closed
--       TimeoutError              a round trip exceeded its timeout
--     ProtocolError               bytes this client cannot decode
--     BufferFullError             buffer.memory stayed full past max.block.ms
--     NoOffsetForPartitionError   auto.offset.reset=none with no commit
--     ConfigError                 bad or unknown configuration

local M = {}

local parents = {
  ServerError = "BrahmaputraError",
  ConnectionError = "BrahmaputraError",
  TimeoutError = "ConnectionError",
  ProtocolError = "BrahmaputraError",
  BufferFullError = "BrahmaputraError",
  NoOffsetForPartitionError = "BrahmaputraError",
  ConfigError = "BrahmaputraError",
}

local Error = {}
Error.__index = Error
Error.__tostring = function(e)
  return e.kind .. ": " .. e.message
end

M.Error = Error

--- Build (not raise) an error value. `cause` is an optional wrapped error.
function M.new(kind, message, fields)
  local e = setmetatable({ kind = kind, message = message }, Error)
  if fields then
    for k, v in pairs(fields) do e[k] = v end
  end
  return e
end

--- Raise an error of `kind`.
function M.raise(kind, message, fields)
  error(M.new(kind, message, fields), 0)
end

--- True when `err` is a driver error of `kind` or of a kind derived from it.
function M.is(err, kind)
  if getmetatable(err) ~= Error then return false end
  local k = err.kind
  while k do
    if k == kind then return true end
    k = parents[k]
  end
  return false
end

--- A readable message for any error value (driver error or plain string).
function M.message(err)
  if getmetatable(err) == Error then return tostring(err) end
  return tostring(err)
end

--- Normalise a foreign error (a string from a Lua runtime fault) into a
--- driver error so callers can rely on `err.kind`.
function M.wrap(err)
  if getmetatable(err) == Error then return err end
  return M.new("BrahmaputraError", tostring(err))
end

return M
