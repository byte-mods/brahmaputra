-- Batch compression codecs.
--
-- `none` is always available. `gzip` is built in when lua-zlib is installed
-- (`require "zlib"`, Debian/Ubuntu package lua-zlib); it writes the RFC 1952
-- gzip container the broker's flate2 GzEncoder/GzDecoder use (windowBits
-- 16+15), not the zlib container. lz4, zstd and snappy are opt-in through
-- register(), so an application that does not want those libraries does
-- not need them.
--
-- If you register lz4, the broker expects lz4_flex's compress_prepend_size
-- layout: a little-endian uint32 of the uncompressed length, then a raw LZ4
-- *block* -- not the LZ4 frame format.

local errors = require("brahmaputra.errors")

local M = {}

M.NONE = 0
M.LZ4 = 1
M.ZSTD = 2
M.SNAPPY = 3
M.GZIP = 4

--- Caps decompressed output so a corrupt batch cannot exhaust memory.
M.MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024

local names = { none = M.NONE, lz4 = M.LZ4, zstd = M.ZSTD, snappy = M.SNAPPY, gzip = M.GZIP }
local byCode = {}
for name, code in pairs(names) do byCode[code] = name end

local okZlib, zlib = pcall(require, "zlib")
if not okZlib then zlib = nil end

--- True when the gzip codec is usable (lua-zlib is installed).
M.gzipAvailable = zlib ~= nil

local registered = {}

--- Plug in a codec this driver does not carry. `codec` is a code (M.LZ4...)
--- or name ("lz4"); both functions take and return a Lua string and raise
--- on failure.
function M.register(codec, compress, decompress)
  if type(codec) == "string" then codec = M.parse(codec) end
  registered[codec] = { compress, decompress }
end

function M.parse(name)
  if type(name) == "number" then
    if byCode[name] then return name end
  else
    local code = names[string.lower(tostring(name))]
    if code then return code end
  end
  errors.raise("ConfigError", "unknown compression.type " .. tostring(name) .. " (none, gzip, lz4, zstd, snappy)")
end

function M.name(code) return byCode[code] or ("unknown(" .. tostring(code) .. ")") end

local function gzipCompress(payload)
  -- level 6, windowBits 31 = gzip wrapper
  local out, eof = zlib.deflate(6, 31)(payload, "finish")
  if not eof then errors.raise("BrahmaputraError", "gzip compression did not finish") end
  return out
end

local function gzipDecompress(payload)
  local ok, out, eof = pcall(function()
    return zlib.inflate(31)(payload)
  end)
  if not ok or out == nil then
    errors.raise("ProtocolError", "gzip batch payload failed to decompress: " .. tostring(out))
  end
  if not eof then errors.raise("ProtocolError", "gzip batch payload is truncated") end
  if #out > M.MAX_DECOMPRESSED_BYTES then
    errors.raise("ProtocolError", "gzip batch payload decompresses past the size cap")
  end
  return out
end

--- Is `codec` usable for producing right now?
function M.available(codec)
  return registered[codec] ~= nil or codec == M.NONE or (codec == M.GZIP and zlib ~= nil)
end

function M.compress(codec, payload)
  local custom = registered[codec]
  if custom then return custom[1](payload) end
  if codec == M.NONE then return payload end
  if codec == M.GZIP and zlib then return gzipCompress(payload) end
  errors.raise("BrahmaputraError", M.name(codec) ..
    " compression is not available; install lua-zlib for gzip or register it with compression.register()")
end

function M.decompress(codec, payload)
  local custom = registered[codec]
  if custom then return custom[2](payload) end
  if codec == M.NONE then return payload end
  if codec == M.GZIP and zlib then return gzipDecompress(payload) end
  errors.raise("BrahmaputraError", M.name(codec) ..
    " decompression is not available; install lua-zlib for gzip or register it with compression.register()")
end

return M
