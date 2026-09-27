-- Brahmaputra client for Lua 5.4.
--
--   local brahmaputra = require("brahmaputra")
--   local producer = brahmaputra.Producer.new{ ["bootstrap.servers"] = "127.0.0.1:9092" }
--
-- Speaks the broker's wire protocol directly over LuaSocket; see README.md.

local M = {
  _VERSION = "0.1.0",
  Producer = require("brahmaputra.producer"),
  Consumer = require("brahmaputra.consumer"),
  GroupConsumer = require("brahmaputra.group_consumer"),
  Router = require("brahmaputra.router"),
  Connection = require("brahmaputra.connection"),
  assignor = require("brahmaputra.assignor"),
  compression = require("brahmaputra.compression"),
  protocol = require("brahmaputra.protocol"),
  record_batch = require("brahmaputra.record_batch"),
  errors = require("brahmaputra.errors"),
  hash = require("brahmaputra.hash"),
}

--- listOffsets() sentinels.
M.EARLIEST = M.Consumer.EARLIEST
M.LATEST = M.Consumer.LATEST

M.ErrorCode = M.protocol.ErrorCode

--- Kafka's murmur2 (unsigned 32-bit). murmur2("") == 275646681.
M.murmur2 = M.hash.murmur2

--- The partition a key maps to among `partitions` (ascending ids), as
--- Kafka's default partitioner picks it.
function M.partitionForKey(key, partitions)
  return partitions[M.hash.partitionIndex(key, #partitions) + 1]
end

--- A record header. A nil value is kept distinct from "".
function M.header(key, value)
  return { key = key, value = value }
end

--- The first value of header `name` on a consumed record, or nil.
M.headerValue = M.Consumer.header

--- Register a compression codec this driver does not carry (lz4, zstd,
--- snappy): functions string -> string that raise on failure.
M.registerCodec = M.compression.register

return M
