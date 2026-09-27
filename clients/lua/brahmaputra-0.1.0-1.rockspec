rockspec_format = "3.0"
package = "brahmaputra"
version = "0.1.0-1"
source = {
   url = "git+https://github.com/byte-mods/brahmaputra.git",
   tag = "v0.1.0",
   dir = "brahmaputra/clients/lua",
}
description = {
   summary = "Native Lua 5.4 client for the Brahmaputra log broker",
   detailed = [[
      Producer (batching, acks 0/1/all, retries, bounded buffer, murmur2
      partitioning, headers, tombstones), partition consumer and consumer
      groups (range/roundrobin/sticky, auto commit, static membership),
      speaking Brahmaputra's wire protocol directly over LuaSocket.
      Single-threaded: batches and heartbeats run inside your calls.
      gzip compression is available when lua-zlib is installed.
   ]],
   homepage = "https://github.com/byte-mods/brahmaputra",
   license = "Apache-2.0",
}
dependencies = {
   "lua >= 5.4",
   "luasocket >= 3.0",
}
-- lua-zlib is optional: without it the gzip codec is unavailable and
-- compression.type = "gzip" is rejected at construction.
test_dependencies = {
   "lua-zlib >= 1.2",
}
build = {
   type = "builtin",
   modules = {
      ["brahmaputra"] = "brahmaputra.lua",
      ["brahmaputra.assignor"] = "brahmaputra/assignor.lua",
      ["brahmaputra.compression"] = "brahmaputra/compression.lua",
      ["brahmaputra.config"] = "brahmaputra/config.lua",
      ["brahmaputra.connection"] = "brahmaputra/connection.lua",
      ["brahmaputra.consumer"] = "brahmaputra/consumer.lua",
      ["brahmaputra.errors"] = "brahmaputra/errors.lua",
      ["brahmaputra.group_consumer"] = "brahmaputra/group_consumer.lua",
      ["brahmaputra.hash"] = "brahmaputra/hash.lua",
      ["brahmaputra.producer"] = "brahmaputra/producer.lua",
      ["brahmaputra.protocol"] = "brahmaputra/protocol.lua",
      ["brahmaputra.record_batch"] = "brahmaputra/record_batch.lua",
      ["brahmaputra.router"] = "brahmaputra/router.lua",
   },
}
