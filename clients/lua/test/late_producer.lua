-- Child process for the e2e suite: sleeps, then produces a few records.
-- Lua has no threads, so this is how records arrive while the parent sits
-- inside one long GroupConsumer:poll().
--
--   lua5.4 test/late_producer.lua BOOTSTRAP TOPIC DELAY_MS COUNT

local socket = require("socket")
local brahmaputra = require("brahmaputra")

local bootstrap, topic = arg[1], arg[2]
local delayMs, count = tonumber(arg[3]), tonumber(arg[4])
socket.sleep(delayMs / 1000)
local producer = brahmaputra.Producer.new({ ["bootstrap.servers"] = bootstrap, ["linger.ms"] = 0 })
for i = 0, count - 1 do
  producer:send(topic, "j" .. i)
end
producer:close()
