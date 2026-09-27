-- End-to-end suite for the Lua driver against a live broker. A port of
-- clients/go/cmd/manualtest/main.go with the same sections and checks.
--
--   brahmaputra-server --data-dir ./data --default-partitions 4
--   ./test.sh 127.0.0.1 9092        (sets LUA_PATH; or run this file directly
--                                    with LUA_PATH covering clients/lua)
--
-- Every check asserts a property of the system, not that a function ran:
-- records come back byte-identical, keys pin partitions, headers survive,
-- offsets are contiguous. Exits non-zero on any failure.
--
-- Lua has no threads or fork, so the two places the Go suite uses a
-- goroutine run a child process instead: the connection-dropping TCP proxy
-- (test/proxy.lua) and the producer that writes while the parent is inside
-- one long poll (test/late_producer.lua).

local socket = require("socket")
local brahmaputra = require("brahmaputra")
local errors = brahmaputra.errors
local Producer, Consumer, GroupConsumer = brahmaputra.Producer, brahmaputra.Consumer, brahmaputra.GroupConsumer
local header = brahmaputra.header

local passed, failed = 0, 0

local function check(name, ok, detail)
  if ok then
    passed = passed + 1
    print("  ok   " .. name)
    return
  end
  failed = failed + 1
  if detail ~= nil and detail ~= "" then
    print("  FAIL " .. name .. ": " .. tostring(detail))
  else
    print("  FAIL " .. name)
  end
end

local function section(title) print("\n" .. title) end

local uniqueCounter = 0
local function unique(prefix)
  uniqueCounter = uniqueCounter + 1
  return string.format("%s-%d", prefix, (math.floor(socket.gettime() * 1e6) + uniqueCounter) % 1000000000)
end

local function nowMs() return math.floor(socket.gettime() * 1000) end
local function sleepMs(ms) socket.sleep(ms / 1000) end

local function msg(err)
  if err == nil then return "nil" end
  return errors.message(err)
end

local function show(v)
  if v == nil then return "nil" end
  return string.format("%q", v)
end

local host = arg[1] or "127.0.0.1"
local port = arg[2] or "9092"
local bootstrap = host .. ":" .. port

-- Child processes run with the same interpreter and LUA_PATH.
local interpreter = arg[-1] or "lua5.4"
local testDir = (arg[0]:match("^(.*)/[^/]*$") or ".")

local function merge(base, overrides)
  local out = {}
  for k, v in pairs(base) do out[k] = v end
  for k, v in pairs(overrides or {}) do out[k] = v end
  return out
end

local function producerConfig(overrides)
  return merge({ ["bootstrap.servers"] = bootstrap, ["linger.ms"] = 0 }, overrides)
end
local function consumerConfig() return { ["bootstrap.servers"] = bootstrap } end
local function groupConfig(groupId, overrides)
  return merge({ ["bootstrap.servers"] = bootstrap, ["group.id"] = groupId, ["enable.auto.commit"] = false }, overrides)
end

local function main()
  section("connection and metadata")
  do
    local consumer = Consumer.new(consumerConfig())
    local ok, versions, brokerVersion = pcall(function() return consumer:router():seed():apiVersions() end)
    check("ApiVersions answers", ok and #versions > 0, ok and "" or msg(versions))
    check("broker reports a version", ok and brokerVersion ~= nil and brokerVersion ~= "", tostring(brokerVersion))
    local metadata = consumer:router():metadata({}, true)
    check("metadata lists brokers", #metadata.brokers >= 1, #metadata.brokers .. " brokers")
    consumer:close()
  end

  section("produce and consume round trip")
  local topic = unique("lua-roundtrip")
  local payloads = {}
  for i = 0, 49 do payloads[#payloads + 1] = "record-" .. i end
  do
    local producer = Producer.new(producerConfig())
    for _, payload in ipairs(payloads) do producer:send(topic, payload, nil, { partition = 0 }) end
    producer:flush()
    producer:close()
  end
  do
    local consumer = Consumer.new(consumerConfig())
    local got = consumer:fetch(topic, 0, 0, 500)
    check("every record comes back", #got == #payloads, "got " .. #got)
    local identical = #got == #payloads
    for i = 1, #got do
      if not identical then break end
      if got[i].value ~= payloads[i] or got[i].offset ~= i - 1 or math.type(got[i].offset) ~= "integer" then
        identical = false
      end
    end
    check("values byte-identical and offsets contiguous", identical)
    consumer:close()
  end

  section("compression codecs")
  -- Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
  -- brahmaputra.registerCodec().
  for _, codec in ipairs({ "none", "gzip" }) do
    local codecTopic = unique("lua-" .. codec)
    local body = string.rep("the same line over and over. ", 40)
    local producer = Producer.new(producerConfig({ ["compression.type"] = codec }))
    for i = 0, 19 do
      producer:send(codecTopic, body .. string.char(string.byte("0") + i % 10), nil, { partition = 0 })
    end
    producer:flush()
    producer:close()

    local consumer = Consumer.new(consumerConfig())
    local got = consumer:fetch(codecTopic, 0, 0, 500)
    check(codec .. ": round trips", #got == 20 and got[1].value:sub(1, #body) == body,
      "got " .. #got .. " records")
    consumer:close()
  end

  section("keys, partitioning and ordering")
  do
    local keyTopic = unique("lua-keys")
    local producer = Producer.new(producerConfig())
    local partitions = producer:router():partitions(keyTopic)
    for i = 0, 29 do producer:send(keyTopic, "v" .. i, "user-7") end
    producer:flush()
    producer:close()

    local target = brahmaputra.partitionForKey("user-7", partitions)
    local consumer = Consumer.new(consumerConfig())
    local onTarget = consumer:fetch(keyTopic, target, 0, 500)
    check("a key pins every record to one partition", #onTarget == 30,
      string.format("partition %d holds %d of 30", target, #onTarget))
    local ordered = #onTarget == 30
    for i = 1, #onTarget do
      if onTarget[i].value ~= "v" .. (i - 1) then ordered = false end
    end
    check("per-key order is preserved", ordered)
    local strays = 0
    for _, partition in ipairs(partitions) do
      if partition ~= target then strays = strays + #consumer:fetch(keyTopic, partition, 0, 200) end
    end
    check("no keyed record landed elsewhere", strays == 0, strays .. " strays")
    consumer:close()
  end

  section("murmur2 agrees with the broker's partitioner")
  check('murmur2("") is stable', brahmaputra.murmur2("") == 275646681, tostring(brahmaputra.murmur2("")))
  check("murmur2 is deterministic", brahmaputra.murmur2("user-7") == brahmaputra.murmur2("user-7"))
  check("different keys hash differently", brahmaputra.murmur2("user-7") ~= brahmaputra.murmur2("user-8"))

  section("record headers and timestamps")
  do
    local headerTopic = unique("lua-headers")
    local before = nowMs() - 1000
    local producer = Producer.new(producerConfig())
    producer:send(headerTopic, "annotated", nil, { partition = 0, headers = {
      header("trace-id", "abc-123"),
      header("content-type", "application/json"),
      header("tombstone-reason", nil),
    } })
    producer:send(headerTopic, "plain", nil, { partition = 0 })
    producer:flush()
    producer:close()
    local after = nowMs() + 1000

    local consumer = Consumer.new(consumerConfig())
    local got = consumer:fetch(headerTopic, 0, 0, 500)
    check("both records arrive", #got == 2, "got " .. #got)
    if #got == 2 then
      local annotated, plain = got[1], got[2]
      check("headers survive the round trip", #annotated.headers == 3, #annotated.headers .. " headers")
      check("header values are exact", brahmaputra.headerValue(annotated, "trace-id") == "abc-123")
      check("a null header value stays null", #annotated.headers == 3 and annotated.headers[3].value == nil)
      check("a record with no headers gains none from its batch", #plain.headers == 0,
        #plain.headers .. " headers")
      local inWindow = true
      for _, r in ipairs(got) do
        if r.timestamp < before or r.timestamp > after then inWindow = false end
      end
      check("timestamps are real wall-clock values", inWindow,
        string.format("%d,%d outside %d..%d", got[1].timestamp, got[2].timestamp, before, after))
    end
    consumer:close()
  end

  section("tombstones")
  do
    local tombTopic = unique("lua-tombstones")
    local producer = Producer.new(producerConfig())
    producer:send(tombTopic, "set", "k1", { partition = 0 })
    producer:send(tombTopic, "", "k2", { partition = 0 })
    -- A nil value is a deletion, and must stay distinguishable from the
    -- empty value above all the way through the round trip.
    producer:send(tombTopic, nil, "k3", { partition = 0 })
    producer:flush()
    producer:close()

    local consumer = Consumer.new(consumerConfig())
    local got = consumer:fetch(tombTopic, 0, 0, 500)
    check("all three records arrive", #got == 3, "got " .. #got)
    if #got == 3 then
      check("an ordinary value round-trips", got[1].value == "set")
      check("an empty value is empty, not null", got[2].value == "", show(got[2].value))
      check("a tombstone arrives as a null value", got[3].value == nil, show(got[3].value))
    end
    consumer:close()
  end

  section("offsets")
  do
    local consumer = Consumer.new(consumerConfig())
    local earliest = consumer:listOffsets(topic, 0, brahmaputra.EARLIEST)
    local latest = consumer:listOffsets(topic, 0, brahmaputra.LATEST)
    check("earliest is 0 on a fresh topic", earliest == 0, tostring(earliest))
    check("latest equals the record count", latest == 50, tostring(latest))
    consumer:close()
  end

  section("acks")
  for _, acks in ipairs({ 0, 1, -1 }) do
    local acksTopic = unique("lua-acks" .. acks)
    local producer = Producer.new(producerConfig({ acks = acks }))
    producer:send(acksTopic, "durable", nil, { partition = 0 })
    producer:flush()
    producer:close()
    sleepMs(400)

    local consumer = Consumer.new(consumerConfig())
    local got = consumer:fetch(acksTopic, 0, 0, 500)
    check("acks=" .. acks .. " stores the record", #got == 1, "got " .. #got)
    consumer:close()
  end

  section("consumer group: assignment, commit, resume")
  do
    local groupTopic = unique("lua-group")
    local groupId = unique("lua-billing")
    local producer = Producer.new(producerConfig())
    for i = 0, 39 do producer:send(groupTopic, "g" .. i) end
    producer:flush()
    producer:close()

    local consumer = GroupConsumer.new(groupConfig(groupId))
    consumer:subscribe({ groupTopic })
    local seen = {}
    local deadline = nowMs() + 30000
    while #seen < 40 and nowMs() < deadline do
      for _, r in ipairs(consumer:poll(500)) do seen[#seen + 1] = r end
    end
    check("the group consumes every record", #seen == 40, "got " .. #seen)
    local distinct, count = {}, 0
    for _, r in ipairs(seen) do
      local k = r.partition .. "-" .. r.offset
      if not distinct[k] then
        distinct[k] = true
        count = count + 1
      end
    end
    check("no record is delivered twice", count == #seen)

    consumer:commit()
    local total = 0
    for _, tp in ipairs(consumer:committed()) do total = total + tp.offset end
    check("commit records a position", total == 40, tostring(total))
    consumer:close()

    -- A second consumer in the same group must resume, not replay.
    local rejoined = GroupConsumer.new(groupConfig(groupId))
    rejoined:subscribe({ groupTopic })
    local replayed = 0
    local untilMs = nowMs() + 5000
    while nowMs() < untilMs do replayed = replayed + #rejoined:poll(300) end
    check("a rejoining group resumes from its commit", replayed == 0,
      "replayed " .. replayed .. " records it had already committed")
    rejoined:close()
  end

  section("auto.offset.reset")
  do
    local resetTopic = unique("lua-reset")
    local producer = Producer.new(producerConfig())
    for i = 0, 9 do producer:send(resetTopic, "r" .. i) end
    producer:flush()
    producer:close()

    local consumer = GroupConsumer.new(groupConfig(unique("lua-latest"), { ["auto.offset.reset"] = "latest" }))
    consumer:subscribe({ resetTopic })
    local skipped = 0
    local untilMs = nowMs() + 4000
    while nowMs() < untilMs do skipped = skipped + #consumer:poll(300) end
    check("latest skips records produced before the group existed", skipped == 0, "saw " .. skipped)
    consumer:close()

    local strict = GroupConsumer.new(groupConfig(unique("lua-none"), { ["auto.offset.reset"] = "none" }))
    strict:subscribe({ resetTopic })
    local raised = false
    untilMs = nowMs() + 5000
    while nowMs() < untilMs and not raised do
      local ok, err = pcall(strict.poll, strict, 300)
      if not ok and errors.is(err, "NoOffsetForPartitionError") then raised = true end
    end
    check("none refuses to guess a position", raised)
    strict:close()
  end

  section("assignors")
  for _, assignor in ipairs({ "range", "roundrobin", "sticky" }) do
    local assignorTopic = unique("lua-" .. assignor)
    local producer = Producer.new(producerConfig())
    for i = 0, 19 do producer:send(assignorTopic, "a" .. i) end
    producer:flush()
    producer:close()

    local consumer = GroupConsumer.new(groupConfig(unique("lua-grp-" .. assignor),
      { ["partition.assignment.strategy"] = assignor }))
    consumer:subscribe({ assignorTopic })
    local collected = 0
    local deadline = nowMs() + 20000
    while collected < 20 and nowMs() < deadline do collected = collected + #consumer:poll(500) end
    check(assignor .. ": consumes every record", collected == 20, "got " .. collected)
    consumer:close()
  end

  section("bounded client buffer")
  do
    local bufferTopic = unique("lua-buffer")
    local producer = Producer.new({
      ["bootstrap.servers"] = bootstrap,
      ["linger.ms"] = 10000, -- never flush on time during this check
      ["buffer.memory"] = 2048,
      ["max.block.ms"] = 300,
    })
    local blocked = false
    local value = string.rep("x", 256)
    for _ = 1, 500 do
      local ok, err = pcall(producer.send, producer, bufferTopic, value, nil, { partition = 0 })
      if not ok then
        blocked = errors.is(err, "BufferFullError") and err.message:find("buffer full", 1, true) ~= nil
        break
      end
    end
    check("a full buffer blocks and then reports", blocked)
    pcall(producer.close, producer)
  end

  section("wire edge cases")
  do
    local edgeTopic = unique("lua-edge")
    local producer = Producer.new(producerConfig())
    local chunks = {}
    for i = 0, (1 << 20) - 1 do chunks[#chunks + 1] = string.char((i * 7) & 0xff) end
    local large = table.concat(chunks)
    local unicodeKey = "ключ-✓-🔑"
    local unicodeValue = "значение — 数据 — 🚀"
    producer:send(edgeTopic, large, nil, { partition = 0 })
    producer:send(edgeTopic, unicodeValue, unicodeKey, { partition = 0, headers = { header("ünïcødé-🏷", "✓") } })
    -- An empty key and an empty header value are values, not nulls.
    producer:send(edgeTopic, "empty-key", "", { partition = 0, headers = { header("empty", ""), header("null", nil) } })
    producer:send(edgeTopic, "null-key", nil, { partition = 0 })
    producer:close()

    local consumer = Consumer.new(consumerConfig())
    local got = {}
    local offset = 0
    while #got < 4 do
      local ok, batch = pcall(consumer.fetch, consumer, edgeTopic, 0, offset, 500)
      if not ok or #batch == 0 then break end
      for _, r in ipairs(batch) do got[#got + 1] = r end
      offset = batch[#batch].offset + 1
    end
    check("edge records all arrive", #got == 4, "got " .. #got)
    if #got == 4 then
      check("a 1 MiB value round-trips byte-identical", got[1].value == large, #(got[1].value or "") .. " bytes")
      check("unicode key, value and header key round-trip",
        got[2].key == unicodeKey and got[2].value == unicodeValue
          and #got[2].headers == 1 and got[2].headers[1].key == "ünïcødé-🏷")
      check("an empty key stays empty, not null", got[3].key == "", show(got[3].key))
      local h = got[3].headers
      check("an empty header value stays empty, not null",
        #h == 2 and h[1].value == "" and h[2].value == nil,
        #h .. " headers: " .. show(h[1] and h[1].value) .. ", " .. show(h[2] and h[2].value))
      check("a null key stays null", got[4].key == nil, show(got[4].key))
    end
    consumer:close()
  end

  section("ordering under linger flushes")
  do
    local orderTopic = unique("lua-order")
    local producer = Producer.new({ ["bootstrap.servers"] = bootstrap, ["linger.ms"] = 1, ["batch.size"] = 256 })
    local total = 5000
    for i = 0, total - 1 do producer:send(orderTopic, tostring(i), nil, { partition = 0 }) end
    producer:close()
    local consumer = Consumer.new(consumerConfig())
    local values = {}
    local offset = 0
    while #values < total do
      local ok, batch = pcall(consumer.fetch, consumer, orderTopic, 0, offset, 500)
      if not ok or #batch == 0 then break end
      for _, r in ipairs(batch) do values[#values + 1] = tonumber(r.value) end
      offset = batch[#batch].offset + 1
    end
    local inversions = 0
    for i = 2, #values do
      if values[i] < values[i - 1] then inversions = inversions + 1 end
    end
    check("every record of a partition arrives", #values == total, "got " .. #values)
    check("a partition's records keep send order", inversions == 0, inversions .. " inversions")
    consumer:close()
  end

  section("background flush failures are reported")
  do
    local producer = Producer.new({ ["bootstrap.servers"] = bootstrap, ["linger.ms"] = 20 })
    -- Partition 999 does not exist. Lua has no ticker thread, so the
    -- "background" flush is the linger-expired one poll() performs; its
    -- failure must not vanish, and must not be raised from poll() either.
    local sendOk, sendErr = pcall(producer.send, producer, unique("lua-bgfail"), "lost", nil, { partition = 999 })
    sleepMs(300)
    local pollOk, pollErr = pcall(producer.poll, producer, 0)
    local flushOk, flushErr = pcall(producer.flush, producer)
    check("a failed linger flush surfaces on the next Flush", sendOk and pollOk and not flushOk,
      string.format("send=%s poll=%s flush=%s", sendOk and "nil" or msg(sendErr),
        pollOk and "nil" or msg(pollErr), flushOk and "nil" or msg(flushErr)))
    local started = nowMs()
    pcall(producer.close, producer)
    check("Close returns after a failed flush", nowMs() - started < 5000, "hung")
  end

  section("connection failures")
  do
    -- A broker that accepts and never answers must cost an error, not a
    -- process blocked forever. The kernel completes the handshake from the
    -- listen backlog, so this socket never needs to accept().
    local silent = assert(socket.bind("127.0.0.1", 0))
    local silentHost, silentPort = silent:getsockname()
    local conn = brahmaputra.Connection.open(silentHost, tonumber(silentPort), { clientId = "lua-test", connectTimeoutMs = 1000 })
    conn:setRequestTimeout(300)
    local started = nowMs()
    local ok, err = pcall(conn.apiVersions, conn)
    check("a request to an unresponsive broker times out",
      not ok and errors.is(err, "TimeoutError") and nowMs() - started < 3000, ok and "no error" or msg(err))
    check("a timed-out connection is not reused", conn.broken == true and conn:isBroken())
    conn:close()
    silent:close()

    -- A connection the broker drops is redialled, not kept forever.
    local proxyCmd = string.format("%s %s/proxy.lua %s %s", interpreter, testDir, host, port)
    local proxy = assert(io.popen(proxyCmd, "r"))
    local line = proxy:read("l")
    local proxyPort, controlPort = line:match("^(%d+) (%d+)$")
    local control = assert(socket.connect("127.0.0.1", tonumber(controlPort)))
    control:settimeout(5)
    local function dropAll()
      control:send("drop\n")
      control:receive("*l")
      sleepMs(100)
    end
    local proxyAddress = "127.0.0.1:" .. proxyPort

    local dropTopic = unique("lua-drop")
    local producer = Producer.new({ ["bootstrap.servers"] = proxyAddress, ["linger.ms"] = 0 })
    producer:send(dropTopic, "before", nil, { partition = 0 })
    dropAll()
    local recovered = "not attempted"
    for _ = 1, 3 do
      local sok, serr = pcall(producer.send, producer, dropTopic, "after", nil, { partition = 0 })
      if sok then
        recovered = nil
        break
      end
      recovered = msg(serr)
    end
    check("a producer recovers after its connection drops", recovered == nil, recovered)
    pcall(producer.close, producer)

    local consumer = Consumer.new({ ["bootstrap.servers"] = proxyAddress })
    consumer:fetch(dropTopic, 0, 0, 100)
    dropAll()
    local fetchError, fetched = "not attempted", {}
    for _ = 1, 3 do
      local fok, result = pcall(consumer.fetch, consumer, dropTopic, 0, 0, 100)
      if fok then
        fetchError, fetched = nil, result
        break
      end
      fetchError = msg(result)
    end
    check("a consumer recovers after its connection drops", fetchError == nil and #fetched >= 1, fetchError)
    consumer:close()
    control:send("quit\n")
    control:receive("*l")
    control:close()
    proxy:close()
  end

  section("consumer group: max.poll.interval and rejoin")
  do
    local slowTopic = unique("lua-slow")
    local producer = Producer.new(producerConfig())
    for i = 0, 9 do producer:send(slowTopic, "s" .. i) end
    local consumer = GroupConsumer.new(groupConfig(unique("lua-slow-grp"), { ["max.poll.interval.ms"] = 1500 }))
    consumer:subscribe({ slowTopic })
    local first, second = 0, 0
    local deadline = nowMs() + 15000
    while first < 10 and nowMs() < deadline do
      local ok, records = pcall(consumer.poll, consumer, 300)
      if not ok then break end
      first = first + #records
    end
    consumer:commit()
    -- Stall past max.poll.interval.ms: the member leaves the group.
    sleepMs(2500)
    for i = 10, 19 do producer:send(slowTopic, "s" .. i) end
    producer:close()
    local pollError
    deadline = nowMs() + 15000
    while second < 10 and nowMs() < deadline do
      local ok, records = pcall(consumer.poll, consumer, 300)
      if not ok then
        pollError = records
        break
      end
      second = second + #records
    end
    check("a member that stalled rejoins on its next poll", first == 10 and second == 10 and pollError == nil,
      string.format("first=%d second=%d err=%s", first, second, msg(pollError)))
    consumer:close()
  end

  section("consumer group: time inside poll does not count against max.poll.interval")
  do
    local joinTopic = unique("lua-inpoll")
    local producer = Producer.new(producerConfig())
    producer:router():partitions(joinTopic)
    producer:close()
    -- Far shorter than the poll below, which spends ~1s joining (the
    -- broker's initial rebalance delay) and then waits for data.
    local consumer = GroupConsumer.new(groupConfig(unique("lua-inpoll-grp"), { ["max.poll.interval.ms"] = 600 }))
    consumer:subscribe({ joinTopic })
    -- Lua has no threads: a child process produces while this one polls.
    local child = assert(io.popen(string.format("%s %s/late_producer.lua %s %s 2000 10 2>&1",
      interpreter, testDir, bootstrap, joinTopic), "r"))
    -- One long poll: it joins, then waits for the records above.
    local pollOk, got = pcall(consumer.poll, consumer, 4000)
    -- Committed straight away, before another poll could quietly rejoin:
    -- this fails if the member left the group mid-poll.
    local commitOk, commitErr = pcall(consumer.commit, consumer)
    local count = pollOk and #got or 0
    check("a member is still in its group after a long poll", pollOk and count > 0 and commitOk,
      string.format("got=%d poll=%s commit=%s", count, pollOk and "nil" or msg(got),
        commitOk and "nil" or msg(commitErr)))
    local childOutput = child:read("a")
    child:close()
    if childOutput ~= "" then io.write("  (late producer: " .. childOutput .. ")\n") end
    consumer:close()
  end
end

local ok, err = xpcall(main, debug.traceback)
if not ok then
  print("  FATAL " .. msg(err))
  os.exit(2)
end
print(string.format("\n%d passed, %d failed", passed, failed))
os.exit(failed > 0 and 1 or 0)
