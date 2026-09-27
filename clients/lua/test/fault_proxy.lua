-- A frame-aware TCP proxy for the e2e suite, run as a child process (Lua
-- has no threads or fork). It forwards every request to the broker except
-- Produce, which it can answer itself with an error code for the next N
-- requests -- how a leader move or an under-replicated partition looks to
-- a producer -- and it records what each Produce asked for.
--
--   lua5.4 test/fault_proxy.lua BROKER_HOST BROKER_PORT
--
-- Prints "LISTEN_PORT CONTROL_PORT" on its first stdout line. A client of
-- the control port sends one command per line and reads one line back:
-- "fail N CODE" (answered "ok"), "stats" (answered "PRODUCES ACKS TIMEOUT")
-- or "quit".

local socket = require("socket")
local protocol = require("brahmaputra.protocol")

local brokerHost, brokerPort = arg[1], tonumber(arg[2])

local server = assert(socket.bind("127.0.0.1", 0))
local control = assert(socket.bind("127.0.0.1", 0))
server:settimeout(0)
control:settimeout(0)
local _, listenPort = server:getsockname()
local _, controlPort = control:getsockname()
io.stdout:write(listenPort, " ", controlPort, "\n")
io.stdout:flush()

local failures, failCode, produces, lastAcks, lastTimeout = 0, 0, 0, 0, 0
local pairs_ = {}     -- list of {a = client, b = upstream, buf = pending client bytes}
local controls = {}

local function sendAll(to, data)
  to:settimeout(5)
  local sent = 0
  while sent < #data do
    local last, err, partial = to:send(data, sent + 1)
    if last then
      sent = last
    elseif err == "timeout" then
      sent = partial or sent
    else
      to:settimeout(0)
      return false
    end
  end
  to:settimeout(0)
  return true
end

local function closePair(i)
  local p = table.remove(pairs_, i)
  p.a:close()
  p.b:close()
end

-- Forwards complete frames from the client, answering Produce itself while
-- failures remain.
local function fromClient(p, chunk)
  p.buf = p.buf .. chunk
  while #p.buf >= 4 do
    local length = string.unpack(">i4", p.buf)
    if #p.buf < 4 + length then break end
    local frame = string.sub(p.buf, 1, 4 + length)
    p.buf = string.sub(p.buf, 5 + length)
    local apiKey = string.unpack(">i2", frame, 5)
    local answered = false
    if apiKey == protocol.ApiKey.PRODUCE then
      local correlation, body = protocol.decodeFramePayload(string.sub(frame, 5))
      local r = protocol.Reader.body(body)
      local topic = r:string()
      local partition = r:int32()
      lastAcks = r:int32()
      lastTimeout = r:int32()
      produces = produces + 1
      if failures > 0 then
        failures = failures - 1
        local reply = protocol.Writer.body():string(topic):int32(partition):int32(failCode)
          :int64(-1):int64(-1):bytes()
        if not sendAll(p.a, protocol.encodeFrame(apiKey, correlation, "", reply)) then return false end
        answered = true
      end
    end
    if not answered and not sendAll(p.b, frame) then return false end
  end
  return true
end

local function handleControl(c)
  local line = c:receive("*l")
  if line == nil then return false end
  local words = {}
  for w in line:gmatch("%S+") do words[#words + 1] = w end
  if words[1] == "fail" then
    failures, failCode, produces = tonumber(words[2]), tonumber(words[3]), 0
    c:send("ok\n")
  elseif words[1] == "stats" then
    c:send(string.format("%d %d %d\n", produces, lastAcks, lastTimeout))
  elseif words[1] == "quit" then
    c:send("ok\n")
    os.exit(0)
  end
  return true
end

while true do
  local watch = { server, control }
  for _, c in ipairs(controls) do watch[#watch + 1] = c end
  for _, p in ipairs(pairs_) do
    watch[#watch + 1] = p.a
    watch[#watch + 1] = p.b
  end
  local readable = socket.select(watch, nil, 0.05)
  for _, s in ipairs(readable) do
    if s == server then
      local client = server:accept()
      if client then
        local upstream = socket.tcp()
        upstream:settimeout(2)
        if upstream:connect(brokerHost, brokerPort) then
          client:settimeout(0)
          upstream:settimeout(0)
          client:setoption("tcp-nodelay", true)
          upstream:setoption("tcp-nodelay", true)
          pairs_[#pairs_ + 1] = { a = client, b = upstream, buf = "" }
        else
          client:close()
        end
      end
    elseif s == control then
      local c = control:accept()
      if c then
        c:settimeout(1)
        controls[#controls + 1] = c
      end
    else
      local handled = false
      for i, c in ipairs(controls) do
        if c == s then
          handled = true
          if not handleControl(c) then
            c:close()
            table.remove(controls, i)
          end
          break
        end
      end
      if not handled then
        for i, p in ipairs(pairs_) do
          if s == p.a or s == p.b then
            local data, err, partial = s:receive(65536)
            local chunk = data or partial
            local ok = true
            if chunk and #chunk > 0 then
              if s == p.a then ok = fromClient(p, chunk) else ok = sendAll(p.a, chunk) end
            end
            if not ok or (not data and err == "closed") then closePair(i) end
            break
          end
        end
      end
    end
  end
end
