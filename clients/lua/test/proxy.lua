-- A TCP proxy for the e2e suite, run as a child process (Lua has no threads
-- or fork). It forwards to the broker and, on command, severs every live
-- connection -- which is how a broker restart or an idle timeout looks to a
-- client.
--
--   lua5.4 test/proxy.lua BROKER_HOST BROKER_PORT
--
-- Prints "LISTEN_PORT CONTROL_PORT" on its first stdout line. A client of
-- the control port sends "drop\n" (answered "ok\n" once every proxied
-- connection is closed) or "quit\n".

local socket = require("socket")

local brokerHost, brokerPort = arg[1], tonumber(arg[2])

local server = assert(socket.bind("127.0.0.1", 0))
local control = assert(socket.bind("127.0.0.1", 0))
server:settimeout(0)
control:settimeout(0)
local _, listenPort = server:getsockname()
local _, controlPort = control:getsockname()
io.stdout:write(listenPort, " ", controlPort, "\n")
io.stdout:flush()

local pairs_ = {}     -- list of {a = client, b = upstream}
local controls = {}   -- control connections

local function closePair(i)
  local p = table.remove(pairs_, i)
  p.a:close()
  p.b:close()
end

local function dropAll()
  while #pairs_ > 0 do closePair(#pairs_) end
end

local function forward(from, to)
  local data, err, partial = from:receive(65536)
  local chunk = data or partial
  if chunk and #chunk > 0 then
    local sent = 0
    to:settimeout(5)
    while sent < #chunk do
      local last, serr, lastPartial = to:send(chunk, sent + 1)
      if last then sent = last else
        if serr ~= "timeout" then return false end
        sent = lastPartial or sent
      end
    end
    to:settimeout(0)
  end
  if not data and err == "closed" then return false end
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
          pairs_[#pairs_ + 1] = { a = client, b = upstream }
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
          local line = c:receive("*l")
          if line == "drop" then
            dropAll()
            c:send("ok\n")
          elseif line == "quit" then
            dropAll()
            c:send("ok\n")
            os.exit(0)
          else
            c:close()
            table.remove(controls, i)
          end
          break
        end
      end
      if not handled then
        for i, p in ipairs(pairs_) do
          if s == p.a or s == p.b then
            local ok = forward(s, s == p.a and p.b or p.a)
            if not ok then closePair(i) end
            break
          end
        end
      end
    end
  end
end
