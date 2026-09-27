-- Cross-language conformance test for the BitPacker Lua target.
-- Usage: lua5.4 test_lua.lua <generated dir> <cross_lang_test dir>
local gen_dir, dir = arg[1], arg[2]
package.path = gen_dir .. "/?.lua;" .. package.path
local B = require("bench_complex")
local E = require("edge")
local F = require("f32")

local passed, failed = 0, 0
local function check(ok, name, detail)
  if ok then
    passed = passed + 1
    print("  ok   " .. name)
  else
    failed = failed + 1
    print("  FAIL " .. name .. " (" .. tostring(detail or "") .. ")")
  end
end

local function expect(name, fn)
  local ok, res = pcall(fn)
  if not ok then check(false, name, "error: " .. tostring(res)) else check(res, name, "mismatch") end
end

local function slurp(path)
  local f = assert(io.open(path, "rb"))
  local d = f:read("a")
  f:close()
  return d
end

local function same(a, b)
  if #a ~= #b then return false end
  for i = 1, #b do
    if a[i] ~= b[i] or math.type(a[i]) ~= math.type(b[i]) then return false end
  end
  return true
end

-- ---------------- bench ----------------

local function build_world()
  local hero = B.Character.new {
    name = "TestHero", level = 99, hp = 1000, mp = 500, is_alive = true,
    position = B.Vec3.new { x = 10, y = -20, z = 30 },
    skills = { 1, 2, 3, 100 },
    inventory = { B.Item.new { id = 1, name = "Excalibur", value = 9999, weight = 15, rarity = "Legendary" } },
  }
  local guild = B.Guild.new { name = "TestGuild", description = "A test guild for cross-language", members = { hero } }
  return B.WorldState.new {
    world_id = 42, seed = "cross_lang_test", guilds = { guild },
    loot_table = { B.Item.new { id = 2, name = "HealthPotion", value = 50, weight = 1, rarity = "Common" } },
  }
end

local function verify_world(d)
  if not (d.world_id == 42 and d.seed == "cross_lang_test" and #d.guilds == 1) then return false end
  local g = d.guilds[1]
  if not (g.name == "TestGuild" and g.description == "A test guild for cross-language" and #g.members == 1) then return false end
  local h = g.members[1]
  if not (h.name == "TestHero" and h.level == 99 and h.hp == 1000 and h.mp == 500 and h.is_alive == true) then return false end
  if not (h.position.x == 10 and h.position.y == -20 and h.position.z == 30) then return false end
  if not (same(h.skills, { 1, 2, 3, 100 }) and #h.inventory == 1) then return false end
  local s = h.inventory[1]
  if not (s.id == 1 and s.name == "Excalibur" and s.value == 9999 and s.weight == 15 and s.rarity == "Legendary") then return false end
  if #d.loot_table ~= 1 then return false end
  local p = d.loot_table[1]
  return p.id == 2 and p.name == "HealthPotion" and p.value == 50 and p.weight == 1 and p.rarity == "Common"
end

local ref = slurp(dir .. "/test_data.bin")
local out = build_world():encode()
do
  local f = assert(io.open(dir .. "/test_data_lua.bin", "wb"))
  f:write(out)
  f:close()
end
check(out == ref, "bench: encode == test_data.bin", #out .. " vs " .. #ref .. " bytes")
local decoded, err = B.WorldState.decode(ref)
check(decoded ~= nil, "bench: decode test_data.bin", err)
expect("bench: decoded fields", function() return verify_world(decoded) end)
expect("bench: re-encode decoded == ref", function() return decoded:encode() == ref end)
expect("bench: round-trip own encoding", function() return verify_world(assert(B.WorldState.decode(out))) end)
expect("bench: decoded ints are integers", function() return math.type(decoded.world_id) == "integer" end)

-- ---------------- edge ----------------

local IMIN, IMAX = -2147483648, 2147483647
local LMIN, LMAX = math.mininteger, math.maxinteger
local UNICODE = "h\u{E9}llo w\u{F6}rld \u{2713} \u{65E5}\u{672C} \u{1F680}"

local function build_edge()
  return E.Edge.new {
    i_min = IMIN, i_max = IMAX, i_zero = 0, i_neg = -1,
    l_min = LMIN, l_max = LMAX, l_neg = -300,
    f = -1.25, d = 1234.5625, d_neg = -0.5, yes = true, no = false,
    empty = "", unicode = UNICODE,
    ints = { 0, -1, 1, -64, 64, IMIN, IMAX },
    longs = { 0, -1, LMAX, LMIN, 4294967296 },
    floats = { 0.0, 0.5, -2.25 }, doubles = { 0.0, 3.5, -1000000.25 },
    bools = { true, false, true }, strings = { "", "a", "\u{65E5}\u{672C}\u{8A9E}" }, no_ints = {},
    inner = E.Inner.new { big = 1099511627776, label = "inner" },
    inners = { E.Inner.new { big = -1, label = "" }, E.Inner.new { big = 0, label = "x" } },
    no_inners = {},
  }
end

local eref = slurp(dir .. "/edge/edge_ref.bin")
local eout = build_edge():encode()
check(eout == eref, "edge: encode == edge_ref.bin", #eout .. " vs " .. #eref .. " bytes")
local e
e, err = E.Edge.decode(eref)
check(e ~= nil, "edge: decode edge_ref.bin", err)
if e then
  local checks = {
    { "i_min", function() return e.i_min == IMIN and math.type(e.i_min) == "integer" end },
    { "i_max", function() return e.i_max == IMAX end },
    { "i_zero", function() return e.i_zero == 0 end },
    { "i_neg", function() return e.i_neg == -1 end },
    { "l_min", function() return e.l_min == LMIN and math.type(e.l_min) == "integer" end },
    { "l_max", function() return e.l_max == LMAX and math.type(e.l_max) == "integer" end },
    { "l_neg", function() return e.l_neg == -300 end },
    { "f", function() return e.f == -1.25 end },
    { "d", function() return e.d == 1234.5625 end },
    { "d_neg", function() return e.d_neg == -0.5 end },
    { "yes", function() return e.yes == true end },
    { "no", function() return e.no == false end },
    { "empty", function() return e.empty == "" end },
    { "unicode", function() return e.unicode == UNICODE and utf8.len(e.unicode) == 18 end },
    { "ints", function() return same(e.ints, { 0, -1, 1, -64, 64, IMIN, IMAX }) end },
    { "longs", function() return same(e.longs, { 0, -1, LMAX, LMIN, 4294967296 }) end },
    { "floats", function() return same(e.floats, { 0.0, 0.5, -2.25 }) end },
    { "doubles", function() return same(e.doubles, { 0.0, 3.5, -1000000.25 }) end },
    { "bools", function() return same(e.bools, { true, false, true }) end },
    { "strings", function() return same(e.strings, { "", "a", "\u{65E5}\u{672C}\u{8A9E}" }) end },
    { "no_ints", function() return type(e.no_ints) == "table" and #e.no_ints == 0 end },
    { "inner", function() return e.inner.big == 1099511627776 and e.inner.label == "inner" end },
    { "inners", function()
      return #e.inners == 2 and e.inners[1].big == -1 and e.inners[1].label == ""
        and e.inners[2].big == 0 and e.inners[2].label == "x"
    end },
    { "no_inners", function() return type(e.no_inners) == "table" and #e.no_inners == 0 end },
  }
  for _, c in ipairs(checks) do expect("edge: field " .. c[1], c[2]) end
  expect("edge: re-encode decoded == ref", function() return e:encode() == eref end)
  expect("edge: decoded value has class metatable", function() return getmetatable(e) == E.Edge and getmetatable(e.inner) == E.Inner end)
end

do
  local bad = eref:sub(1, 1) .. "3" .. eref:sub(3)
  local v, msg = E.Edge.decode(bad)
  check(v == nil and tostring(msg):find("version") ~= nil, "edge: wrong version rejected", msg)
end

do
  local first_bad
  for n = 0, #eref - 1 do
    local v, msg = E.Edge.decode(eref:sub(1, n))
    if v ~= nil then first_bad = "prefix of " .. n .. " bytes decoded"; break end
    if not tostring(msg):find("^bitpacker: ") then first_bad = "prefix of " .. n .. ": " .. tostring(msg); break end
  end
  check(first_bad == nil, "edge: every truncation rejected", first_bad)
end

local function rejects(...)
  local bytes = { 10, ("2.1.0"):byte(1, -1) }
  for _, part in ipairs({ ... }) do
    for _, b in ipairs(part) do bytes[#bytes + 1] = b end
  end
  local s = string.char(table.unpack(bytes))
  return E.Edge.decode(s) == nil
end
local function zeros(n) local t = {} for i = 1, n do t[i] = 0 end return t end
check(rejects(zeros(17), { 0xfe, 0xff, 0xff, 0xff, 0x0f }), "edge: huge array count rejected")
check(rejects(zeros(12), { 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 97 }), "edge: huge string length rejected")
check(rejects(zeros(13), { 1 }), "edge: negative string length rejected")
check(rejects({ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1 }), "edge: 11-byte varint rejected")
check(E.Edge.decode(42) == nil, "edge: non-string input rejected")

expect("edge: int wraps to 32 bits", function()
  return E.Edge.decode(E.Edge.new({ i_min = 2147483648 }):encode()).i_min == IMIN
end)
expect("edge: integral float accepted for long", function()
  return E.Inner.decode(E.Inner.new({ big = 3.0 }):encode()).big == 3
end)
expect("edge: fractional value for int raises", function()
  return not pcall(function() return E.Inner.new({ big = 1.5 }):encode() end)
end)
expect("edge: unknown constructor field raises", function()
  return not pcall(E.Inner.new, { bigg = 1 })
end)

-- ---------------- float32 ----------------
do
  local ref = ("0a312e302e30b06d06a82db06d808080a00100"):gsub("..", function(h) return string.char(tonumber(h, 16)) end)
  local function f32(x) return (string.unpack("f", string.pack("f", x))) end
  local out = F.F32.new({ f = 0.7, fs = { 0.29, 0.7, 16777.217 } }):encode()
  check(out == ref, "f32: 0.7, [0.29, 0.7, 16777.217] -> 7000, [2900, 7000, 167772160]",
    (out:gsub(".", function(c) return string.format("%02x", c:byte()) end)))
  expect("f32: decodes to the nearest float32 values", function()
    local v = assert(F.F32.decode(ref))
    return v.f == f32(0.7) and same(v.fs, { f32(0.29), f32(0.7), f32(16777.217) })
      and same(v.fs, { 0.28999999165534973, 0.699999988079071, 16777.216796875 })
  end)
end

print(string.format("lua: %d passed, %d failed", passed, failed))
os.exit(failed == 0 and 0 or 1)
