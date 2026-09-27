package main

// Lua 5.4 target (also runs on 5.3): one module per schema, <schema>.lua,
// returning a table with one class (a metatable) per schema class. Uses
// native 64-bit integers and bitwise operators; no C modules. See
// docs/lua.md.

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func init() { registerGenerator(genLua, "lua") }

var luaKeywords = map[string]bool{
	"and": true, "break": true, "do": true, "else": true, "elseif": true, "end": true,
	"false": true, "for": true, "function": true, "goto": true, "if": true, "in": true,
	"local": true, "nil": true, "not": true, "or": true, "repeat": true, "return": true,
	"then": true, "true": true, "until": true, "while": true,
}

var luaIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// luaIndex renders obj.name, or obj["name"] when name is not a valid Lua
// identifier.
func luaIndex(obj, name string) string {
	if luaIdentRe.MatchString(name) && !luaKeywords[name] {
		return obj + "." + name
	}
	return fmt.Sprintf("%s[%q]", obj, name)
}

// luaKey renders a table-constructor key.
func luaKey(name string) string {
	if luaIdentRe.MatchString(name) && !luaKeywords[name] {
		return name
	}
	return fmt.Sprintf("[%q]", name)
}

func luaIsScalar(t string) bool {
	switch t {
	case "int", "long", "float", "double", "bool", "string":
		return true
	}
	return false
}

func luaMinSize(classes []Class, name string, seen map[string]bool) int {
	if seen[name] {
		return 0
	}
	seen[name] = true
	defer delete(seen, name)
	n := 0
	for _, c := range classes {
		if c.Name != name {
			continue
		}
		for _, f := range c.Fields {
			if f.IsArray || luaIsScalar(f.Type) {
				n++
			} else {
				n += luaMinSize(classes, f.Type, seen)
			}
		}
	}
	return n
}

const luaRuntime = `local M = { SCHEMA_VERSION = %q }
local VERSION = M.SCHEMA_VERSION

assert(math.type ~= nil and math.maxinteger == 0x7FFFFFFFFFFFFFFF,
  "BitPacker: needs Lua 5.3+ with 64-bit integers")

local sbyte, schar, ssub, concat = string.byte, string.char, string.sub, table.concat
local tointeger, floor, ceil = math.tointeger, math.floor, math.ceil
local type, error, setmetatable, pcall, pairs = type, error, setmetatable, pcall, pairs
local MAX_DEPTH = 256

local function fail(msg) error("bitpacker: " .. msg, 0) end

-- ---- encoding (internal); a writer is { n = count, [1..n] = pieces } ----

local function put_uvarint(w, v)
  local n = w.n
  while (v & ~0x7F) ~= 0 do -- unsigned: v >= 0x80
    n = n + 1
    w[n] = schar((v & 0x7F) | 0x80)
    v = v >> 7 -- logical shift
  end
  n = n + 1
  w[n] = schar(v)
  w.n = n
end

local function toint(v, what)
  if v == nil then return 0 end
  local i = tointeger(v)
  if i == nil then error(what .. " must be an integer, got " .. tostring(v), 3) end
  return i
end

-- int: wrapped to 32 bits (like the fixed-width targets), zigzag, varint.
local function put_int(w, v)
  local u = toint(v, "int") & 0xFFFFFFFF
  local sign = (u & 0x80000000) ~= 0 and 0xFFFFFFFF or 0
  put_uvarint(w, ((u << 1) & 0xFFFFFFFF) ~ sign)
end

-- long: zigzag on 64 bits; v >> 63 is a logical shift, so negate it.
local function put_long(w, v)
  v = toint(v, "long")
  put_uvarint(w, (v << 1) ~ -(v >> 63))
end

-- Rounds a number to the nearest single-precision value.
local spack, sunpack = string.pack, string.unpack
local function f32(x) return (sunpack("f", spack("f", x))) end

-- NaN encodes as 0; values beyond the long range saturate.
local function put_scaled(w, x)
  local n
  if x ~= x then n = 0
  elseif x >= 0x1p63 then n = math.maxinteger
  elseif x < -0x1p63 then n = math.mininteger
  else n = tointeger(x >= 0 and floor(x) or ceil(x)) end
  put_long(w, n)
end

-- float: single precision, like the fixed-width targets: round v to float32,
-- multiply by 10000 rounding the product to float32 (a float32 product is
-- exact in a double, so one rounding equals a float32 multiply), then trunc.
local function put_f32(w, v) put_scaled(w, f32(f32(v or 0.0) * 10000.0)) end

-- double: trunc(v * 10000) as a long, in double precision.
local function put_fixed(w, v) put_scaled(w, (v or 0) * 10000.0) end

local function put_bool(w, v)
  local n = w.n + 1
  w[n] = v and "\1" or "\0"
  w.n = n
end

-- Strings are byte strings and go on the wire unchanged (normally UTF-8).
local function put_string(w, s)
  if s == nil then s = "" end
  if type(s) ~= "string" then error("string field must be a string, got " .. type(s), 3) end
  put_long(w, #s)
  local n = w.n + 1
  w[n] = s
  w.n = n
end

local function put_count(w, a)
  local n = #a
  if n > 0x7FFFFFFF then error("array longer than 2^31-1", 3) end
  put_int(w, n)
  return n
end

-- ---- decoding (internal); a reader is { d = data, p = next index, n = #data } ----

local function get_uvarint(r)
  local d, p, len = r.d, r.p, r.n
  local v, shift = 0, 0
  for i = 0, 9 do
    if p > len then fail("truncated input") end
    local b = sbyte(d, p)
    p = p + 1
    if i == 9 and b > 1 then fail("varint overflows 64 bits") end
    v = v | ((b & 0x7F) << shift)
    if b < 0x80 then
      r.p = p
      return v
    end
    shift = shift + 7
  end
  fail("varint longer than 10 bytes")
end

local function get_int(r)
  local u = get_uvarint(r) & 0xFFFFFFFF
  return (u >> 1) ~ -(u & 1)
end

local function get_long(r)
  local u = get_uvarint(r)
  return (u >> 1) ~ -(u & 1)
end

local function get_fixed(r) return get_long(r) / 10000 end

-- float32(n) / 10000, rounded to float32.
local function get_f32(r) return f32(f32(get_long(r) + 0.0) / 10000.0) end

local function get_bool(r)
  local p = r.p
  if p > r.n then fail("truncated input") end
  r.p = p + 1
  return sbyte(r.d, p) ~= 0
end

local function get_string(r)
  local n = get_long(r)
  if n < 0 then fail("negative string length " .. n) end
  local p = r.p
  if n > r.n - p + 1 then fail("truncated input") end
  r.p = p + n
  return ssub(r.d, p, p + n - 1)
end

-- Array count; each element needs at least min bytes, so a count the rest
-- of the input cannot hold is rejected before looping.
local function get_count(r, min)
  local n = get_int(r)
  if n < 0 then fail("negative array count " .. n) end
  local left = r.n - r.p + 1
  if min > 0 then
    if n > left // min then fail("truncated input") end
  elseif n > left + (1 << 20) then
    fail("array count " .. n .. " too large")
  end
  return n
end

local function check_version(r)
  local got = get_string(r)
  if got ~= VERSION then
    fail(string.format("version mismatch: got %%q, want %%q", got, VERSION))
  end
end

local enc, dec = {}, {}

local function define(name, fields)
  local C = { FIELDS = fields }
  C.__index = C
  C.__name = name
  local known = {}
  for _, f in pairs(fields) do known[f] = true end

  -- Encodes with the version prefix; returns a byte string.
  function C:encode()
    local w = { n = 0 }
    put_string(w, VERSION)
    enc[name](w, self)
    return concat(w, "", 1, w.n)
  end

  -- Appends the body (no version prefix) to a writer { n = 0 }.
  function C:encode_to(w)
    enc[name](w, self)
    return w
  end

  -- Decodes a version-prefixed message: returns the value, or nil and an
  -- error message. Never raises for malformed input.
  function C.decode(data)
    if type(data) ~= "string" then return nil, "bitpacker: decode expects a string" end
    local ok, v = pcall(function()
      local r = { d = data, p = 1, n = #data }
      check_version(r)
      return dec[name](r, 0)
    end)
    if ok then return v end
    return nil, v
  end

  C._check = function(t)
    for k in pairs(t) do
      if not known[k] then error(name .. ": unknown field '" .. tostring(k) .. "'", 3) end
    end
  end

  M[name] = C
  return C
end
`

func luaEnc(t, expr string) string {
	switch t {
	case "int":
		return "put_int(w, " + expr + ")"
	case "long":
		return "put_long(w, " + expr + ")"
	case "float":
		return "put_f32(w, " + expr + ")"
	case "double":
		return "put_fixed(w, " + expr + ")"
	case "bool":
		return "put_bool(w, " + expr + ")"
	case "string":
		return "put_string(w, " + expr + ")"
	}
	return fmt.Sprintf("enc[%q](w, %s or %s.new())", t, expr, luaIndex("M", t))
}

func luaDec(t string) string {
	switch t {
	case "int":
		return "get_int(r)"
	case "long":
		return "get_long(r)"
	case "float":
		return "get_f32(r)"
	case "double":
		return "get_fixed(r)"
	case "bool":
		return "get_bool(r)"
	case "string":
		return "get_string(r)"
	}
	return fmt.Sprintf("dec[%q](r, depth + 1)", t)
}

func genLua(classes []Class, cfg GeneratorConfig) error {
	var s strings.Builder
	fmt.Fprintf(&s, "-- Generated by BitPacker from %s.buff. Do not edit.\n", cfg.InputFileName)
	fmt.Fprintf(&s, "-- Lua 5.3+/5.4, 64-bit integers.\n\n")
	fmt.Fprintf(&s, luaRuntime, cfg.Version)

	for _, c := range classes {
		cls := luaIndex("M", c.Name)
		quoted := []string{}
		for _, f := range c.Fields {
			quoted = append(quoted, fmt.Sprintf("%q", f.Name))
		}
		fmt.Fprintf(&s, "\n-- %s\ndefine(%q, { %s })\n\n", c.Name, c.Name, strings.Join(quoted, ", "))

		// new
		fmt.Fprintf(&s, "function %s.new(t)\n  t = t or {}\n  %s._check(t)\n  return setmetatable({\n", cls, cls)
		for _, f := range c.Fields {
			var d string
			switch {
			case f.IsArray:
				d = "{}"
			case f.Type == "int" || f.Type == "long":
				d = "0"
			case f.Type == "float" || f.Type == "double":
				d = "0.0"
			case f.Type == "bool":
				d = "false"
			case f.Type == "string":
				d = `""`
			default:
				d = luaIndex("M", f.Type) + ".new()"
			}
			src := luaIndex("t", f.Name)
			if f.Type == "bool" && !f.IsArray {
				fmt.Fprintf(&s, "    %s = not not %s,\n", luaKey(f.Name), src)
			} else {
				fmt.Fprintf(&s, "    %s = %s or %s,\n", luaKey(f.Name), src, d)
			}
		}
		fmt.Fprintf(&s, "  }, %s)\nend\n\n", cls)

		// enc
		fmt.Fprintf(&s, "enc[%q] = function(w, o)\n", c.Name)
		for _, f := range c.Fields {
			acc := luaIndex("o", f.Name)
			if f.IsArray {
				fmt.Fprintf(&s, "  do\n    local a = %s or {}\n    for i = 1, put_count(w, a) do %s end\n  end\n", acc, luaEnc(f.Type, "a[i]"))
			} else {
				fmt.Fprintf(&s, "  %s\n", luaEnc(f.Type, acc))
			}
		}
		s.WriteString("end\n\n")

		// dec
		fmt.Fprintf(&s, "dec[%q] = function(r, depth)\n", c.Name)
		s.WriteString("  if depth > MAX_DEPTH then fail(\"nesting too deep\") end\n")
		fmt.Fprintf(&s, "  local o = setmetatable({}, %s)\n", cls)
		for _, f := range c.Fields {
			acc := luaIndex("o", f.Name)
			if f.IsArray {
				minSz := 1
				if !luaIsScalar(f.Type) {
					minSz = luaMinSize(classes, f.Type, map[string]bool{})
				}
				fmt.Fprintf(&s, "  do\n    local a = {}\n    for i = 1, get_count(r, %d) do a[i] = %s end\n    %s = a\n  end\n", minSz, luaDec(f.Type), acc)
			} else {
				fmt.Fprintf(&s, "  %s = %s\n", acc, luaDec(f.Type))
			}
		}
		s.WriteString("  return o\nend\n")
	}
	s.WriteString("\nreturn M\n")
	return os.WriteFile(filepath.Join(cfg.OutDir, cfg.InputFileName+".lua"), []byte(s.String()), 0644)
}
