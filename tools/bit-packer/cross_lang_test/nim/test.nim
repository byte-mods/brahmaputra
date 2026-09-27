import std/[os, strutils, random]
import gen/nim/bench_complex as bc
import gen/nim/edge as eg

let dir = if paramCount() >= 1: paramStr(1) else: currentSourcePath().parentDir / ".."

var passed, failed = 0

proc check(name: string, ok: bool, detail = "") =
  if ok:
    inc passed
    echo "  ok   ", name
  else:
    inc failed
    echo "  FAIL ", name, " (", detail, ")"

proc readBytes(path: string): seq[byte] =
  let s = readFile(path)
  result = newSeq[byte](s.len)
  for i, c in s: result[i] = byte(c)

template rejects(body: untyped): bool =
  var raised = false
  try:
    discard body
  except bc.DecodeError, eg.DecodeError:
    raised = true
  raised

# ---------------------------------------------------------------- bench

proc makeWorld(): WorldState =
  let sword = Item(id: 1, name: "Excalibur", value: 9999, weight: 15, rarity: "Legendary")
  let hero = Character(name: "TestHero", level: 99, hp: 1000, mp: 500, isAlive: true,
                       position: Vec3(x: 10, y: -20, z: 30),
                       skills: @[1'i32, 2, 3, 100], inventory: @[sword])
  let guild = Guild(name: "TestGuild", description: "A test guild for cross-language",
                    members: @[hero])
  let potion = Item(id: 2, name: "HealthPotion", value: 50, weight: 1, rarity: "Common")
  WorldState(worldId: 42, seed: "cross_lang_test", guilds: @[guild], lootTable: @[potion])

proc verifyWorld(w: WorldState): string =
  if w.worldId != 42: return "world_id"
  if w.seed != "cross_lang_test": return "seed"
  if w.guilds.len != 1: return "guilds length"
  let g = w.guilds[0]
  if g.name != "TestGuild": return "guild name"
  if g.description != "A test guild for cross-language": return "guild description"
  if g.members.len != 1: return "members length"
  let h = g.members[0]
  if h.name != "TestHero": return "hero name"
  if h.level != 99 or h.hp != 1000 or h.mp != 500: return "hero stats"
  if not h.isAlive: return "hero is_alive"
  if h.position.x != 10 or h.position.y != -20 or h.position.z != 30: return "position"
  if h.skills != @[1'i32, 2, 3, 100]: return "skills " & $h.skills
  if h.inventory.len != 1: return "inventory length"
  let s = h.inventory[0]
  if s.name != "Excalibur" or s.value != 9999 or s.rarity != "Legendary": return "sword"
  if w.lootTable.len != 1: return "loot length"
  let p = w.lootTable[0]
  if p.name != "HealthPotion" or p.rarity != "Common": return "potion"
  ""

let benchRef = readBytes(dir / "test_data.bin")
let enc = makeWorld().encode()
writeFile(dir / "test_data_nim.bin", cast[string](enc))
check("bench encode == test_data.bin", enc == benchRef, $enc.len & " vs " & $benchRef.len & " bytes")
try:
  let dec = WorldState.decode(benchRef)
  let err = verifyWorld(dec)
  check("bench decode test_data.bin", err == "", err)
  check("bench round-trip", dec.encode() == benchRef)
  check("bench decoded == built value", dec == makeWorld())
  check("bench decode from string", WorldState.decode(cast[string](benchRef)) == dec)
except bc.DecodeError as ex:
  check("bench decode test_data.bin", false, ex.msg)

# ---------------------------------------------------------------- edge

proc makeEdge(): Edge =
  Edge(iMin: low(int32), iMax: high(int32), iZero: 0, iNeg: -1,
       lMin: low(int64), lMax: high(int64), lNeg: -300,
       f: -1.25'f32, d: 1234.5625, dNeg: -0.5,
       yes: true, no: false,
       empty: "", unicode: "héllo wörld ✓ 日本 \u{1F680}",
       ints: @[0'i32, -1, 1, -64, 64, low(int32), high(int32)],
       longs: @[0'i64, -1, high(int64), low(int64), 4294967296'i64],
       floats: @[0.0'f32, 0.5, -2.25],
       doubles: @[0.0, 3.5, -1000000.25],
       bools: @[true, false, true],
       strings: @["", "a", "日本語"],
       noInts: @[],
       inner: Inner(big: 1099511627776'i64, label: "inner"),
       inners: @[Inner(big: -1, label: ""), Inner(big: 0, label: "x")],
       noInners: @[])

let edgeRef = readBytes(dir / "edge" / "edge_ref.bin")
let eenc = makeEdge().encode()
check("edge encode == edge_ref.bin", eenc == edgeRef, $eenc.len & " vs " & $edgeRef.len & " bytes")

try:
  let e = Edge.decode(edgeRef)
  let x = makeEdge()
  for name, a, b in fieldPairs(e, x):
    check("edge field " & name, a == b, "got " & $a)
  check("edge float values exact", e.f == -1.25'f32 and e.floats[2] == -2.25'f32 and
        e.doubles[2] == -1000000.25)
  check("edge round-trip", e.encode() == edgeRef)
except eg.DecodeError as ex:
  check("edge decode edge_ref.bin", false, ex.msg)

# float fields: x10000 must happen in float32, exactly as Go/Java do.
# (float32 bits, Go's int64(v * 10000.0) for v float32, bits of Go's float32(n) / 10000.0)
const floatCases = [
  (0x3dcccccd'u32, 1000'i64, 0x3dcccccd'u32),       # 0.1
  (0x3f333333'u32, 7000'i64, 0x3f333333'u32),       # 0.7   (float64 math gives 6999)
  (0x40490fd0'u32, 31415'i64, 0x40490e56'u32),      # 3.14159
  (0x4683126f'u32, 167772160'i64, 0x4683126f'u32),  # 16777.217 (float64: 167772167)
  (0x4851b717'u32, 2147483648'i64, 0x4851b717'u32), # 214748.36 (float64: 2147483593)
  (0x391d4952'u32, 1'i64, 0x38d1b717'u32),          # 0.00015
]

proc readZigzag(b: seq[byte], pos: int): int64 =
  var u = 0'u64
  var shift = 0
  var p = pos
  while true:
    let x = b[p]
    inc p
    u = u or (uint64(x and 0x7F) shl shift)
    if x < 0x80: break
    shift += 7
  cast[int64]((u shr 1) xor (0'u64 - (u and 1)))

for (bits, want, decBits) in floatCases:
  let v = cast[float32](bits)
  let msg = Edge(f: v).encode()
  let got = readZigzag(msg, 13) # 6 bytes version + 4 ints + 3 longs, one byte each
  let back = Edge.decode(msg).f
  check("float32 fixed-point " & $v & " -> " & $want,
        got == want and cast[uint32](back) == decBits,
        "encoded " & $got & ", decoded bits 0x" & toHex(int64(cast[uint32](back)), 8))

var bad = edgeRef
bad[5] = byte('9') # "2.1.0" -> "2.1.9"
check("edge wrong version rejected", rejects(Edge.decode(bad)))
check("edge other schema's message rejected", rejects(Edge.decode(benchRef)))

var firstBad = -1
for n in 0 ..< edgeRef.len:
  if not rejects(Edge.decode(edgeRef.toOpenArray(0, n - 1))):
    firstBad = n
    break
check("edge all " & $edgeRef.len & " truncations rejected", firstBad == -1,
      "prefix of " & $firstBad & " bytes decoded")

# a bogus huge length must fail fast, not allocate
var huge = @[5'u8] & cast[seq[byte]]("2.1.0")
for _ in 1 .. 12: huge.add 0'u8 # 4 ints, 3 longs, 3 floats, 2 bools
huge.add [0xfe'u8, 0xff, 0xff, 0xff, 0x0f] # `empty` claims ~2^31 bytes
check("edge huge length rejected", rejects(Edge.decode(huge)))

# random byte mutations: must decode or raise DecodeError, never anything else
var rng = initRand(12345)
var fuzzOk = true
var fuzzDetail = ""
for _ in 1 .. 3000:
  var m = edgeRef
  for _ in 0 .. rng.rand(3): m[rng.rand(m.high)] = byte(rng.rand(255))
  try:
    discard Edge.decode(m)
  except eg.DecodeError:
    discard
  except CatchableError, Defect:
    fuzzOk = false
    fuzzDetail = $getCurrentException().name & ": " & getCurrentExceptionMsg()
    break
check("edge 3000 random mutations: value or DecodeError", fuzzOk, fuzzDetail)

echo "nim: ", passed, " passed, ", failed, " failed"
quit(if failed == 0: 0 else: 1)
