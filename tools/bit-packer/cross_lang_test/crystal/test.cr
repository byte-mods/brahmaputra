require "./gen/crystal/bench_complex"
require "./gen/crystal/edge"

DIR = ARGV[0]? || File.join(__DIR__, "..")

module T
  class_property passed = 0
  class_property failed = 0
end

def check(name : String, ok : Bool, detail = "")
  if ok
    T.passed += 1
    puts "  ok   #{name}"
  else
    T.failed += 1
    puts "  FAIL #{name} (#{detail})"
  end
end

def raises_decode_error(&) : Bool
  yield
  false
rescue Edge::DecodeError | BenchComplex::DecodeError
  true
end

# ---------------------------------------------------------------- bench
alias B = BenchComplex

def make_world : B::WorldState
  sword = B::Item.new(id: 1, name: "Excalibur", value: 9999, weight: 15, rarity: "Legendary")
  hero = B::Character.new(
    name: "TestHero", level: 99, hp: 1000, mp: 500, is_alive: true,
    position: B::Vec3.new(x: 10, y: -20, z: 30),
    skills: [1, 2, 3, 100], inventory: [sword])
  guild = B::Guild.new(name: "TestGuild", description: "A test guild for cross-language", members: [hero])
  potion = B::Item.new(id: 2, name: "HealthPotion", value: 50, weight: 1, rarity: "Common")
  B::WorldState.new(world_id: 42, seed: "cross_lang_test", guilds: [guild], loot_table: [potion])
end

def verify_world(w : B::WorldState) : String?
  return "world_id" unless w.world_id == 42
  return "seed" unless w.seed == "cross_lang_test"
  return "guilds length" unless w.guilds.size == 1
  g = w.guilds[0]
  return "guild name" unless g.name == "TestGuild"
  return "guild description" unless g.description == "A test guild for cross-language"
  return "members length" unless g.members.size == 1
  h = g.members[0]
  return "hero name" unless h.name == "TestHero"
  return "hero level" unless h.level == 99
  return "hero hp" unless h.hp == 1000
  return "hero mp" unless h.mp == 500
  return "hero is_alive" unless h.is_alive == true
  return "position" unless h.position.x == 10 && h.position.y == -20 && h.position.z == 30
  return "skills #{h.skills}" unless h.skills == [1, 2, 3, 100]
  return "inventory length" unless h.inventory.size == 1
  s = h.inventory[0]
  return "sword" unless s.name == "Excalibur" && s.value == 9999 && s.rarity == "Legendary"
  return "loot length" unless w.loot_table.size == 1
  p = w.loot_table[0]
  return "potion" unless p.name == "HealthPotion" && p.rarity == "Common"
  nil
end

bench_ref = File.read(File.join(DIR, "test_data.bin")).to_slice
enc = make_world.encode
File.write(File.join(DIR, "test_data_crystal.bin"), enc)
check("bench encode == test_data.bin", enc == bench_ref, "#{enc.size} vs #{bench_ref.size} bytes")
begin
  dec = B::WorldState.decode(bench_ref)
  err = verify_world(dec)
  check("bench decode test_data.bin", err.nil?, err || "")
  check("bench round-trip", dec.encode == bench_ref)
  check("bench decoded == built value", dec == make_world)
rescue ex : B::DecodeError
  check("bench decode test_data.bin", false, ex.message || "")
end

# ---------------------------------------------------------------- edge
alias E = Edge

def make_edge : E::Edge
  E::Edge.new(
    i_min: Int32::MIN, i_max: Int32::MAX, i_zero: 0, i_neg: -1,
    l_min: Int64::MIN, l_max: Int64::MAX, l_neg: -300_i64,
    f: -1.25_f32, d: 1234.5625, d_neg: -0.5,
    yes: true, no: false,
    empty: "", unicode: "héllo wörld ✓ 日本 \u{1F680}",
    ints: [0, -1, 1, -64, 64, Int32::MIN, Int32::MAX],
    longs: [0_i64, -1_i64, Int64::MAX, Int64::MIN, 4294967296_i64],
    floats: [0.0_f32, 0.5_f32, -2.25_f32],
    doubles: [0.0, 3.5, -1000000.25],
    bools: [true, false, true],
    strings: ["", "a", "日本語"],
    no_ints: [] of Int32,
    inner: E::Inner.new(big: 1099511627776_i64, label: "inner"),
    inners: [E::Inner.new(big: -1_i64, label: ""), E::Inner.new(big: 0_i64, label: "x")],
    no_inners: [] of E::Inner)
end

edge_ref = File.read(File.join(DIR, "edge", "edge_ref.bin")).to_slice
eenc = make_edge.encode
check("edge encode == edge_ref.bin", eenc == edge_ref, "#{eenc.size} vs #{edge_ref.size} bytes")

begin
  e = E::Edge.decode(edge_ref)
  x = make_edge
  {% for f in %w(i_min i_max i_zero i_neg l_min l_max l_neg f d d_neg yes no empty unicode
                ints longs floats doubles bools strings no_ints inner inners no_inners) %}
    check("edge field {{f.id}}", e.{{f.id}} == x.{{f.id}}, "got #{e.{{f.id}}.inspect}")
  {% end %}
  check("edge float types exact", e.f.is_a?(Float32) && e.f == -1.25_f32 && e.floats[2] == -2.25_f32)
  check("edge round-trip", e.encode == edge_ref)
rescue ex : E::DecodeError
  check("edge decode edge_ref.bin", false, ex.message || "")
end

# float fields: x10000 must happen in Float32, exactly as Go/Java do.
# {float32 bits, Go's int64(v * 10000.0) with v float32, bits of Go's float32(n) / 10000.0}
FLOAT_CASES = [
  {0x3dcccccd_u32, 1000_i64, 0x3dcccccd_u32},       # 0.1
  {0x3f333333_u32, 7000_i64, 0x3f333333_u32},       # 0.7   (float64 math gives 6999)
  {0x40490fd0_u32, 31415_i64, 0x40490e56_u32},      # 3.14159
  {0x4683126f_u32, 167772160_i64, 0x4683126f_u32},  # 16777.217 (float64: 167772167)
  {0x4851b717_u32, 2147483648_i64, 0x4851b717_u32}, # 214748.36 (float64: 2147483593)
  {0x391d4952_u32, 1_i64, 0x38d1b717_u32},          # 0.00015
]

def read_zigzag(b : Bytes, pos : Int32) : Int64
  u = 0_u64
  shift = 0
  loop do
    x = b[pos]
    pos += 1
    u |= (x & 0x7f).to_u64 << shift
    break if x < 0x80
    shift += 7
  end
  ((u >> 1) ^ (0_u64 &- (u & 1))).to_i64!
end

FLOAT_CASES.each do |(bits, want, dec_bits)|
  v = bits.unsafe_as(Float32)
  msg = E::Edge.new(f: v).encode
  got = read_zigzag(msg, 13) # 6 bytes version + 4 ints + 3 longs, all one byte
  back = E::Edge.decode(msg).f
  check("float32 fixed-point #{v} -> #{want}", got == want && back.unsafe_as(UInt32) == dec_bits,
    "encoded #{got}, decoded bits 0x#{back.unsafe_as(UInt32).to_s(16)}")
end

bad = edge_ref.dup
bad[5] = '9'.ord.to_u8 # "2.1.0" -> "2.1.9"
check("edge wrong version rejected", raises_decode_error { E::Edge.decode(bad) })
check("edge wrong version decode? -> nil", E::Edge.decode?(bad).nil?)
check("edge other schema's message rejected", raises_decode_error { E::Edge.decode(bench_ref) })

truncs_ok = true
first_bad = -1
(0...edge_ref.size).each do |n|
  unless raises_decode_error { E::Edge.decode(edge_ref[0, n]) }
    truncs_ok = false
    first_bad = n
    break
  end
end
check("edge all #{edge_ref.size} truncations rejected", truncs_ok, "prefix of #{first_bad} bytes decoded")

# bogus huge lengths must fail fast, not allocate
huge_io = IO::Memory.new
huge_io.write(Bytes[5]); huge_io << "2.1.0"
huge_io.write(Bytes.new(12, 0_u8))           # 4 ints, 3 longs, 3 floats, 2 bools
huge_io.write(Bytes[0xfe, 0xff, 0xff, 0xff, 0x0f]) # `empty` claims ~2^31 bytes
huge_str = huge_io.to_slice
check("edge huge length rejected", raises_decode_error { E::Edge.decode(huge_str) })

# random byte mutations: must decode or raise DecodeError, never anything else
rng = Random.new(12345)
fuzz_ok = true
fuzz_detail = ""
3000.times do
  m = edge_ref.dup
  (1 + rng.rand(4)).times { m[rng.rand(m.size)] = rng.rand(256).to_u8 }
  begin
    E::Edge.decode(m)
  rescue E::DecodeError
  rescue ex
    fuzz_ok = false
    fuzz_detail = "#{ex.class}: #{ex.message}"
    break
  end
end
check("edge 3000 random mutations: value or DecodeError", fuzz_ok, fuzz_detail)

puts "crystal: #{T.passed} passed, #{T.failed} failed"
exit(T.failed == 0 ? 0 : 1)
