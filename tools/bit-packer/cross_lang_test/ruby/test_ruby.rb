# frozen_string_literal: true

# Cross-language conformance test for the BitPacker Ruby target.
# Usage: ruby test_ruby.rb <generated dir> <cross_lang_test dir>

gen_dir, dir = ARGV
require File.join(gen_dir, 'bench_complex')
require File.join(gen_dir, 'edge')
require File.join(gen_dir, 'f32')

$passed = 0
$failed = 0

def check(ok, name, detail = '')
  if ok
    $passed += 1
    puts "  ok   #{name}"
  else
    $failed += 1
    puts "  FAIL #{name} (#{detail})"
  end
end

def expect(name)
  ok = begin
    yield
  rescue StandardError => e
    check(false, name, "#{e.class}: #{e.message}")
    return
  end
  check(ok, name, 'mismatch')
end

# ---------------- bench ----------------
B = BenchComplex

def build_world
  hero = B::Character.new(
    name: 'TestHero', level: 99, hp: 1000, mp: 500, is_alive: true,
    position: B::Vec3.new(x: 10, y: -20, z: 30),
    skills: [1, 2, 3, 100],
    inventory: [B::Item.new(id: 1, name: 'Excalibur', value: 9999, weight: 15, rarity: 'Legendary')]
  )
  guild = B::Guild.new(name: 'TestGuild', description: 'A test guild for cross-language', members: [hero])
  B::WorldState.new(
    world_id: 42, seed: 'cross_lang_test', guilds: [guild],
    loot_table: [B::Item.new(id: 2, name: 'HealthPotion', value: 50, weight: 1, rarity: 'Common')]
  )
end

def verify_world(d)
  return false unless d.world_id == 42 && d.seed == 'cross_lang_test' && d.guilds.length == 1
  g = d.guilds[0]
  return false unless g.name == 'TestGuild' && g.description == 'A test guild for cross-language'
  return false unless g.members.length == 1
  h = g.members[0]
  return false unless h.name == 'TestHero' && h.level == 99 && h.hp == 1000 && h.mp == 500 && h.is_alive == true
  return false unless h.position.x == 10 && h.position.y == -20 && h.position.z == 30
  return false unless h.skills == [1, 2, 3, 100] && h.inventory.length == 1
  s = h.inventory[0]
  return false unless s.id == 1 && s.name == 'Excalibur' && s.value == 9999 && s.weight == 15 && s.rarity == 'Legendary'
  return false unless d.loot_table.length == 1
  p = d.loot_table[0]
  p.id == 2 && p.name == 'HealthPotion' && p.value == 50 && p.weight == 1 && p.rarity == 'Common'
end

ref = File.binread(File.join(dir, 'test_data.bin'))
out = build_world.encode
File.binwrite(File.join(dir, 'test_data_ruby.bin'), out)
check(out.encoding == Encoding::BINARY, 'bench: encode returns a binary String', out.encoding.to_s)
check(out == ref, 'bench: encode == test_data.bin', "#{out.bytesize} vs #{ref.bytesize} bytes")
decoded = nil
expect('bench: decode test_data.bin') { decoded = B::WorldState.decode(ref); true }
expect('bench: decoded fields') { verify_world(decoded) }
expect('bench: re-encode decoded == ref') { decoded.encode == ref }
expect('bench: round-trip equals built value') { B::WorldState.decode(out) == build_world }
expect('bench: decoded strings are UTF-8') { decoded.seed.encoding == Encoding::UTF_8 }

# ---------------- edge ----------------
E = Edge

UNICODE = "héllo wörld ✓ 日本 \u{1F680}"

def build_edge
  E::Edge.new(
    i_min: -2_147_483_648, i_max: 2_147_483_647, i_zero: 0, i_neg: -1,
    l_min: -9_223_372_036_854_775_808, l_max: 9_223_372_036_854_775_807, l_neg: -300,
    f: -1.25, d: 1234.5625, d_neg: -0.5, yes: true, no: false,
    empty: '', unicode: UNICODE,
    ints: [0, -1, 1, -64, 64, -2_147_483_648, 2_147_483_647],
    longs: [0, -1, 9_223_372_036_854_775_807, -9_223_372_036_854_775_808, 4_294_967_296],
    floats: [0.0, 0.5, -2.25], doubles: [0.0, 3.5, -1_000_000.25],
    bools: [true, false, true], strings: ['', 'a', "日本語"], no_ints: [],
    inner: E::Inner.new(big: 1_099_511_627_776, label: 'inner'),
    inners: [E::Inner.new(big: -1, label: ''), E::Inner.new(big: 0, label: 'x')],
    no_inners: []
  )
end

eref = File.binread(File.join(dir, 'edge', 'edge_ref.bin'))
eout = build_edge.encode
check(eout == eref, 'edge: encode == edge_ref.bin', "#{eout.bytesize} vs #{eref.bytesize} bytes")
e = nil
expect('edge: decode edge_ref.bin') { e = E::Edge.decode(eref); true }
if e
  {
    'i_min' => -> { e.i_min == -2_147_483_648 },
    'i_max' => -> { e.i_max == 2_147_483_647 },
    'i_zero' => -> { e.i_zero.zero? },
    'i_neg' => -> { e.i_neg == -1 },
    'l_min' => -> { e.l_min == -9_223_372_036_854_775_808 },
    'l_max' => -> { e.l_max == 9_223_372_036_854_775_807 },
    'l_neg' => -> { e.l_neg == -300 },
    'f' => -> { e.f == -1.25 },
    'd' => -> { e.d == 1234.5625 },
    'd_neg' => -> { e.d_neg == -0.5 },
    'yes' => -> { e.yes == true },
    'no' => -> { e.no == false },
    'empty' => -> { e.empty == '' },
    'unicode' => -> { e.unicode == UNICODE && e.unicode.encoding == Encoding::UTF_8 },
    'ints' => -> { e.ints == [0, -1, 1, -64, 64, -2_147_483_648, 2_147_483_647] },
    'longs' => -> { e.longs == [0, -1, 9_223_372_036_854_775_807, -9_223_372_036_854_775_808, 4_294_967_296] },
    'floats' => -> { e.floats == [0.0, 0.5, -2.25] },
    'doubles' => -> { e.doubles == [0.0, 3.5, -1_000_000.25] },
    'bools' => -> { e.bools == [true, false, true] },
    'strings' => -> { e.strings == ['', 'a', "日本語"] },
    'no_ints' => -> { e.no_ints == [] },
    'inner' => -> { e.inner.big == 1_099_511_627_776 && e.inner.label == 'inner' },
    'inners' => -> { e.inners.map { |i| [i.big, i.label] } == [[-1, ''], [0, 'x']] },
    'no_inners' => -> { e.no_inners == [] }
  }.each { |name, fn| expect("edge: field #{name}", &fn) }
end
expect('edge: re-encode decoded == ref') { e.encode == eref }
expect('edge: decoded == built value') { e == build_edge }

bad = eref.dup
bad.setbyte(1, '3'.ord)
begin
  E::Edge.decode(bad)
  check(false, 'edge: wrong version rejected', 'no error')
rescue E::DecodeError => ex
  check(ex.message.include?('version'), 'edge: wrong version rejected', ex.message)
end

first_bad = nil
(0...eref.bytesize).each do |n|
  begin
    E::Edge.decode(eref.byteslice(0, n))
    first_bad = "prefix of #{n} bytes decoded"
  rescue E::DecodeError
    next
  rescue StandardError => ex
    first_bad = "prefix of #{n} bytes: #{ex.class}: #{ex.message}"
  end
  break
end
check(first_bad.nil?, 'edge: every truncation rejected', first_bad)

def rejects(bytes)
  E::Edge.decode(bytes.pack('C*'))
  false
rescue E::DecodeError
  true
end
ver = [10] + '2.1.0'.bytes
check(rejects(ver + [0] * 17 + [0xfe, 0xff, 0xff, 0xff, 0x0f]), 'edge: huge array count rejected')
check(rejects(ver + [0] * 12 + [0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 97]), 'edge: huge string length rejected')
check(rejects(ver + [0] * 13 + [1]), 'edge: negative string length rejected')
check(rejects(ver + [0xff] * 10 + [1]), 'edge: 11-byte varint rejected')
check(rejects(ver + [0] * 12 + [2, 0xff]), 'edge: invalid UTF-8 rejected')

# 32/64-bit wrapping of out-of-range integers, like the fixed-width targets
w = E::Inner.new(big: 2**64 - 1, label: '').encode
expect('edge: long wraps to 64 bits') { E::Inner.decode(w).big == -1 }
expect('edge: int wraps to 32 bits') { E::Edge.decode(E::Edge.new(i_min: 2**31).encode).i_min == -2**31 }

# ---------------- float32 ----------------
F32_REF = ['0a312e302e30b06d06a82db06d808080a00100'].pack('H*')
def f32(x) = [x].pack('f').unpack1('f')
fout = F32::F32.new(f: 0.7, fs: [0.29, 0.7, 16_777.217]).encode
check(fout == F32_REF, 'f32: 0.7, [0.29, 0.7, 16777.217] -> 7000, [2900, 7000, 167772160]', fout.unpack1('H*'))
expect('f32: decodes to the nearest float32 values') do
  v = F32::F32.decode(F32_REF)
  v.f == f32(0.7) && v.fs == [f32(0.29), f32(0.7), f32(16_777.217)] &&
    v.fs == [0.28999999165534973, 0.699999988079071, 16_777.216796875]
end

puts "ruby: #{$passed} passed, #{$failed} failed"
exit($failed.zero? ? 0 : 1)
