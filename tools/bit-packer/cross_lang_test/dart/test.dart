// Cross-language conformance test for the BitPacker Dart target.
import 'dart:io';
import 'dart:typed_data';

import 'gen/dart/bench_complex.dart' as bench;
import 'gen/dart/edge.dart' as edge;

var passed = 0;
var failed = 0;

void check(String name, bool ok, [String detail = '']) {
  if (ok) {
    passed++;
    print('  ok   $name');
  } else {
    failed++;
    print('  FAIL $name${detail.isEmpty ? '' : ' ($detail)'}');
  }
}

bool _deepEq(Object? a, Object? b) {
  if (a is List && b is List) {
    if (a.length != b.length) return false;
    for (var i = 0; i < a.length; i++) {
      if (!_deepEq(a[i], b[i])) return false;
    }
    return true;
  }
  return a == b;
}

void checkEq(String name, Object? got, Object? want) =>
    check(name, _deepEq(got, want), 'got $got, want $want');

void checkBytes(String name, Uint8List got, Uint8List want) =>
    check(name, _deepEq(got, want), 'got ${got.length} bytes, want ${want.length}');

bench.WorldState benchWorld() {
  const sword = bench.Item(id: 1, name: 'Excalibur', value: 9999, weight: 15, rarity: 'Legendary');
  const hero = bench.Character(
    name: 'TestHero',
    level: 99,
    hp: 1000,
    mp: 500,
    isAlive: true,
    position: bench.Vec3(x: 10, y: -20, z: 30),
    skills: [1, 2, 3, 100],
    inventory: [sword],
  );
  const guild = bench.Guild(name: 'TestGuild', description: 'A test guild for cross-language', members: [hero]);
  const potion = bench.Item(id: 2, name: 'HealthPotion', value: 50, weight: 1, rarity: 'Common');
  return const bench.WorldState(worldId: 42, seed: 'cross_lang_test', guilds: [guild], lootTable: [potion]);
}

void verifyBench(String label, bench.WorldState w) {
  checkEq('$label world_id', w.worldId, 42);
  checkEq('$label seed', w.seed, 'cross_lang_test');
  checkEq('$label guilds length', w.guilds.length, 1);
  for (final g in w.guilds.take(1)) {
    checkEq('$label guild name', g.name, 'TestGuild');
    checkEq('$label guild description', g.description, 'A test guild for cross-language');
    checkEq('$label members length', g.members.length, 1);
    for (final h in g.members.take(1)) {
      checkEq('$label hero name', h.name, 'TestHero');
      checkEq('$label hero level', h.level, 99);
      checkEq('$label hero hp', h.hp, 1000);
      checkEq('$label hero mp', h.mp, 500);
      checkEq('$label hero is_alive', h.isAlive, true);
      checkEq('$label position', h.position, const bench.Vec3(x: 10, y: -20, z: 30));
      checkEq('$label skills', h.skills, [1, 2, 3, 100]);
      checkEq('$label inventory length', h.inventory.length, 1);
      for (final s in h.inventory.take(1)) {
        checkEq('$label sword name', s.name, 'Excalibur');
        checkEq('$label sword value', s.value, 9999);
        checkEq('$label sword rarity', s.rarity, 'Legendary');
      }
    }
  }
  checkEq('$label loot length', w.lootTable.length, 1);
  for (final p in w.lootTable.take(1)) {
    checkEq('$label potion name', p.name, 'HealthPotion');
    checkEq('$label potion rarity', p.rarity, 'Common');
  }
}

const int i64Max = 9223372036854775807;
const int i64Min = -9223372036854775807 - 1;

edge.Edge canonicalEdge() => const edge.Edge(
      iMin: -2147483648,
      iMax: 2147483647,
      iZero: 0,
      iNeg: -1,
      lMin: i64Min,
      lMax: i64Max,
      lNeg: -300,
      f: -1.25,
      d: 1234.5625,
      dNeg: -0.5,
      yes: true,
      no: false,
      empty: '',
      unicode: 'héllo wörld ✓ 日本 \u{1F680}',
      ints: [0, -1, 1, -64, 64, -2147483648, 2147483647],
      longs: [0, -1, i64Max, i64Min, 4294967296],
      floats: [0.0, 0.5, -2.25],
      doubles: [0.0, 3.5, -1000000.25],
      bools: [true, false, true],
      strings: ['', 'a', '日本語'],
      noInts: [],
      inner: edge.Inner(big: 1099511627776, label: 'inner'),
      inners: [edge.Inner(big: -1, label: ''), edge.Inner(big: 0, label: 'x')],
      noInners: [],
    );

edge.Edge edgeWith(edge.Edge e, {required double f, required List<double> floats}) => edge.Edge(
      iMin: e.iMin, iMax: e.iMax, iZero: e.iZero, iNeg: e.iNeg,
      lMin: e.lMin, lMax: e.lMax, lNeg: e.lNeg,
      f: f, d: e.d, dNeg: e.dNeg, yes: e.yes, no: e.no, empty: e.empty, unicode: e.unicode,
      ints: e.ints, longs: e.longs, floats: floats, doubles: e.doubles, bools: e.bools,
      strings: e.strings, noInts: e.noInts, inner: e.inner, inners: e.inners, noInners: e.noInners,
    );

/// '' when decode throws BitPackerException, else what went wrong.
String rejects(Uint8List data) {
  try {
    edge.Edge.decode(data);
    return 'no error';
  } on edge.BitPackerException {
    return '';
  } catch (e) {
    return 'wrong error: $e';
  }
}

double f32(double x) => (Float32List(1)..[0] = x)[0];

void main(List<String> args) {
  final dir = args.isNotEmpty ? args[0] : '..';

  print('bench_complex');
  final benchRef = File('$dir/test_data.bin').readAsBytesSync();
  final benchEnc = benchWorld().encode();
  File('$dir/test_data_dart.bin').writeAsBytesSync(benchEnc);
  checkBytes('bench encode == test_data.bin', benchEnc, benchRef);
  final benchDec = bench.WorldState.decode(benchRef);
  verifyBench('bench decode', benchDec);
  checkEq('bench decode == canonical value', benchDec, benchWorld());
  checkBytes('bench re-encode == test_data.bin', benchDec.encode(), benchRef);
  verifyBench('bench roundtrip', bench.WorldState.decode(benchEnc));

  print('edge');
  final ref = File('$dir/edge/edge_ref.bin').readAsBytesSync();
  final want = canonicalEdge();
  checkBytes('edge encode == edge_ref.bin', want.encode(), ref);
  edge.Edge? dec;
  try {
    dec = edge.Edge.decode(ref);
  } catch (e) {
    check('edge decode edge_ref.bin', false, '$e');
  }
  if (dec != null) {
    checkEq('edge field i_min', dec.iMin, -2147483648);
    checkEq('edge field i_max', dec.iMax, 2147483647);
    checkEq('edge field i_zero', dec.iZero, 0);
    checkEq('edge field i_neg', dec.iNeg, -1);
    checkEq('edge field l_min', dec.lMin, i64Min);
    checkEq('edge field l_max', dec.lMax, i64Max);
    checkEq('edge field l_neg', dec.lNeg, -300);
    checkEq('edge field f', dec.f, -1.25);
    checkEq('edge field d', dec.d, 1234.5625);
    checkEq('edge field d_neg', dec.dNeg, -0.5);
    checkEq('edge field yes', dec.yes, true);
    checkEq('edge field no', dec.no, false);
    checkEq('edge field empty', dec.empty, '');
    checkEq('edge field unicode', dec.unicode, want.unicode);
    checkEq('edge field ints', dec.ints, want.ints);
    checkEq('edge field longs', dec.longs, want.longs);
    checkEq('edge field floats', dec.floats, want.floats);
    checkEq('edge field doubles', dec.doubles, want.doubles);
    checkEq('edge field bools', dec.bools, want.bools);
    checkEq('edge field strings', dec.strings, want.strings);
    checkEq('edge field no_ints', dec.noInts, <int>[]);
    checkEq('edge field inner', dec.inner, want.inner);
    checkEq('edge field inners', dec.inners, want.inners);
    checkEq('edge field no_inners', dec.noInners, <edge.Inner>[]);
    checkEq('edge decode == canonical value', dec, want);
    checkBytes('edge re-encode == edge_ref.bin', dec.encode(), ref);
  }

  final bad = Uint8List.fromList(ref);
  bad[5] = '9'.codeUnitAt(0); // "2.1.0" -> "2.1.9"
  final badResult = rejects(bad);
  check('edge wrong version rejected', badResult.isEmpty, badResult);

  var truncFail = '';
  for (var n = 0; n < ref.length; n++) {
    final r = rejects(Uint8List.sublistView(ref, 0, n));
    if (r.isNotEmpty) {
      truncFail = 'prefix $n: $r';
      break;
    }
  }
  check('edge every truncation (${ref.length} prefixes) rejected', truncFail.isEmpty, truncFail);

  // float fields are scaled in single precision, like the Go and Java
  // targets: 1.0005f * 10000f = 10005 in float32 (10004 in
  // float64).
  final back = edge.Edge.decode(const edge.Edge(f: 1.0005, floats: [1.0013]).encode());
  checkEq('float field scaled in float32', back.f, f32(10005 / 10000));
  checkEq('float[] element scaled in float32', back.floats, [f32(10013 / 10000)]);

  const bom = edge.Inner(big: 7, label: '\uFEFF\uFEFFbom');
  checkEq('string with leading U+FEFF round-trips', edge.Inner.decode(bom.encode()).label, bom.label);
  final overlong = Uint8List.fromList([10, 50, 46, 49, 46, 48, 0, 4, 0xC0, 0x80]);
  String ovr;
  try {
    edge.Inner.decode(overlong);
    ovr = 'no error';
  } on edge.BitPackerException {
    ovr = '';
  }
  check('invalid UTF-8 rejected', ovr.isEmpty, ovr);

  // edge_float32_ref.bin: f and floats whose x10000 is inexact in float32.
  final f32ref = File('$dir/edge/edge_float32_ref.bin').readAsBytesSync();
  final f32want = edgeWith(canonicalEdge(), f: f32(0.29), floats: [f32(0.7), f32(16777.217), f32(-0.29)]);
  checkBytes('float32 fixture: encode == edge_float32_ref.bin', f32want.encode(), f32ref);
  checkBytes('float32 fixture: encode from double literals == edge_float32_ref.bin',
      edgeWith(canonicalEdge(), f: 0.29, floats: [0.7, 16777.217, -0.29]).encode(), f32ref);
  try {
    final f32dec = edge.Edge.decode(f32ref);
    checkEq('float32 fixture: decoded f == 0.29f', f32dec.f, f32(0.29));
    checkEq('float32 fixture: decoded floats', f32dec.floats, [f32(0.7), f32(16777.217), f32(-0.29)]);
    checkEq('float32 fixture: decode == variant', f32dec, f32want);
  } catch (e) {
    check('float32 fixture: decode', false, '$e');
  }

  try {
    checkEq('trailing bytes ignored', edge.Edge.decode(Uint8List.fromList([...ref, 0xFF, 0, 0x7F])), want);
  } catch (e) {
    check('trailing bytes ignored', false, '$e');
  }

  void encodeRejects(String name, edge.Edge e) {
    String r;
    try {
      e.encode();
      r = 'no error';
    } on ArgumentError {
      r = '';
    }
    check(name, r.isEmpty, r);
  }

  encodeRejects('NaN float rejected on encode', const edge.Edge(f: double.nan));
  encodeRejects('infinite float rejected on encode', const edge.Edge(floats: [double.infinity]));
  encodeRejects('out-of-range float rejected on encode', const edge.Edge(f: 1e15));
  encodeRejects('NaN double rejected on encode', const edge.Edge(d: double.nan));
  encodeRejects('infinite double rejected on encode', const edge.Edge(dNeg: double.negativeInfinity));
  encodeRejects('out-of-range double rejected on encode', const edge.Edge(doubles: [1e300]));

  void decodeRejects(String name, List<int> data, Object Function(Uint8List) f) {
    String r;
    try {
      f(Uint8List.fromList(data));
      r = 'no error';
    } on edge.BitPackerException {
      r = '';
    }
    check(name, r.isEmpty, r);
  }

  final dflt = const edge.Edge().encode(); // ints count at offset 20
  List<int> withCount(List<int> count) => [...dflt.sublist(0, 20), ...count, ...dflt.sublist(21)];
  decodeRejects('negative array length rejected', withCount([0x01]), edge.Edge.decode);
  decodeRejects('oversized array length rejected', withCount([0xFE, 0xFF, 0xFF, 0xFF, 0x0F]), edge.Edge.decode);
  const innerHead = [10, 50, 46, 49, 46, 48, 0];
  decodeRejects('negative string length rejected', [...innerHead, 0x01, 0x61], edge.Inner.decode);
  decodeRejects('oversized string length rejected',
      [...innerHead, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x40, 0x61], edge.Inner.decode);
  decodeRejects('varint longer than 10 bytes rejected',
      [...innerHead.sublist(0, 6), 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00],
      edge.Inner.decode);

  print('dart: $passed passed, $failed failed');
  if (failed > 0) exitCode = 1;
}
