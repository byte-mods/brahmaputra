// Cross-language conformance test for the BitPacker TypeScript target.
// Compiled with tsc in strict mode against lib ES2020 only (no DOM, no
// @types/node): the generated code must not need either. The only Node API
// the test itself uses, fs, is declared by hand below.

import * as bench from "./gen/typescript/bench_complex.js";
import * as edge from "./gen/typescript/edge.js";

declare function require(name: "fs"): {
  readFileSync(path: string): Uint8Array;
  writeFileSync(path: string, data: Uint8Array): void;
};
declare const process: { argv: string[]; exitCode: number | undefined };
declare const console: { log(message: string): void };

const fs = require("fs");
const dir = process.argv[2] ?? "..";

let passed = 0;
let failed = 0;

function check(name: string, ok: boolean, detail = ""): void {
  if (ok) {
    passed++;
    console.log("  ok   " + name);
  } else {
    failed++;
    console.log("  FAIL " + name + (detail ? " (" + detail + ")" : ""));
  }
}

function show(v: unknown): string {
  return JSON.stringify(v, (_k, x: unknown) => (typeof x === "bigint" ? x.toString() + "n" : x));
}

function eq(a: unknown, b: unknown): boolean {
  if (typeof a === "number" && typeof b === "number") return Object.is(a, b) || a === b;
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((x: unknown, i: number) => eq(x, b[i]));
  }
  if (a instanceof Uint8Array && b instanceof Uint8Array) {
    return a.length === b.length && a.every((x, i) => x === b[i]);
  }
  if (a !== null && b !== null && typeof a === "object" && typeof b === "object") {
    const ka = Object.keys(a);
    const kb = Object.keys(b);
    const ra = a as Record<string, unknown>;
    const rb = b as Record<string, unknown>;
    return ka.length === kb.length && ka.every((k) => eq(ra[k], rb[k]));
  }
  return a === b;
}

function checkEq(name: string, got: unknown, want: unknown): void {
  check(name, eq(got, want), "got " + show(got) + ", want " + show(want));
}

function bytes(b: Uint8Array): Uint8Array {
  return new Uint8Array(b.buffer, b.byteOffset, b.byteLength);
}

function throwsBitPackerError(f: () => unknown, errorClass: new (m: string) => Error): string {
  try {
    f();
    return "no error";
  } catch (e: unknown) {
    return e instanceof errorClass ? "" : "wrong error: " + String(e);
  }
}

// ---------------------------------------------------------------- bench
function benchWorld(): bench.WorldState {
  const sword = new bench.Item({ id: 1, name: "Excalibur", value: 9999, weight: 15, rarity: "Legendary" });
  const hero = new bench.Character({
    name: "TestHero",
    level: 99,
    hp: 1000,
    mp: 500,
    isAlive: true,
    position: new bench.Vec3({ x: 10, y: -20, z: 30 }),
    skills: [1, 2, 3, 100],
    inventory: [sword],
  });
  const guild = new bench.Guild({ name: "TestGuild", description: "A test guild for cross-language", members: [hero] });
  const potion = new bench.Item({ id: 2, name: "HealthPotion", value: 50, weight: 1, rarity: "Common" });
  return new bench.WorldState({ worldId: 42, seed: "cross_lang_test", guilds: [guild], lootTable: [potion] });
}

function verifyBench(label: string, w: bench.WorldState): void {
  checkEq(label + " world_id", w.worldId, 42);
  checkEq(label + " seed", w.seed, "cross_lang_test");
  checkEq(label + " guilds length", w.guilds.length, 1);
  const g = w.guilds[0];
  if (!g) return;
  checkEq(label + " guild name", g.name, "TestGuild");
  checkEq(label + " guild description", g.description, "A test guild for cross-language");
  checkEq(label + " members length", g.members.length, 1);
  const h = g.members[0];
  if (!h) return;
  checkEq(label + " hero name", h.name, "TestHero");
  checkEq(label + " hero level", h.level, 99);
  checkEq(label + " hero hp", h.hp, 1000);
  checkEq(label + " hero mp", h.mp, 500);
  checkEq(label + " hero is_alive", h.isAlive, true);
  checkEq(label + " position", [h.position.x, h.position.y, h.position.z], [10, -20, 30]);
  checkEq(label + " skills", h.skills, [1, 2, 3, 100]);
  checkEq(label + " inventory length", h.inventory.length, 1);
  const s = h.inventory[0];
  if (s) {
    checkEq(label + " sword name", s.name, "Excalibur");
    checkEq(label + " sword value", s.value, 9999);
    checkEq(label + " sword rarity", s.rarity, "Legendary");
  }
  checkEq(label + " loot length", w.lootTable.length, 1);
  const p = w.lootTable[0];
  if (p) {
    checkEq(label + " potion name", p.name, "HealthPotion");
    checkEq(label + " potion rarity", p.rarity, "Common");
  }
}

console.log("bench_complex");
{
  const ref = bytes(fs.readFileSync(dir + "/test_data.bin"));
  const enc = benchWorld().encode();
  fs.writeFileSync(dir + "/test_data_typescript.bin", enc);
  checkEq("bench encode == test_data.bin", enc, ref);
  const dec = bench.WorldState.decode(ref);
  verifyBench("bench decode", dec);
  checkEq("bench re-encode == test_data.bin", dec.encode(), ref);
  verifyBench("bench roundtrip", bench.WorldState.decode(enc));
}

// ---------------------------------------------------------------- edge
const I64_MIN = -(1n << 63n);
const I64_MAX = (1n << 63n) - 1n;

function canonicalEdge(): edge.Edge {
  return new edge.Edge({
    iMin: -2147483648,
    iMax: 2147483647,
    iZero: 0,
    iNeg: -1,
    lMin: I64_MIN,
    lMax: I64_MAX,
    lNeg: -300n,
    f: -1.25,
    d: 1234.5625,
    dNeg: -0.5,
    yes: true,
    no: false,
    empty: "",
    unicode: "héllo wörld ✓ 日本 \u{1F680}",
    ints: [0, -1, 1, -64, 64, -2147483648, 2147483647],
    longs: [0n, -1n, I64_MAX, I64_MIN, 4294967296n],
    floats: [0.0, 0.5, -2.25],
    doubles: [0.0, 3.5, -1000000.25],
    bools: [true, false, true],
    strings: ["", "a", "日本語"],
    noInts: [],
    inner: new edge.Inner({ big: 1099511627776n, label: "inner" }),
    inners: [new edge.Inner({ big: -1n, label: "" }), new edge.Inner({ big: 0n, label: "x" })],
    noInners: [],
  });
}

console.log("edge");
{
  const ref = bytes(fs.readFileSync(dir + "/edge/edge_ref.bin"));
  const want = canonicalEdge();
  const enc = want.encode();
  checkEq("edge encode == edge_ref.bin", enc, ref);

  let dec: edge.Edge | undefined;
  try {
    dec = edge.Edge.decode(ref);
  } catch (e: unknown) {
    check("edge decode edge_ref.bin", false, String(e));
  }
  if (dec) {
    const d = dec as unknown as Record<string, unknown>;
    const w = want as unknown as Record<string, unknown>;
    for (const k of Object.keys(w)) checkEq("edge field " + k, d[k], w[k]);
    check("edge decoded inner is an Inner", dec.inner instanceof edge.Inner);
    checkEq("edge float f exactly -1.25", dec.f, -1.25);
    checkEq("edge l_min exactly", dec.lMin, I64_MIN);
    checkEq("edge re-encode == edge_ref.bin", dec.encode(), ref);
  }

  // A plain object of the Fields shape encodes too.
  const plain: edge.EdgeFields = { ...canonicalEdge() };
  checkEq("edge plain-object encode == edge_ref.bin", edge.Edge.encode(plain), ref);

  const bad = ref.slice();
  bad[5] = "9".charCodeAt(0); // "2.1.0" -> "2.1.9"
  check("edge wrong version rejected", throwsBitPackerError(() => edge.Edge.decode(bad), edge.BitPackerError) === "",
    throwsBitPackerError(() => edge.Edge.decode(bad), edge.BitPackerError));

  let truncFail = "";
  for (let n = 0; n < ref.length; n++) {
    const r = throwsBitPackerError(() => edge.Edge.decode(ref.subarray(0, n)), edge.BitPackerError);
    if (r !== "") {
      truncFail = "prefix " + n + ": " + r;
      break;
    }
  }
  check("edge every truncation (" + ref.length + " prefixes) rejected", truncFail === "", truncFail);

  // float fields are scaled in single precision, exactly like the Go and
  // Java targets: float32(1.0005) * 10000f = 10005 in float32,
  // but 10004 if the product were taken in float64.
  const back = edge.Edge.decode(new edge.Edge({ f: 1.0005, floats: [1.0013] }).encode());
  checkEq("float field scaled in float32", back.f, Math.fround(10005 / 10000));
  checkEq("float[] element scaled in float32", back.floats, [Math.fround(10013 / 10000)]);

  const bom = new edge.Inner({ big: 7n, label: "\uFEFF\uFEFFbom" });
  checkEq("string with leading U+FEFF round-trips", edge.Inner.decode(bom.encode()).label, bom.label);
  const overlong = new Uint8Array([10, 50, 46, 49, 46, 48, 0, 4, 0xc0, 0x80]);
  check("invalid UTF-8 rejected", throwsBitPackerError(() => edge.Inner.decode(overlong), edge.BitPackerError) === "",
    throwsBitPackerError(() => edge.Inner.decode(overlong), edge.BitPackerError));

  // edge_float32_ref.bin: f and floats whose x10000 is inexact in float32.
  const f32ref = bytes(fs.readFileSync(dir + "/edge/edge_float32_ref.bin"));
  const f32want = canonicalEdge();
  f32want.f = Math.fround(0.29);
  f32want.floats = [Math.fround(0.7), Math.fround(16777.217), Math.fround(-0.29)];
  checkEq("float32 fixture: encode == edge_float32_ref.bin", f32want.encode(), f32ref);
  const plainF32 = new edge.Edge({ ...f32want, f: 0.29, floats: [0.7, 16777.217, -0.29] });
  checkEq("float32 fixture: encode from double literals == edge_float32_ref.bin", plainF32.encode(), f32ref);
  try {
    const f32dec = edge.Edge.decode(f32ref);
    checkEq("float32 fixture: decoded f == 0.29f", f32dec.f, Math.fround(0.29));
    checkEq("float32 fixture: decoded floats", f32dec.floats, f32want.floats);
    checkEq("float32 fixture: re-encode == edge_float32_ref.bin", f32dec.encode(), f32ref);
  } catch (e: unknown) {
    check("float32 fixture: decode", false, String(e));
  }

  const trailing = new Uint8Array(ref.length + 3);
  trailing.set(ref);
  trailing.set([0xff, 0x00, 0x7f], ref.length);
  try {
    checkEq("trailing bytes ignored", edge.Edge.decode(trailing).encode(), ref);
  } catch (e: unknown) {
    check("trailing bytes ignored", false, String(e));
  }

  const encodeRejects = (name: string, e: edge.Edge): void => {
    const r = throwsBitPackerError(() => e.encode(), RangeError);
    check(name, r === "", r);
  };
  encodeRejects("NaN float rejected on encode", new edge.Edge({ f: NaN }));
  encodeRejects("infinite float rejected on encode", new edge.Edge({ floats: [Infinity] }));
  encodeRejects("out-of-range float rejected on encode", new edge.Edge({ f: 1e15 }));
  encodeRejects("NaN double rejected on encode", new edge.Edge({ d: NaN }));
  encodeRejects("infinite double rejected on encode", new edge.Edge({ dNeg: -Infinity }));
  encodeRejects("out-of-range double rejected on encode", new edge.Edge({ doubles: [1e300] }));

  const decodeRejects = (name: string, data: Uint8Array, f: (d: Uint8Array) => unknown): void => {
    const r = throwsBitPackerError(() => f(data), edge.BitPackerError);
    check(name, r === "", r);
  };
  const dflt = new edge.Edge().encode(); // ints count at offset 20
  const withCount = (count: number[]): Uint8Array =>
    new Uint8Array([...dflt.subarray(0, 20), ...count, ...dflt.subarray(21)]);
  decodeRejects("negative array length rejected", withCount([0x01]), (d) => edge.Edge.decode(d));
  decodeRejects("oversized array length rejected", withCount([0xfe, 0xff, 0xff, 0xff, 0x0f]), (d) => edge.Edge.decode(d));
  const innerHead = [10, 50, 46, 49, 46, 48, 0];
  decodeRejects("negative string length rejected", new Uint8Array([...innerHead, 0x01, 0x61]), (d) => edge.Inner.decode(d));
  decodeRejects("oversized string length rejected",
    new Uint8Array([...innerHead, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x40, 0x61]), (d) => edge.Inner.decode(d));
  decodeRejects("varint longer than 10 bytes rejected",
    new Uint8Array([...innerHead.slice(0, 6), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0x00]), (d) => edge.Inner.decode(d));
}

console.log("typescript: " + passed + " passed, " + failed + " failed");
if (failed > 0) process.exitCode = 1;
