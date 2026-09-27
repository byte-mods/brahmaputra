// Cross-language conformance test for the JavaScript target.
// usage: node crosstest.js <cross_lang_test dir> <gen dir>
'use strict';
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const [root, gen] = process.argv.slice(2);
const B = require(path.join(gen, 'bench', 'js', 'vec3.js'));
const E = require(path.join(gen, 'edge', 'js', 'inner.js'));

let passed = 0, failed = 0;
function check(name, ok, detail) {
    if (ok) { passed++; console.log(`  ok   ${name}`); }
    else { failed++; console.log(`  FAIL ${name} (${detail})`); }
}
const bytesEq = (a, b) => Buffer.from(a).equals(Buffer.from(b));
function rejects(decode, b) { try { decode(b); return false; } catch (e) { return true; } }
function truncations(name, data, decode) {
    for (let cut = 0; cut < data.length; cut++) {
        if (!rejects(decode, data.subarray(0, cut))) {
            check(name, false, `prefix of ${cut}/${data.length} bytes decoded`);
            return;
        }
    }
    check(name, true);
}
function badVersion(data) { const b = Uint8Array.from(data); b[1] ^= 1; return b; }
const fromFile = (p) => new Uint8Array(fs.readFileSync(p));

// --- bench ---
function makeWorld() {
    const pos = Object.assign(new B.Vec3(), { x: 10, y: -20, z: 30 });
    const sword = Object.assign(new B.Item(), { id: 1, name: 'Excalibur', value: 9999, weight: 15, rarity: 'Legendary' });
    const hero = Object.assign(new B.Character(), {
        name: 'TestHero', level: 99, hp: 1000, mp: 500, is_alive: true,
        position: pos, skills: [1, 2, 3, 100], inventory: [sword],
    });
    const guild = Object.assign(new B.Guild(), { name: 'TestGuild', description: 'A test guild for cross-language', members: [hero] });
    const potion = Object.assign(new B.Item(), { id: 2, name: 'HealthPotion', value: 50, weight: 1, rarity: 'Common' });
    return Object.assign(new B.WorldState(), { world_id: 42, seed: 'cross_lang_test', guilds: [guild], loot_table: [potion] });
}
function verifyWorld(w) {
    const g = w.guilds[0], h = g && g.members[0];
    return w.world_id === 42 && w.seed === 'cross_lang_test' && w.guilds.length === 1
        && g.name === 'TestGuild' && g.description === 'A test guild for cross-language' && g.members.length === 1
        && h.name === 'TestHero' && h.level === 99 && h.hp === 1000 && h.mp === 500 && h.is_alive === true
        && h.position.x === 10 && h.position.y === -20 && h.position.z === 30
        && JSON.stringify(h.skills) === '[1,2,3,100]'
        && h.inventory.length === 1 && h.inventory[0].name === 'Excalibur'
        && h.inventory[0].value === 9999 && h.inventory[0].rarity === 'Legendary'
        && w.loot_table.length === 1 && w.loot_table[0].name === 'HealthPotion' && w.loot_table[0].rarity === 'Common';
}

// --- edge --- (long fields are BigInt)
const inner = (big, label) => Object.assign(new E.Inner(), { big, label });
function makeEdge() {
    return Object.assign(new E.Edge(), {
        i_min: -2147483648, i_max: 2147483647, i_zero: 0, i_neg: -1,
        l_min: -9223372036854775808n, l_max: 9223372036854775807n, l_neg: -300n,
        f: -1.25, d: 1234.5625, d_neg: -0.5,
        yes: true, no: false,
        empty: '', unicode: 'héllo wörld ✓ 日本 \u{1F680}',
        ints: [0, -1, 1, -64, 64, -2147483648, 2147483647],
        longs: [0n, -1n, 9223372036854775807n, -9223372036854775808n, 4294967296n],
        floats: [0.0, 0.5, -2.25],
        doubles: [0.0, 3.5, -1000000.25],
        bools: [true, false, true],
        strings: ['', 'a', '日本語'],
        no_ints: [],
        inner: inner(1099511627776n, 'inner'),
        inners: [inner(-1n, ''), inner(0n, 'x')],
        no_inners: [],
    });
}
const show = (v) => require('util').inspect(v, { depth: 5 });

const ref = fromFile(path.join(root, 'test_data.bin'));
const edgeRef = fromFile(path.join(root, 'edge', 'edge_ref.bin'));

const enc = makeWorld().encode();
fs.writeFileSync(path.join(root, 'test_data_js.bin'), enc);
check('bench encode == test_data.bin', bytesEq(enc, ref), `${enc.length} vs ${ref.length} bytes`);
try { check('bench decode test_data.bin', verifyWorld(B.WorldState.decode(ref)), 'field mismatch'); }
catch (e) { check('bench decode test_data.bin', false, e.message); }
try {
    const w = B.WorldState.decode(Uint8Array.from(enc));
    check('bench round-trip', verifyWorld(w), 'field mismatch');
    check('bench re-encode == encode', bytesEq(w.encode(), enc), 'bytes differ');
} catch (e) { check('bench round-trip', false, e.message); }
check('bench wrong version rejected', rejects((b) => B.WorldState.decode(b), badVersion(ref)), 'decoded');
truncations('bench every truncation rejected', ref, (b) => B.WorldState.decode(b));

const eenc = makeEdge().encode();
check('edge encode == edge_ref.bin', bytesEq(eenc, edgeRef), Buffer.from(eenc).toString('hex'));
try {
    const e = E.Edge.decode(edgeRef);
    const w = makeEdge();
    for (const field of Object.keys(w)) {
        let ok = true;
        try { assert.deepStrictEqual(e[field], w[field]); } catch (_) { ok = false; }
        check(`edge decode ${field}`, ok, `got ${show(e[field])}, want ${show(w[field])}`);
    }
    check('edge re-encode == edge_ref.bin', bytesEq(e.encode(), edgeRef), 'bytes differ');
} catch (e) { check('edge decode edge_ref.bin', false, e.message); }
check('edge wrong version rejected', rejects((b) => E.Edge.decode(b), badVersion(edgeRef)), 'decoded');
truncations('edge every truncation rejected', edgeRef, (b) => E.Edge.decode(b));

// float32 variant: x10000 must be computed in single precision
const f32ref = fromFile(path.join(root, 'edge', 'edge_float32_ref.bin'));
const fv = Object.assign(makeEdge(), { f: 0.29, floats: [0.7, 16777.217, -0.29] });
check('edge float32 encode == edge_float32_ref.bin', bytesEq(fv.encode(), f32ref), Buffer.from(fv.encode()).toString('hex'));
try {
    const e = E.Edge.decode(f32ref);
    const want = fv.floats.map(Math.fround);
    check('edge float32 decode', e.f === Math.fround(0.29) && JSON.stringify(e.floats) === JSON.stringify(want), `${e.f} ${e.floats}`);
} catch (e) { check('edge float32 decode', false, e.message); }
// trailing bytes after the root class are ignored
try {
    const trailing = Uint8Array.from([...edgeRef, 0x00, 0xff]);
    check('edge trailing bytes ignored', bytesEq(E.Edge.decode(trailing).encode(), edgeRef), 'decoded value differs');
} catch (e) { check('edge trailing bytes ignored', false, e.message); }
// unencodable floats are errors, not saturated
const nan = Object.assign(makeEdge(), { d: NaN });
check('edge encode NaN double rejected', rejects(() => nan.encode(), null), 'encoded');
const big = Object.assign(makeEdge(), { f: 1e30 });
check('edge encode out-of-range float rejected', rejects(() => big.encode(), null), 'encoded');

// hostile input: bogus lengths must fail fast, not allocate or crash
check('bench hostile huge array length rejected', rejects((b) => B.WorldState.decode(b), Buffer.from('0a312e302e300000feffffff0f', 'hex')), 'decoded');
check('bench hostile negative array length rejected', rejects((b) => B.WorldState.decode(b), Buffer.from('0a312e302e30000001', 'hex')), 'decoded');
check('edge hostile huge string length rejected', rejects((b) => E.Inner.decode(b), Buffer.from('0a322e312e30008080808010', 'hex')), 'decoded');
check('edge hostile negative string length rejected', rejects((b) => E.Inner.decode(b), Buffer.from('0a322e312e300001', 'hex')), 'decoded');
check('edge hostile endless varint rejected', rejects((b) => E.Inner.decode(b), Buffer.from('0a322e312e30ffffffffffffffffffffff', 'hex')), 'decoded');
check('edge hostile invalid UTF-8 string rejected', rejects((b) => E.Inner.decode(b), Buffer.from('0a322e312e300002ff', 'hex')), 'decoded');

process.exit(failed > 0 ? 1 : 0);
