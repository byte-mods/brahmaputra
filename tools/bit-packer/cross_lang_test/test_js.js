#!/usr/bin/env node
/**
 * JavaScript roundtrip test + cross-language decode test.
 */
const fs = require('fs');
const path = require('path');
const { WorldState, Guild, Character, Item, Vec3 } = require('./generated/js/vec3');

function createTestData() {
    const w = new WorldState();
    w.world_id = 42;
    w.seed = "cross_lang_test";

    const hero = new Character();
    hero.name = "TestHero";
    hero.level = 99;
    hero.hp = 1000;
    hero.mp = 500;
    hero.is_alive = true;
    hero.position = new Vec3();
    hero.position.x = 10;
    hero.position.y = -20;
    hero.position.z = 30;
    hero.skills = [1, 2, 3, 100];

    const sword = new Item();
    sword.id = 1;
    sword.name = "Excalibur";
    sword.value = 9999;
    sword.weight = 15;
    sword.rarity = "Legendary";
    hero.inventory = [sword];

    const guild = new Guild();
    guild.name = "TestGuild";
    guild.description = "A test guild for cross-language";
    guild.members = [hero];

    w.guilds = [guild];

    const potion = new Item();
    potion.id = 2;
    potion.name = "HealthPotion";
    potion.value = 50;
    potion.weight = 1;
    potion.rarity = "Common";
    w.loot_table = [potion];
    return w;
}

function verify(d, label) {
    console.assert(d.world_id === 42, `${label} world_id`);
    console.assert(d.seed === "cross_lang_test", `${label} seed`);
    console.assert(d.guilds.length === 1, `${label} guilds len`);
    console.assert(d.guilds[0].name === "TestGuild", `${label} guild name`);
    console.assert(d.guilds[0].members[0].name === "TestHero", `${label} hero name`);
    console.assert(d.guilds[0].members[0].level === 99, `${label} hero level`);
    console.assert(d.guilds[0].members[0].hp === 1000, `${label} hero hp`);
    console.assert(d.guilds[0].members[0].position.x === 10, `${label} pos x`);
    console.assert(d.guilds[0].members[0].position.y === -20, `${label} pos y`);
    console.assert(d.guilds[0].members[0].position.z === 30, `${label} pos z`);
    console.assert(d.guilds[0].members[0].skills.length === 4, `${label} skills len`);
    console.assert(d.guilds[0].members[0].inventory[0].name === "Excalibur", `${label} sword`);
    console.assert(d.guilds[0].members[0].inventory[0].value === 9999, `${label} sword val`);
    console.assert(d.loot_table[0].name === "HealthPotion", `${label} potion`);
    console.assert(d.loot_table[0].rarity === "Common", `${label} potion rarity`);
}

console.log("🟨 JavaScript (Node.js)");

// 1. Roundtrip
const w = createTestData();
const encoded = w.encode();
console.log(`   Encoded: ${encoded.length} bytes`);

const decoded = WorldState.decode(encoded);
verify(decoded, "JS roundtrip");
console.log("   ✅ Roundtrip PASS");

// 2. Write encoded data to file
const outfile = path.join(__dirname, "test_data_js.bin");
fs.writeFileSync(outfile, Buffer.from(encoded));
console.log(`   📁 Written to ${outfile}`);

// 3. Cross-language: decode Python's encoded data
const pyFile = path.join(__dirname, "test_data.bin");
if (fs.existsSync(pyFile)) {
    const pyData = new Uint8Array(fs.readFileSync(pyFile));
    const pyDecoded = WorldState.decode(pyData);
    verify(pyDecoded, "JS←Python cross-lang");
    console.log("   ✅ Cross-language decode (Python→JS) PASS");
} else {
    console.log("   ⚠️ No Python test_data.bin found (run test_python.py first)");
}
