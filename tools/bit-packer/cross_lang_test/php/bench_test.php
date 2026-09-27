<?php
// PHP target, bench_complex.buff. usage: php bench_test.php <cross_lang_test dir> <gen dir>
require __DIR__ . '/check.php';
require $argv[2] . '/bench/php/Vec3.php';
$root = $argv[1];

function makeWorld(): WorldState {
    $pos = new Vec3(); $pos->x = 10; $pos->y = -20; $pos->z = 30;
    $sword = new Item(); $sword->id = 1; $sword->name = "Excalibur"; $sword->value = 9999; $sword->weight = 15; $sword->rarity = "Legendary";
    $hero = new Character();
    $hero->name = "TestHero"; $hero->level = 99; $hero->hp = 1000; $hero->mp = 500; $hero->is_alive = true;
    $hero->position = $pos; $hero->skills = [1, 2, 3, 100]; $hero->inventory = [$sword];
    $guild = new Guild(); $guild->name = "TestGuild"; $guild->description = "A test guild for cross-language"; $guild->members = [$hero];
    $potion = new Item(); $potion->id = 2; $potion->name = "HealthPotion"; $potion->value = 50; $potion->weight = 1; $potion->rarity = "Common";
    $w = new WorldState(); $w->world_id = 42; $w->seed = "cross_lang_test"; $w->guilds = [$guild]; $w->loot_table = [$potion];
    return $w;
}

function verifyWorld(WorldState $w): bool {
    if ($w->world_id !== 42 || $w->seed !== "cross_lang_test" || count($w->guilds) !== 1) return false;
    $g = $w->guilds[0];
    if ($g->name !== "TestGuild" || $g->description !== "A test guild for cross-language" || count($g->members) !== 1) return false;
    $h = $g->members[0];
    return $h->name === "TestHero" && $h->level === 99 && $h->hp === 1000 && $h->mp === 500 && $h->is_alive === true
        && $h->position->x === 10 && $h->position->y === -20 && $h->position->z === 30
        && $h->skills === [1, 2, 3, 100]
        && count($h->inventory) === 1 && $h->inventory[0]->name === "Excalibur"
        && $h->inventory[0]->value === 9999 && $h->inventory[0]->rarity === "Legendary"
        && count($w->loot_table) === 1 && $w->loot_table[0]->name === "HealthPotion" && $w->loot_table[0]->rarity === "Common";
}

$ref = file_get_contents("$root/test_data.bin");
$enc = makeWorld()->encode();
file_put_contents("$root/test_data_php.bin", $enc);
check("bench encode == test_data.bin", $enc === $ref, strlen($enc) . " vs " . strlen($ref) . " bytes");
try { check("bench decode test_data.bin", verifyWorld(WorldState::decode($ref)), "field mismatch"); }
catch (Throwable $e) { check("bench decode test_data.bin", false, $e->getMessage()); }
try {
    $w = WorldState::decode($enc);
    check("bench round-trip", verifyWorld($w), "field mismatch");
    check("bench re-encode == encode", $w->encode() === $enc, "bytes differ");
} catch (Throwable $e) { check("bench round-trip", false, $e->getMessage()); }
$dec = fn(string $b) => WorldState::decode($b);
check("bench wrong version rejected", rejects($dec, badVersion($ref)), "decoded");
truncations("bench every truncation rejected", $ref, $dec);
// hostile input: bogus lengths must fail fast, not allocate or crash
check("bench hostile huge array length rejected", rejects($dec, hex2bin("0a312e302e300000feffffff0f")), "decoded");
check("bench hostile negative array length rejected", rejects($dec, hex2bin("0a312e302e30000001")), "decoded");
exit(finish());
