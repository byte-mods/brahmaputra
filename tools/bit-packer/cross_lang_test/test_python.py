#!/usr/bin/env python3
"""Python roundtrip test + write encoded data to file for cross-language testing."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'generated/python'))

from bench_complex import WorldState, Guild, Character, Item, Vec3, _USING_C_EXT

def create_test_data():
    """Create a deterministic test WorldState object."""
    w = WorldState()
    w.world_id = 42
    w.seed = "cross_lang_test"

    # Create a character with nested Vec3 and items
    hero = Character()
    hero.name = "TestHero"
    hero.level = 99
    hero.hp = 1000
    hero.mp = 500
    hero.is_alive = True
    hero.position = Vec3()
    hero.position.x = 10
    hero.position.y = -20
    hero.position.z = 30
    hero.skills = [1, 2, 3, 100]
    
    sword = Item()
    sword.id = 1
    sword.name = "Excalibur"
    sword.value = 9999
    sword.weight = 15
    sword.rarity = "Legendary"
    hero.inventory = [sword]

    guild = Guild()
    guild.name = "TestGuild"
    guild.description = "A test guild for cross-language"
    guild.members = [hero]

    w.guilds = [guild]
    
    potion = Item()
    potion.id = 2
    potion.name = "HealthPotion"
    potion.value = 50
    potion.weight = 1
    potion.rarity = "Common"
    w.loot_table = [potion]
    return w

def verify(decoded, label=""):
    """Verify decoded data matches expected values."""
    assert decoded.world_id == 42, f"{label} world_id mismatch: {decoded.world_id}"
    assert decoded.seed == "cross_lang_test", f"{label} seed mismatch"
    assert len(decoded.guilds) == 1, f"{label} guilds length"
    g = decoded.guilds[0]
    assert g.name == "TestGuild", f"{label} guild name"
    assert g.description == "A test guild for cross-language", f"{label} guild desc"
    assert len(g.members) == 1, f"{label} members length"
    h = g.members[0]
    assert h.name == "TestHero", f"{label} hero name"
    assert h.level == 99, f"{label} hero level"
    assert h.hp == 1000, f"{label} hero hp"
    assert h.mp == 500, f"{label} hero mp"
    assert h.is_alive == True, f"{label} hero alive"
    assert h.position.x == 10, f"{label} pos x"
    assert h.position.y == -20, f"{label} pos y"
    assert h.position.z == 30, f"{label} pos z"
    assert h.skills == [1, 2, 3, 100], f"{label} skills: {h.skills}"
    assert len(h.inventory) == 1, f"{label} inventory length"
    assert h.inventory[0].name == "Excalibur", f"{label} sword name"
    assert h.inventory[0].value == 9999, f"{label} sword value"
    assert h.inventory[0].rarity == "Legendary", f"{label} sword rarity"
    assert len(decoded.loot_table) == 1, f"{label} loot length"
    assert decoded.loot_table[0].name == "HealthPotion", f"{label} potion name"
    assert decoded.loot_table[0].rarity == "Common", f"{label} potion rarity"

if __name__ == "__main__":
    print(f"🐍 Python (C ext: {_USING_C_EXT})")
    
    # 1. Roundtrip test
    w = create_test_data()
    encoded = w.encode()
    print(f"   Encoded: {len(encoded)} bytes")
    
    decoded = WorldState.decode(encoded)
    verify(decoded, "Python roundtrip")
    print("   ✅ Roundtrip PASS")
    
    # 2. Write to file for cross-language testing
    outfile = os.path.join(os.path.dirname(__file__), "test_data.bin")
    with open(outfile, "wb") as f:
        f.write(encoded)
    print(f"   📁 Written to {outfile} ({len(encoded)} bytes)")
    
    # 3. Print hex for debugging
    print(f"   Hex: {encoded[:40].hex()}...")
