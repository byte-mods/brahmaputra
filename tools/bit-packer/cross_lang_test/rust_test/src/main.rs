mod lib;
use lib::*;
use std::fs;

fn create_test_data() -> WorldState {
    WorldState {
        world_id: 42,
        seed: "cross_lang_test".to_string(),
        guilds: vec![Guild {
            name: "TestGuild".to_string(),
            description: "A test guild for cross-language".to_string(),
            members: vec![Character {
                name: "TestHero".to_string(),
                level: 99,
                hp: 1000,
                mp: 500,
                is_alive: true,
                position: Vec3 { x: 10, y: -20, z: 30 },
                skills: vec![1, 2, 3, 100],
                inventory: vec![Item {
                    id: 1,
                    name: "Excalibur".to_string(),
                    value: 9999,
                    weight: 15,
                    rarity: "Legendary".to_string(),
                }],
            }],
        }],
        loot_table: vec![Item {
            id: 2,
            name: "HealthPotion".to_string(),
            value: 50,
            weight: 1,
            rarity: "Common".to_string(),
        }],
    }
}

fn verify(d: &WorldState, label: &str) {
    assert_eq!(d.world_id, 42, "{} world_id", label);
    assert_eq!(d.seed, "cross_lang_test", "{} seed", label);
    assert_eq!(d.guilds.len(), 1, "{} guilds", label);
    assert_eq!(d.guilds[0].name, "TestGuild", "{} guild", label);
    assert_eq!(d.guilds[0].members[0].name, "TestHero", "{} hero", label);
    assert_eq!(d.guilds[0].members[0].level, 99, "{} level", label);
    assert_eq!(d.guilds[0].members[0].hp, 1000, "{} hp", label);
    assert_eq!(d.guilds[0].members[0].position.x, 10, "{} x", label);
    assert_eq!(d.guilds[0].members[0].position.y, -20, "{} y", label);
    assert_eq!(d.guilds[0].members[0].position.z, 30, "{} z", label);
    assert_eq!(d.guilds[0].members[0].skills, vec![1, 2, 3, 100], "{} skills", label);
    assert_eq!(d.guilds[0].members[0].inventory[0].name, "Excalibur", "{} sword", label);
    assert_eq!(d.guilds[0].members[0].inventory[0].value, 9999, "{} sword_val", label);
    assert_eq!(d.loot_table[0].name, "HealthPotion", "{} potion", label);
    assert_eq!(d.loot_table[0].rarity, "Common", "{} rarity", label);
}

fn main() {
    println!("🦀 Rust");

    // 1. Roundtrip
    let w = create_test_data();
    let encoded = w.encode().unwrap();
    println!("   Encoded: {} bytes", encoded.len());

    let decoded = WorldState::decode(&encoded).unwrap();
    verify(&decoded, "Rust roundtrip");
    println!("   ✅ Roundtrip PASS");

    // 2. Write to file
    fs::write("test_data_rust.bin", &encoded).unwrap();
    println!("   📁 Written to test_data_rust.bin");

    // 3. Cross-language: decode Python's data
    if let Ok(py_data) = fs::read("test_data.bin") {
        let py_decoded = WorldState::decode(&py_data).unwrap();
        verify(&py_decoded, "Rust←Python cross-lang");
        println!("   ✅ Cross-language decode (Python→Rust) PASS");
    } else {
        println!("   ⚠️  No Python test_data.bin found");
    }
}
