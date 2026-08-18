mod world_state; // Generated module. NOTE: File name might be worldstate.rs depending on generator.
use std::io::Write;
use std::time::Instant;
use world_state::{Character, Guild, Item, Vec3, WorldState};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup Data
    let mut guilds = vec![];
    for g in 0..1000 {
        let mut members = vec![];
        for c in 0..20 {
            let mut inventory = vec![];
            for i in 0..10 {
                inventory.push(Item {
                    id: (g * 1000 + c * 100 + i) as i32,
                    name: format!("ItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItemItem_{}_{}_{}", g, c, i),
                    value: (i * 10) as i32,
                    weight: 1,
                    rarity: if i % 5 == 0 {
                        "Rare".to_string()
                    } else {
                        "Common".to_string()
                    },
                });
            }
            members.push(Character {
                name: format!("Char_{}_{}", g, c),
                level: (c + 1) as i32,
                hp: 100,
                mp: 50,
                is_alive: true,
                position: Vec3 {
                    x: 10,
                    y: 20,
                    z: 30,
                },
                skills: vec![1, 2, 3, 4, 5],
                inventory,
            });
        }
        guilds.push(Guild {
            name: format!("Guild_{}", g),
            description: "A very powerful guild".to_string(),
            members,
        });
    }

    let world = WorldState {
        world_id: 1,
        seed: "benchmark_seed".to_string(),
        guilds,
        loot_table: vec![],
    };

    println!("Starting Benchmark with {} Guilds...", world.guilds.len());
    let iterations = 100;

    // --- BitPacker ---
    let start = Instant::now();
    let mut bitpacker_size = 0;
    for _ in 0..iterations {
        let encoded = world.encode()?;
        bitpacker_size = encoded.len();
        let _decoded = WorldState::decode(&encoded)?;
    }
    let duration_bp = start.elapsed();
    println!(
        "BitPacker: {:?} (Size: {} bytes)",
        duration_bp, bitpacker_size
    );

    // --- JSON ---
    let start = Instant::now();
    let mut json_size = 0;
    for _ in 0..iterations {
        let encoded = serde_json::to_string(&world)?;
        json_size = encoded.len();
        let _decoded: WorldState = serde_json::from_str(&encoded)?;
    }
    let duration_json = start.elapsed();
    println!("JSON:      {:?} (Size: {} bytes)", duration_json, json_size);

    // --- MsgPack ---
    let start = Instant::now();
    let mut msgpack_size = 0;
    for _ in 0..iterations {
        let encoded = rmp_serde::to_vec(&world)?;
        msgpack_size = encoded.len();
        let _decoded: WorldState = rmp_serde::from_slice(&encoded)?;
    }
    let duration_msg = start.elapsed();
    println!(
        "MsgPack:   {:?} (Size: {} bytes)",
        duration_msg, msgpack_size
    );

    // Comparison
    println!("\n--- Comparison (Lower is Better) ---");
    println!("Time Ratio (vs BitPacker):");
    println!(
        "  JSON:    {:.2}x",
        duration_json.as_secs_f64() / duration_bp.as_secs_f64()
    );
    println!(
        "  MsgPack: {:.2}x",
        duration_msg.as_secs_f64() / duration_bp.as_secs_f64()
    );
    println!("Size Ratio (vs BitPacker):");
    println!(
        "  JSON:    {:.2}x",
        json_size as f64 / bitpacker_size as f64
    );
    println!(
        "  MsgPack: {:.2}x",
        msgpack_size as f64 / bitpacker_size as f64
    );

    Ok(())
}
