use rand::prelude::*;
use std::time::Instant;

// BitPacker generated code
mod bench_complex_impl;
mod bench_complex_structs;
use bench_complex_impl::*;
use bench_complex_structs::{
    Character as BpCharacter, Guild as BpGuild, Item as BpItem, Vec3 as BpVec3,
    WorldState as BpWorldState,
};

// FlatBuffers generated code
#[allow(dead_code, unused_imports)]
#[path = "bench_complex_generated.rs"]
pub mod bench_complex_generated;

mod bench_complex_fb;
mod bench_complex_proto;

fn main() {
    println!("Generating Data...");
    let bitpacker_world = create_bitpacker_data();
    let proto_world = bench_complex_proto::from_bp(&bitpacker_world);

    println!("Data Generation Complete. Starting Benchmark...");
    const ITERATIONS: usize = 10;

    // --- BitPacker ---
    {
        let start = Instant::now();
        let mut size = 0;
        for _ in 0..ITERATIONS {
            let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
            bitpacker_world.encode_to(&mut buf).unwrap();
            let bytes_vec = buf.finish();
            if size == 0 {
                size = bytes_vec.len();
            }
            let bytes = bytes_vec.as_slice();
            let mut dbuf = ZeroCopyByteBuff::from_slice(bytes, Endian::Big);
            let _ = BpWorldState::decode_from(&mut dbuf).unwrap();
        }
        let duration = start.elapsed().as_secs_f64();
        println!("BitPacker:   {:.3}s (Size: {} bytes)", duration, size);
    }

    // --- JSON ---
    {
        let start = Instant::now();
        let mut size = 0;
        for _ in 0..ITERATIONS {
            let json_bytes = serde_json::to_vec(&bitpacker_world).unwrap();
            if size == 0 {
                size = json_bytes.len();
            }
            let _: BpWorldState = serde_json::from_slice(&json_bytes).unwrap();
        }
        let duration = start.elapsed().as_secs_f64();
        println!("JSON:        {:.3}s (Size: {} bytes)", duration, size);
    }

    // --- MsgPack ---
    {
        let start = Instant::now();
        let mut size = 0;
        for _ in 0..ITERATIONS {
            let msgpack_bytes = rmp_serde::to_vec(&bitpacker_world).unwrap();
            if size == 0 {
                size = msgpack_bytes.len();
            }
            let _: BpWorldState = rmp_serde::from_slice(&msgpack_bytes).unwrap();
        }
        let duration = start.elapsed().as_secs_f64();
        println!("MsgPack:     {:.3}s (Size: {} bytes)", duration, size);
    }

    // --- Protobuf ---
    {
        let start = Instant::now();
        let mut size = 0;
        for _ in 0..ITERATIONS {
            let bytes = bench_complex_proto::encode(&proto_world);
            if size == 0 {
                size = bytes.len();
            }
            let _ = bench_complex_proto::decode(&bytes);
        }
        let duration = start.elapsed().as_secs_f64();
        println!("Protobuf:    {:.3}s (Size: {} bytes)", duration, size);
    }

    // --- FlatBuffers ---
    {
        let start = Instant::now();
        let mut size = 0;
        for _ in 0..ITERATIONS {
            let bytes = bench_complex_fb::encode(&bitpacker_world);
            if size == 0 {
                size = bytes.len();
            }
            let _ = bench_complex_fb::decode(&bytes);
        }
        let duration = start.elapsed().as_secs_f64();
        println!("FlatBuffers: {:.3}s (Size: {} bytes)", duration, size);
    }
}

fn create_bitpacker_data() -> BpWorldState {
    let mut world = BpWorldState {
        world_id: 1,
        seed: "benchmark_seed".to_string(),
        guilds: Vec::new(),
        loot_table: Vec::new(),
    };

    for g in 0..1000 {
        let mut guild = BpGuild {
            name: format!("Guild_{}", g),
            description: "A very powerful guild".to_string(),
            members: Vec::new(),
        };
        for c in 0..20 {
            let mut char = BpCharacter {
                name: format!("Char_{}_{}", g, c),
                level: (c + 1) as i32,
                hp: 100,
                mp: 50,
                is_alive: true,
                position: BpVec3 {
                    x: 10,
                    y: 20,
                    z: 30,
                },
                skills: vec![1, 2, 3, 4, 5],
                inventory: Vec::new(),
            };
            for i in 0..10 {
                let item = BpItem {
                    id: (g * 1000 + c * 100 + i) as i32,
                    name: "Item".repeat(50) + &format!("_{}_{}_{}", g, c, i),
                    value: (i * 10) as i32,
                    weight: 1,
                    rarity: if i % 5 == 0 {
                        "Rare".to_string()
                    } else {
                        "Common".to_string()
                    },
                };
                char.inventory.push(item);
            }
            guild.members.push(char);
        }
        world.guilds.push(guild);
    }
    world
}
