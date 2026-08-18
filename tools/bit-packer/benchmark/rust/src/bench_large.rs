use std::fs::File;
use std::io::Read;
use std::time::Instant;

mod bench_complex_impl;
mod bench_complex_structs;
use bench_complex_impl::*;
use bench_complex_structs::WorldState;

fn main() {
    let file_name = "../../large_payload.bin"; // Path relative to where cargo run is executed (benchmark/rust)
    let mut file = match File::open(file_name) {
        Ok(f) => f,
        Err(_) => {
            // Try relative from project root if running from there
            match File::open("large_payload.bin") {
                Ok(f) => f,
                Err(e) => panic!("Failed to open {}: {}", file_name, e),
            }
        }
    };

    let mut data = Vec::new();
    file.read_to_end(&mut data).unwrap();
    println!("Read {} bytes from large_payload.bin", data.len());

    // Decode (1 run is usually enough for compiled languages like Rust for this size, but let's be consistent)
    // Actually, Rust is AOT compiled, so no JIT warmup like Java/JS needed.
    let start = Instant::now();
    let world = WorldState::decode(&data).expect("Failed to decode");
    // Access something to ensure no DCE (though decode typically parses fully)
    println!("Decoded {} guilds", world.guilds.len());
    let duration = start.elapsed().as_secs_f64();
    println!("Decode time: {:.4}s", duration);

    // Encode
    let start = Instant::now();
    let encoded_data = world.encode().expect("Failed to encode");
    let duration = start.elapsed().as_secs_f64();
    println!("Encode time: {:.4}s", duration);
    println!("Encoded size: {} bytes", encoded_data.len());

    if encoded_data.len() != data.len() {
        println!("WARNING: Encoded size mismatch! Original: {}, New: {}", data.len(), encoded_data.len());
    }
}
