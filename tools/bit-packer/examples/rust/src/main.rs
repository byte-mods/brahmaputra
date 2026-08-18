mod player_impl;
mod player_structs;

use player_impl::*;
pub use player_structs::{GameState, Player};

fn main() {
    println!("--- BitPacker Rust Example ---");

    // 1. Create a GameState object
    let mut game = GameState {
        id: 12345,
        isActive: true,
        players: Vec::new(),
    };

    // 2. Add some players
    let p1 = Player {
        username: "Alice".to_string(),
        level: 10,
        score: 5000,
        inventory: vec!["Sword".to_string(), "Shield".to_string()],
    };
    game.players.push(p1);

    let p2 = Player {
        username: "Bob".to_string(),
        level: 5,
        score: 1200,
        inventory: vec!["Potion".to_string()],
    };
    game.players.push(p2);

    println!("Original Data:");
    println!("  ID: {}", game.id);
    println!("  Active: {}", game.isActive);
    println!("  Players: {}", game.players.len());
    for p in &game.players {
        println!(
            "    - {} (Lvl {}) [Inv: {:?}]",
            p.username, p.level, p.inventory
        );
    }

    // 3. Encode (Serialize)
    let encoded_data = game.encode().expect("Failed to encode");
    println!("\nSerialized to {} bytes", encoded_data.len());

    // 4. Decode (Deserialize)
    let decoded_game = GameState::decode(&encoded_data).expect("Failed to decode");

    println!("\nDecoded Data:");
    println!("  ID: {}", decoded_game.id);
    println!("  Active: {}", decoded_game.isActive);
    println!("  Players: {}", decoded_game.players.len());
    for p in &decoded_game.players {
        println!(
            "    - {} (Lvl {}) [Inv: {:?}]",
            p.username, p.level, p.inventory
        );
    }

    assert_eq!(game.id, decoded_game.id);
    assert_eq!(game.players.len(), decoded_game.players.len());
    println!("\n✅ Roundtrip successful!");
}
