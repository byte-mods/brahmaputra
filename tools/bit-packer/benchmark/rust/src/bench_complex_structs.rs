// Generated Structs
use serde::{Serialize, Deserialize};


#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Vec3 {
	pub x: i32,
	pub y: i32,
	pub z: i32,
	
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Item {
	pub id: i32,
	pub name: String,
	pub value: i32,
	pub weight: i32,
	pub rarity: String,
	
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Character {
	pub name: String,
	pub level: i32,
	pub hp: i32,
	pub mp: i32,
	pub is_alive: bool,
	pub position: Vec3,
	pub skills: Vec<i32>,
	pub inventory: Vec<Item>,
	
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Guild {
	pub name: String,
	pub description: String,
	pub members: Vec<Character>,
	
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct WorldState {
	pub world_id: i32,
	pub seed: String,
	pub guilds: Vec<Guild>,
	pub loot_table: Vec<Item>,
	
}
