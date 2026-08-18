// Generated Structs


#[derive(Debug, Default, Clone)]
pub struct Player {
	pub username: String,
	pub level: i32,
	pub score: i32,
	pub inventory: Vec<String>,
	
}

#[derive(Debug, Default, Clone)]
pub struct GameState {
	pub id: i32,
	pub isActive: bool,
	pub players: Vec<Player>,
	
}
