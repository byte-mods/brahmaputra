const { Player, GameState } = require('./game');

const p = new Player();
p.username = "Hero";
p.level = 10;
p.score = 5000;
p.inventory = ["Sword", "Shield", "Potion"];

const g = new GameState();
g.id = 1;
g.isActive = true;
g.players = [p];

// Encode
const data = g.encode();
console.log(`Encoded size: ${data.length} bytes`);

// Decode
const decoded = GameState.decode(data);
console.log(`Decoded Game ID: ${decoded.id}`);
if (decoded.players.length > 0) {
    console.log(`Decoded Player: ${decoded.players[0].username} (Level ${decoded.players[0].level})`);
}
