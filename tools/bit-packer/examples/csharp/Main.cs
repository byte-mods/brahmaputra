using System;
using Generated;

class Program {
    static void Main() {
        var p = new Player {
            username = "Hero",
            level = 10,
            score = 5000,
            inventory = new [] { "Sword", "Shield", "Potion" }
        };

        var g = new GameState {
            id = 1,
            isActive = true,
            players = new [] { p }
        };

        var data = g.Encode();
        Console.WriteLine($"Encoded size: {data.Length} bytes");

        var decoded = GameState.Decode(data);
        Console.WriteLine($"Decoded Game ID: {decoded.id}");
        if (decoded.players.Length > 0) {
            Console.WriteLine($"Decoded Player: {decoded.players[0].username} (Level {decoded.players[0].level})");
        }
    }
}
