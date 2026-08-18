package example;

import java.util.Arrays;

public class Example {
    public static void main(String[] args) {
        Player p = new Player();
        p.username = "Hero";
        p.level = 10;
        p.score = 5000;
        p.inventory = new String[]{"Sword", "Shield", "Potion"};

        GameState g = new GameState();
        g.id = 1;
        g.isActive = true;
        g.players = new Player[]{p};

        // Encode
        byte[] data = g.encode();
        System.out.println("Encoded size: " + data.length + " bytes");

        // Decode
        try {
            GameState decoded = GameState.decode(data);
            System.out.println("Decoded Game ID: " + decoded.id);
            if (decoded.players.length > 0) {
                System.out.println("Decoded Player: " + decoded.players[0].username + " (Level " + decoded.players[0].level + ")");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
