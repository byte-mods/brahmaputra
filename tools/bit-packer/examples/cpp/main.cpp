#include <iostream>
#include <vector>
#include "game.hpp"

int main() {
    Player p;
    p.username = "Hero";
    p.level = 10;
    p.score = 5000;
    p.inventory = {"Sword", "Shield", "Potion"};

    GameState g;
    g.id = 1;
    g.isActive = true;
    g.players.push_back(p);

    // Encode
    std::vector<uint8_t> data = g.Encode();
    std::cout << "Encoded size: " << data.size() << " bytes" << std::endl;

    // Decode
    GameState decoded = GameState::Decode(data);
    std::cout << "Decoded Game ID: " << decoded.id << std::endl;
    if (!decoded.players.empty()) {
        std::cout << "Decoded Player: " << decoded.players[0].username << " (Level " << decoded.players[0].level << ")" << std::endl;
    }

    return 0;
}
