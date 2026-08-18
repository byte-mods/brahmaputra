// C++ roundtrip test + cross-language decode test
#include "generated/cpp/bench_complex.hpp"
#include "generated/cpp/bench_complex.cpp"
#include <iostream>
#include <fstream>
#include <cassert>
#include <cstring>
#include <filesystem>

WorldState createTestData() {
    WorldState w;
    w.world_id = 42;
    w.seed = "cross_lang_test";

    Character hero;
    hero.name = "TestHero";
    hero.level = 99;
    hero.hp = 1000;
    hero.mp = 500;
    hero.is_alive = true;
    hero.position = {10, -20, 30};
    hero.skills = {1, 2, 3, 100};

    Item sword;
    sword.id = 1;
    sword.name = "Excalibur";
    sword.value = 9999;
    sword.weight = 15;
    sword.rarity = "Legendary";
    hero.inventory.push_back(sword);

    Guild guild;
    guild.name = "TestGuild";
    guild.description = "A test guild for cross-language";
    guild.members.push_back(hero);

    w.guilds.push_back(guild);

    Item potion;
    potion.id = 2;
    potion.name = "HealthPotion";
    potion.value = 50;
    potion.weight = 1;
    potion.rarity = "Common";
    w.loot_table.push_back(potion);
    return w;
}

void verify(const WorldState& d, const std::string& label) {
    assert(d.world_id == 42);
    assert(d.seed == "cross_lang_test");
    assert(d.guilds.size() == 1);
    assert(d.guilds[0].name == "TestGuild");
    assert(d.guilds[0].description == "A test guild for cross-language");
    assert(d.guilds[0].members.size() == 1);
    auto& h = d.guilds[0].members[0];
    assert(h.name == "TestHero");
    assert(h.level == 99);
    assert(h.hp == 1000);
    assert(h.mp == 500);
    assert(h.is_alive == true);
    assert(h.position.x == 10);
    assert(h.position.y == -20);
    assert(h.position.z == 30);
    assert(h.skills.size() == 4);
    assert(h.skills[0] == 1);
    assert(h.skills[3] == 100);
    assert(h.inventory.size() == 1);
    assert(h.inventory[0].name == "Excalibur");
    assert(h.inventory[0].value == 9999);
    assert(h.inventory[0].rarity == "Legendary");
    assert(d.loot_table.size() == 1);
    assert(d.loot_table[0].name == "HealthPotion");
    assert(d.loot_table[0].rarity == "Common");
}

int main() {
    std::cout << "🔷 C++" << std::endl;

    // 1. Roundtrip
    WorldState w = createTestData();
    auto encoded = w.encode();
    std::cout << "   Encoded: " << encoded.size() << " bytes" << std::endl;

    WorldState decoded = WorldState::decode(encoded);
    verify(decoded, "C++ roundtrip");
    std::cout << "   ✅ Roundtrip PASS" << std::endl;

    // 2. Write to file
    std::ofstream out("test_data_cpp.bin", std::ios::binary);
    out.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
    out.close();
    std::cout << "   📁 Written to test_data_cpp.bin" << std::endl;

    // 3. Cross-language: decode Python's data
    std::ifstream pyIn("test_data.bin", std::ios::binary);
    if (pyIn.good()) {
        std::vector<uint8_t> pyData((std::istreambuf_iterator<char>(pyIn)),
                                     std::istreambuf_iterator<char>());
        pyIn.close();
        WorldState pyDecoded = WorldState::decode(pyData);
        verify(pyDecoded, "C++←Python cross-lang");
        std::cout << "   ✅ Cross-language decode (Python→C++) PASS" << std::endl;
    } else {
        std::cout << "   ⚠️ No Python test_data.bin found" << std::endl;
    }

    return 0;
}
