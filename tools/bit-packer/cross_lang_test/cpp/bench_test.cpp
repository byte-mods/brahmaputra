// C++ target, bench_complex.buff. run.sh generates gen/bench/cpp first.
#include "bench_complex.hpp"
#include "check.hpp"

static WorldState makeWorld() {
    Item sword{1, "Excalibur", 9999, 15, "Legendary"};
    Character hero;
    hero.name = "TestHero"; hero.level = 99; hero.hp = 1000; hero.mp = 500; hero.is_alive = true;
    hero.position = Vec3{10, -20, 30};
    hero.skills = {1, 2, 3, 100};
    hero.inventory = {sword};
    Guild guild;
    guild.name = "TestGuild"; guild.description = "A test guild for cross-language"; guild.members = {hero};
    WorldState w;
    w.world_id = 42; w.seed = "cross_lang_test"; w.guilds = {guild};
    w.loot_table = {Item{2, "HealthPotion", 50, 1, "Common"}};
    return w;
}

static bool verifyWorld(const WorldState& w) {
    if (w.world_id != 42 || w.seed != "cross_lang_test" || w.guilds.size() != 1) return false;
    const Guild& g = w.guilds[0];
    if (g.name != "TestGuild" || g.description != "A test guild for cross-language" || g.members.size() != 1) return false;
    const Character& h = g.members[0];
    return h.name == "TestHero" && h.level == 99 && h.hp == 1000 && h.mp == 500 && h.is_alive
        && h.position.x == 10 && h.position.y == -20 && h.position.z == 30
        && h.skills == std::vector<int32_t>{1, 2, 3, 100}
        && h.inventory.size() == 1 && h.inventory[0].name == "Excalibur"
        && h.inventory[0].value == 9999 && h.inventory[0].rarity == "Legendary"
        && w.loot_table.size() == 1 && w.loot_table[0].name == "HealthPotion" && w.loot_table[0].rarity == "Common";
}

int main(int argc, char** argv) {
    std::string root = argv[1];
    auto ref = readFile(root + "/test_data.bin");
    auto enc = makeWorld().encode();
    writeFile(root + "/test_data_cpp.bin", enc);
    check("bench encode == test_data.bin", enc == ref, std::to_string(enc.size()) + " vs " + std::to_string(ref.size()) + " bytes");
    try { check("bench decode test_data.bin", verifyWorld(WorldState::decode(ref)), "field mismatch"); }
    catch (const std::exception& e) { check("bench decode test_data.bin", false, e.what()); }
    try {
        WorldState w = WorldState::decode(enc);
        check("bench round-trip", verifyWorld(w), "field mismatch");
        check("bench re-encode == encode", w.encode() == enc, "bytes differ");
    } catch (const std::exception& e) { check("bench round-trip", false, e.what()); }
    auto dec = [](const std::vector<uint8_t>& b) { WorldState::decode(b); };
    check("bench wrong version rejected", rejects(dec, badVersion(ref)), "decoded");
    truncations("bench every truncation rejected", ref, dec);
    check("bench hostile huge array length rejected", rejects(dec, unhex("0a312e302e300000feffffff0f")), "decoded");
    check("bench hostile negative array length rejected", rejects(dec, unhex("0a312e302e30000001")), "decoded");
    return g_failed > 0 ? 1 : 0;
}
