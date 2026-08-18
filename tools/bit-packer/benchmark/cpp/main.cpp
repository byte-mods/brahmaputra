// ... (includes) -> Keep existing includes
#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <string>

// JSON
#include <nlohmann/json.hpp>
using json = nlohmann::json;

// BitPacker
#include "bench_complex.hpp"

// MsgPack (if available)
#ifdef USE_MSGPACK
#include <msgpack.hpp>
#endif

// Protobuf
#include "bench_complex.pb.h"
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/arena.h>

// FlatBuffers
#include "bench_complex_generated.h"

// --- Mock/Data Generation ---

std::string random_string(size_t length) {
    auto randchar = []() -> char {
        const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[rand() % max_index];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
}

WorldState create_complex_world() {
    WorldState world;
    world.world_id = 1;
    world.seed = "benchmark_seed";

    // MATCHING RUST: 50 Guilds (Reduced from 1000 due to C++ Protobuf crash on macOS)
    for (int i = 0; i < 50; ++i) {
        Guild g;
        g.name = "Guild_" + std::to_string(i);
        g.description = "A very powerful guild";

        for (int j = 0; j < 20; ++j) {
            Character c;
            c.name = "Char_" + std::to_string(i) + "_" + std::to_string(j);
            c.level = (j + 1);
            c.hp = 100;
            c.mp = 50;
            c.is_alive = true;
            c.position.x = 10;
            c.position.y = 20;
            c.position.z = 30;

            for (int k = 0; k < 5; ++k) c.skills.push_back(k + 1);

            for (int k = 0; k < 10; ++k) {
                Item it;
                it.id = (i * 1000 + j * 100 + k);
                it.name = std::string(50, 'I') + "tem_" + std::to_string(i) + "_" + std::to_string(j) + "_" + std::to_string(k); // Rust does "Item".repeat(50) + ...
                it.value = k * 10;
                it.weight = 1;
                it.rarity = (k % 5 == 0) ? "Rare" : "Common";
                c.inventory.push_back(it);
            }
            g.members.push_back(c);
        }
        world.guilds.push_back(g);
    }

    // MATCHING RUST: Empty loot table
    // for (int i = 0; i < 20; ++i) { ... } 

    return world;
}

// --- FlatBuffers Helper ---
// Basic conversion from WorldState to FB buffer
std::vector<uint8_t> encode_fb(const WorldState& world, flatbuffers::FlatBufferBuilder& builder) {
    builder.Clear();

    std::vector<flatbuffers::Offset<bench_fb::Guild>> guild_offsets;
    for (const auto& g : world.guilds) {
        auto name = builder.CreateString(g.name);
        auto desc = builder.CreateString(g.description);
        
        std::vector<flatbuffers::Offset<bench_fb::Character>> member_offsets;
        for (const auto& c : g.members) {
             auto c_name = builder.CreateString(c.name);
             auto skills = builder.CreateVector(c.skills);
             
             std::vector<flatbuffers::Offset<bench_fb::Item>> inv_offsets;
             for (const auto& item : c.inventory) {
                 auto i_name = builder.CreateString(item.name);
                 auto i_rarity = builder.CreateString(item.rarity);
                 auto i_off = bench_fb::CreateItem(builder, item.id, i_name, item.value, item.weight, i_rarity);
                 inv_offsets.push_back(i_off);
             }
             auto inventory = builder.CreateVector(inv_offsets);
             
             auto pos = bench_fb::Vec3(c.position.x, c.position.y, c.position.z);
             
             auto c_off = bench_fb::CreateCharacter(builder, c_name, c.level, c.hp, c.mp, c.is_alive, &pos, skills, inventory);
             member_offsets.push_back(c_off);
        }
        auto members = builder.CreateVector(member_offsets);
        
        auto g_off = bench_fb::CreateGuild(builder, name, desc, members);
        guild_offsets.push_back(g_off);
    }
    auto guilds = builder.CreateVector(guild_offsets);
    
    // Empty loot table
    std::vector<flatbuffers::Offset<bench_fb::Item>> loot_offsets;
    auto loot_table = builder.CreateVector(loot_offsets);
    
    auto seed = builder.CreateString(world.seed);
    
    auto world_off = bench_fb::CreateWorldState(builder, world.world_id, seed, guilds, loot_table);
    
    builder.Finish(world_off);
    
    return std::vector<uint8_t>(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
}

// Helper to fully decode FB to C++ struct (Deep Copy)
WorldState decode_fb(const std::vector<uint8_t>& data) {
    auto fb_world = bench_fb::GetWorldState(data.data());
    WorldState w;
    w.world_id = fb_world->world_id();
    w.seed = fb_world->seed()->str();
    
    if (fb_world->guilds()) {
        for (const auto* fb_g : *fb_world->guilds()) {
            Guild g;
            g.name = fb_g->name()->str();
            g.description = fb_g->description()->str();
            
            if (fb_g->members()) {
                for (const auto* fb_c : *fb_g->members()) {
                    Character c;
                    c.name = fb_c->name()->str();
                    c.level = fb_c->level();
                    c.hp = fb_c->hp();
                    c.mp = fb_c->mp();
                    c.is_alive = fb_c->is_alive();
                    if (fb_c->position()) {
                        c.position.x = fb_c->position()->x();
                        c.position.y = fb_c->position()->y();
                        c.position.z = fb_c->position()->z();
                    }
                    if (fb_c->skills()) {
                        for (auto s : *fb_c->skills()) c.skills.push_back(s);
                    }
                    if (fb_c->inventory()) {
                        for (const auto* fb_i : *fb_c->inventory()) {
                            Item item;
                            item.id = fb_i->id();
                            item.name = fb_i->name()->str();
                            item.value = fb_i->value();
                            item.weight = fb_i->weight();
                            item.rarity = fb_i->rarity()->str();
                            c.inventory.push_back(item);
                        }
                    }
                    g.members.push_back(c);
                }
            }
            w.guilds.push_back(g);
        }
    }
    return w;
}

// --- JSON Helpers ---
void to_json(json& j, const Vec3& p) { j = json{{"x", p.x}, {"y", p.y}, {"z", p.z}}; }
void to_json(json& j, const Item& p) { j = json{{"id", p.id}, {"name", p.name}, {"value", p.value}, {"weight", p.weight}, {"rarity", p.rarity}}; }
void to_json(json& j, const Character& p) { j = json{{"name", p.name}, {"level", p.level}, {"hp", p.hp}, {"mp", p.mp}, {"is_alive", p.is_alive}, {"position", p.position}, {"skills", p.skills}, {"inventory", p.inventory}}; }
void to_json(json& j, const Guild& p) { j = json{{"name", p.name}, {"description", p.description}, {"members", p.members}}; }
void to_json(json& j, const WorldState& p) { j = json{{"world_id", p.world_id}, {"seed", p.seed}, {"guilds", p.guilds}, {"loot_table", p.loot_table}}; }

// --- Benchmarking ---

int main() {
    srand(time(0));
    std::cout << "🔥 Preparing Benchmark Data..." << std::endl;
    WorldState world = create_complex_world();
    
    // JSON Prep
    json j_world;
    to_json(j_world, world);
    std::string json_str = j_world.dump();

    // MsgPack Prep (via nlohmann)
    std::vector<uint8_t> msgpack_data = json::to_msgpack(j_world);
    
    // Protobuf Prep
    bench_proto::WorldState proto_world;
    proto_world.set_world_id(world.world_id);
    proto_world.set_seed(world.seed);
    for (const auto& g : world.guilds) {
        auto* pg = proto_world.add_guilds();
        pg->set_name(g.name);
        pg->set_description(g.description);
        for (const auto& c : g.members) {
            auto* pc = pg->add_members();
            pc->set_name(c.name);
            pc->set_level(c.level);
            pc->set_hp(c.hp);
            pc->set_mp(c.mp);
            pc->set_is_alive(c.is_alive);
            pc->mutable_position()->set_x(c.position.x);
            pc->mutable_position()->set_y(c.position.y);
            pc->mutable_position()->set_z(c.position.z);
            for (int s : c.skills) pc->add_skills(s);
            for (const auto& i : c.inventory) {
                auto* pi = pc->add_inventory();
                pi->set_id(i.id);
                pi->set_name(i.name);
                pi->set_value(i.value);
                pi->set_weight(i.weight);
                pi->set_rarity(i.rarity);
            }
        }
    }
    std::string proto_data;
    proto_world.SerializeToString(&proto_data);

    // BitPacker Prep
    std::vector<uint8_t> bp_data = world.encode();

    // FlatBuffers Prep
    flatbuffers::FlatBufferBuilder builder(65536);
    std::vector<uint8_t> fb_data = encode_fb(world, builder);
    // std::vector<uint8_t> fb_data; // Dummy

    // Filter iterations - standard is 10 for Complex
    int iterations = 10;
    std::chrono::high_resolution_clock::time_point start, end;

    std::cout << "\nPayload Sizes:\nJSON: " << json_str.length() << "\nMsgPack: " << msgpack_data.size() << "\nBitPacker: " << bp_data.size() << "\nProtobuf: " << proto_data.size() << "\nFlatBuffers: " << fb_data.size() << std::endl;

    // --- Protobuf First for Debugging ---
    std::cout << "\n--- Protobuf Benchmark (10 iterations) ---" << std::endl;
    // Protobuf Encode
    start = std::chrono::high_resolution_clock::now();
    for(int i=0; i<iterations; i++) {
        std::string s;
        proto_world.SerializeToString(&s);
        volatile size_t len = s.length(); (void)len;
    }
    end = std::chrono::high_resolution_clock::now();
    std::cout << "Protobuf Encode: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0 << "s" << std::endl;

    // Protobuf Decode
    /*
    start = std::chrono::high_resolution_clock::now();
    for(int i=0; i<iterations; i++) {
         google::protobuf::Arena arena;
         bench_proto::WorldState* w_decode = google::protobuf::Arena::CreateMessage<bench_proto::WorldState>(&arena);
         if (!w_decode->ParseFromString(proto_data)) {
             std::cout << "Protobuf Parse Failed!" << std::endl;
         }
    }
    end = std::chrono::high_resolution_clock::now();
    std::cout << "Protobuf Decode: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0 << "s" << std::endl;
    */
    std::cout << "Protobuf Decode: SKIPPED (Crashes on this env)" << std::endl;

    std::cout << "\n--- Other Benchmarks ---" << std::endl;
    // JSON
    start = std::chrono::high_resolution_clock::now();
    for(int i=0; i<iterations; i++) {
        std::string s = j_world.dump();
        volatile size_t len = s.length(); (void)len;
    }
    end = std::chrono::high_resolution_clock::now();
    std::cout << "JSON Encode: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0 << "s" << std::endl;

    // MsgPack (nlohmann)
    start = std::chrono::high_resolution_clock::now();
    for(int i=0; i<iterations; i++) {
        std::vector<uint8_t> bytes = json::to_msgpack(j_world);
        volatile size_t len = bytes.size(); (void)len;
    }
    end = std::chrono::high_resolution_clock::now();
    std::cout << "MsgPack Encode: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0 << "s" << std::endl;

    // BitPacker
    start = std::chrono::high_resolution_clock::now();
    for(int i=0; i<iterations; i++) {
        std::vector<uint8_t> bytes = world.encode();
        volatile size_t len = bytes.size(); (void)len;
    }
    end = std::chrono::high_resolution_clock::now();
    std::cout << "BitPacker Encode: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0 << "s" << std::endl;
    
    
    // FlatBuffers Encode
    start = std::chrono::high_resolution_clock::now();
    size_t fb_size = 0;
    for(int i=0; i<iterations; i++) {
         auto bytes = encode_fb(world, builder);
         if (fb_size == 0) fb_size = bytes.size();
    }
    end = std::chrono::high_resolution_clock::now();
    std::cout << "FlatBuffers Encode: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0 << "s" << std::endl;


    // Decoding
    std::cout << "\n--- Decoding Benchmark (10 iterations) ---" << std::endl;
    
    
    // JSON Decode
    start = std::chrono::high_resolution_clock::now();
    for(int i=0; i<iterations; i++) {
         auto j = json::parse(json_str);
    }
    end = std::chrono::high_resolution_clock::now();
    std::cout << "JSON Decode: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0 << "s" << std::endl;

    // MsgPack Decode
    start = std::chrono::high_resolution_clock::now();
    for(int i=0; i<iterations; i++) {
         auto j = json::from_msgpack(msgpack_data);
    }
    end = std::chrono::high_resolution_clock::now();
    std::cout << "MsgPack Decode: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0 << "s" << std::endl;

    // BitPacker Decode
    start = std::chrono::high_resolution_clock::now();
    for(int i=0; i<iterations; i++) {
        WorldState w = WorldState::decode(bp_data);
    }
    end = std::chrono::high_resolution_clock::now();
    std::cout << "BitPacker Decode: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0 << "s" << std::endl;
    

    // FlatBuffers Decode
    std::cout << "[DEBUG] Starting FlatBuffers Decode..." << std::endl;
    start = std::chrono::high_resolution_clock::now();
    for(int i=0; i<iterations; i++) {
         WorldState w = decode_fb(fb_data);
    }
    end = std::chrono::high_resolution_clock::now();
    std::cout << "FlatBuffers Decode: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0 << "s" << std::endl;

    std::cout << "\nPayload Sizes:\nJSON: " << json_str.length() << "\nMsgPack: " << msgpack_data.size() << "\nBitPacker: " << bp_data.size() << "\nProtobuf: " << proto_data.size() << "\nFlatBuffers: " << fb_data.size() << std::endl;

    return 0;
}
