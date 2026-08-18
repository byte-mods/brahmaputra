
#ifndef Bench_complex_HPP
#define Bench_complex_HPP

#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#define VERSION "1.0.0"

// --- ZeroCopyByteBuff Class ---
class ZeroCopyByteBuff {
private:
    std::vector<uint8_t> buffer;
    size_t offset;

public:
    ZeroCopyByteBuff();
    ZeroCopyByteBuff(const std::vector<uint8_t>& data);
    
    const std::vector<uint8_t>& getBuffer() const;
    
    // Write
    void putInt32(int32_t v);
    void putInt64(int64_t v);
    void putFloat(float v);
    void putDouble(double v);
    void putBool(bool v);
    void putString(const std::string& v);
    void putVarInt64(int64_t v);

    // Read
    int32_t getInt32();
    int64_t getInt64();
    float getFloat();
    double getDouble();
    bool getBool();
    std::string getString();
    int64_t getVarInt64();
};

// --- Generated Classes ---

struct Vec3 {
    int32_t x;
    int32_t y;
    int32_t z;
    

    void encode(ZeroCopyByteBuff& buf) const;
    void decode(ZeroCopyByteBuff& buf);
    
    // Helper to encode directly to bytes
    std::vector<uint8_t> encode() const;
    
    // Helper to decode directly from bytes
    static Vec3 decode(const std::vector<uint8_t>& data);
};

struct Item {
    int32_t id;
    std::string name;
    int32_t value;
    int32_t weight;
    std::string rarity;
    

    void encode(ZeroCopyByteBuff& buf) const;
    void decode(ZeroCopyByteBuff& buf);
    
    // Helper to encode directly to bytes
    std::vector<uint8_t> encode() const;
    
    // Helper to decode directly from bytes
    static Item decode(const std::vector<uint8_t>& data);
};

struct Character {
    std::string name;
    int32_t level;
    int32_t hp;
    int32_t mp;
    bool is_alive;
    Vec3 position;
    std::vector<int32_t> skills;
    std::vector<Item> inventory;
    

    void encode(ZeroCopyByteBuff& buf) const;
    void decode(ZeroCopyByteBuff& buf);
    
    // Helper to encode directly to bytes
    std::vector<uint8_t> encode() const;
    
    // Helper to decode directly from bytes
    static Character decode(const std::vector<uint8_t>& data);
};

struct Guild {
    std::string name;
    std::string description;
    std::vector<Character> members;
    

    void encode(ZeroCopyByteBuff& buf) const;
    void decode(ZeroCopyByteBuff& buf);
    
    // Helper to encode directly to bytes
    std::vector<uint8_t> encode() const;
    
    // Helper to decode directly from bytes
    static Guild decode(const std::vector<uint8_t>& data);
};

struct WorldState {
    int32_t world_id;
    std::string seed;
    std::vector<Guild> guilds;
    std::vector<Item> loot_table;
    

    void encode(ZeroCopyByteBuff& buf) const;
    void decode(ZeroCopyByteBuff& buf);
    
    // Helper to encode directly to bytes
    std::vector<uint8_t> encode() const;
    
    // Helper to decode directly from bytes
    static WorldState decode(const std::vector<uint8_t>& data);
};


#endif // Bench_complex_HPP
