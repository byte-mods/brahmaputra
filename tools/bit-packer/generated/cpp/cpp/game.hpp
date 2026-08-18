
#ifndef Game_HPP
#define Game_HPP

#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#define VERSION "1.0.2"

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

struct Player {
    std::string username;
    int32_t level;
    int32_t score;
    std::vector<std::string> inventory;
    

    void encode(ZeroCopyByteBuff& buf) const;
    void decode(ZeroCopyByteBuff& buf);
    
    // Helper to encode directly to bytes
    std::vector<uint8_t> encode() const;
    
    // Helper to decode directly from bytes
    static Player decode(const std::vector<uint8_t>& data);
};

struct GameState {
    int32_t id;
    bool isActive;
    std::vector<Player> players;
    

    void encode(ZeroCopyByteBuff& buf) const;
    void decode(ZeroCopyByteBuff& buf);
    
    // Helper to encode directly to bytes
    std::vector<uint8_t> encode() const;
    
    // Helper to decode directly from bytes
    static GameState decode(const std::vector<uint8_t>& data);
};


#endif // Game_HPP
