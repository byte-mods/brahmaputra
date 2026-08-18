
#include "game.hpp"
#include <cmath>

// --- ZeroCopyByteBuff Implementation ---

ZeroCopyByteBuff::ZeroCopyByteBuff() : offset(0) {
    buffer.reserve(65536);
}

ZeroCopyByteBuff::ZeroCopyByteBuff(const std::vector<uint8_t>& data) : buffer(data), offset(0) {}

const std::vector<uint8_t>& ZeroCopyByteBuff::getBuffer() const {
    return buffer;
}

// ZigZag Helpers
static uint32_t zigzag_encode32(int32_t n) { return (n << 1) ^ (n >> 31); }
static int32_t zigzag_decode32(uint32_t n) { return (n >> 1) ^ -(n & 1); }
static uint64_t zigzag_encode64(int64_t n) { return (n << 1) ^ (n >> 63); }
static int64_t zigzag_decode64(uint64_t n) { return (n >> 1) ^ -(n & 1); }

inline void ZeroCopyByteBuff::putVarInt64(int64_t v) {
    uint64_t uv = (uint64_t)v;
    // FAST PATH: 1 byte
    if (uv < 0x80) {
        buffer.push_back((uint8_t)uv);
        return;
    }
    // FAST PATH: 2 bytes
    if (uv < 0x4000) {
        buffer.push_back((uint8_t)((uv & 0x7F) | 0x80));
        buffer.push_back((uint8_t)(uv >> 7));
        return;
    }
    // General path
    while (uv >= 0x80) {
        buffer.push_back((uint8_t)((uv & 0x7F) | 0x80));
        uv >>= 7;
    }
    buffer.push_back((uint8_t)uv);
}

inline int64_t ZeroCopyByteBuff::getVarInt64() {
    uint64_t result = 0;
    int shift = 0;
    // FAST PATH: 1 byte
    uint8_t byte = buffer[offset++];
    if (!(byte & 0x80)) {
        return (int64_t)byte;
    }
    result = byte & 0x7F;
    shift = 7;
    while (true) {
        byte = buffer[offset++];
        result |= ((uint64_t)(byte & 0x7F)) << shift;
        if (!(byte & 0x80)) break;
        shift += 7;
    }
    return (int64_t)result;
}

inline void ZeroCopyByteBuff::putInt32(int32_t v) { putVarInt64(zigzag_encode32(v)); }
inline void ZeroCopyByteBuff::putInt64(int64_t v) { putVarInt64(zigzag_encode64(v)); }
inline void ZeroCopyByteBuff::putFloat(float v) { putVarInt64(zigzag_encode64((int64_t)(v * 10000.0f))); }
inline void ZeroCopyByteBuff::putDouble(double v) { putVarInt64(zigzag_encode64((int64_t)(v * 10000.0))); }
inline void ZeroCopyByteBuff::putBool(bool v) { buffer.push_back(v ? 1 : 0); }
inline void ZeroCopyByteBuff::putString(const std::string& v) {
    putVarInt64(zigzag_encode64(v.length()));
    buffer.insert(buffer.end(), v.begin(), v.end());
}

inline int32_t ZeroCopyByteBuff::getInt32() { return zigzag_decode32((uint32_t)getVarInt64()); }
inline int64_t ZeroCopyByteBuff::getInt64() { return zigzag_decode64((uint64_t)getVarInt64()); }
inline float ZeroCopyByteBuff::getFloat() { return (float)zigzag_decode64((uint64_t)getVarInt64()) / 10000.0f; }
inline double ZeroCopyByteBuff::getDouble() { return (double)zigzag_decode64((uint64_t)getVarInt64()) / 10000.0; }

inline bool ZeroCopyByteBuff::getBool() {
    return buffer[offset++] != 0;
}

inline std::string ZeroCopyByteBuff::getString() {
    size_t len = (size_t)zigzag_decode64((uint64_t)getVarInt64());
    if (offset + len > buffer.size()) throw std::runtime_error("Buffer underflow");
    std::string s(buffer.begin() + offset, buffer.begin() + offset + len);
    offset += len;
    return s;
}

// --- Generated Implementation ---


// Player Implementation
void Player::encode(ZeroCopyByteBuff& buf) const {
    
    
    buf.putString(this->username);
    
    
    
    buf.putInt32(this->level);
    
    
    
    buf.putInt32(this->score);
    
    
    
    buf.putInt32((int32_t)inventory.size());
    for(const auto& item : inventory) {
        buf.putString(item);
    }
    
    
}

void Player::decode(ZeroCopyByteBuff& buf) {
    
    
    this->username = buf.getString();
    
    
    
    this->level = buf.getInt32();
    
    
    
    this->score = buf.getInt32();
    
    
    
    int32_t len_inventory = buf.getInt32();
    inventory.resize(len_inventory);
    for(int i=0; i<len_inventory; i++) {
        inventory[i] = buf.getString();
    }
    
    
}

std::vector<uint8_t> Player::encode() const {
    ZeroCopyByteBuff buf;
    buf.putString(VERSION);
    encode(buf);
    return buf.getBuffer();
}

Player Player::decode(const std::vector<uint8_t>& data) {
    ZeroCopyByteBuff buf(data);
    std::string ver = buf.getString();
    if (ver != VERSION) {
        throw std::runtime_error("Version mismatch");
    }
    Player obj;
    obj.decode(buf);
    return obj;
}


// GameState Implementation
void GameState::encode(ZeroCopyByteBuff& buf) const {
    
    
    buf.putInt32(this->id);
    
    
    
    buf.putBool(this->isActive);
    
    
    
    buf.putInt32((int32_t)players.size());
    for(const auto& item : players) {
        item.encode(buf);
    }
    
    
}

void GameState::decode(ZeroCopyByteBuff& buf) {
    
    
    this->id = buf.getInt32();
    
    
    
    this->isActive = buf.getBool();
    
    
    
    int32_t len_players = buf.getInt32();
    players.resize(len_players);
    for(int i=0; i<len_players; i++) {
        players[i].decode(buf);
    }
    
    
}

std::vector<uint8_t> GameState::encode() const {
    ZeroCopyByteBuff buf;
    buf.putString(VERSION);
    encode(buf);
    return buf.getBuffer();
}

GameState GameState::decode(const std::vector<uint8_t>& data) {
    ZeroCopyByteBuff buf(data);
    std::string ver = buf.getString();
    if (ver != VERSION) {
        throw std::runtime_error("Version mismatch");
    }
    GameState obj;
    obj.decode(buf);
    return obj;
}

