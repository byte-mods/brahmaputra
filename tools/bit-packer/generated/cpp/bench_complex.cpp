
#include "bench_complex.hpp"
#include <cmath>

// --- ZeroCopyByteBuff Implementation ---

ZeroCopyByteBuff::ZeroCopyByteBuff() : offset(0) {
    buffer.reserve(1024);
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

void ZeroCopyByteBuff::putVarInt64(int64_t v) {
    uint64_t uv = (uint64_t)v;
    while (uv >= 0x80) {
        buffer.push_back((uint8_t)((uv & 0x7F) | 0x80));
        uv >>= 7;
    }
    buffer.push_back((uint8_t)uv);
}

int64_t ZeroCopyByteBuff::getVarInt64() {
    uint64_t result = 0;
    int shift = 0;
    while (true) {
        if (offset >= buffer.size()) throw std::runtime_error("Buffer underflow");
        uint8_t byte = buffer[offset++];
        result |= ((uint64_t)(byte & 0x7F)) << shift;
        if (!(byte & 0x80)) break;
        shift += 7;
    }
    return (int64_t)result;
}

void ZeroCopyByteBuff::putInt32(int32_t v) { putVarInt64(zigzag_encode32(v)); }
void ZeroCopyByteBuff::putInt64(int64_t v) { putVarInt64(zigzag_encode64(v)); }
void ZeroCopyByteBuff::putFloat(float v) { putVarInt64(zigzag_encode64((int64_t)(v * 10000.0f))); }
void ZeroCopyByteBuff::putDouble(double v) { putVarInt64(zigzag_encode64((int64_t)(v * 10000.0))); }
void ZeroCopyByteBuff::putBool(bool v) { buffer.push_back(v ? 1 : 0); }
void ZeroCopyByteBuff::putString(const std::string& v) {
    putVarInt64(v.length());
    buffer.insert(buffer.end(), v.begin(), v.end());
}

int32_t ZeroCopyByteBuff::getInt32() { return zigzag_decode32((uint32_t)getVarInt64()); }
int64_t ZeroCopyByteBuff::getInt64() { return zigzag_decode64((uint64_t)getVarInt64()); }
float ZeroCopyByteBuff::getFloat() { return (float)zigzag_decode64((uint64_t)getVarInt64()) / 10000.0f; }
double ZeroCopyByteBuff::getDouble() { return (double)zigzag_decode64((uint64_t)getVarInt64()) / 10000.0; }

bool ZeroCopyByteBuff::getBool() {
    if (offset >= buffer.size()) throw std::runtime_error("Buffer underflow");
    return buffer[offset++] != 0;
}

std::string ZeroCopyByteBuff::getString() {
    size_t len = (size_t)getVarInt64();
    if (offset + len > buffer.size()) throw std::runtime_error("Buffer underflow");
    std::string s(buffer.begin() + offset, buffer.begin() + offset + len);
    offset += len;
    return s;
}

// --- Generated Implementation ---


// Vec3 Implementation
void Vec3::encode(ZeroCopyByteBuff& buf) const {
    
    
    buf.putInt32(this->x);
    
    
    
    buf.putInt32(this->y);
    
    
    
    buf.putInt32(this->z);
    
    
}

void Vec3::decode(ZeroCopyByteBuff& buf) {
    
    
    this->x = buf.getInt32();
    
    
    
    this->y = buf.getInt32();
    
    
    
    this->z = buf.getInt32();
    
    
}

std::vector<uint8_t> Vec3::encode() const {
    ZeroCopyByteBuff buf;
    buf.putString(VERSION);
    encode(buf);
    return buf.getBuffer();
}

Vec3 Vec3::decode(const std::vector<uint8_t>& data) {
    ZeroCopyByteBuff buf(data);
    std::string ver = buf.getString();
    if (ver != VERSION) {
        throw std::runtime_error("Version mismatch");
    }
    Vec3 obj;
    obj.decode(buf);
    return obj;
}


// Item Implementation
void Item::encode(ZeroCopyByteBuff& buf) const {
    
    
    buf.putInt32(this->id);
    
    
    
    buf.putString(this->name);
    
    
    
    buf.putInt32(this->value);
    
    
    
    buf.putInt32(this->weight);
    
    
    
    buf.putString(this->rarity);
    
    
}

void Item::decode(ZeroCopyByteBuff& buf) {
    
    
    this->id = buf.getInt32();
    
    
    
    this->name = buf.getString();
    
    
    
    this->value = buf.getInt32();
    
    
    
    this->weight = buf.getInt32();
    
    
    
    this->rarity = buf.getString();
    
    
}

std::vector<uint8_t> Item::encode() const {
    ZeroCopyByteBuff buf;
    buf.putString(VERSION);
    encode(buf);
    return buf.getBuffer();
}

Item Item::decode(const std::vector<uint8_t>& data) {
    ZeroCopyByteBuff buf(data);
    std::string ver = buf.getString();
    if (ver != VERSION) {
        throw std::runtime_error("Version mismatch");
    }
    Item obj;
    obj.decode(buf);
    return obj;
}


// Character Implementation
void Character::encode(ZeroCopyByteBuff& buf) const {
    
    
    buf.putString(this->name);
    
    
    
    buf.putInt32(this->level);
    
    
    
    buf.putInt32(this->hp);
    
    
    
    buf.putInt32(this->mp);
    
    
    
    buf.putBool(this->is_alive);
    
    
    
    this->position.encode(buf);
    
    
    
    buf.putInt32((int32_t)skills.size());
    for(const auto& item : skills) {
        buf.putInt32(item);
    }
    
    
    
    buf.putInt32((int32_t)inventory.size());
    for(const auto& item : inventory) {
        item.encode(buf);
    }
    
    
}

void Character::decode(ZeroCopyByteBuff& buf) {
    
    
    this->name = buf.getString();
    
    
    
    this->level = buf.getInt32();
    
    
    
    this->hp = buf.getInt32();
    
    
    
    this->mp = buf.getInt32();
    
    
    
    this->is_alive = buf.getBool();
    
    
    
    this->position.decode(buf);
    
    
    
    int32_t len_skills = buf.getInt32();
    skills.resize(len_skills);
    for(int i=0; i<len_skills; i++) {
        skills[i] = buf.getInt32();
    }
    
    
    
    int32_t len_inventory = buf.getInt32();
    inventory.resize(len_inventory);
    for(int i=0; i<len_inventory; i++) {
        inventory[i].decode(buf);
    }
    
    
}

std::vector<uint8_t> Character::encode() const {
    ZeroCopyByteBuff buf;
    buf.putString(VERSION);
    encode(buf);
    return buf.getBuffer();
}

Character Character::decode(const std::vector<uint8_t>& data) {
    ZeroCopyByteBuff buf(data);
    std::string ver = buf.getString();
    if (ver != VERSION) {
        throw std::runtime_error("Version mismatch");
    }
    Character obj;
    obj.decode(buf);
    return obj;
}


// Guild Implementation
void Guild::encode(ZeroCopyByteBuff& buf) const {
    
    
    buf.putString(this->name);
    
    
    
    buf.putString(this->description);
    
    
    
    buf.putInt32((int32_t)members.size());
    for(const auto& item : members) {
        item.encode(buf);
    }
    
    
}

void Guild::decode(ZeroCopyByteBuff& buf) {
    
    
    this->name = buf.getString();
    
    
    
    this->description = buf.getString();
    
    
    
    int32_t len_members = buf.getInt32();
    members.resize(len_members);
    for(int i=0; i<len_members; i++) {
        members[i].decode(buf);
    }
    
    
}

std::vector<uint8_t> Guild::encode() const {
    ZeroCopyByteBuff buf;
    buf.putString(VERSION);
    encode(buf);
    return buf.getBuffer();
}

Guild Guild::decode(const std::vector<uint8_t>& data) {
    ZeroCopyByteBuff buf(data);
    std::string ver = buf.getString();
    if (ver != VERSION) {
        throw std::runtime_error("Version mismatch");
    }
    Guild obj;
    obj.decode(buf);
    return obj;
}


// WorldState Implementation
void WorldState::encode(ZeroCopyByteBuff& buf) const {
    
    
    buf.putInt32(this->world_id);
    
    
    
    buf.putString(this->seed);
    
    
    
    buf.putInt32((int32_t)guilds.size());
    for(const auto& item : guilds) {
        item.encode(buf);
    }
    
    
    
    buf.putInt32((int32_t)loot_table.size());
    for(const auto& item : loot_table) {
        item.encode(buf);
    }
    
    
}

void WorldState::decode(ZeroCopyByteBuff& buf) {
    
    
    this->world_id = buf.getInt32();
    
    
    
    this->seed = buf.getString();
    
    
    
    int32_t len_guilds = buf.getInt32();
    guilds.resize(len_guilds);
    for(int i=0; i<len_guilds; i++) {
        guilds[i].decode(buf);
    }
    
    
    
    int32_t len_loot_table = buf.getInt32();
    loot_table.resize(len_loot_table);
    for(int i=0; i<len_loot_table; i++) {
        loot_table[i].decode(buf);
    }
    
    
}

std::vector<uint8_t> WorldState::encode() const {
    ZeroCopyByteBuff buf;
    buf.putString(VERSION);
    encode(buf);
    return buf.getBuffer();
}

WorldState WorldState::decode(const std::vector<uint8_t>& data) {
    ZeroCopyByteBuff buf(data);
    std::string ver = buf.getString();
    if (ver != VERSION) {
        throw std::runtime_error("Version mismatch");
    }
    WorldState obj;
    obj.decode(buf);
    return obj;
}

