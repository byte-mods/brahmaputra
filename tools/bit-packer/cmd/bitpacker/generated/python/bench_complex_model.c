
#include "bench_complex_model.h"
#include <stdio.h>

// --- ZeroCopyByteBuff Implementation ---

static void ensure_capacity(ZeroCopyByteBuff *self, size_t needed) {
    if (self->offset + needed > self->capacity) {
        size_t new_cap = self->capacity * 2;
        if (new_cap < self->offset + needed) new_cap = self->offset + needed;
        char *new_buf = realloc(self->buf, new_cap);
        if (new_buf) {
            self->buf = new_buf;
            self->capacity = new_cap;
        }
    }
}

ZeroCopyByteBuff *ZeroCopyByteBuff_new(size_t capacity) {
    ZeroCopyByteBuff *b = malloc(sizeof(ZeroCopyByteBuff));
    b->buf = malloc(capacity);
    b->capacity = capacity;
    b->offset = 0;
    b->own_memory = true;
    return b;
}

ZeroCopyByteBuff *ZeroCopyByteBuff_from_data(char *data, size_t size) {
    ZeroCopyByteBuff *b = malloc(sizeof(ZeroCopyByteBuff));
    b->buf = data;
    b->capacity = size;
    b->offset = 0;
    b->own_memory = false; 
    return b;
}

void ZeroCopyByteBuff_free(ZeroCopyByteBuff *b) {
    if (b->own_memory && b->buf) free(b->buf);
    free(b);
}

// ZigZag Helpers
static uint32_t zigzag_encode32(int32_t n) { return (n << 1) ^ (n >> 31); }
static int32_t zigzag_decode32(uint32_t n) { return (n >> 1) ^ -(n & 1); }
static uint64_t zigzag_encode64(int64_t n) { return (n << 1) ^ (n >> 63); }
static int64_t zigzag_decode64(uint64_t n) { return (n >> 1) ^ -(n & 1); }

// VarInt Logic
static void put_varint64(ZeroCopyByteBuff *b, uint64_t v) {
    ensure_capacity(b, 10);
    char *p = b->buf + b->offset;
    while (v >= 0x80) {
        *p++ = (char)((v & 0x7F) | 0x80);
        v >>= 7;
    }
    *p++ = (char)v;
    b->offset = p - b->buf;
}

static uint64_t get_varint64(ZeroCopyByteBuff *b) {
    uint64_t result = 0;
    int shift = 0;
    char *p = b->buf + b->offset;
    while (1) {
        uint8_t byte = *p++;
        result |= ((uint64_t)(byte & 0x7F)) << shift;
        if (!(byte & 0x80)) break;
        shift += 7;
    }
    b->offset = p - b->buf;
    return result;
}

void ZeroCopyByteBuff_put_int32(ZeroCopyByteBuff *b, int32_t v) {
    put_varint64(b, zigzag_encode32(v));
}
void ZeroCopyByteBuff_put_int64(ZeroCopyByteBuff *b, int64_t v) {
    put_varint64(b, zigzag_encode64(v));
}
void ZeroCopyByteBuff_put_float(ZeroCopyByteBuff *b, float v) {
    put_varint64(b, zigzag_encode64((int64_t)(v * 10000.0f)));
}
void ZeroCopyByteBuff_put_double(ZeroCopyByteBuff *b, double v) {
    put_varint64(b, zigzag_encode64((int64_t)(v * 10000.0)));
}
void ZeroCopyByteBuff_put_bool(ZeroCopyByteBuff *b, bool v) {
    ensure_capacity(b, 1);
    b->buf[b->offset++] = v ? 1 : 0;
}
void ZeroCopyByteBuff_put_string(ZeroCopyByteBuff *b, const char *v) {
    size_t len = strlen(v);
    put_varint64(b, len * 2); // Simple positive encoding
    ensure_capacity(b, len);
    memcpy(b->buf + b->offset, v, len);
    b->offset += len;
}

int32_t ZeroCopyByteBuff_get_int32(ZeroCopyByteBuff *b) {
    return zigzag_decode32((uint32_t)get_varint64(b));
}
int64_t ZeroCopyByteBuff_get_int64(ZeroCopyByteBuff *b) {
    return zigzag_decode64(get_varint64(b));
}
float ZeroCopyByteBuff_get_float(ZeroCopyByteBuff *b) {
    return (float)zigzag_decode64(get_varint64(b)) / 10000.0f;
}
double ZeroCopyByteBuff_get_double(ZeroCopyByteBuff *b) {
    return (double)zigzag_decode64(get_varint64(b)) / 10000.0;
}
bool ZeroCopyByteBuff_get_bool(ZeroCopyByteBuff *b) {
    return b->buf[b->offset++] != 0;
}
char *ZeroCopyByteBuff_get_string(ZeroCopyByteBuff *b) {
    uint64_t zz = get_varint64(b);
    size_t len = zz / 2;
    char *s = malloc(len + 1);
    memcpy(s, b->buf + b->offset, len);
    s[len] = '\0';
    b->offset += len;
    return s;
}

// --- Generated Implementation ---


void Vec3_encode(Vec3 *self, ZeroCopyByteBuff *buf) {
    
    
    ZeroCopyByteBuff_put_int32(buf, self->x);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->y);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->z);
    
    
}

Vec3 *Vec3_decode(ZeroCopyByteBuff *buf) {
    Vec3 *obj = malloc(sizeof(Vec3));
    
    
    obj->x = ZeroCopyByteBuff_get_int32(buf);
    
    
    
    obj->y = ZeroCopyByteBuff_get_int32(buf);
    
    
    
    obj->z = ZeroCopyByteBuff_get_int32(buf);
    
    
    return obj;
}

void Vec3_free(Vec3 *self) {
    
    
    
    
    
    
    
    
    
    
    
    
    
    free(self);
}


void Item_encode(Item *self, ZeroCopyByteBuff *buf) {
    
    
    ZeroCopyByteBuff_put_int32(buf, self->id);
    
    
    
    ZeroCopyByteBuff_put_string(buf, self->name);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->value);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->weight);
    
    
    
    ZeroCopyByteBuff_put_string(buf, self->rarity);
    
    
}

Item *Item_decode(ZeroCopyByteBuff *buf) {
    Item *obj = malloc(sizeof(Item));
    
    
    obj->id = ZeroCopyByteBuff_get_int32(buf);
    
    
    
    obj->name = ZeroCopyByteBuff_get_string(buf);
    
    
    
    obj->value = ZeroCopyByteBuff_get_int32(buf);
    
    
    
    obj->weight = ZeroCopyByteBuff_get_int32(buf);
    
    
    
    obj->rarity = ZeroCopyByteBuff_get_string(buf);
    
    
    return obj;
}

void Item_free(Item *self) {
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    free(self);
}


void Character_encode(Character *self, ZeroCopyByteBuff *buf) {
    
    
    ZeroCopyByteBuff_put_string(buf, self->name);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->level);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->hp);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->mp);
    
    
    
    ZeroCopyByteBuff_put_bool(buf, self->is_alive);
    
    
    
    Vec3_encode(&self->position, buf);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->skills_len);
    for(int i=0; i<self->skills_len; i++) {
        ZeroCopyByteBuff_put_int32(buf, self->skills[i]);
    }
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->inventory_len);
    for(int i=0; i<self->inventory_len; i++) {
        Item_encode(&self->inventory[i], buf);
    }
    
    
}

Character *Character_decode(ZeroCopyByteBuff *buf) {
    Character *obj = malloc(sizeof(Character));
    
    
    obj->name = ZeroCopyByteBuff_get_string(buf);
    
    
    
    obj->level = ZeroCopyByteBuff_get_int32(buf);
    
    
    
    obj->hp = ZeroCopyByteBuff_get_int32(buf);
    
    
    
    obj->mp = ZeroCopyByteBuff_get_int32(buf);
    
    
    
    obj->is_alive = ZeroCopyByteBuff_get_bool(buf);
    
    
    
    Vec3* val = Vec3_decode(buf); obj->position = *val; free(val);
    
    
    
    obj->skills_len = ZeroCopyByteBuff_get_int32(buf);
    obj->skills = malloc(obj->skills_len * sizeof(int32_t));
    for(int i=0; i<obj->skills_len; i++) {
        obj->skills[i] = ZeroCopyByteBuff_get_int32(buf);
    }
    
    
    
    obj->inventory_len = ZeroCopyByteBuff_get_int32(buf);
    obj->inventory = malloc(obj->inventory_len * sizeof(Item));
    for(int i=0; i<obj->inventory_len; i++) {
        Item* val = Item_decode(buf); obj->inventory[i] = *val; free(val);
    }
    
    
    return obj;
}

void Character_free(Character *self) {
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    free(self->skills);
    
    
    
    
    for(int i=0; i<self->inventory_len; i++) {
        Item_free(&self->inventory[i]);
    }
    
    free(self->inventory);
    
    
    free(self);
}


void Guild_encode(Guild *self, ZeroCopyByteBuff *buf) {
    
    
    ZeroCopyByteBuff_put_string(buf, self->name);
    
    
    
    ZeroCopyByteBuff_put_string(buf, self->description);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->members_len);
    for(int i=0; i<self->members_len; i++) {
        Character_encode(&self->members[i], buf);
    }
    
    
}

Guild *Guild_decode(ZeroCopyByteBuff *buf) {
    Guild *obj = malloc(sizeof(Guild));
    
    
    obj->name = ZeroCopyByteBuff_get_string(buf);
    
    
    
    obj->description = ZeroCopyByteBuff_get_string(buf);
    
    
    
    obj->members_len = ZeroCopyByteBuff_get_int32(buf);
    obj->members = malloc(obj->members_len * sizeof(Character));
    for(int i=0; i<obj->members_len; i++) {
        Character* val = Character_decode(buf); obj->members[i] = *val; free(val);
    }
    
    
    return obj;
}

void Guild_free(Guild *self) {
    
    
    
    
    
    
    
    
    
    
    
    for(int i=0; i<self->members_len; i++) {
        Character_free(&self->members[i]);
    }
    
    free(self->members);
    
    
    free(self);
}


void WorldState_encode(WorldState *self, ZeroCopyByteBuff *buf) {
    
    
    ZeroCopyByteBuff_put_int32(buf, self->world_id);
    
    
    
    ZeroCopyByteBuff_put_string(buf, self->seed);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->guilds_len);
    for(int i=0; i<self->guilds_len; i++) {
        Guild_encode(&self->guilds[i], buf);
    }
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->loot_table_len);
    for(int i=0; i<self->loot_table_len; i++) {
        Item_encode(&self->loot_table[i], buf);
    }
    
    
}

WorldState *WorldState_decode(ZeroCopyByteBuff *buf) {
    WorldState *obj = malloc(sizeof(WorldState));
    
    
    obj->world_id = ZeroCopyByteBuff_get_int32(buf);
    
    
    
    obj->seed = ZeroCopyByteBuff_get_string(buf);
    
    
    
    obj->guilds_len = ZeroCopyByteBuff_get_int32(buf);
    obj->guilds = malloc(obj->guilds_len * sizeof(Guild));
    for(int i=0; i<obj->guilds_len; i++) {
        Guild* val = Guild_decode(buf); obj->guilds[i] = *val; free(val);
    }
    
    
    
    obj->loot_table_len = ZeroCopyByteBuff_get_int32(buf);
    obj->loot_table = malloc(obj->loot_table_len * sizeof(Item));
    for(int i=0; i<obj->loot_table_len; i++) {
        Item* val = Item_decode(buf); obj->loot_table[i] = *val; free(val);
    }
    
    
    return obj;
}

void WorldState_free(WorldState *self) {
    
    
    
    
    
    
    
    
    
    
    
    for(int i=0; i<self->guilds_len; i++) {
        Guild_free(&self->guilds[i]);
    }
    
    free(self->guilds);
    
    
    
    
    for(int i=0; i<self->loot_table_len; i++) {
        Item_free(&self->loot_table[i]);
    }
    
    free(self->loot_table);
    
    
    free(self);
}

