
#ifndef Bench_complex_MODEL_H
#define Bench_complex_MODEL_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define VERSION "1.0.0"

// --- ZeroCopyByteBuff (Mini) ---
typedef struct {
    char *buf;
    size_t capacity;
    size_t offset;
    bool own_memory;
} ZeroCopyByteBuff;

ZeroCopyByteBuff *ZeroCopyByteBuff_new(size_t capacity);
ZeroCopyByteBuff *ZeroCopyByteBuff_from_data(char *data, size_t size);
void ZeroCopyByteBuff_free(ZeroCopyByteBuff *b);
void ZeroCopyByteBuff_put_int32(ZeroCopyByteBuff *b, int32_t v);
void ZeroCopyByteBuff_put_int64(ZeroCopyByteBuff *b, int64_t v);
void ZeroCopyByteBuff_put_float(ZeroCopyByteBuff *b, float v);
void ZeroCopyByteBuff_put_double(ZeroCopyByteBuff *b, double v);
void ZeroCopyByteBuff_put_bool(ZeroCopyByteBuff *b, bool v);
void ZeroCopyByteBuff_put_string(ZeroCopyByteBuff *b, const char *v);

int32_t ZeroCopyByteBuff_get_int32(ZeroCopyByteBuff *b);
int64_t ZeroCopyByteBuff_get_int64(ZeroCopyByteBuff *b);
float ZeroCopyByteBuff_get_float(ZeroCopyByteBuff *b);
double ZeroCopyByteBuff_get_double(ZeroCopyByteBuff *b);
bool ZeroCopyByteBuff_get_bool(ZeroCopyByteBuff *b);
char *ZeroCopyByteBuff_get_string(ZeroCopyByteBuff *b);

// --- Generated Structs ---

typedef struct Vec3 {
    int32_t x;
    int32_t y;
    int32_t z;
    
} Vec3;

void Vec3_encode(Vec3 *self, ZeroCopyByteBuff *buf);
Vec3 *Vec3_decode(ZeroCopyByteBuff *buf);
void Vec3_free(Vec3 *self);

typedef struct Item {
    int32_t id;
    char* name;
    int32_t value;
    int32_t weight;
    char* rarity;
    
} Item;

void Item_encode(Item *self, ZeroCopyByteBuff *buf);
Item *Item_decode(ZeroCopyByteBuff *buf);
void Item_free(Item *self);

typedef struct Character {
    char* name;
    int32_t level;
    int32_t hp;
    int32_t mp;
    bool is_alive;
    Vec3 position;
    int32_t *skills; int skills_len;
    Item *inventory; int inventory_len;
    
} Character;

void Character_encode(Character *self, ZeroCopyByteBuff *buf);
Character *Character_decode(ZeroCopyByteBuff *buf);
void Character_free(Character *self);

typedef struct Guild {
    char* name;
    char* description;
    Character *members; int members_len;
    
} Guild;

void Guild_encode(Guild *self, ZeroCopyByteBuff *buf);
Guild *Guild_decode(ZeroCopyByteBuff *buf);
void Guild_free(Guild *self);

typedef struct WorldState {
    int32_t world_id;
    char* seed;
    Guild *guilds; int guilds_len;
    Item *loot_table; int loot_table_len;
    
} WorldState;

void WorldState_encode(WorldState *self, ZeroCopyByteBuff *buf);
WorldState *WorldState_decode(ZeroCopyByteBuff *buf);
void WorldState_free(WorldState *self);


#endif
