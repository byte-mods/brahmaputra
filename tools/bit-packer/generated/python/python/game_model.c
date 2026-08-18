
#include "game_model.h"
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


void Player_encode(Player *self, ZeroCopyByteBuff *buf) {
    
    
    ZeroCopyByteBuff_put_string(buf, self->username);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->level);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->score);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->inventory_len);
    for(int i=0; i<self->inventory_len; i++) {
        ZeroCopyByteBuff_put_string(buf, self->inventory[i]);
    }
    
    
}

Player *Player_decode(ZeroCopyByteBuff *buf) {
    Player *obj = malloc(sizeof(Player));
    
    
    obj->username = ZeroCopyByteBuff_get_string(buf);
    
    
    
    obj->level = ZeroCopyByteBuff_get_int32(buf);
    
    
    
    obj->score = ZeroCopyByteBuff_get_int32(buf);
    
    
    
    obj->inventory_len = ZeroCopyByteBuff_get_int32(buf);
    obj->inventory = malloc(obj->inventory_len * sizeof(char*));
    for(int i=0; i<obj->inventory_len; i++) {
        obj->inventory[i] = ZeroCopyByteBuff_get_string(buf);
    }
    
    
    return obj;
}

void Player_free(Player *self) {
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    free(self->inventory);
    
    
    free(self);
}


void GameState_encode(GameState *self, ZeroCopyByteBuff *buf) {
    
    
    ZeroCopyByteBuff_put_int32(buf, self->id);
    
    
    
    ZeroCopyByteBuff_put_bool(buf, self->isActive);
    
    
    
    ZeroCopyByteBuff_put_int32(buf, self->players_len);
    for(int i=0; i<self->players_len; i++) {
        Player_encode(&self->players[i], buf);
    }
    
    
}

GameState *GameState_decode(ZeroCopyByteBuff *buf) {
    GameState *obj = malloc(sizeof(GameState));
    
    
    obj->id = ZeroCopyByteBuff_get_int32(buf);
    
    
    
    obj->isActive = ZeroCopyByteBuff_get_bool(buf);
    
    
    
    obj->players_len = ZeroCopyByteBuff_get_int32(buf);
    obj->players = malloc(obj->players_len * sizeof(Player));
    for(int i=0; i<obj->players_len; i++) {
        Player* val = Player_decode(buf); obj->players[i] = *val; free(val);
    }
    
    
    return obj;
}

void GameState_free(GameState *self) {
    
    
    
    
    
    
    
    
    
    
    
    for(int i=0; i<self->players_len; i++) {
        Player_free(&self->players[i]);
    }
    
    free(self->players);
    
    
    free(self);
}

