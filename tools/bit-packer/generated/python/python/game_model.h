
#ifndef Game_MODEL_H
#define Game_MODEL_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define VERSION "1.0.2"

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

typedef struct Player {
    char* username;
    int32_t level;
    int32_t score;
    char* *inventory; int inventory_len;
    
} Player;

void Player_encode(Player *self, ZeroCopyByteBuff *buf);
Player *Player_decode(ZeroCopyByteBuff *buf);
void Player_free(Player *self);

typedef struct GameState {
    int32_t id;
    bool isActive;
    Player *players; int players_len;
    
} GameState;

void GameState_encode(GameState *self, ZeroCopyByteBuff *buf);
GameState *GameState_decode(ZeroCopyByteBuff *buf);
void GameState_free(GameState *self);


#endif
