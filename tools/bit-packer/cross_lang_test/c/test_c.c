/* Cross-language conformance test for the BitPacker C target.
 * Usage: test_c <cross_lang_test dir> */
#include "bench_complex.h"
#include "edge.h"
#include "f32.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int passed, failed;

static void check(int ok, const char *name, const char *detail) {
    if (ok) {
        passed++;
        printf("  ok   %s\n", name);
    } else {
        failed++;
        printf("  FAIL %s (%s)\n", name, detail ? detail : "");
    }
}

static uint8_t *read_file(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    uint8_t *buf = NULL;
    size_t cap = 0, n = 0;
    for (;;) {
        if (n == cap) {
            cap = cap ? cap * 2 : 4096;
            uint8_t *p = realloc(buf, cap);
            if (!p) { free(buf); fclose(f); return NULL; }
            buf = p;
        }
        size_t got = fread(buf + n, 1, cap - n, f);
        n += got;
        if (got == 0) break;
    }
    fclose(f);
    *len = n;
    return buf;
}

static int write_file(const char *path, const uint8_t *data, size_t len) {
    FILE *f = fopen(path, "wb");
    if (!f) return 0;
    int ok = fwrite(data, 1, len, f) == len;
    return fclose(f) == 0 && ok;
}

static int str_eq(bp_str s, const char *want) {
    size_t n = strlen(want);
    return s.len == n && (n == 0 || memcmp(s.data, want, n) == 0);
}

static int same_bytes(const bp_buffer *b, const uint8_t *ref, size_t ref_len) {
    return b->len == ref_len && memcmp(b->data, ref, ref_len) == 0;
}

/* ---------------- bench ---------------- */

static void build_world(WorldState *w, Character *hero, Guild *guild, Item *sword, Item *potion,
                        int32_t *skills) {
    WorldState_init(w);
    w->world_id = 42;
    w->seed = BP_STR_LIT("cross_lang_test");

    Character_init(hero);
    hero->name = BP_STR_LIT("TestHero");
    hero->level = 99;
    hero->hp = 1000;
    hero->mp = 500;
    hero->is_alive = true;
    hero->position.x = 10;
    hero->position.y = -20;
    hero->position.z = 30;
    skills[0] = 1; skills[1] = 2; skills[2] = 3; skills[3] = 100;
    hero->skills = skills;
    hero->skills_len = 4;

    Item_init(sword);
    sword->id = 1;
    sword->name = BP_STR_LIT("Excalibur");
    sword->value = 9999;
    sword->weight = 15;
    sword->rarity = BP_STR_LIT("Legendary");
    hero->inventory = sword;
    hero->inventory_len = 1;

    Guild_init(guild);
    guild->name = BP_STR_LIT("TestGuild");
    guild->description = BP_STR_LIT("A test guild for cross-language");
    guild->members = hero;
    guild->members_len = 1;
    w->guilds = guild;
    w->guilds_len = 1;

    Item_init(potion);
    potion->id = 2;
    potion->name = BP_STR_LIT("HealthPotion");
    potion->value = 50;
    potion->weight = 1;
    potion->rarity = BP_STR_LIT("Common");
    w->loot_table = potion;
    w->loot_table_len = 1;
}

static int verify_world(const WorldState *d) {
    if (d->world_id != 42 || !str_eq(d->seed, "cross_lang_test")) return 0;
    if (d->guilds_len != 1) return 0;
    const Guild *g = &d->guilds[0];
    if (!str_eq(g->name, "TestGuild") || !str_eq(g->description, "A test guild for cross-language")) return 0;
    if (g->members_len != 1) return 0;
    const Character *h = &g->members[0];
    if (!str_eq(h->name, "TestHero") || h->level != 99 || h->hp != 1000 || h->mp != 500 || !h->is_alive) return 0;
    if (h->position.x != 10 || h->position.y != -20 || h->position.z != 30) return 0;
    if (h->skills_len != 4 || h->skills[0] != 1 || h->skills[1] != 2 || h->skills[2] != 3 || h->skills[3] != 100) return 0;
    if (h->inventory_len != 1) return 0;
    const Item *s = &h->inventory[0];
    if (s->id != 1 || !str_eq(s->name, "Excalibur") || s->value != 9999 || s->weight != 15 || !str_eq(s->rarity, "Legendary")) return 0;
    if (d->loot_table_len != 1) return 0;
    const Item *p = &d->loot_table[0];
    if (p->id != 2 || !str_eq(p->name, "HealthPotion") || p->value != 50 || p->weight != 1 || !str_eq(p->rarity, "Common")) return 0;
    return 1;
}

static void test_bench(const char *dir) {
    char path[4096];
    snprintf(path, sizeof path, "%s/test_data.bin", dir);
    size_t ref_len = 0;
    uint8_t *ref = read_file(path, &ref_len);
    check(ref != NULL, "bench: read test_data.bin", path);
    if (!ref) return;

    WorldState w;
    Character hero;
    Guild guild;
    Item sword, potion;
    int32_t skills[4];
    build_world(&w, &hero, &guild, &sword, &potion, skills);

    bp_buffer out = {0};
    bp_status st = WorldState_encode(&w, &out);
    check(st == BP_OK, "bench: encode", bp_status_str(st));
    snprintf(path, sizeof path, "%s/test_data_c.bin", dir);
    check(write_file(path, out.data, out.len), "bench: write test_data_c.bin", path);
    check(same_bytes(&out, ref, ref_len), "bench: encode == test_data.bin", "bytes differ");

    WorldState d;
    st = WorldState_decode(&d, ref, ref_len);
    check(st == BP_OK, "bench: decode test_data.bin", bp_status_str(st));
    check(st == BP_OK && verify_world(&d), "bench: decoded fields", "field mismatch");

    bp_buffer again = {0};
    st = WorldState_encode(&d, &again);
    check(st == BP_OK && same_bytes(&again, ref, ref_len), "bench: re-encode decoded == ref", bp_status_str(st));

    WorldState d2;
    st = WorldState_decode(&d2, out.data, out.len);
    check(st == BP_OK && verify_world(&d2), "bench: round-trip own encoding", bp_status_str(st));

    WorldState_free(&d);
    WorldState_free(&d2);
    bp_buffer_free(&out);
    bp_buffer_free(&again);
    free(ref);
}

/* ---------------- edge ---------------- */

static void build_edge(Edge *e, int32_t *ints, int64_t *longs, float *floats, double *doubles,
                       bool *bools, bp_str *strings, Inner *inners) {
    Edge_init(e);
    e->i_min = INT32_MIN;
    e->i_max = INT32_MAX;
    e->i_zero = 0;
    e->i_neg = -1;
    e->l_min = INT64_MIN;
    e->l_max = INT64_MAX;
    e->l_neg = -300;
    e->f = -1.25f;
    e->d = 1234.5625;
    e->d_neg = -0.5;
    e->yes = true;
    e->no = false;
    e->empty = BP_STR_LIT("");
    e->unicode = BP_STR_LIT("h\xc3\xa9llo w\xc3\xb6rld \xe2\x9c\x93 \xe6\x97\xa5\xe6\x9c\xac \xf0\x9f\x9a\x80");
    const int32_t iv[] = {0, -1, 1, -64, 64, INT32_MIN, INT32_MAX};
    memcpy(ints, iv, sizeof iv);
    e->ints = ints;
    e->ints_len = 7;
    const int64_t lv[] = {0, -1, INT64_MAX, INT64_MIN, 4294967296LL};
    memcpy(longs, lv, sizeof lv);
    e->longs = longs;
    e->longs_len = 5;
    floats[0] = 0.0f; floats[1] = 0.5f; floats[2] = -2.25f;
    e->floats = floats;
    e->floats_len = 3;
    doubles[0] = 0.0; doubles[1] = 3.5; doubles[2] = -1000000.25;
    e->doubles = doubles;
    e->doubles_len = 3;
    bools[0] = true; bools[1] = false; bools[2] = true;
    e->bools = bools;
    e->bools_len = 3;
    strings[0] = BP_STR_LIT("");
    strings[1] = BP_STR_LIT("a");
    strings[2] = BP_STR_LIT("\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e");
    e->strings = strings;
    e->strings_len = 3;
    e->no_ints = NULL;
    e->no_ints_len = 0;
    e->inner.big = 1099511627776LL;
    e->inner.label = BP_STR_LIT("inner");
    inners[0].big = -1;
    inners[0].label = BP_STR_LIT("");
    inners[1].big = 0;
    inners[1].label = BP_STR_LIT("x");
    e->inners = inners;
    e->inners_len = 2;
    e->no_inners = NULL;
    e->no_inners_len = 0;
}

#define EXPECT(cond, name)                                  \
    do {                                                    \
        char d_[256];                                       \
        snprintf(d_, sizeof d_, "%s is false", #cond);      \
        check((cond), "edge: field " name, d_);             \
    } while (0)

static void verify_edge(const Edge *e) {
    EXPECT(e->i_min == INT32_MIN, "i_min");
    EXPECT(e->i_max == INT32_MAX, "i_max");
    EXPECT(e->i_zero == 0, "i_zero");
    EXPECT(e->i_neg == -1, "i_neg");
    EXPECT(e->l_min == INT64_MIN, "l_min");
    EXPECT(e->l_max == INT64_MAX, "l_max");
    EXPECT(e->l_neg == -300, "l_neg");
    EXPECT(e->f == -1.25f, "f");
    EXPECT(e->d == 1234.5625, "d");
    EXPECT(e->d_neg == -0.5, "d_neg");
    EXPECT(e->yes == true, "yes");
    EXPECT(e->no == false, "no");
    EXPECT(str_eq(e->empty, "") && e->empty.data && e->empty.data[0] == 0, "empty");
    EXPECT(str_eq(e->unicode, "h\xc3\xa9llo w\xc3\xb6rld \xe2\x9c\x93 \xe6\x97\xa5\xe6\x9c\xac \xf0\x9f\x9a\x80"), "unicode");
    EXPECT(e->ints_len == 7 && e->ints[0] == 0 && e->ints[1] == -1 && e->ints[2] == 1 && e->ints[3] == -64 &&
               e->ints[4] == 64 && e->ints[5] == INT32_MIN && e->ints[6] == INT32_MAX, "ints");
    EXPECT(e->longs_len == 5 && e->longs[0] == 0 && e->longs[1] == -1 && e->longs[2] == INT64_MAX &&
               e->longs[3] == INT64_MIN && e->longs[4] == 4294967296LL, "longs");
    EXPECT(e->floats_len == 3 && e->floats[0] == 0.0f && e->floats[1] == 0.5f && e->floats[2] == -2.25f, "floats");
    EXPECT(e->doubles_len == 3 && e->doubles[0] == 0.0 && e->doubles[1] == 3.5 && e->doubles[2] == -1000000.25, "doubles");
    EXPECT(e->bools_len == 3 && e->bools[0] && !e->bools[1] && e->bools[2], "bools");
    EXPECT(e->strings_len == 3 && str_eq(e->strings[0], "") && str_eq(e->strings[1], "a") &&
               str_eq(e->strings[2], "\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e"), "strings");
    EXPECT(e->no_ints_len == 0 && e->no_ints == NULL, "no_ints");
    EXPECT(e->inner.big == 1099511627776LL && str_eq(e->inner.label, "inner"), "inner");
    EXPECT(e->inners_len == 2 && e->inners[0].big == -1 && str_eq(e->inners[0].label, "") &&
               e->inners[1].big == 0 && str_eq(e->inners[1].label, "x"), "inners");
    EXPECT(e->no_inners_len == 0 && e->no_inners == NULL, "no_inners");
}

static void test_edge(const char *dir) {
    char path[4096];
    snprintf(path, sizeof path, "%s/edge/edge_ref.bin", dir);
    size_t ref_len = 0;
    uint8_t *ref = read_file(path, &ref_len);
    check(ref != NULL, "edge: read edge_ref.bin", path);
    if (!ref) return;

    Edge e;
    int32_t ints[7];
    int64_t longs[5];
    float floats[3];
    double doubles[3];
    bool bools[3];
    bp_str strings[3];
    Inner inners[2];
    build_edge(&e, ints, longs, floats, doubles, bools, strings, inners);

    bp_buffer out = {0};
    bp_status st = Edge_encode(&e, &out);
    check(st == BP_OK, "edge: encode", bp_status_str(st));
    check(same_bytes(&out, ref, ref_len), "edge: encode == edge_ref.bin", "bytes differ");

    Edge d;
    st = Edge_decode(&d, ref, ref_len);
    check(st == BP_OK, "edge: decode edge_ref.bin", bp_status_str(st));
    if (st == BP_OK) verify_edge(&d);

    bp_buffer again = {0};
    st = Edge_encode(&d, &again);
    check(st == BP_OK && same_bytes(&again, ref, ref_len), "edge: re-encode decoded == ref", bp_status_str(st));
    Edge_free(&d);
    check(d.ints == NULL && d.ints_len == 0, "edge: free resets the value", NULL);

    /* wrong version: flip the first version character ("2.1.0" -> "3.1.0") */
    uint8_t *bad = malloc(ref_len);
    memcpy(bad, ref, ref_len);
    bad[1] = '3';
    st = Edge_decode(&d, bad, ref_len);
    check(st == BP_ERR_VERSION, "edge: wrong version rejected", bp_status_str(st));
    /* wrong version length */
    bad[0] = 8; /* zigzag 4 */
    st = Edge_decode(&d, bad, ref_len);
    check(st != BP_OK, "edge: wrong version length rejected", bp_status_str(st));
    free(bad);

    /* every strict prefix must fail; copy into an exact-size heap block so
     * ASan catches any read past the end */
    int trunc_ok = 1;
    char detail[128] = "";
    for (size_t n = 0; n < ref_len; n++) {
        uint8_t *p = malloc(n ? n : 1);
        memcpy(p, ref, n);
        st = Edge_decode(&d, p, n);
        free(p);
        if (st == BP_OK) {
            trunc_ok = 0;
            snprintf(detail, sizeof detail, "prefix of %zu bytes decoded", n);
            Edge_free(&d);
            break;
        }
    }
    check(trunc_ok, "edge: every truncation rejected", detail);
    st = Edge_decode(&d, NULL, 0);
    check(st != BP_OK, "edge: NULL input rejected", bp_status_str(st));

    /* bogus lengths: huge array count / string length after a valid version */
    static const uint8_t huge_count[] = {10, '2', '.', '1', '.', '0', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                         0, 0, 0, 0, 0, 0xfe, 0xff, 0xff, 0xff, 0x0f};
    st = Edge_decode(&d, huge_count, sizeof huge_count);
    check(st == BP_ERR_TRUNCATED, "edge: huge array count rejected", bp_status_str(st));
    static const uint8_t huge_str[] = {10, '2', '.', '1', '.', '0', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                       0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 'a'};
    st = Edge_decode(&d, huge_str, sizeof huge_str);
    check(st == BP_ERR_TRUNCATED, "edge: huge string length rejected", bp_status_str(st));
    static const uint8_t neg_len[] = {10, '2', '.', '1', '.', '0', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
    st = Edge_decode(&d, neg_len, sizeof neg_len);
    check(st == BP_ERR_LENGTH, "edge: negative string length rejected", bp_status_str(st));
    static const uint8_t long_varint[] = {10, '2', '.', '1', '.', '0', 0xff, 0xff, 0xff, 0xff, 0xff,
                                          0xff, 0xff, 0xff, 0xff, 0xff, 0x01};
    st = Edge_decode(&d, long_varint, sizeof long_varint);
    check(st == BP_ERR_OVERFLOW, "edge: 11-byte varint rejected", bp_status_str(st));

    /* encode rejects inconsistent pointer/length and leaves the buffer as it was */
    Edge bogus;
    Edge_init(&bogus);
    bogus.ints_len = 3; /* ints == NULL */
    size_t before = out.len;
    st = Edge_encode(&bogus, &out);
    check(st == BP_ERR_INVALID && out.len == before, "edge: encode rejects NULL array with length", bp_status_str(st));

    bp_buffer_free(&out);
    bp_buffer_free(&again);
    free(ref);
}

/* ---------------- float32 ---------------- */

static const uint8_t f32_ref[] = {0x0a, '1', '.', '0', '.', '0', 0xb0, 0x6d, 0x06, 0xa8, 0x2d, 0xb0, 0x6d,
                                  0x80, 0x80, 0x80, 0xa0, 0x01, 0x00};

static void test_f32(void) {
    float fs[] = {0.29f, 0.7f, 16777.217f};
    F32 v;
    F32_init(&v);
    v.f = 0.7f;
    v.fs = fs;
    v.fs_len = 3;
    bp_buffer out = {0};
    bp_status st = F32_encode(&v, &out);
    check(st == BP_OK && same_bytes(&out, f32_ref, sizeof f32_ref),
          "f32: 0.7, [0.29, 0.7, 16777.217] -> 7000, [2900, 7000, 167772160]", "bytes differ");
    F32 d;
    st = F32_decode(&d, f32_ref, sizeof f32_ref);
    check(st == BP_OK && d.f == 0.7f && d.fs_len == 3 && d.fs[0] == 0.29f && d.fs[1] == 0.7f &&
              d.fs[2] == 16777.216796875f && d.fs[2] == 16777.217f,
          "f32: decodes to the nearest float32 values", bp_status_str(st));
    if (st == BP_OK) F32_free(&d);
    bp_buffer_free(&out);
}

int main(int argc, char **argv) {
    const char *dir = argc > 1 ? argv[1] : "..";
    test_bench(dir);
    test_edge(dir);
    test_f32();
    printf("c: %d passed, %d failed\n", passed, failed);
    return failed ? 1 : 0;
}
