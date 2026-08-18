package main

// ==========================================
// 7. PURE C TEMPLATE (Generated alongside Python)
// ==========================================

const tmplPureH = `
#ifndef {{.Config.InputFileName | Title}}_MODEL_H
#define {{.Config.InputFileName | Title}}_MODEL_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define VERSION "{{.Config.Version}}"

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
{{range .Classes}}
typedef struct {{.Name}} {
    {{range .Fields}}{{mapTypeC .Type}} {{if .IsArray}}*{{.Name}}; int {{.Name}}_len;{{else}}{{.Name}};{{end}}
    {{end}}
} {{.Name}};

void {{.Name}}_encode({{.Name}} *self, ZeroCopyByteBuff *buf);
{{.Name}} *{{.Name}}_decode(ZeroCopyByteBuff *buf);
void {{.Name}}_free({{.Name}} *self);
{{end}}

#endif
`

const tmplPureC = `
#include "{{.Config.InputFileName}}_model.h"
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
{{range .Classes}}

void {{.Name}}_encode({{.Name}} *self, ZeroCopyByteBuff *buf) {
    {{range .Fields}}
    {{if .IsArray}}
    ZeroCopyByteBuff_put_int32(buf, self->{{.Name}}_len);
    for(int i=0; i<self->{{.Name}}_len; i++) {
        {{encodeFieldC "self" .Name "i" .Type}}
    }
    {{else}}
    {{encodeFieldC "self" .Name "" .Type}}
    {{end}}
    {{end}}
}

{{.Name}} *{{.Name}}_decode(ZeroCopyByteBuff *buf) {
    {{.Name}} *obj = malloc(sizeof({{.Name}}));
    {{range .Fields}}
    {{if .IsArray}}
    obj->{{.Name}}_len = ZeroCopyByteBuff_get_int32(buf);
    obj->{{.Name}} = malloc(obj->{{.Name}}_len * sizeof({{mapTypeC .Type}}));
    for(int i=0; i<obj->{{.Name}}_len; i++) {
        {{decodeFieldC "obj" .Name "i" .Type}}
    }
    {{else}}
    {{decodeFieldC "obj" .Name "" .Type}}
    {{end}}
    {{end}}
    return obj;
}

void {{.Name}}_free({{.Name}} *self) {
    {{range .Fields}}
    {{if .IsArray}}
    {{if isClass .Type}}
    for(int i=0; i<self->{{.Name}}_len; i++) {
        {{.Type}}_free(&self->{{.Name}}[i]);
    }
    {{end}}
    free(self->{{.Name}});
    {{else}}
    {{/* if string or class, need free logic, but for simplicity assuming shallow structs for now or relying on simple types */}}
    {{end}}
    {{end}}
    free(self);
}
{{end}}
`
