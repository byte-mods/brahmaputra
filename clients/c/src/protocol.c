/*
 * Wire encodings, checksums, partitioner, codecs and record batches.
 *
 * Three encodings share one connection and they do not agree with each
 * other, so keeping them straight is most of the work:
 *
 *  - The frame header is fixed big-endian (see client.c).
 *  - A request body is BitPacker: every integer is a zigzag varint, every
 *    string and array is a varint count followed by its contents, and the
 *    whole body is prefixed with the schema version string.
 *  - A record batch is neither: fixed big-endian header fields and plain
 *    (non-zigzag) varints inside each record, because the broker stamps
 *    offsets into it in place and validates its CRC without decoding it.
 */
#define _POSIX_C_SOURCE 200809L
#include "internal.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef BRP_WITH_ZLIB
#include <zlib.h>
#endif

/* ------------------------------------------------------------------------ */
/* Errors                                                                   */
/* ------------------------------------------------------------------------ */

static _Thread_local char g_last_error[512] = "no error";

const char *brp_last_error(void) { return g_last_error; }

brp_err_t brp_set_error(brp_err_t err, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vsnprintf(g_last_error, sizeof g_last_error, fmt, args);
    va_end(args);
    return err;
}

const char *brp_err_name(brp_err_t err) {
    switch (err) {
    case BRP_OK: return "NONE";
    case BRP_ERR_INVALID_ARG: return "INVALID_ARG";
    case BRP_ERR_NOMEM: return "NOMEM";
    case BRP_ERR_IO: return "IO";
    case BRP_ERR_TIMEOUT: return "TIMEOUT";
    case BRP_ERR_PROTOCOL: return "PROTOCOL";
    case BRP_ERR_CORRUPT: return "CORRUPT";
    case BRP_ERR_CODEC: return "CODEC";
    case BRP_ERR_BUFFER_FULL: return "BUFFER_FULL";
    case BRP_ERR_NO_OFFSET: return "NO_OFFSET_FOR_PARTITION";
    case BRP_ERR_STATE: return "STATE";
    case BRP_ERR_DELIVERY_TIMEOUT: return "DELIVERY_TIMEOUT";
    case BRP_ERR_UNKNOWN_TOPIC: return "UNKNOWN_TOPIC";
    case BRP_ERR_REBALANCE_FAILED: return "REBALANCE_FAILED";
    case BRP_ERR_UNKNOWN_TOPIC_OR_PARTITION: return "UNKNOWN_TOPIC_OR_PARTITION";
    case BRP_ERR_OFFSET_OUT_OF_RANGE: return "OFFSET_OUT_OF_RANGE";
    case BRP_ERR_INVALID_REQUEST: return "INVALID_REQUEST";
    case BRP_ERR_UNSUPPORTED_VERSION: return "UNSUPPORTED_VERSION";
    case BRP_ERR_INTERNAL: return "INTERNAL";
    case BRP_ERR_NOT_LEADER_OR_FOLLOWER: return "NOT_LEADER_OR_FOLLOWER";
    case BRP_ERR_FENCED_BROKER_EPOCH: return "FENCED_BROKER_EPOCH";
    case BRP_ERR_FENCED_LEADER_EPOCH: return "FENCED_LEADER_EPOCH";
    case BRP_ERR_UNKNOWN_LEADER_EPOCH: return "UNKNOWN_LEADER_EPOCH";
    case BRP_ERR_NOT_ENOUGH_REPLICAS: return "NOT_ENOUGH_REPLICAS";
    case BRP_ERR_FENCED_PRODUCER_EPOCH: return "FENCED_PRODUCER_EPOCH";
    case BRP_ERR_OUT_OF_ORDER_SEQUENCE: return "OUT_OF_ORDER_SEQUENCE";
    case BRP_ERR_UNKNOWN_MEMBER_ID: return "UNKNOWN_MEMBER_ID";
    case BRP_ERR_REBALANCE_IN_PROGRESS: return "REBALANCE_IN_PROGRESS";
    case BRP_ERR_NOT_COORDINATOR: return "NOT_COORDINATOR";
    case BRP_ERR_ILLEGAL_GENERATION: return "ILLEGAL_GENERATION";
    case BRP_ERR_COORDINATOR_LOAD_IN_PROGRESS: return "COORDINATOR_LOAD_IN_PROGRESS";
    case BRP_ERR_SASL_AUTHENTICATION_FAILED: return "SASL_AUTHENTICATION_FAILED";
    case BRP_ERR_AUTHORIZATION_FAILED: return "AUTHORIZATION_FAILED";
    }
    return "UNKNOWN";
}

brp_err_t brp_server_error(int32_t code, const char *context) {
    return brp_set_error((brp_err_t)code, "broker returned %s[%d] (%s)",
                         brp_err_name((brp_err_t)code), (int)code, context);
}

/* Every code here is one the broker returns strictly before it appends, so
 * a retry cannot duplicate a record. */
bool brp_retriable(int32_t code) {
    switch (code) {
    case BRP_ERR_NOT_LEADER_OR_FOLLOWER:
    case BRP_ERR_FENCED_LEADER_EPOCH:
    case BRP_ERR_UNKNOWN_LEADER_EPOCH:
    case BRP_ERR_NOT_ENOUGH_REPLICAS:
    case BRP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
    case BRP_ERR_INTERNAL:
        return true;
    default:
        return false;
    }
}

void brp_free(void *ptr) { free(ptr); }

/* ------------------------------------------------------------------------ */
/* Time and allocation helpers                                              */
/* ------------------------------------------------------------------------ */

int64_t brp_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

void brp_sleep_ms(int64_t ms) {
    if (ms <= 0) return;
    struct timespec ts = {(time_t)(ms / 1000), (long)(ms % 1000) * 1000000L};
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
    }
}

void brp_deadline_ts(struct timespec *ts, int64_t ms_from_now) {
    clock_gettime(CLOCK_REALTIME, ts);
    ts->tv_sec += (time_t)(ms_from_now / 1000);
    ts->tv_nsec += (long)(ms_from_now % 1000) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) {
        ts->tv_sec += 1;
        ts->tv_nsec -= 1000000000L;
    }
}

char *brp_strdup(const char *s) {
    if (!s) s = "";
    size_t n = strlen(s) + 1;
    char *out = malloc(n);
    if (out) memcpy(out, s, n);
    return out;
}

void *brp_memdup(const void *p, size_t n) {
    uint8_t *out = malloc(n ? n : 1);
    if (out && n) memcpy(out, p, n);
    return out;
}

/* ------------------------------------------------------------------------ */
/* Byte buffer                                                              */
/* ------------------------------------------------------------------------ */

void buf_free(buf_t *b) {
    free(b->data);
    b->data = NULL;
    b->len = b->cap = 0;
}

static bool buf_reserve(buf_t *b, size_t extra) {
    if (b->oom) return false;
    if (b->len + extra <= b->cap) return true;
    size_t cap = b->cap ? b->cap : 64;
    while (cap < b->len + extra) cap *= 2;
    uint8_t *grown = realloc(b->data, cap);
    if (!grown) {
        b->oom = true;
        return false;
    }
    b->data = grown;
    b->cap = cap;
    return true;
}

void buf_append(buf_t *b, const void *p, size_t n) {
    if (n == 0 || !buf_reserve(b, n)) return;
    memcpy(b->data + b->len, p, n);
    b->len += n;
}

void buf_u8(buf_t *b, uint8_t v) { buf_append(b, &v, 1); }

void buf_be16(buf_t *b, uint16_t v) {
    uint8_t x[2] = {(uint8_t)(v >> 8), (uint8_t)v};
    buf_append(b, x, 2);
}

void buf_be32(buf_t *b, uint32_t v) {
    uint8_t x[4] = {(uint8_t)(v >> 24), (uint8_t)(v >> 16), (uint8_t)(v >> 8),
                    (uint8_t)v};
    buf_append(b, x, 4);
}

void buf_be64(buf_t *b, uint64_t v) {
    buf_be32(b, (uint32_t)(v >> 32));
    buf_be32(b, (uint32_t)v);
}

void buf_uvarint(buf_t *b, uint64_t v) {
    uint8_t x[10];
    size_t n = 0;
    while (v >= 0x80) {
        x[n++] = (uint8_t)(v | 0x80);
        v >>= 7;
    }
    x[n++] = (uint8_t)v;
    buf_append(b, x, n);
}

static uint32_t rd_be32(const uint8_t *p) {
    return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 | (uint32_t)p[2] << 8 |
           (uint32_t)p[3];
}
static uint64_t rd_be64(const uint8_t *p) {
    return (uint64_t)rd_be32(p) << 32 | rd_be32(p + 4);
}
static void wr_be32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

/* ------------------------------------------------------------------------ */
/* BitPacker                                                                */
/* ------------------------------------------------------------------------ */

void bp_init(buf_t *b) {
    memset(b, 0, sizeof *b);
    bp_str(b, BRP_SCHEMA_VERSION);
}

void bp_i32(buf_t *b, int32_t v) {
    uint32_t u = (uint32_t)v;
    buf_uvarint(b, (uint64_t)((u << 1) ^ (uint32_t)(v >> 31)));
}

void bp_i64(buf_t *b, int64_t v) {
    uint64_t u = (uint64_t)v;
    buf_uvarint(b, (u << 1) ^ (uint64_t)(v >> 63));
}

void bp_bool(buf_t *b, bool v) { buf_u8(b, v ? 1 : 0); }

void bp_strn(buf_t *b, const char *s, size_t n) {
    bp_i32(b, (int32_t)n);
    buf_append(b, s, n);
}

void bp_str(buf_t *b, const char *s) { bp_strn(b, s ? s : "", s ? strlen(s) : 0); }

brp_err_t bpr_init(bpr_t *r, const uint8_t *data, size_t len) {
    r->data = data;
    r->len = len;
    r->pos = 0;
    r->err = false;
    char *version = bpr_str(r);
    if (!version)
        return brp_set_error(BRP_ERR_PROTOCOL, "response body too short for schema version");
    /* A mismatch means broker and client disagree about the message shapes
     * themselves; failing loudly beats decoding garbage. */
    if (strcmp(version, BRP_SCHEMA_VERSION) != 0) {
        brp_set_error(BRP_ERR_PROTOCOL,
                      "schema version mismatch: broker speaks \"%s\", client speaks \"%s\"",
                      version, BRP_SCHEMA_VERSION);
        free(version);
        return BRP_ERR_PROTOCOL;
    }
    free(version);
    return BRP_OK;
}

uint64_t bpr_uvarint(bpr_t *r) {
    uint64_t result = 0;
    unsigned shift = 0;
    for (;;) {
        if (r->err || r->pos >= r->len) {
            r->err = true;
            return 0;
        }
        uint8_t b = r->data[r->pos++];
        result |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) return result;
        shift += 7;
        if (shift > 63) {
            r->err = true;
            return 0;
        }
    }
}

int32_t bpr_i32(bpr_t *r) {
    uint32_t v = (uint32_t)bpr_uvarint(r);
    return (int32_t)((v >> 1) ^ (uint32_t)(-(int32_t)(v & 1)));
}

int64_t bpr_i64(bpr_t *r) {
    uint64_t v = bpr_uvarint(r);
    return (int64_t)((v >> 1) ^ (uint64_t)(-(int64_t)(v & 1)));
}

bool bpr_bool(bpr_t *r) {
    if (r->err || r->pos >= r->len) {
        r->err = true;
        return false;
    }
    return r->data[r->pos++] != 0;
}

char *bpr_str(bpr_t *r) {
    int32_t n = bpr_i32(r);
    if (r->err || n < 0 || (size_t)n > r->len - r->pos) {
        r->err = true;
        return NULL;
    }
    char *out = malloc((size_t)n + 1);
    if (!out) {
        r->err = true;
        return NULL;
    }
    memcpy(out, r->data + r->pos, (size_t)n);
    out[n] = 0;
    r->pos += (size_t)n;
    return out;
}

void bpr_skip_str(bpr_t *r) {
    int32_t n = bpr_i32(r);
    if (r->err || n < 0 || (size_t)n > r->len - r->pos) {
        r->err = true;
        return;
    }
    r->pos += (size_t)n;
}

size_t bpr_count(bpr_t *r) {
    int32_t n = bpr_i32(r);
    if (r->err || n < 0) return 0;
    /* Every element takes at least one byte; a larger count is corrupt and
     * allocating on it would let a tiny response ask for gigabytes. */
    if ((size_t)n > r->len - r->pos) {
        r->err = true;
        return 0;
    }
    return (size_t)n;
}

/* ------------------------------------------------------------------------ */
/* CRC32C and murmur2                                                       */
/* ------------------------------------------------------------------------ */

static uint32_t crc_table[256];
static pthread_once_t crc_once = PTHREAD_ONCE_INIT;

static void crc_init(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++) c = (c & 1) ? (c >> 1) ^ 0x82F63B78u : c >> 1;
        crc_table[i] = c;
    }
}

uint32_t brp_crc32c(const void *data, size_t len) {
    pthread_once(&crc_once, crc_init);
    const uint8_t *p = data;
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) crc = crc_table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

/* Kafka's murmur2, transcribed rather than "a murmur2": a C producer and a
 * Rust producer writing the same key must pick the same partition. */
uint32_t brp_murmur2(const void *data, size_t len) {
    const uint8_t *d = data;
    const uint32_t seed = 0x9747b28cu, m = 0x5bd1e995u;
    const int r = 24;
    uint32_t h = seed ^ (uint32_t)len;
    size_t chunks = len / 4;
    for (size_t i = 0; i < chunks; i++) {
        const uint8_t *p = d + i * 4;
        uint32_t k = (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 |
                     (uint32_t)p[3] << 24;
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
    }
    size_t tail = chunks * 4;
    switch (len - tail) {
    case 3: h ^= (uint32_t)d[tail + 2] << 16; /* fall through */
    case 2: h ^= (uint32_t)d[tail + 1] << 8;  /* fall through */
    case 1:
        h ^= (uint32_t)d[tail];
        h *= m;
        break;
    default: break;
    }
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

int32_t brp_partition_for_key(const void *key, size_t key_len,
                              const int32_t *partitions, size_t count) {
    if (count == 0) return -1;
    return partitions[(brp_murmur2(key, key_len) & 0x7fffffffu) % count];
}

/* ------------------------------------------------------------------------ */
/* Codecs                                                                   */
/* ------------------------------------------------------------------------ */

#define MAX_DECOMPRESSED ((size_t)256 * 1024 * 1024)

typedef struct codec_entry {
    brp_codec_fn compress;
    brp_codec_fn decompress;
    void *opaque;
} codec_entry_t;

static codec_entry_t g_codecs[5];
static pthread_mutex_t g_codecs_mu = PTHREAD_MUTEX_INITIALIZER;

static const char *codec_name(brp_compression_t c) {
    switch (c) {
    case BRP_COMPRESSION_NONE: return "none";
    case BRP_COMPRESSION_LZ4: return "lz4";
    case BRP_COMPRESSION_ZSTD: return "zstd";
    case BRP_COMPRESSION_SNAPPY: return "snappy";
    case BRP_COMPRESSION_GZIP: return "gzip";
    }
    return "unknown";
}

brp_err_t brp_compression_parse(const char *name, brp_compression_t *out) {
    static const brp_compression_t all[] = {
        BRP_COMPRESSION_NONE, BRP_COMPRESSION_LZ4, BRP_COMPRESSION_ZSTD,
        BRP_COMPRESSION_SNAPPY, BRP_COMPRESSION_GZIP};
    if (!name || !*name) name = "none";
    for (size_t i = 0; i < sizeof all / sizeof all[0]; i++) {
        if (strcmp(name, codec_name(all[i])) == 0) {
            *out = all[i];
            return BRP_OK;
        }
    }
    return brp_set_error(BRP_ERR_INVALID_ARG,
                         "unknown compression \"%s\" (none, lz4, zstd, snappy, gzip)", name);
}

brp_err_t brp_register_codec(brp_compression_t codec, brp_codec_fn compress,
                             brp_codec_fn decompress, void *opaque) {
    if ((int)codec <= 0 || (int)codec > 4)
        return brp_set_error(BRP_ERR_INVALID_ARG, "cannot register codec %d", (int)codec);
    if ((compress == NULL) != (decompress == NULL))
        return brp_set_error(BRP_ERR_INVALID_ARG, "a codec needs both directions");
    pthread_mutex_lock(&g_codecs_mu);
    g_codecs[codec].compress = compress;
    g_codecs[codec].decompress = decompress;
    g_codecs[codec].opaque = opaque;
    pthread_mutex_unlock(&g_codecs_mu);
    return BRP_OK;
}

static codec_entry_t codec_lookup(brp_compression_t codec) {
    codec_entry_t e = {0};
    if ((int)codec < 0 || (int)codec > 4) return e;
    pthread_mutex_lock(&g_codecs_mu);
    e = g_codecs[codec];
    pthread_mutex_unlock(&g_codecs_mu);
    return e;
}

int brp_codec_available(brp_compression_t codec) {
    if (codec == BRP_COMPRESSION_NONE) return 1;
#ifdef BRP_WITH_ZLIB
    if (codec == BRP_COMPRESSION_GZIP) return 1;
#endif
    return codec_lookup(codec).compress != NULL;
}

#ifdef BRP_WITH_ZLIB
static int gzip_compress(const uint8_t *in, size_t in_len, uint8_t **out,
                         size_t *out_len) {
    z_stream zs;
    memset(&zs, 0, sizeof zs);
    if (deflateInit2(&zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8,
                     Z_DEFAULT_STRATEGY) != Z_OK)
        return -1;
    size_t bound = deflateBound(&zs, (uLong)in_len) + 32;
    uint8_t *dst = malloc(bound);
    if (!dst) {
        deflateEnd(&zs);
        return -1;
    }
    zs.next_in = (Bytef *)(uintptr_t)in;
    zs.avail_in = (uInt)in_len;
    zs.next_out = dst;
    zs.avail_out = (uInt)bound;
    int rc = deflate(&zs, Z_FINISH);
    if (rc != Z_STREAM_END) {
        deflateEnd(&zs);
        free(dst);
        return -1;
    }
    *out_len = zs.total_out;
    *out = dst;
    deflateEnd(&zs);
    return 0;
}

static int gzip_decompress(const uint8_t *in, size_t in_len, uint8_t **out,
                           size_t *out_len) {
    z_stream zs;
    memset(&zs, 0, sizeof zs);
    if (inflateInit2(&zs, 15 + 32) != Z_OK) return -1;
    size_t cap = in_len * 4 + 64;
    uint8_t *dst = malloc(cap);
    if (!dst) {
        inflateEnd(&zs);
        return -1;
    }
    zs.next_in = (Bytef *)(uintptr_t)in;
    zs.avail_in = (uInt)in_len;
    for (;;) {
        zs.next_out = dst + zs.total_out;
        zs.avail_out = (uInt)(cap - zs.total_out);
        int rc = inflate(&zs, Z_NO_FLUSH);
        if (rc == Z_STREAM_END) break;
        if (rc != Z_OK && rc != Z_BUF_ERROR) goto fail;
        if (zs.avail_out == 0) {
            /* Capped so a corrupt or hostile batch cannot name gigabytes of
             * output that this process allocates before rejecting it. */
            if (cap >= MAX_DECOMPRESSED) goto fail;
            size_t next = cap * 2 > MAX_DECOMPRESSED ? MAX_DECOMPRESSED : cap * 2;
            uint8_t *grown = realloc(dst, next);
            if (!grown) goto fail;
            dst = grown;
            cap = next;
        } else if (rc == Z_BUF_ERROR) {
            goto fail; /* truncated input */
        }
    }
    *out_len = zs.total_out;
    *out = dst;
    inflateEnd(&zs);
    return 0;
fail:
    inflateEnd(&zs);
    free(dst);
    return -1;
}
#endif

/* On success *out is either a new malloc'd buffer (*owned=true) or `in`. */
static brp_err_t codec_apply(brp_compression_t codec, bool compress,
                             const uint8_t *in, size_t in_len, uint8_t **out,
                             size_t *out_len, bool *owned) {
    *owned = false;
    if (codec == BRP_COMPRESSION_NONE) {
        *out = (uint8_t *)(uintptr_t)in;
        *out_len = in_len;
        return BRP_OK;
    }
    codec_entry_t e = codec_lookup(codec);
    brp_codec_fn fn = compress ? e.compress : e.decompress;
    int rc;
    if (fn) {
        rc = fn(in, in_len, out, out_len, e.opaque);
    }
#ifdef BRP_WITH_ZLIB
    else if (codec == BRP_COMPRESSION_GZIP) {
        rc = compress ? gzip_compress(in, in_len, out, out_len)
                      : gzip_decompress(in, in_len, out, out_len);
    }
#endif
    else {
        return brp_set_error(BRP_ERR_CODEC,
                             "%s compression is not available; register it with "
                             "brp_register_codec or use none/gzip",
                             codec_name(codec));
    }
    if (rc != 0)
        return brp_set_error(BRP_ERR_CODEC, "%s %s failed", codec_name(codec),
                             compress ? "compression" : "decompression");
    *owned = true;
    return BRP_OK;
}

/* ------------------------------------------------------------------------ */
/* Record batches                                                           */
/* ------------------------------------------------------------------------ */

#define BATCH_HEADER_LEN 12
#define MIN_BATCH_LENGTH (4 + 1 + 4 + 2 + 4 + 8)
#define PRODUCER_EXTENSION_LEN (8 + 2 + 4)
#define MAGIC_V1 1
#define MAGIC_V2 2
#define COMPRESSION_MASK 0x0007
#define HEADERS_BIT 0x0008
/* Some record in the batch has a null value (a tombstone). Set only when
 * one is present, so a batch without one encodes exactly as it always did. */
#define NULL_VALUE_BIT 0x0040

void raw_record_clear(raw_record_t *r) {
    free(r->key);
    free(r->value);
    for (size_t i = 0; i < r->header_count; i++) {
        free((void *)(uintptr_t)r->headers[i].key);
        free((void *)(uintptr_t)r->headers[i].value);
    }
    free(r->headers);
    memset(r, 0, sizeof *r);
}

/* The broker never re-encodes this: it validates the header, stamps
 * base_offset and leader_epoch in place (both sit before the CRC, so it
 * stays valid) and writes these bytes to disk. */
brp_err_t brp_encode_batch(const raw_record_t *records, size_t count,
                           brp_compression_t codec, buf_t *out) {
    bool has_headers = false, has_nulls = false;
    int64_t max_ts = count ? records[0].timestamp_ms : brp_now_ms();
    for (size_t i = 0; i < count; i++) {
        if (records[i].header_count) has_headers = true;
        if (!records[i].value) has_nulls = true;
        if (records[i].timestamp_ms > max_ts) max_ts = records[i].timestamp_ms;
    }

    buf_t payload = {0}, rec = {0};
    for (size_t i = 0; i < count; i++) {
        const raw_record_t *r = &records[i];
        rec.len = 0;
        if (!r->key) {
            buf_uvarint(&rec, 0);
        } else {
            buf_uvarint(&rec, (uint64_t)r->key_len + 1);
            buf_append(&rec, r->key, r->key_len);
        }
        if (has_nulls) {
            /* Widened encoding: 0 = null, n+1 = n bytes. An empty value is
             * an ordinary record and must not read back as a tombstone. */
            if (!r->value) {
                buf_uvarint(&rec, 0);
            } else {
                buf_uvarint(&rec, (uint64_t)r->value_len + 1);
                buf_append(&rec, r->value, r->value_len);
            }
        } else {
            buf_uvarint(&rec, (uint64_t)r->value_len);
            buf_append(&rec, r->value, r->value_len);
        }
        int64_t delta = r->timestamp_ms - max_ts;
        buf_uvarint(&rec, ((uint64_t)delta << 1) ^ (uint64_t)(delta >> 63));
        if (has_headers) {
            buf_uvarint(&rec, r->header_count);
            for (size_t h = 0; h < r->header_count; h++) {
                const brp_header_t *hd = &r->headers[h];
                size_t klen = strlen(hd->key);
                buf_uvarint(&rec, klen);
                buf_append(&rec, hd->key, klen);
                if (!hd->value) {
                    buf_uvarint(&rec, 0);
                } else {
                    buf_uvarint(&rec, (uint64_t)hd->value_len + 1);
                    buf_append(&rec, hd->value, hd->value_len);
                }
            }
        }
        buf_uvarint(&payload, rec.len);
        buf_append(&payload, rec.data, rec.len);
    }
    buf_free(&rec);
    if (payload.oom || rec.oom) {
        buf_free(&payload);
        return brp_set_error(BRP_ERR_NOMEM, "out of memory encoding batch");
    }

    uint8_t *compressed;
    size_t compressed_len;
    bool owned;
    brp_err_t err = codec_apply(codec, true, payload.data, payload.len,
                                &compressed, &compressed_len, &owned);
    if (err) {
        buf_free(&payload);
        return err;
    }

    uint16_t attributes = (uint16_t)((unsigned)codec & COMPRESSION_MASK);
    if (has_headers) attributes |= HEADERS_BIT;
    if (has_nulls) attributes |= NULL_VALUE_BIT;
    size_t batch_length = MIN_BATCH_LENGTH + compressed_len;

    memset(out, 0, sizeof *out);
    buf_be64(out, 0); /* base_offset, stamped by the broker */
    buf_be32(out, (uint32_t)batch_length);
    buf_be32(out, 0); /* leader_epoch, likewise */
    buf_u8(out, MAGIC_V1);
    size_t crc_at = out->len;
    buf_be32(out, 0);
    buf_be16(out, attributes);
    buf_be32(out, (uint32_t)(count ? count - 1 : 0)); /* last_offset_delta */
    buf_be64(out, (uint64_t)max_ts);
    buf_append(out, compressed, compressed_len);
    if (owned) free(compressed);
    buf_free(&payload);
    if (out->oom) {
        buf_free(out);
        return brp_set_error(BRP_ERR_NOMEM, "out of memory encoding batch");
    }
    wr_be32(out->data + crc_at, brp_crc32c(out->data + crc_at + 4, out->len - crc_at - 4));
    return BRP_OK;
}

static bool get_uvarint(const uint8_t *d, size_t len, size_t *pos, uint64_t *out) {
    uint64_t result = 0;
    unsigned shift = 0;
    for (;;) {
        if (*pos >= len) return false;
        uint8_t b = d[(*pos)++];
        result |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) {
            *out = result;
            return true;
        }
        shift += 7;
        if (shift > 63) return false;
    }
}

brp_err_t brp_records_push(brp_record_t **records, size_t *count, size_t *cap,
                           brp_record_t *record) {
    if (*count == *cap) {
        size_t next = *cap ? *cap * 2 : 16;
        brp_record_t *grown = realloc(*records, next * sizeof **records);
        if (!grown) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
        *records = grown;
        *cap = next;
    }
    (*records)[(*count)++] = *record;
    return BRP_OK;
}

static void record_clear(brp_record_t *r) {
    free(r->topic);
    free(r->key);
    free(r->value);
    for (size_t i = 0; i < r->header_count; i++) {
        free((void *)(uintptr_t)r->headers[i].key);
        free((void *)(uintptr_t)r->headers[i].value);
    }
    free(r->headers);
}

void brp_records_free(brp_record_t *records, size_t count) {
    if (!records) return;
    for (size_t i = 0; i < count; i++) record_clear(&records[i]);
    free(records);
}

const brp_header_t *brp_record_find_header(const brp_record_t *record,
                                           const char *key) {
    for (size_t i = 0; i < record->header_count; i++)
        if (strcmp(record->headers[i].key, key) == 0) return &record->headers[i];
    return NULL;
}

const uint8_t *brp_record_header(const brp_record_t *record, const char *key,
                                 size_t *value_len) {
    const brp_header_t *h = brp_record_find_header(record, key);
    if (value_len) *value_len = h ? h->value_len : 0;
    return h ? h->value : NULL;
}

#define CORRUPT(msg) do { err = brp_set_error(BRP_ERR_CORRUPT, "%s", msg); goto fail; } while (0)

static brp_err_t decode_records(const uint8_t *p, size_t len, bool has_headers,
                                bool has_nulls, const char *topic,
                                int32_t partition, int64_t base_offset,
                                int64_t max_ts, int64_t min_offset,
                                brp_record_t **records, size_t *count,
                                size_t *cap) {
    size_t pos = 0;
    int64_t index = 0;
    brp_err_t err = BRP_OK;
    while (pos < len) {
        brp_record_t rec;
        memset(&rec, 0, sizeof rec);
        uint64_t rec_len, v;
        if (!get_uvarint(p, len, &pos, &rec_len) || rec_len > len - pos)
            CORRUPT("truncated record");
        size_t end = pos + (size_t)rec_len;

        if (!get_uvarint(p, end, &pos, &v)) CORRUPT("truncated record key");
        if (v > 0) {
            if (v - 1 > end - pos) CORRUPT("truncated record key");
            rec.key_len = (size_t)(v - 1);
            rec.key = brp_memdup(p + pos, rec.key_len);
            if (!rec.key) goto oom;
            pos += rec.key_len;
        }
        if (!get_uvarint(p, end, &pos, &v)) CORRUPT("truncated record value");
        if (has_nulls && v == 0) {
            rec.value = NULL; /* tombstone */
        } else {
            size_t vlen = (size_t)(has_nulls ? v - 1 : v);
            if (vlen > end - pos) CORRUPT("truncated record value");
            rec.value = brp_memdup(p + pos, vlen);
            if (!rec.value) goto oom;
            rec.value_len = vlen;
            pos += vlen;
        }
        if (!get_uvarint(p, end, &pos, &v)) CORRUPT("truncated timestamp");
        int64_t delta = (int64_t)((v >> 1) ^ (uint64_t)(-(int64_t)(v & 1)));
        rec.timestamp = max_ts + delta;

        if (has_headers) {
            uint64_t hcount;
            if (!get_uvarint(p, end, &pos, &hcount)) CORRUPT("truncated header count");
            if (hcount > end - pos) CORRUPT("record header count exceeds record");
            if (hcount) {
                rec.headers = calloc((size_t)hcount, sizeof *rec.headers);
                if (!rec.headers) goto oom;
            }
            for (uint64_t h = 0; h < hcount; h++) {
                uint64_t klen, vp1;
                if (!get_uvarint(p, end, &pos, &klen) || klen > end - pos)
                    CORRUPT("truncated header key");
                char *key = malloc((size_t)klen + 1);
                if (!key) goto oom;
                memcpy(key, p + pos, (size_t)klen);
                key[klen] = 0;
                pos += (size_t)klen;
                rec.headers[h].key = key;
                rec.header_count = (size_t)h + 1;
                if (!get_uvarint(p, end, &pos, &vp1)) CORRUPT("truncated header value");
                if (vp1 > 0) {
                    if (vp1 - 1 > end - pos) CORRUPT("truncated header value");
                    rec.headers[h].value = brp_memdup(p + pos, (size_t)(vp1 - 1));
                    if (!rec.headers[h].value) goto oom;
                    rec.headers[h].value_len = (size_t)(vp1 - 1);
                    pos += (size_t)(vp1 - 1);
                }
            }
        }
        if (pos != end) CORRUPT("trailing bytes in record");

        int64_t offset = base_offset + index++;
        /* A batch can start before the requested offset; skip what the
         * caller has already seen. */
        if (offset < min_offset) {
            record_clear(&rec);
            continue;
        }
        rec.offset = offset;
        rec.partition = partition;
        rec.topic = brp_strdup(topic);
        if (!rec.topic) goto oom;
        if ((err = brp_records_push(records, count, cap, &rec)) != BRP_OK) {
            record_clear(&rec);
            return err;
        }
        continue;
    oom:
        err = brp_set_error(BRP_ERR_NOMEM, "out of memory decoding records");
    fail:
        record_clear(&rec);
        return err;
    }
    return BRP_OK;
}

brp_err_t brp_decode_batches(const uint8_t *data, size_t len, const char *topic,
                             int32_t partition, int64_t min_offset,
                             brp_record_t **records, size_t *count, size_t *cap) {
    size_t offset = 0;
    while (offset < len) {
        if (len - offset < BATCH_HEADER_LEN)
            return brp_set_error(BRP_ERR_CORRUPT, "truncated batch header");
        const uint8_t *b = data + offset;
        int64_t base_offset = (int64_t)rd_be64(b);
        int32_t batch_length = (int32_t)rd_be32(b + 8);
        if (batch_length < MIN_BATCH_LENGTH)
            return brp_set_error(BRP_ERR_CORRUPT, "batch_length too small");
        size_t end = offset + BATCH_HEADER_LEN + (size_t)batch_length;
        if (end > len) return brp_set_error(BRP_ERR_CORRUPT, "truncated batch body");
        const uint8_t *body = b + BATCH_HEADER_LEN;
        uint8_t magic = body[4];
        if (magic != MAGIC_V1 && magic != MAGIC_V2)
            return brp_set_error(BRP_ERR_CORRUPT, "unsupported magic %u", magic);
        uint32_t stored = rd_be32(body + 5);
        uint32_t computed = brp_crc32c(body + 9, (size_t)batch_length - 9);
        if (stored != computed)
            return brp_set_error(BRP_ERR_CORRUPT,
                                 "crc mismatch: stored %#010x, computed %#010x",
                                 stored, computed);
        const uint8_t *cursor = body + 9;
        uint16_t attributes = (uint16_t)(cursor[0] << 8 | cursor[1]);
        int64_t max_ts = (int64_t)rd_be64(cursor + 6);
        cursor += 14;
        if (magic == MAGIC_V2) cursor += PRODUCER_EXTENSION_LEN;
        const uint8_t *batch_end = data + end;
        if (cursor > batch_end) return brp_set_error(BRP_ERR_CORRUPT, "truncated batch");

        uint8_t *payload;
        size_t payload_len;
        bool owned;
        brp_err_t err = codec_apply((brp_compression_t)(attributes & COMPRESSION_MASK),
                                    false, cursor, (size_t)(batch_end - cursor),
                                    &payload, &payload_len, &owned);
        if (err) return err;
        err = decode_records(payload, payload_len, attributes & HEADERS_BIT,
                             attributes & NULL_VALUE_BIT, topic, partition,
                             base_offset, max_ts, min_offset, records, count, cap);
        if (owned) free(payload);
        if (err) return err;
        offset = end;
    }
    return BRP_OK;
}
