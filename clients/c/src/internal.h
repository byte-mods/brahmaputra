/* Internal declarations shared by the library's translation units. */
#ifndef BRP_INTERNAL_H
#define BRP_INTERNAL_H

#include "brahmaputra.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

/* API keys, in wire order. */
enum {
    API_PRODUCE = 0,
    API_FETCH = 1,
    API_LIST_OFFSETS = 2,
    API_METADATA = 3,
    API_JOIN_GROUP = 7,
    API_SYNC_GROUP = 8,
    API_HEARTBEAT = 9,
    API_OFFSET_COMMIT = 10,
    API_OFFSET_FETCH = 11,
    API_API_VERSIONS = 14,
    API_LEAVE_GROUP = 18
};

/* ---- errors ------------------------------------------------------------ */

brp_err_t brp_set_error(brp_err_t err, const char *fmt, ...)
#if defined(__GNUC__)
    __attribute__((format(printf, 2, 3)))
#endif
    ;
brp_err_t brp_server_error(int32_t code, const char *context);
bool brp_retriable(int32_t code);

/* ---- time / misc ------------------------------------------------------- */

int64_t brp_now_ms(void);
void brp_sleep_ms(int64_t ms);
void brp_deadline_ts(struct timespec *ts, int64_t ms_from_now);
char *brp_strdup(const char *s);
void *brp_memdup(const void *p, size_t n); /* n==0 -> 1-byte allocation */

/* ---- growable byte buffer --------------------------------------------- */

typedef struct buf {
    uint8_t *data;
    size_t len;
    size_t cap;
    bool oom;
} buf_t;

void buf_free(buf_t *b);
void buf_append(buf_t *b, const void *p, size_t n);
void buf_u8(buf_t *b, uint8_t v);
void buf_be16(buf_t *b, uint16_t v);
void buf_be32(buf_t *b, uint32_t v);
void buf_be64(buf_t *b, uint64_t v);
void buf_uvarint(buf_t *b, uint64_t v);

/* BitPacker writer: zigzag varints, count-prefixed strings. */
void bp_init(buf_t *b); /* starts with the schema version */
void bp_i32(buf_t *b, int32_t v);
void bp_i64(buf_t *b, int64_t v);
void bp_bool(buf_t *b, bool v);
void bp_str(buf_t *b, const char *s);
void bp_strn(buf_t *b, const char *s, size_t n);

/* BitPacker reader. Errors are sticky: check r->err at the end. */
typedef struct bpr {
    const uint8_t *data;
    size_t len;
    size_t pos;
    bool err;
} bpr_t;

brp_err_t bpr_init(bpr_t *r, const uint8_t *data, size_t len);
uint64_t bpr_uvarint(bpr_t *r);
int32_t bpr_i32(bpr_t *r);
int64_t bpr_i64(bpr_t *r);
bool bpr_bool(bpr_t *r);
char *bpr_str(bpr_t *r); /* malloc'd, NUL-terminated; NULL on error */
void bpr_skip_str(bpr_t *r);
/* Returns a count read as int32, 0 if negative or implausibly large. */
size_t bpr_count(bpr_t *r);

/* ---- record batches ---------------------------------------------------- */

typedef struct raw_record {
    uint8_t *key; /* NULL = null */
    size_t key_len;
    uint8_t *value; /* NULL = null */
    size_t value_len;
    int64_t timestamp_ms;
    brp_header_t *headers; /* owned copies */
    size_t header_count;
} raw_record_t;

void raw_record_clear(raw_record_t *r);

brp_err_t brp_encode_batch(const raw_record_t *records, size_t count,
                           brp_compression_t codec, buf_t *out);

/* Decodes every batch in data, appending consumed records whose offset is
 * >= min_offset. */
brp_err_t brp_decode_batches(const uint8_t *data, size_t len,
                             const char *topic, int32_t partition,
                             int64_t min_offset, brp_record_t **records,
                             size_t *count, size_t *cap);

/* ---- connection / router ---------------------------------------------- */

typedef struct brp_conn brp_conn_t;

brp_err_t brp_conn_request(brp_conn_t *conn, int16_t api_key, const buf_t *body,
                           uint8_t **response, size_t *response_len);
brp_err_t brp_conn_send_oneway(brp_conn_t *conn, int16_t api_key,
                               const buf_t *body);

brp_err_t brp_client_new_internal(const char *bootstrap, const char *client_id,
                                  int dial_timeout_ms, int io_timeout_ms,
                                  brp_client_t **out);
brp_err_t brp_client_conn_for(brp_client_t *client, const char *topic,
                              int32_t partition, brp_conn_t **out);
brp_conn_t *brp_client_seed(brp_client_t *client);

/* ---- record array helpers --------------------------------------------- */

brp_err_t brp_records_push(brp_record_t **records, size_t *count, size_t *cap,
                           brp_record_t *record);

#endif
