/*
 * Producer: per-partition batching, linger thread, bounded buffer, retries.
 */
#define _POSIX_C_SOURCE 200809L
#include "internal.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct slot {
    char *topic;
    int32_t partition;
    raw_record_t *recs;
    size_t n, cap;
    size_t bytes;
} slot_t;

struct brp_producer {
    brp_producer_config_t config;
    char *client_id;
    brp_compression_t codec;
    brp_client_t *client;

    pthread_mutex_t mu;
    pthread_cond_t space_cond;  /* buffer space released */
    pthread_cond_t linger_cond; /* wakes the linger thread on close */
    /* Serialises take-and-send so two flushes of one partition (the linger
     * thread and a caller) cannot reorder its batches. */
    pthread_mutex_t flush_mu;

    slot_t *slots; /* never shrinks, so an index stays valid */
    size_t slot_count;
    size_t buffered_bytes;
    uint32_t round_robin;
    bool closed;

    brp_err_t async_err; /* failure from a background flush, reported once */
    char async_msg[512];

    pthread_t linger_thread;
    bool linger_started;
};

void brp_producer_config_init(brp_producer_config_t *config) {
    config->client_id = "brahmaputra-c";
    config->acks = 1;
    config->batch_size = 16 * 1024;
    /* Kafka defaults to 0; 5 because an unbatched producer is slow enough
     * to look broken. */
    config->linger_ms = 5;
    config->compression_type = "none";
    config->request_timeout_ms = 30000;
    config->retries = 5;
    config->retry_backoff_ms = 100;
    config->delivery_timeout_ms = 120000;
    config->buffer_memory = 32 * 1024 * 1024;
    config->max_block_ms = 60000;
    config->socket_connection_setup_timeout_ms = 30000;
}

void brp_message_init(brp_message_t *m) {
    memset(m, 0, sizeof *m);
    m->partition = BRP_PARTITION_ANY;
}

brp_client_t *brp_producer_client(brp_producer_t *p) { return p->client; }

static void *linger_main(void *arg);

brp_err_t brp_producer_new(const char *bootstrap, const brp_producer_config_t *config,
                           brp_producer_t **out) {
    brp_producer_config_t defaults;
    if (!config) {
        brp_producer_config_init(&defaults);
        config = &defaults;
    }
    if (config->acks != 0 && config->acks != 1 && config->acks != -1)
        return brp_set_error(BRP_ERR_INVALID_ARG, "acks must be 0, 1 or -1 (all), got %d",
                             (int)config->acks);
    brp_compression_t codec;
    brp_err_t err = brp_compression_parse(config->compression_type, &codec);
    if (err) return err;
    if (!brp_codec_available(codec))
        return brp_set_error(BRP_ERR_CODEC,
                             "compression.type=%s is not available; register it with "
                             "brp_register_codec first",
                             config->compression_type);

    brp_producer_t *p = calloc(1, sizeof *p);
    if (!p) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    p->config = *config;
    p->codec = codec;
    p->client_id = brp_strdup(config->client_id);
    if (!p->client_id) {
        free(p);
        return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    }
    p->config.client_id = p->client_id;
    p->config.compression_type = NULL; /* parsed into codec */
    pthread_mutex_init(&p->mu, NULL);
    pthread_mutex_init(&p->flush_mu, NULL);
    pthread_cond_init(&p->space_cond, NULL);
    pthread_cond_init(&p->linger_cond, NULL);

    int io_timeout = (config->request_timeout_ms > 0 ? config->request_timeout_ms : 0) + 5000;
    err = brp_client_new_internal(bootstrap, p->client_id,
                                  config->socket_connection_setup_timeout_ms, io_timeout,
                                  &p->client);
    if (!err && config->linger_ms > 0) {
        if (pthread_create(&p->linger_thread, NULL, linger_main, p) != 0)
            err = brp_set_error(BRP_ERR_STATE, "cannot start linger thread");
        else
            p->linger_started = true;
    }
    if (err) {
        brp_client_destroy(p->client);
        pthread_mutex_destroy(&p->mu);
        pthread_mutex_destroy(&p->flush_mu);
        pthread_cond_destroy(&p->space_cond);
        pthread_cond_destroy(&p->linger_cond);
        free(p->client_id);
        free(p);
        return err;
    }
    *out = p;
    return BRP_OK;
}

/* ---- partitioning ------------------------------------------------------ */

static brp_err_t choose_partition(brp_producer_t *p, const brp_message_t *m, int32_t *out) {
    if (m->partition >= 0) {
        *out = m->partition;
        return BRP_OK;
    }
    int32_t *partitions;
    size_t count;
    brp_err_t err = brp_client_partitions(p->client, m->topic, &partitions, &count);
    if (err) return err;
    if (m->key) {
        *out = brp_partition_for_key(m->key, m->key_len, partitions, count);
    } else {
        pthread_mutex_lock(&p->mu);
        uint32_t index = p->round_robin++;
        pthread_mutex_unlock(&p->mu);
        *out = partitions[index % count];
    }
    free(partitions);
    return BRP_OK;
}

/* ---- record copies ----------------------------------------------------- */

static brp_err_t copy_record(const brp_message_t *m, raw_record_t *r, size_t *size) {
    memset(r, 0, sizeof *r);
    *size = m->key_len + m->value_len + 16;
    if (m->key) {
        if (!(r->key = brp_memdup(m->key, m->key_len))) goto oom;
        r->key_len = m->key_len;
    }
    if (m->value) {
        if (!(r->value = brp_memdup(m->value, m->value_len))) goto oom;
        r->value_len = m->value_len;
    }
    r->timestamp_ms = m->timestamp_ms > 0 ? m->timestamp_ms : brp_now_ms();
    if (m->header_count) {
        r->headers = calloc(m->header_count, sizeof *r->headers);
        if (!r->headers) goto oom;
        for (size_t i = 0; i < m->header_count; i++) {
            const brp_header_t *h = &m->headers[i];
            if (!h->key) {
                raw_record_clear(r);
                return brp_set_error(BRP_ERR_INVALID_ARG, "header %zu has a NULL key", i);
            }
            r->header_count = i + 1;
            if (!(r->headers[i].key = brp_strdup(h->key))) goto oom;
            if (h->value) {
                if (!(r->headers[i].value = brp_memdup(h->value, h->value_len))) goto oom;
                r->headers[i].value_len = h->value_len;
            }
            *size += strlen(h->key) + h->value_len + 4;
        }
    }
    return BRP_OK;
oom:
    raw_record_clear(r);
    return brp_set_error(BRP_ERR_NOMEM, "out of memory copying record");
}

static void free_records(raw_record_t *recs, size_t n) {
    for (size_t i = 0; i < n; i++) raw_record_clear(&recs[i]);
    free(recs);
}

/* ---- buffer accounting ------------------------------------------------- */

/* Blocks until `size` more bytes may be buffered. This is what makes
 * buffer.memory real: a producer faster than its broker is slowed down
 * here rather than allowed to grow without limit. */
static brp_err_t reserve(brp_producer_t *p, size_t size) {
    size_t limit = p->config.buffer_memory;
    brp_err_t err = BRP_OK;
    pthread_mutex_lock(&p->mu);
    if (limit == 0 || size >= limit) {
        /* A record larger than the whole budget is admitted rather than
         * waiting forever on a condition that can never hold. */
        p->buffered_bytes += size;
        goto out;
    }
    struct timespec deadline;
    brp_deadline_ts(&deadline, p->config.max_block_ms > 0 ? p->config.max_block_ms : 0);
    while (p->buffered_bytes + size > limit) {
        int rc = pthread_cond_timedwait(&p->space_cond, &p->mu, &deadline);
        if (rc == ETIMEDOUT && p->buffered_bytes + size > limit) {
            err = brp_set_error(BRP_ERR_BUFFER_FULL,
                                "producer buffer full: %zu of %zu bytes unflushed after "
                                "max.block.ms=%d",
                                p->buffered_bytes, limit, p->config.max_block_ms);
            goto out;
        }
    }
    p->buffered_bytes += size;
out:
    pthread_mutex_unlock(&p->mu);
    return err;
}

static void release(brp_producer_t *p, size_t size) {
    pthread_mutex_lock(&p->mu);
    p->buffered_bytes = size > p->buffered_bytes ? 0 : p->buffered_bytes - size;
    pthread_cond_broadcast(&p->space_cond);
    pthread_mutex_unlock(&p->mu);
}

/* ---- sending ----------------------------------------------------------- */

static brp_err_t produce(brp_producer_t *p, const char *topic, int32_t partition,
                         const raw_record_t *recs, size_t n, int64_t *base_offset) {
    if (n == 0) return BRP_OK;
    buf_t batch;
    brp_err_t err = brp_encode_batch(recs, n, p->codec, &batch);
    if (err) return err;
    buf_t w;
    bp_init(&w);
    bp_str(&w, topic);
    bp_i32(&w, partition);
    bp_i32(&w, p->config.acks);
    bp_i32(&w, p->config.request_timeout_ms);
    bp_i64(&w, (int64_t)batch.len);
    buf_append(&w, batch.data, batch.len);
    buf_free(&batch);

    brp_conn_t *conn;
    if (p->config.acks == 0) {
        err = brp_client_conn_for(p->client, topic, partition, &conn);
        if (!err) err = brp_conn_send_oneway(conn, API_PRODUCE, &w);
        if (base_offset) *base_offset = -1;
        buf_free(&w);
        return err;
    }

    /* Retries alone do not bound latency; delivery.timeout.ms does. */
    int64_t deadline = brp_now_ms() + p->config.delivery_timeout_ms;
    int attempts_left = p->config.retries;
    for (;;) {
        uint8_t *resp;
        size_t resp_len;
        err = brp_client_conn_for(p->client, topic, partition, &conn);
        if (!err) err = brp_conn_request(conn, API_PRODUCE, &w, &resp, &resp_len);
        if (err) break;
        bpr_t r;
        err = bpr_init(&r, resp, resp_len);
        int32_t code = 0;
        int64_t offset = -1;
        if (!err) {
            bpr_skip_str(&r); /* topic */
            bpr_i32(&r);      /* partition */
            code = bpr_i32(&r);
            offset = bpr_i64(&r);
            bpr_i64(&r); /* log_append_time_ms */
            if (r.err) err = brp_set_error(BRP_ERR_PROTOCOL, "malformed produce response");
        }
        free(resp);
        if (err) break;
        if (code == 0) {
            if (base_offset) *base_offset = offset;
            break;
        }
        char context[300];
        snprintf(context, sizeof context, "produce to %s-%d", topic, (int)partition);
        if (!brp_retriable(code) || attempts_left <= 0) {
            err = brp_server_error(code, context);
            break;
        }
        if (brp_now_ms() + p->config.retry_backoff_ms > deadline) {
            err = brp_set_error(BRP_ERR_DELIVERY_TIMEOUT,
                                "delivery.timeout.ms=%d expired; last error %s[%d] (%s)",
                                p->config.delivery_timeout_ms, brp_err_name((brp_err_t)code),
                                (int)code, context);
            break;
        }
        attempts_left--;
        if (code == BRP_ERR_NOT_LEADER_OR_FOLLOWER || code == BRP_ERR_FENCED_LEADER_EPOCH ||
            code == BRP_ERR_UNKNOWN_LEADER_EPOCH) {
            /* A stale route is the common cause; resending to the same
             * broker would just repeat it. */
            brp_client_refresh(p->client, topic);
        }
        brp_sleep_ms(p->config.retry_backoff_ms);
    }
    buf_free(&w);
    return err;
}

static brp_err_t flush_slot(brp_producer_t *p, size_t index) {
    pthread_mutex_lock(&p->flush_mu);
    pthread_mutex_lock(&p->mu);
    slot_t *s = &p->slots[index];
    raw_record_t *recs = s->recs;
    size_t n = s->n, bytes = s->bytes;
    const char *topic = s->topic;
    int32_t partition = s->partition;
    s->recs = NULL;
    s->n = s->cap = 0;
    s->bytes = 0;
    pthread_mutex_unlock(&p->mu);

    release(p, bytes);
    brp_err_t err = produce(p, topic, partition, recs, n, NULL);
    free_records(recs, n);
    pthread_mutex_unlock(&p->flush_mu);
    return err;
}

static brp_err_t flush_all(brp_producer_t *p) {
    brp_err_t first = BRP_OK;
    char message[512] = "";
    pthread_mutex_lock(&p->mu);
    size_t count = p->slot_count;
    pthread_mutex_unlock(&p->mu);
    for (size_t i = 0; i < count; i++) {
        pthread_mutex_lock(&p->mu);
        bool pending = p->slots[i].n > 0;
        pthread_mutex_unlock(&p->mu);
        if (!pending) continue;
        brp_err_t err = flush_slot(p, i);
        if (err && !first) {
            first = err;
            snprintf(message, sizeof message, "%s", brp_last_error());
        }
    }
    if (first) brp_set_error(first, "%s", message);
    return first;
}

static void *linger_main(void *arg) {
    brp_producer_t *p = arg;
    pthread_mutex_lock(&p->mu);
    while (!p->closed) {
        struct timespec deadline;
        brp_deadline_ts(&deadline, p->config.linger_ms);
        pthread_cond_timedwait(&p->linger_cond, &p->mu, &deadline);
        if (p->closed) break;
        pthread_mutex_unlock(&p->mu);
        /* A background flush that fails must not kill the thread; the next
         * explicit flush reports it to a caller who can act on it. */
        brp_err_t err = flush_all(p);
        pthread_mutex_lock(&p->mu);
        if (err && !p->async_err) {
            p->async_err = err;
            snprintf(p->async_msg, sizeof p->async_msg, "%s", brp_last_error());
        }
    }
    pthread_mutex_unlock(&p->mu);
    return NULL;
}

brp_err_t brp_producer_send(brp_producer_t *p, const brp_message_t *m) {
    if (!p || !m || !m->topic || !*m->topic)
        return brp_set_error(BRP_ERR_INVALID_ARG, "message needs a topic");
    int32_t partition;
    brp_err_t err = choose_partition(p, m, &partition);
    if (err) return err;
    raw_record_t rec;
    size_t size;
    if ((err = copy_record(m, &rec, &size)) != BRP_OK) return err;
    if ((err = reserve(p, size)) != BRP_OK) {
        raw_record_clear(&rec);
        return err;
    }

    pthread_mutex_lock(&p->mu);
    size_t index = p->slot_count;
    for (size_t i = 0; i < p->slot_count; i++) {
        if (p->slots[i].partition == partition && strcmp(p->slots[i].topic, m->topic) == 0) {
            index = i;
            break;
        }
    }
    if (index == p->slot_count) {
        slot_t *grown = realloc(p->slots, (p->slot_count + 1) * sizeof *grown);
        char *topic = brp_strdup(m->topic);
        if (!grown || !topic) {
            if (grown) p->slots = grown;
            free(topic);
            goto oom;
        }
        p->slots = grown;
        memset(&p->slots[index], 0, sizeof p->slots[index]);
        p->slots[index].topic = topic;
        p->slots[index].partition = partition;
        p->slot_count++;
    }
    slot_t *s = &p->slots[index];
    if (s->n == s->cap) {
        size_t next = s->cap ? s->cap * 2 : 16;
        raw_record_t *grown = realloc(s->recs, next * sizeof *grown);
        if (!grown) goto oom;
        s->recs = grown;
        s->cap = next;
    }
    s->recs[s->n++] = rec;
    s->bytes += size;
    bool full = s->bytes >= p->config.batch_size;
    pthread_mutex_unlock(&p->mu);

    if (p->config.linger_ms <= 0 || full) return flush_slot(p, index);
    return BRP_OK;
oom:
    pthread_mutex_unlock(&p->mu);
    release(p, size);
    raw_record_clear(&rec);
    return brp_set_error(BRP_ERR_NOMEM, "out of memory buffering record");
}

brp_err_t brp_producer_send_sync(brp_producer_t *p, const brp_message_t *m, int64_t *offset) {
    if (!p || !m || !m->topic || !*m->topic)
        return brp_set_error(BRP_ERR_INVALID_ARG, "message needs a topic");
    int32_t partition;
    brp_err_t err = choose_partition(p, m, &partition);
    if (err) return err;
    raw_record_t rec;
    size_t size;
    if ((err = copy_record(m, &rec, &size)) != BRP_OK) return err;
    int64_t base = -1;
    err = produce(p, m->topic, partition, &rec, 1, &base);
    raw_record_clear(&rec);
    if (!err && offset) *offset = base;
    return err;
}

brp_err_t brp_producer_flush(brp_producer_t *p) {
    brp_err_t err = flush_all(p);
    if (err) return err;
    pthread_mutex_lock(&p->mu);
    err = p->async_err;
    if (err) brp_set_error(err, "background flush failed: %s", p->async_msg);
    p->async_err = BRP_OK;
    pthread_mutex_unlock(&p->mu);
    return err;
}

brp_err_t brp_producer_close(brp_producer_t *p) {
    if (!p) return BRP_OK;
    brp_err_t err = brp_producer_flush(p);
    char message[512];
    snprintf(message, sizeof message, "%s", brp_last_error());
    pthread_mutex_lock(&p->mu);
    p->closed = true;
    pthread_cond_broadcast(&p->linger_cond);
    pthread_mutex_unlock(&p->mu);
    if (p->linger_started) pthread_join(p->linger_thread, NULL);
    /* Anything a racing background flush left behind is dropped here. */
    for (size_t i = 0; i < p->slot_count; i++) {
        free_records(p->slots[i].recs, p->slots[i].n);
        free(p->slots[i].topic);
    }
    free(p->slots);
    brp_client_destroy(p->client);
    pthread_mutex_destroy(&p->mu);
    pthread_mutex_destroy(&p->flush_mu);
    pthread_cond_destroy(&p->space_cond);
    pthread_cond_destroy(&p->linger_cond);
    free(p->client_id);
    free(p);
    if (err) brp_set_error(err, "%s", message);
    return err;
}
