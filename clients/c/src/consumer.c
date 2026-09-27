/* Single-partition consumer: Fetch and ListOffsets with leader routing. */
#define _POSIX_C_SOURCE 200809L
#include "internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct brp_consumer {
    brp_consumer_config_t config;
    char *client_id;
    char *rack;
    brp_client_t *client;
};

void brp_consumer_config_init(brp_consumer_config_t *config) {
    config->client_id = "brahmaputra-c";
    config->fetch_max_bytes = 8 * 1024 * 1024;
    config->fetch_min_bytes = 1;
    config->fetch_max_wait_ms = 500;
    config->isolation_level = BRP_READ_UNCOMMITTED;
    config->client_rack = "";
    config->max_poll_records = 500;
    config->socket_connection_setup_timeout_ms = 30000;
}

brp_err_t brp_consumer_new(const char *bootstrap, const brp_consumer_config_t *config,
                           brp_consumer_t **out) {
    brp_consumer_config_t defaults;
    if (!config) {
        brp_consumer_config_init(&defaults);
        config = &defaults;
    }
    brp_consumer_t *c = calloc(1, sizeof *c);
    if (!c) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    c->config = *config;
    c->client_id = brp_strdup(config->client_id);
    c->rack = brp_strdup(config->client_rack);
    if (!c->client_id || !c->rack) {
        brp_consumer_close(c);
        return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    }
    c->config.client_id = c->client_id;
    c->config.client_rack = c->rack;
    /* The socket timeout must outlast the longest long-poll. */
    int io_timeout = (config->fetch_max_wait_ms > 0 ? config->fetch_max_wait_ms : 0) + 30000;
    brp_err_t err = brp_client_new_internal(bootstrap, c->client_id,
                                            config->socket_connection_setup_timeout_ms,
                                            io_timeout, &c->client);
    if (err) {
        brp_consumer_close(c);
        return err;
    }
    *out = c;
    return BRP_OK;
}

void brp_consumer_close(brp_consumer_t *c) {
    if (!c) return;
    brp_client_destroy(c->client);
    free(c->client_id);
    free(c->rack);
    free(c);
}

brp_client_t *brp_consumer_client(brp_consumer_t *c) { return c->client; }

brp_err_t brp_consumer_list_offsets(brp_consumer_t *c, const char *topic, int32_t partition,
                                    int64_t timestamp, int64_t *offset) {
    buf_t w;
    bp_init(&w);
    bp_str(&w, topic);
    bp_i32(&w, partition);
    bp_i64(&w, timestamp);
    brp_conn_t *conn;
    brp_err_t err = brp_client_conn_for(c->client, topic, partition, &conn);
    uint8_t *resp = NULL;
    size_t resp_len;
    if (!err) err = brp_conn_request(conn, API_LIST_OFFSETS, &w, &resp, &resp_len);
    buf_free(&w);
    if (err) return err;
    bpr_t r;
    if ((err = bpr_init(&r, resp, resp_len)) == BRP_OK) {
        bpr_skip_str(&r); /* topic */
        bpr_i32(&r);      /* partition */
        int32_t code = bpr_i32(&r);
        int64_t value = bpr_i64(&r);
        bpr_i64(&r); /* timestamp */
        if (r.err) {
            err = brp_set_error(BRP_ERR_PROTOCOL, "malformed list_offsets response");
        } else if (code != 0) {
            char context[300];
            snprintf(context, sizeof context, "list_offsets %s-%d", topic, (int)partition);
            err = brp_server_error(code, context);
        } else {
            *offset = value;
        }
    }
    free(resp);
    return err;
}

/* One Fetch round trip. On BRP_OK *code holds the broker's error code. */
static brp_err_t fetch_once(brp_consumer_t *c, brp_conn_t *conn, const buf_t *body,
                            const char *topic, int32_t partition, int64_t offset,
                            int32_t *code, int64_t *hwm, brp_record_t **records,
                            size_t *count, size_t *cap) {
    uint8_t *resp;
    size_t resp_len;
    brp_err_t err = brp_conn_request(conn, API_FETCH, body, &resp, &resp_len);
    if (err) return err;
    (void)c;
    bpr_t r;
    if ((err = bpr_init(&r, resp, resp_len)) != BRP_OK) goto done;
    bpr_skip_str(&r); /* topic */
    bpr_i32(&r);      /* partition */
    *code = bpr_i32(&r);
    *hwm = bpr_i64(&r);
    bpr_i64(&r); /* last_stable_offset */
    int64_t batches_len = bpr_i64(&r);
    /* Read even though unused: the batches trail the whole struct, so
     * skipping a field would take them from the wrong offset. */
    bpr_i32(&r); /* preferred_read_replica */
    if (r.err) {
        err = brp_set_error(BRP_ERR_PROTOCOL, "malformed fetch response");
        goto done;
    }
    if (batches_len < 0 || (uint64_t)batches_len > resp_len - r.pos) {
        err = brp_set_error(BRP_ERR_PROTOCOL,
                            "fetch response claims more batch bytes than it carries");
        goto done;
    }
    if (*code == 0)
        err = brp_decode_batches(resp + r.pos, (size_t)batches_len, topic, partition, offset,
                                 records, count, cap);
done:
    free(resp);
    return err;
}

brp_err_t brp_consumer_fetch(brp_consumer_t *c, const char *topic, int32_t partition,
                             int64_t offset, int32_t max_wait_ms, brp_record_t **records,
                             size_t *count, int64_t *high_watermark) {
    if (!topic || !records || !count) return brp_set_error(BRP_ERR_INVALID_ARG, "NULL argument");
    *records = NULL;
    *count = 0;
    if (max_wait_ms > c->config.fetch_max_wait_ms) max_wait_ms = c->config.fetch_max_wait_ms;
    if (max_wait_ms < 0) max_wait_ms = 0;
    buf_t w;
    bp_init(&w);
    bp_str(&w, topic);
    bp_i32(&w, partition);
    bp_i64(&w, offset);
    bp_i32(&w, c->config.fetch_max_bytes);
    bp_i32(&w, max_wait_ms);
    bp_i32(&w, c->config.fetch_min_bytes);
    bp_i32(&w, c->config.isolation_level);
    /* client.rack: with it set the leader may name a same-rack replica. */
    bp_str(&w, c->config.client_rack);

    brp_record_t *out = NULL;
    size_t n = 0, cap = 0;
    int32_t code = 0;
    int64_t hwm = 0;
    brp_conn_t *conn;
    brp_err_t err = brp_client_conn_for(c->client, topic, partition, &conn);
    if (!err) err = fetch_once(c, conn, &w, topic, partition, offset, &code, &hwm, &out, &n, &cap);
    if (!err && code == BRP_ERR_NOT_LEADER_OR_FOLLOWER) {
        err = brp_client_refresh(c->client, topic);
        if (!err) err = brp_client_conn_for(c->client, topic, partition, &conn);
        if (!err)
            err = fetch_once(c, conn, &w, topic, partition, offset, &code, &hwm, &out, &n, &cap);
    }
    buf_free(&w);
    if (!err && code != 0) {
        char context[300];
        snprintf(context, sizeof context, "fetch %s-%d", topic, (int)partition);
        err = brp_server_error(code, context);
    }
    /* max.poll.records: the caller resumes from the last returned offset
     * + 1, so what is cut here is fetched again next time, not lost. */
    int limit = c->config.max_poll_records;
    if (!err && limit > 0 && n > (size_t)limit) {
        size_t extra = n - (size_t)limit;
        brp_record_t *tail = malloc(extra * sizeof *tail);
        if (!tail) {
            err = brp_set_error(BRP_ERR_NOMEM, "out of memory");
        } else {
            memcpy(tail, out + limit, extra * sizeof *tail);
            brp_records_free(tail, extra);
            n = (size_t)limit;
        }
    }
    if (err) {
        brp_records_free(out, n);
        return err;
    }
    *records = out;
    *count = n;
    if (high_watermark) *high_watermark = hwm;
    return BRP_OK;
}
