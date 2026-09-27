/*
 * Connections, framing, metadata and leader routing.
 *
 * The frame header is fixed big-endian — int32 length prefix, int16 api
 * key, int16 api version, int32 correlation id, int16-prefixed client id —
 * because the broker has to read it before it knows which body decoder to
 * use. The body that follows is BitPacker.
 */
#define _POSIX_C_SOURCE 200809L
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

/* ------------------------------------------------------------------------ */
/* Connection                                                               */
/* ------------------------------------------------------------------------ */

/* One TCP connection to one broker. A mutex serialises request/response
 * pairs, and responses are matched by correlation id; a mismatch means the
 * stream has desynchronised, so the connection is dropped. A dropped
 * connection is redialled on its next request. */
struct brp_conn {
    char *host;
    char *port;
    const char *client_id; /* owned by the client */
    int dial_timeout_ms;
    int io_timeout_ms;
    int fd;
    int32_t next_correlation;
    pthread_mutex_t mu;
};

static brp_conn_t *conn_new(const char *host, const char *port, const char *client_id,
                            int dial_timeout_ms, int io_timeout_ms) {
    brp_conn_t *c = calloc(1, sizeof *c);
    if (!c) return NULL;
    c->host = brp_strdup(host);
    c->port = brp_strdup(port);
    if (!c->host || !c->port) {
        free(c->host);
        free(c->port);
        free(c);
        return NULL;
    }
    c->client_id = client_id;
    c->dial_timeout_ms = dial_timeout_ms;
    c->io_timeout_ms = io_timeout_ms;
    c->fd = -1;
    pthread_mutex_init(&c->mu, NULL);
    return c;
}

static void conn_drop(brp_conn_t *c) {
    if (c->fd >= 0) close(c->fd);
    c->fd = -1;
}

static void conn_free(brp_conn_t *c) {
    if (!c) return;
    conn_drop(c);
    pthread_mutex_destroy(&c->mu);
    free(c->host);
    free(c->port);
    free(c);
}

static brp_err_t conn_dial(brp_conn_t *c) {
    struct addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    int rc = getaddrinfo(c->host, c->port, &hints, &res);
    if (rc != 0)
        return brp_set_error(BRP_ERR_IO, "resolve %s:%s: %s", c->host, c->port,
                             gai_strerror(rc));
    brp_err_t err = BRP_ERR_IO;
    brp_set_error(BRP_ERR_IO, "connect %s:%s: no usable address", c->host, c->port);
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        rc = connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (rc != 0 && errno == EINPROGRESS) {
            struct pollfd pfd = {fd, POLLOUT, 0};
            rc = poll(&pfd, 1, c->dial_timeout_ms > 0 ? c->dial_timeout_ms : -1);
            if (rc == 1) {
                int so_error = 0;
                socklen_t len = sizeof so_error;
                getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len);
                rc = so_error ? -1 : 0;
                errno = so_error;
            } else {
                if (rc == 0) errno = ETIMEDOUT;
                rc = -1;
            }
        }
        if (rc != 0) {
            err = errno == ETIMEDOUT ? BRP_ERR_TIMEOUT : BRP_ERR_IO;
            brp_set_error(err, "connect %s:%s: %s", c->host, c->port, strerror(errno));
            close(fd);
            continue;
        }
        fcntl(fd, F_SETFL, flags);
        /* Responses are small and latency matters more than packet count;
         * without this every request pays Nagle plus delayed ACK. */
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        if (c->io_timeout_ms > 0) {
            struct timeval tv = {c->io_timeout_ms / 1000, (c->io_timeout_ms % 1000) * 1000};
            setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
            setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
        }
        c->fd = fd;
        err = BRP_OK;
        break;
    }
    freeaddrinfo(res);
    return err;
}

static brp_err_t io_error(brp_conn_t *c, const char *what) {
    brp_err_t err = (errno == EAGAIN || errno == EWOULDBLOCK) ? BRP_ERR_TIMEOUT : BRP_ERR_IO;
    brp_set_error(err, "%s %s:%s: %s", what, c->host, c->port,
                  err == BRP_ERR_TIMEOUT ? "timed out" : strerror(errno));
    conn_drop(c);
    return err;
}

static brp_err_t write_all(brp_conn_t *c, const uint8_t *p, size_t n) {
    while (n > 0) {
        ssize_t w = send(c->fd, p, n, MSG_NOSIGNAL);
        if (w < 0) {
            if (errno == EINTR) continue;
            return io_error(c, "write to");
        }
        p += w;
        n -= (size_t)w;
    }
    return BRP_OK;
}

static brp_err_t read_all(brp_conn_t *c, uint8_t *p, size_t n) {
    while (n > 0) {
        ssize_t r = recv(c->fd, p, n, 0);
        if (r < 0) {
            if (errno == EINTR) continue;
            return io_error(c, "read from");
        }
        if (r == 0) {
            brp_set_error(BRP_ERR_IO, "connection to %s:%s closed by broker", c->host, c->port);
            conn_drop(c);
            return BRP_ERR_IO;
        }
        p += r;
        n -= (size_t)r;
    }
    return BRP_OK;
}

static brp_err_t conn_write_frame(brp_conn_t *c, int16_t api_key, const buf_t *body,
                                  int32_t *correlation) {
    if (body->oom) return brp_set_error(BRP_ERR_NOMEM, "out of memory building request");
    if (c->fd < 0) {
        brp_err_t err = conn_dial(c);
        if (err) return err;
    }
    size_t cid_len = strlen(c->client_id);
    buf_t frame = {0};
    buf_be32(&frame, (uint32_t)(8 + 2 + cid_len + body->len));
    buf_be16(&frame, (uint16_t)api_key);
    buf_be16(&frame, (uint16_t)BRP_API_VERSION);
    *correlation = ++c->next_correlation;
    buf_be32(&frame, (uint32_t)*correlation);
    buf_be16(&frame, (uint16_t)cid_len);
    buf_append(&frame, c->client_id, cid_len);
    buf_append(&frame, body->data, body->len);
    if (frame.oom) {
        buf_free(&frame);
        return brp_set_error(BRP_ERR_NOMEM, "out of memory building frame");
    }
    brp_err_t err = write_all(c, frame.data, frame.len);
    buf_free(&frame);
    return err;
}

brp_err_t brp_conn_request(brp_conn_t *c, int16_t api_key, const buf_t *body,
                           uint8_t **response, size_t *response_len) {
    pthread_mutex_lock(&c->mu);
    int32_t correlation;
    brp_err_t err = conn_write_frame(c, api_key, body, &correlation);
    if (err) goto out;

    uint8_t header[4];
    if ((err = read_all(c, header, 4)) != BRP_OK) goto out;
    int32_t length = (int32_t)((uint32_t)header[0] << 24 | (uint32_t)header[1] << 16 |
                               (uint32_t)header[2] << 8 | header[3]);
    if (length < 10) {
        err = brp_set_error(BRP_ERR_PROTOCOL, "bad response frame length %d", (int)length);
        conn_drop(c);
        goto out;
    }
    uint8_t *payload = malloc((size_t)length);
    if (!payload) {
        err = brp_set_error(BRP_ERR_NOMEM, "out of memory reading response");
        conn_drop(c);
        goto out;
    }
    if ((err = read_all(c, payload, (size_t)length)) != BRP_OK) {
        free(payload);
        goto out;
    }
    int32_t got = (int32_t)((uint32_t)payload[4] << 24 | (uint32_t)payload[5] << 16 |
                            (uint32_t)payload[6] << 8 | payload[7]);
    int16_t cid_len = (int16_t)(payload[8] << 8 | payload[9]);
    size_t body_at = 10 + (cid_len > 0 ? (size_t)cid_len : 0);
    if (got != correlation || body_at > (size_t)length) {
        err = brp_set_error(BRP_ERR_PROTOCOL,
                            "correlation id mismatch: expected %d, got %d",
                            (int)correlation, (int)got);
        free(payload);
        conn_drop(c);
        goto out;
    }
    memmove(payload, payload + body_at, (size_t)length - body_at);
    *response = payload;
    *response_len = (size_t)length - body_at;
out:
    pthread_mutex_unlock(&c->mu);
    return err;
}

brp_err_t brp_conn_send_oneway(brp_conn_t *c, int16_t api_key, const buf_t *body) {
    pthread_mutex_lock(&c->mu);
    int32_t correlation;
    brp_err_t err = conn_write_frame(c, api_key, body, &correlation);
    pthread_mutex_unlock(&c->mu);
    return err;
}

/* ------------------------------------------------------------------------ */
/* Metadata                                                                 */
/* ------------------------------------------------------------------------ */

static void topic_info_clear(brp_topic_info_t *t) {
    free(t->name);
    for (size_t p = 0; p < t->partition_count; p++) {
        free(t->partitions[p].replicas);
        free(t->partitions[p].isr);
    }
    free(t->partitions);
}

void brp_metadata_free(brp_metadata_t *m) {
    if (!m) return;
    for (size_t i = 0; i < m->broker_count; i++) {
        free(m->brokers[i].host);
        free(m->brokers[i].rack);
    }
    free(m->brokers);
    for (size_t i = 0; i < m->topic_count; i++) topic_info_clear(&m->topics[i]);
    free(m->topics);
    free(m);
}

static int cmp_partition_info(const void *a, const void *b) {
    int32_t x = ((const brp_partition_info_t *)a)->partition;
    int32_t y = ((const brp_partition_info_t *)b)->partition;
    return (x > y) - (x < y);
}

static int32_t *read_i32_array(bpr_t *r, size_t *count) {
    size_t n = bpr_count(r);
    *count = 0;
    int32_t *out = calloc(n ? n : 1, sizeof *out);
    if (!out) {
        r->err = true;
        return NULL;
    }
    for (size_t i = 0; i < n; i++) out[i] = bpr_i32(r);
    *count = n;
    return out;
}

/* Field order is exactly the schema's: error_code, brokers, controller_id,
 * topics. The leading code is request-level (an authorization denial, say)
 * and distinct from the per-topic one, which is what "no such topic" uses. */
static brp_err_t decode_metadata(const uint8_t *body, size_t len, brp_metadata_t **out) {
    bpr_t r;
    brp_err_t err = bpr_init(&r, body, len);
    if (err) return err;
    int32_t code = bpr_i32(&r);
    if (r.err) return brp_set_error(BRP_ERR_PROTOCOL, "truncated metadata response");
    if (code != 0) return brp_server_error(code, "metadata");
    brp_metadata_t *m = calloc(1, sizeof *m);
    if (!m) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    size_t n = bpr_count(&r);
    m->brokers = calloc(n ? n : 1, sizeof *m->brokers);
    if (!m->brokers) goto oom;
    for (size_t i = 0; i < n && !r.err; i++) {
        brp_broker_info_t *b = &m->brokers[i];
        m->broker_count = i + 1;
        b->node_id = bpr_i32(&r);
        b->host = bpr_str(&r);
        b->port = bpr_i32(&r);
        b->rack = bpr_str(&r);
    }
    m->controller_id = bpr_i32(&r);
    n = bpr_count(&r);
    m->topics = calloc(n ? n : 1, sizeof *m->topics);
    if (!m->topics) goto oom;
    for (size_t i = 0; i < n && !r.err; i++) {
        brp_topic_info_t *t = &m->topics[i];
        m->topic_count = i + 1;
        t->name = bpr_str(&r);
        t->error_code = bpr_i32(&r);
        size_t pc = bpr_count(&r);
        t->partitions = calloc(pc ? pc : 1, sizeof *t->partitions);
        if (!t->partitions) goto oom;
        for (size_t p = 0; p < pc && !r.err; p++) {
            brp_partition_info_t *pi = &t->partitions[p];
            t->partition_count = p + 1;
            pi->partition = bpr_i32(&r);
            pi->leader = bpr_i32(&r);
            pi->replicas = read_i32_array(&r, &pi->replica_count);
            pi->isr = read_i32_array(&r, &pi->isr_count);
            pi->leader_epoch = bpr_i32(&r);
        }
        qsort(t->partitions, t->partition_count, sizeof *t->partitions, cmp_partition_info);
        if (!r.err && t->error_code != 0 &&
            t->error_code != BRP_ERR_UNKNOWN_TOPIC_OR_PARTITION) {
            char context[300];
            snprintf(context, sizeof context, "metadata for %s", t->name ? t->name : "?");
            err = brp_server_error(t->error_code, context);
            brp_metadata_free(m);
            return err;
        }
    }
    if (r.err) {
        brp_metadata_free(m);
        return brp_set_error(BRP_ERR_PROTOCOL, "malformed metadata response");
    }
    *out = m;
    return BRP_OK;
oom:
    brp_metadata_free(m);
    return brp_set_error(BRP_ERR_NOMEM, "out of memory");
}

/* ------------------------------------------------------------------------ */
/* Client / router                                                          */
/* ------------------------------------------------------------------------ */

typedef struct node_conn {
    int32_t node_id;
    brp_conn_t *conn;
} node_conn_t;

/* Metadata is cached and refreshed only when a request comes back saying
 * the route was stale, because refreshing per request would put the
 * control plane on the data path. */
struct brp_client {
    char *client_id;
    int dial_timeout_ms;
    int io_timeout_ms;
    brp_conn_t *seed;
    pthread_mutex_t mu;
    node_conn_t *conns;
    size_t conn_count;
    brp_metadata_t *cache; /* merged across refreshes */
};

void brp_client_config_init(brp_client_config_t *config) {
    config->client_id = "brahmaputra-c";
    config->socket_connection_setup_timeout_ms = 30000;
    config->request_timeout_ms = 30000;
}

static brp_err_t split_address(const char *bootstrap, char **host, char **port) {
    if (!bootstrap) return brp_set_error(BRP_ERR_INVALID_ARG, "bootstrap address is NULL");
    const char *colon = strrchr(bootstrap, ':');
    if (!colon || colon == bootstrap || !colon[1])
        return brp_set_error(BRP_ERR_INVALID_ARG, "bootstrap \"%s\" is not host:port", bootstrap);
    const char *h = bootstrap;
    size_t hlen = (size_t)(colon - bootstrap);
    if (h[0] == '[' && hlen >= 2 && h[hlen - 1] == ']') {
        h++;
        hlen -= 2;
    }
    *host = malloc(hlen + 1);
    *port = brp_strdup(colon + 1);
    if (!*host || !*port) {
        free(*host);
        free(*port);
        return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    }
    memcpy(*host, h, hlen);
    (*host)[hlen] = 0;
    return BRP_OK;
}

brp_err_t brp_client_new_internal(const char *bootstrap, const char *client_id,
                                  int dial_timeout_ms, int io_timeout_ms,
                                  brp_client_t **out) {
    char *host = NULL, *port = NULL;
    brp_err_t err = split_address(bootstrap, &host, &port);
    if (err) return err;
    brp_client_t *c = calloc(1, sizeof *c);
    if (!c) {
        free(host);
        free(port);
        return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    }
    c->client_id = brp_strdup(client_id ? client_id : "brahmaputra-c");
    c->dial_timeout_ms = dial_timeout_ms;
    c->io_timeout_ms = io_timeout_ms;
    pthread_mutex_init(&c->mu, NULL);
    c->seed = c->client_id ? conn_new(host, port, c->client_id, dial_timeout_ms, io_timeout_ms)
                           : NULL;
    free(host);
    free(port);
    if (!c->seed) {
        brp_client_destroy(c);
        return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    }
    /* Dial now so a wrong address fails at construction, not first use. */
    pthread_mutex_lock(&c->seed->mu);
    err = conn_dial(c->seed);
    pthread_mutex_unlock(&c->seed->mu);
    if (err) {
        brp_client_destroy(c);
        return err;
    }
    *out = c;
    return BRP_OK;
}

brp_err_t brp_client_new(const char *bootstrap, const brp_client_config_t *config,
                         brp_client_t **out) {
    brp_client_config_t defaults;
    if (!config) {
        brp_client_config_init(&defaults);
        config = &defaults;
    }
    return brp_client_new_internal(bootstrap, config->client_id,
                                   config->socket_connection_setup_timeout_ms,
                                   config->request_timeout_ms, out);
}

void brp_client_destroy(brp_client_t *c) {
    if (!c) return;
    for (size_t i = 0; i < c->conn_count; i++)
        if (c->conns[i].conn != c->seed) conn_free(c->conns[i].conn);
    free(c->conns);
    conn_free(c->seed);
    brp_metadata_free(c->cache);
    pthread_mutex_destroy(&c->mu);
    free(c->client_id);
    free(c);
}

brp_conn_t *brp_client_seed(brp_client_t *c) { return c->seed; }

int brp_client_connected(brp_client_t *c) {
    pthread_mutex_lock(&c->seed->mu);
    int connected = c->seed->fd >= 0;
    pthread_mutex_unlock(&c->seed->mu);
    return connected;
}

brp_err_t brp_client_api_versions(brp_client_t *c, brp_api_version_range_t **out,
                                  size_t *count, char **broker_version) {
    buf_t w;
    bp_init(&w);
    bp_str(&w, "brahmaputra-c");
    bp_str(&w, BRP_VERSION);
    uint8_t *resp;
    size_t resp_len;
    brp_err_t err = brp_conn_request(c->seed, API_API_VERSIONS, &w, &resp, &resp_len);
    buf_free(&w);
    if (err) return err;
    bpr_t r;
    if ((err = bpr_init(&r, resp, resp_len)) != BRP_OK) goto done;
    int32_t code = bpr_i32(&r);
    if (!r.err && code != 0) {
        err = brp_server_error(code, "api_versions");
        goto done;
    }
    size_t n = bpr_count(&r);
    brp_api_version_range_t *ranges = calloc(n ? n : 1, sizeof *ranges);
    if (!ranges) {
        err = brp_set_error(BRP_ERR_NOMEM, "out of memory");
        goto done;
    }
    for (size_t i = 0; i < n; i++) {
        ranges[i].api_key = bpr_i32(&r);
        ranges[i].min_version = bpr_i32(&r);
        ranges[i].max_version = bpr_i32(&r);
    }
    char *version = bpr_str(&r);
    if (r.err) {
        free(ranges);
        free(version);
        err = brp_set_error(BRP_ERR_PROTOCOL, "malformed api_versions response");
        goto done;
    }
    *out = ranges;
    *count = n;
    if (broker_version)
        *broker_version = version;
    else
        free(version);
done:
    free(resp);
    return err;
}

static brp_err_t metadata_request(brp_client_t *c, const char *const *topics, size_t n,
                                  uint8_t **resp, size_t *resp_len) {
    buf_t w;
    bp_init(&w);
    bp_i32(&w, (int32_t)n);
    for (size_t i = 0; i < n; i++) bp_str(&w, topics[i]);
    brp_err_t err = brp_conn_request(c->seed, API_METADATA, &w, resp, resp_len);
    buf_free(&w);
    return err;
}

/* Merges fresh metadata into the cache: brokers are replaced, topics that
 * came back replace their cached entries, others are kept. Takes ownership
 * of `fresh`. Caller holds c->mu. */
static brp_err_t cache_merge(brp_client_t *c, brp_metadata_t *fresh, bool all_topics) {
    if (!c->cache || all_topics) {
        brp_metadata_free(c->cache);
        c->cache = fresh;
        return BRP_OK;
    }
    brp_metadata_t *old = c->cache;
    for (size_t i = 0; i < old->broker_count; i++) {
        free(old->brokers[i].host);
        free(old->brokers[i].rack);
    }
    free(old->brokers);
    old->brokers = fresh->brokers;
    old->broker_count = fresh->broker_count;
    old->controller_id = fresh->controller_id;
    fresh->brokers = NULL;
    fresh->broker_count = 0;

    brp_topic_info_t *grown =
        realloc(old->topics, (old->topic_count + fresh->topic_count + 1) * sizeof *grown);
    if (!grown) {
        brp_metadata_free(fresh);
        return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    }
    old->topics = grown;
    for (size_t i = 0; i < fresh->topic_count; i++) {
        brp_topic_info_t *t = &fresh->topics[i];
        for (size_t j = 0; j < old->topic_count; j++) {
            if (strcmp(old->topics[j].name, t->name) == 0) {
                topic_info_clear(&old->topics[j]);
                old->topics[j] = old->topics[--old->topic_count];
                break;
            }
        }
        old->topics[old->topic_count++] = *t;
    }
    fresh->topic_count = 0;
    brp_metadata_free(fresh);
    return BRP_OK;
}

static brp_err_t refresh(brp_client_t *c, const char *const *topics, size_t n,
                         brp_metadata_t **copy_out) {
    uint8_t *resp;
    size_t resp_len;
    brp_err_t err = metadata_request(c, topics, n, &resp, &resp_len);
    if (err) return err;
    brp_metadata_t *fresh = NULL;
    err = decode_metadata(resp, resp_len, &fresh);
    if (!err && copy_out) {
        /* Decoded twice so the caller's copy and the cache never share. */
        err = decode_metadata(resp, resp_len, copy_out);
        if (err) brp_metadata_free(fresh);
    }
    free(resp);
    if (err) return err;
    pthread_mutex_lock(&c->mu);
    err = cache_merge(c, fresh, n == 0);
    pthread_mutex_unlock(&c->mu);
    if (err && copy_out) {
        brp_metadata_free(*copy_out);
        *copy_out = NULL;
    }
    return err;
}

brp_err_t brp_client_metadata(brp_client_t *c, const char *const *topics, size_t n,
                              brp_metadata_t **out) {
    return refresh(c, topics, n, out);
}

brp_err_t brp_client_refresh(brp_client_t *c, const char *topic) {
    return refresh(c, &topic, 1, NULL);
}

/* Caller holds c->mu. */
static const brp_topic_info_t *cached_topic(brp_client_t *c, const char *topic) {
    if (!c->cache) return NULL;
    for (size_t i = 0; i < c->cache->topic_count; i++)
        if (strcmp(c->cache->topics[i].name, topic) == 0) return &c->cache->topics[i];
    return NULL;
}

static bool copy_partitions(brp_client_t *c, const char *topic, int32_t **out,
                            size_t *count, bool *oom) {
    bool found = false;
    pthread_mutex_lock(&c->mu);
    const brp_topic_info_t *t = cached_topic(c, topic);
    if (t && t->partition_count > 0) {
        found = true;
        *out = malloc(t->partition_count * sizeof **out);
        if (!*out) {
            *oom = true;
        } else {
            for (size_t i = 0; i < t->partition_count; i++) (*out)[i] = t->partitions[i].partition;
            *count = t->partition_count;
        }
    }
    pthread_mutex_unlock(&c->mu);
    return found;
}

brp_err_t brp_client_partitions(brp_client_t *c, const char *topic, int32_t **out,
                                size_t *count) {
    if (!topic || !*topic) return brp_set_error(BRP_ERR_INVALID_ARG, "empty topic name");
    bool oom = false;
    if (!copy_partitions(c, topic, out, count, &oom)) {
        /* A topic auto-created on first reference is not cached yet; one
         * refresh distinguishes "new" from "absent". */
        brp_err_t err = brp_client_refresh(c, topic);
        if (err) return err;
        if (!copy_partitions(c, topic, out, count, &oom))
            return brp_set_error(BRP_ERR_UNKNOWN_TOPIC, "topic \"%s\" has no partitions", topic);
    }
    if (oom) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    return BRP_OK;
}

static int32_t cached_leader(brp_client_t *c, const char *topic, int32_t partition) {
    const brp_topic_info_t *t = cached_topic(c, topic);
    if (!t) return -1;
    for (size_t i = 0; i < t->partition_count; i++)
        if (t->partitions[i].partition == partition) return t->partitions[i].leader;
    return -1;
}

brp_err_t brp_client_conn_for(brp_client_t *c, const char *topic, int32_t partition,
                              brp_conn_t **out) {
    pthread_mutex_lock(&c->mu);
    int32_t leader = cached_leader(c, topic, partition);
    pthread_mutex_unlock(&c->mu);
    if (leader < 0) {
        brp_err_t err = brp_client_refresh(c, topic);
        if (err) return err;
        pthread_mutex_lock(&c->mu);
        leader = cached_leader(c, topic, partition);
        pthread_mutex_unlock(&c->mu);
    }
    if (leader < 0)
        return brp_set_error(BRP_ERR_UNKNOWN_TOPIC, "no leader for %s-%d", topic, (int)partition);

    brp_err_t err = BRP_OK;
    pthread_mutex_lock(&c->mu);
    for (size_t i = 0; i < c->conn_count; i++) {
        if (c->conns[i].node_id == leader) {
            *out = c->conns[i].conn;
            goto out;
        }
    }
    const brp_broker_info_t *broker = NULL;
    for (size_t i = 0; i < c->cache->broker_count; i++)
        if (c->cache->brokers[i].node_id == leader) broker = &c->cache->brokers[i];
    if (!broker) {
        err = brp_set_error(BRP_ERR_UNKNOWN_TOPIC, "broker %d is not in the metadata", (int)leader);
        goto out;
    }
    node_conn_t *grown = realloc(c->conns, (c->conn_count + 1) * sizeof *grown);
    if (!grown) {
        err = brp_set_error(BRP_ERR_NOMEM, "out of memory");
        goto out;
    }
    c->conns = grown;
    brp_conn_t *conn;
    if (c->cache->broker_count == 1) {
        /* A single-broker cluster advertises the address it was configured
         * with, which may not be the one we dialled; reuse the seed rather
         * than opening a second connection to ourselves. */
        conn = c->seed;
    } else {
        char port[16];
        snprintf(port, sizeof port, "%d", (int)broker->port);
        /* Dialled lazily on first request, outside this lock. */
        conn = conn_new(broker->host, port, c->client_id, c->dial_timeout_ms, c->io_timeout_ms);
        if (!conn) {
            err = brp_set_error(BRP_ERR_NOMEM, "out of memory");
            goto out;
        }
    }
    c->conns[c->conn_count].node_id = leader;
    c->conns[c->conn_count].conn = conn;
    c->conn_count++;
    *out = conn;
out:
    pthread_mutex_unlock(&c->mu);
    return err;
}
