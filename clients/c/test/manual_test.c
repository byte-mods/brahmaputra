/*
 * manual_test - exercises the C driver against a live broker.
 *
 *   brahmaputra-server --data-dir ./data --default-partitions 4
 *   ./build/manual_test 127.0.0.1 9092
 *
 * Every check asserts a property of the system, not that a function ran:
 * records come back byte-identical, keys pin partitions, headers survive,
 * offsets are contiguous. A fatal setup error exits 2; any failed check
 * exits 1.
 */
#define _POSIX_C_SOURCE 200809L
#include "brahmaputra.h"

#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

static int passed, failed;
static char address[512];

static void check(const char *name, bool ok, const char *fmt, ...) {
    if (ok) {
        passed++;
        printf("  ok   %s\n", name);
        return;
    }
    failed++;
    char detail[512] = "";
    if (fmt) {
        va_list args;
        va_start(args, fmt);
        vsnprintf(detail, sizeof detail, fmt, args);
        va_end(args);
    }
    if (detail[0])
        printf("  FAIL %s: %s\n", name, detail);
    else
        printf("  FAIL %s\n", name);
}

static void section(const char *title) { printf("\n%s\n", title); }

static void must(brp_err_t err, const char *what) {
    if (err != BRP_OK) {
        printf("  FATAL %s: %s (%s)\n", what, brp_last_error(), brp_err_name(err));
        exit(2);
    }
}

static int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void sleep_ms(int ms) {
    struct timespec ts = {ms / 1000, (long)(ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

static void unique(char *out, size_t n, const char *prefix) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    static unsigned counter;
    snprintf(out, n, "%s-%ld%03u", prefix, (long)(ts.tv_nsec % 1000000000L) / 1000,
             counter++ % 1000);
}

static brp_producer_t *producer_with(brp_producer_config_t *config) {
    brp_producer_t *p;
    must(brp_producer_new(address, config, &p), "producer");
    return p;
}

static brp_producer_t *immediate_producer(void) {
    brp_producer_config_t config;
    brp_producer_config_init(&config);
    config.linger_ms = 0;
    return producer_with(&config);
}

static brp_consumer_t *new_consumer(void) {
    brp_consumer_t *c;
    must(brp_consumer_new(address, NULL, &c), "consumer");
    return c;
}

static void send_to(brp_producer_t *p, const char *topic, int32_t partition, const void *value,
                    size_t value_len, const char *key) {
    brp_message_t m;
    brp_message_init(&m);
    m.topic = topic;
    m.partition = partition;
    m.value = value;
    m.value_len = value_len;
    m.key = key;
    m.key_len = key ? strlen(key) : 0;
    must(brp_producer_send(p, &m), "send");
}

static void send_str(brp_producer_t *p, const char *topic, int32_t partition, const char *value,
                     const char *key) {
    send_to(p, topic, partition, value, strlen(value), key);
}

static bool value_is(const brp_record_t *r, const char *s) {
    size_t n = strlen(s);
    return r->value && r->value_len == n && memcmp(r->value, s, n) == 0;
}

typedef struct seen {
    brp_record_t *records;
    size_t n;
} seen_t;

static void seen_add(seen_t *s, brp_record_t *records, size_t n) {
    if (!n) {
        brp_records_free(records, n);
        return;
    }
    brp_record_t *grown = realloc(s->records, (s->n + n) * sizeof *grown);
    if (!grown) exit(2);
    s->records = grown;
    memcpy(s->records + s->n, records, n * sizeof *records);
    s->n += n;
    free(records); /* elements moved */
}

static void seen_free(seen_t *s) {
    brp_records_free(s->records, s->n);
    s->records = NULL;
    s->n = 0;
}

static void group_config(brp_group_config_t *config) {
    brp_group_config_init(config);
    config->auto_commit_interval_ms = 0;
}

static brp_group_consumer_t *new_group(const char *group_id, const brp_group_config_t *config,
                                       const char *topic) {
    brp_group_consumer_t *g;
    must(brp_group_consumer_new(address, group_id, config, &g), "group consumer");
    must(brp_group_consumer_subscribe(g, &topic, 1), "subscribe");
    return g;
}

/* ------------------------------------------------------------------------ */
/* Test-only TCP helpers                                                    */
/* ------------------------------------------------------------------------ */

static int listen_local(int *port_out) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof sa;
    if (bind(fd, (struct sockaddr *)&sa, sizeof sa) != 0 || listen(fd, 16) != 0 ||
        getsockname(fd, (struct sockaddr *)&sa, &len) != 0) {
        close(fd);
        return -1;
    }
    *port_out = ntohs(sa.sin_port);
    return fd;
}

static int dial_local(const char *host, const char *port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)atoi(port));
    if (inet_pton(AF_INET, strcmp(host, "localhost") == 0 ? "127.0.0.1" : host, &sa.sin_addr) != 1 ||
        connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* A broker that accepts and never answers. */
typedef struct silent {
    int listen_fd;
    int port;
    int accepted[64];
    int accepted_n;
    pthread_t thread;
} silent_t;

static void *silent_main(void *arg) {
    silent_t *s = arg;
    for (;;) {
        int fd = accept(s->listen_fd, NULL, NULL);
        if (fd < 0) return NULL;
        if (s->accepted_n < 64)
            s->accepted[s->accepted_n++] = fd;
        else
            close(fd);
    }
}

/* Forwards TCP to the broker and can sever every live connection, which is
 * how a broker restart or an idle timeout looks to a client. */
typedef struct pair {
    int client_fd, upstream_fd;
    int refs;
    struct pair *next;
} pair_t;

typedef struct proxy {
    char address[64];
    int listen_fd;
    const char *host, *port;
    pthread_t thread;
    pthread_mutex_t mu;
    pair_t *live;
} proxy_t;

typedef struct pump {
    proxy_t *proxy;
    pair_t *pair;
    int from, to;
} pump_t;

static void *pump_main(void *arg) {
    pump_t *pm = arg;
    char buf[65536];
    for (;;) {
        ssize_t n = read(pm->from, buf, sizeof buf);
        if (n <= 0) break;
        ssize_t off = 0;
        while (off < n) {
            ssize_t w = send(pm->to, buf + off, (size_t)(n - off), MSG_NOSIGNAL);
            if (w <= 0) goto done;
            off += w;
        }
    }
done:
    pthread_mutex_lock(&pm->proxy->mu);
    shutdown(pm->pair->client_fd, SHUT_RDWR);
    shutdown(pm->pair->upstream_fd, SHUT_RDWR);
    if (--pm->pair->refs == 0) {
        for (pair_t **pp = &pm->proxy->live; *pp; pp = &(*pp)->next)
            if (*pp == pm->pair) {
                *pp = pm->pair->next;
                break;
            }
        close(pm->pair->client_fd);
        close(pm->pair->upstream_fd);
        free(pm->pair);
    }
    pthread_mutex_unlock(&pm->proxy->mu);
    free(pm);
    return NULL;
}

static void *proxy_main(void *arg) {
    proxy_t *p = arg;
    for (;;) {
        int client = accept(p->listen_fd, NULL, NULL);
        if (client < 0) return NULL;
        int upstream = dial_local(p->host, p->port);
        if (upstream < 0) {
            close(client);
            continue;
        }
        pair_t *pair = calloc(1, sizeof *pair);
        pump_t *a = calloc(1, sizeof *a), *b = calloc(1, sizeof *b);
        pair->client_fd = client;
        pair->upstream_fd = upstream;
        pair->refs = 2;
        pthread_mutex_lock(&p->mu);
        pair->next = p->live;
        p->live = pair;
        pthread_mutex_unlock(&p->mu);
        *a = (pump_t){p, pair, client, upstream};
        *b = (pump_t){p, pair, upstream, client};
        pthread_t t;
        pthread_create(&t, NULL, pump_main, a);
        pthread_detach(t);
        pthread_create(&t, NULL, pump_main, b);
        pthread_detach(t);
    }
}

static proxy_t *proxy_new(const char *host, const char *port) {
    proxy_t *p = calloc(1, sizeof *p);
    int listen_port;
    p->listen_fd = listen_local(&listen_port);
    if (p->listen_fd < 0) {
        printf("  FATAL proxy listen\n");
        exit(2);
    }
    snprintf(p->address, sizeof p->address, "127.0.0.1:%d", listen_port);
    p->host = host;
    p->port = port;
    pthread_mutex_init(&p->mu, NULL);
    pthread_create(&p->thread, NULL, proxy_main, p);
    return p;
}

static void proxy_drop_all(proxy_t *p) {
    pthread_mutex_lock(&p->mu);
    for (pair_t *pair = p->live; pair; pair = pair->next) {
        shutdown(pair->client_fd, SHUT_RDWR);
        shutdown(pair->upstream_fd, SHUT_RDWR);
    }
    pthread_mutex_unlock(&p->mu);
    sleep_ms(50);
}

static void proxy_close(proxy_t *p) {
    shutdown(p->listen_fd, SHUT_RDWR);
    pthread_join(p->thread, NULL);
    close(p->listen_fd);
    for (int i = 0; i < 100; i++) {
        proxy_drop_all(p);
        pthread_mutex_lock(&p->mu);
        bool empty = p->live == NULL;
        pthread_mutex_unlock(&p->mu);
        if (empty) break;
    }
    pthread_mutex_destroy(&p->mu);
    free(p);
}

/* Fetches from offset 0 until `want` records arrive or a fetch comes back
 * empty. */
static void fetch_all(brp_consumer_t *consumer, const char *topic, size_t want, seen_t *out) {
    int64_t offset = 0;
    while (out->n < want) {
        brp_record_t *batch;
        size_t n;
        if (brp_consumer_fetch(consumer, topic, 0, offset, 500, &batch, &n, NULL) != BRP_OK ||
            n == 0) {
            if (n == 0) brp_free(batch);
            break;
        }
        offset = batch[n - 1].offset + 1;
        seen_add(out, batch, n);
    }
}

typedef struct delayed_send {
    brp_producer_t *producer;
    const char *topic;
} delayed_send_t;

static void *delayed_send_main(void *arg) {
    delayed_send_t *d = arg;
    sleep_ms(2000);
    for (int i = 0; i < 10; i++) {
        char value[32];
        snprintf(value, sizeof value, "j%d", i);
        brp_message_t m;
        brp_message_init(&m);
        m.topic = d->topic;
        m.value = value;
        m.value_len = strlen(value);
        brp_producer_send(d->producer, &m);
    }
    return NULL;
}


/* ------------------------------------------------------------------------ */
/* Checks beyond the Go suite: every item of the client feature checklist   */
/* that the sections above do not already exercise.                        */
/* ------------------------------------------------------------------------ */

/* lz4 in the broker's format (little-endian uncompressed length, then a raw
 * LZ4 block). It compresses by emitting one literal run - valid LZ4 any
 * decoder reads - and decodes full LZ4, matches included, so it reads what
 * the broker's lz4 writes too. `opaque` counts calls. */
static int lz4_literals(const uint8_t *in, size_t in_len, uint8_t **out, size_t *out_len,
                        void *opaque) {
    atomic_int *calls = opaque;
    atomic_fetch_add(&calls[0], 1);
    size_t cap = in_len + in_len / 255 + 16, n = 0;
    uint8_t *buf = malloc(cap);
    if (!buf) return -1;
    for (int shift = 0; shift < 32; shift += 8) buf[n++] = (uint8_t)(in_len >> shift);
    buf[n++] = (uint8_t)((in_len < 15 ? in_len : 15) << 4);
    if (in_len >= 15) {
        size_t rest = in_len - 15;
        for (; rest >= 255; rest -= 255) buf[n++] = 255;
        buf[n++] = (uint8_t)rest;
    }
    if (in_len) memcpy(buf + n, in, in_len);
    *out = buf;
    *out_len = n + in_len;
    return 0;
}

static int lz4_decode(const uint8_t *in, size_t in_len, uint8_t **out, size_t *out_len,
                      void *opaque) {
    atomic_int *calls = opaque;
    atomic_fetch_add(&calls[1], 1);
    if (in_len < 4) return -1;
    size_t size = (size_t)in[0] | (size_t)in[1] << 8 | (size_t)in[2] << 16 | (size_t)in[3] << 24;
    if (size > 256u * 1024 * 1024) return -1;
    uint8_t *buf = malloc(size ? size : 1);
    if (!buf) return -1;
    size_t ip = 4, at = 0;
    while (ip < in_len) {
        unsigned token = in[ip++];
        size_t literals = token >> 4;
        if (literals == 15) {
            unsigned more;
            do {
                if (ip >= in_len) goto fail;
                more = in[ip++];
                literals += more;
            } while (more == 255);
        }
        if (literals > in_len - ip || literals > size - at) goto fail;
        memcpy(buf + at, in + ip, literals);
        ip += literals;
        at += literals;
        if (ip >= in_len) break;
        if (in_len - ip < 2) goto fail;
        size_t distance = (size_t)in[ip] | (size_t)in[ip + 1] << 8;
        ip += 2;
        size_t matched = token & 15;
        if (matched == 15) {
            unsigned more;
            do {
                if (ip >= in_len) goto fail;
                more = in[ip++];
                matched += more;
            } while (more == 255);
        }
        matched += 4;
        if (distance == 0 || distance > at || matched > size - at) goto fail;
        for (size_t i = 0; i < matched; i++, at++) buf[at] = buf[at - distance];
    }
    if (at != size) goto fail;
    *out = buf;
    *out_len = size;
    return 0;
fail:
    free(buf);
    return -1;
}

/* ---- a fault-injecting proxy ------------------------------------------- */

/* Minimal BitPacker for the proxy's fake answers: zigzag varints, and
 * strings as a varint length plus bytes. */
typedef struct wbuf {
    uint8_t data[512];
    size_t len;
} wbuf_t;

static void w_uvarint(wbuf_t *w, uint64_t v) {
    while (v >= 0x80) {
        w->data[w->len++] = (uint8_t)(v | 0x80);
        v >>= 7;
    }
    w->data[w->len++] = (uint8_t)v;
}
static void w_i64(wbuf_t *w, int64_t v) { w_uvarint(w, ((uint64_t)v << 1) ^ (uint64_t)(v >> 63)); }
static void w_i32(wbuf_t *w, int32_t v) { w_i64(w, v); }
static void w_str(wbuf_t *w, const char *s, size_t n) {
    w_i32(w, (int32_t)n);
    memcpy(w->data + w->len, s, n);
    w->len += n;
}

typedef struct rbuf {
    const uint8_t *data;
    size_t len, pos;
    bool bad;
} rbuf_t;

static int64_t r_i64(rbuf_t *r) {
    uint64_t v = 0;
    for (int shift = 0; shift < 64; shift += 7) {
        if (r->pos >= r->len) {
            r->bad = true;
            return 0;
        }
        uint8_t b = r->data[r->pos++];
        v |= (uint64_t)(b & 0x7f) << shift;
        if (!(b & 0x80)) return (int64_t)(v >> 1) ^ -(int64_t)(v & 1);
    }
    r->bad = true;
    return 0;
}
static int32_t r_i32(rbuf_t *r) { return (int32_t)r_i64(r); }
static const char *r_str(rbuf_t *r, size_t *n) {
    int32_t len = r_i32(r);
    if (r->bad || len < 0 || (size_t)len > r->len - r->pos) {
        r->bad = true;
        *n = 0;
        return "";
    }
    const char *s = (const char *)r->data + r->pos;
    r->pos += (size_t)len;
    *n = (size_t)len;
    return s;
}

typedef struct fault_proxy {
    char address[64];
    int listen_fd;
    const char *host, *port;
    pthread_t thread;
    pthread_mutex_t mu;
    int fail_produces; /* >0: fail that many; -1: fail every one */
    int corrupt_fetch; /* 1: negative batch_length; 2: one past the data */
    int produces;
    int32_t last_acks, last_timeout_ms;
    int fds[256]; /* live connections' sockets, for close to shut down */
    int fd_n;
    int active; /* connection threads still running */
} fault_proxy_t;

typedef struct fault_conn {
    fault_proxy_t *proxy;
    int client, upstream;
} fault_conn_t;

static bool read_full(int fd, uint8_t *buf, size_t n) {
    size_t off = 0;
    while (off < n) {
        ssize_t got = read(fd, buf + off, n - off);
        if (got <= 0) return false;
        off += (size_t)got;
    }
    return true;
}

static bool write_full(int fd, const uint8_t *buf, size_t n) {
    size_t off = 0;
    while (off < n) {
        ssize_t put = send(fd, buf + off, n - off, MSG_NOSIGNAL);
        if (put <= 0) return false;
        off += (size_t)put;
    }
    return true;
}

/* Reads one frame (length prefix included) into a malloc'd buffer. */
static uint8_t *read_frame(int fd, size_t *total) {
    uint8_t prefix[4];
    if (!read_full(fd, prefix, 4)) return NULL;
    uint32_t len = (uint32_t)prefix[0] << 24 | (uint32_t)prefix[1] << 16 |
                   (uint32_t)prefix[2] << 8 | prefix[3];
    if (len > 64u * 1024 * 1024) return NULL;
    uint8_t *frame = malloc(4 + (size_t)len);
    if (!frame) return NULL;
    memcpy(frame, prefix, 4);
    if (!read_full(fd, frame + 4, len)) {
        free(frame);
        return NULL;
    }
    *total = 4 + (size_t)len;
    return frame;
}

static void *fault_conn_main(void *arg) {
    fault_conn_t *c = arg;
    fault_proxy_t *p = c->proxy;
    for (;;) {
        size_t total;
        uint8_t *frame = read_frame(c->client, &total);
        if (!frame) break;
        const uint8_t *payload = frame + 4;
        size_t len = total - 4;
        if (len < 10) {
            free(frame);
            break;
        }
        int16_t api = (int16_t)(payload[0] << 8 | payload[1]);
        int16_t client_len = (int16_t)(payload[8] << 8 | payload[9]);
        size_t body_at = 10 + (client_len > 0 ? (size_t)client_len : 0);
        if (body_at > len) {
            free(frame);
            break;
        }
        rbuf_t r = {payload + body_at, len - body_at, 0, false};
        size_t schema_n, topic_n;
        r_str(&r, &schema_n);
        const char *topic = r_str(&r, &topic_n);
        int32_t partition = r_i32(&r);
        wbuf_t w = {{0}, 0};
        bool reply = false, oneway = false;
        if (api == 0 /* Produce */) {
            int32_t acks = r_i32(&r);
            int32_t timeout = r_i32(&r);
            pthread_mutex_lock(&p->mu);
            p->produces++;
            p->last_acks = acks;
            p->last_timeout_ms = timeout;
            if (p->fail_produces != 0) {
                if (p->fail_produces > 0) p->fail_produces--;
                reply = true;
            }
            pthread_mutex_unlock(&p->mu);
            oneway = acks == 0;
            if (reply) {
                w_str(&w, BRP_SCHEMA_VERSION, strlen(BRP_SCHEMA_VERSION));
                w_str(&w, topic, topic_n);
                w_i32(&w, partition);
                w_i32(&w, BRP_ERR_NOT_ENOUGH_REPLICAS);
                w_i64(&w, -1);
                w_i64(&w, -1);
            }
        } else if (api == 1 /* Fetch */) {
            pthread_mutex_lock(&p->mu);
            int mode = p->corrupt_fetch;
            pthread_mutex_unlock(&p->mu);
            if (mode) {
                /* A batch whose batch_length is negative (mode 1) or runs
                 * far past the bytes that follow (mode 2). */
                uint8_t batch[61] = {0};
                uint32_t claimed = mode == 1 ? 0xFFFFFFFFu : 1000000u;
                batch[8] = (uint8_t)(claimed >> 24);
                batch[9] = (uint8_t)(claimed >> 16);
                batch[10] = (uint8_t)(claimed >> 8);
                batch[11] = (uint8_t)claimed;
                w_str(&w, BRP_SCHEMA_VERSION, strlen(BRP_SCHEMA_VERSION));
                w_str(&w, topic, topic_n);
                w_i32(&w, partition);
                w_i32(&w, 0);
                w_i64(&w, 1);
                w_i64(&w, 1);
                w_i64(&w, (int64_t)sizeof batch);
                w_i32(&w, -1);
                memcpy(w.data + w.len, batch, sizeof batch);
                w.len += sizeof batch;
                reply = true;
            }
        }
        bool ok = true;
        if (reply) {
            uint8_t out[600];
            uint32_t out_len = (uint32_t)(10 + w.len);
            out[0] = (uint8_t)(out_len >> 24);
            out[1] = (uint8_t)(out_len >> 16);
            out[2] = (uint8_t)(out_len >> 8);
            out[3] = (uint8_t)out_len;
            memcpy(out + 4, payload, 8); /* api key, api version, correlation id */
            out[12] = 0xFF;              /* no client id */
            out[13] = 0xFF;
            memcpy(out + 14, w.data, w.len);
            ok = write_full(c->client, out, 14 + w.len);
        } else {
            ok = write_full(c->upstream, frame, total);
            if (ok && !oneway) {
                size_t resp_total;
                uint8_t *resp = read_frame(c->upstream, &resp_total);
                ok = resp && write_full(c->client, resp, resp_total);
                free(resp);
            }
        }
        free(frame);
        if (!ok) break;
    }
    pthread_mutex_lock(&p->mu);
    for (int i = 0; i < p->fd_n; i++) {
        if (p->fds[i] == c->client || p->fds[i] == c->upstream) {
            p->fds[i--] = p->fds[--p->fd_n];
        }
    }
    close(c->client);
    close(c->upstream);
    p->active--;
    pthread_mutex_unlock(&p->mu);
    free(c);
    return NULL;
}

static void *fault_proxy_main(void *arg) {
    fault_proxy_t *p = arg;
    for (;;) {
        int client = accept(p->listen_fd, NULL, NULL);
        if (client < 0) return NULL;
        int upstream = dial_local(p->host, p->port);
        if (upstream < 0) {
            close(client);
            continue;
        }
        pthread_mutex_lock(&p->mu);
        if (p->fd_n + 2 <= 256) {
            p->fds[p->fd_n++] = client;
            p->fds[p->fd_n++] = upstream;
        }
        p->active++;
        pthread_mutex_unlock(&p->mu);
        fault_conn_t *c = calloc(1, sizeof *c);
        if (!c) exit(2);
        *c = (fault_conn_t){p, client, upstream};
        pthread_t t;
        pthread_create(&t, NULL, fault_conn_main, c);
        pthread_detach(t);
    }
}

static fault_proxy_t *fault_proxy_new(const char *host, const char *port) {
    fault_proxy_t *p = calloc(1, sizeof *p);
    int listen_port;
    if (!p || (p->listen_fd = listen_local(&listen_port)) < 0) {
        printf("  FATAL fault proxy listen\n");
        exit(2);
    }
    snprintf(p->address, sizeof p->address, "127.0.0.1:%d", listen_port);
    p->host = host;
    p->port = port;
    p->last_acks = p->last_timeout_ms = INT32_MIN;
    pthread_mutex_init(&p->mu, NULL);
    pthread_create(&p->thread, NULL, fault_proxy_main, p);
    return p;
}

/* Fails the next `count` produces (-1: every one) and resets the counter. */
static void fault_fail_produces(fault_proxy_t *p, int count) {
    pthread_mutex_lock(&p->mu);
    p->fail_produces = count;
    p->produces = 0;
    pthread_mutex_unlock(&p->mu);
}

static void fault_proxy_close(fault_proxy_t *p) {
    shutdown(p->listen_fd, SHUT_RDWR);
    pthread_join(p->thread, NULL);
    close(p->listen_fd);
    /* The connection threads are detached; wake them and wait until the
     * last has let go of the proxy before freeing it. */
    for (int i = 0; i < 500; i++) {
        pthread_mutex_lock(&p->mu);
        for (int j = 0; j < p->fd_n; j++) shutdown(p->fds[j], SHUT_RDWR);
        int active = p->active;
        pthread_mutex_unlock(&p->mu);
        if (active == 0) break;
        sleep_ms(10);
    }
    pthread_mutex_destroy(&p->mu);
    free(p);
}

/* ---- group helpers ----------------------------------------------------- */

static size_t poll_count(brp_group_consumer_t *g, int timeout_ms) {
    brp_record_t *records = NULL;
    size_t n = 0;
    if (brp_group_consumer_poll(g, timeout_ms, &records, &n) != BRP_OK) return 0;
    brp_records_free(records, n);
    return n;
}

static size_t drain(brp_group_consumer_t *g, size_t want, int timeout_ms) {
    size_t got = 0;
    int64_t deadline = now_ms() + timeout_ms;
    while (got < want && now_ms() < deadline) got += poll_count(g, 300);
    return got;
}

static size_t assignment_count(brp_group_consumer_t *g) {
    brp_partition_offset_t *held = NULL;
    size_t n = 0;
    if (brp_group_consumer_assignment(g, &held, &n) != BRP_OK) return 0;
    brp_offsets_free(held, n);
    return n;
}

static void await_assignment(brp_group_consumer_t *g, int timeout_ms) {
    int64_t deadline = now_ms() + timeout_ms;
    while (assignment_count(g) == 0 && now_ms() < deadline) poll_count(g, 200);
}

static void member_id_of(brp_group_consumer_t *g, char *out, size_t n) {
    char *id = brp_group_consumer_member_id(g);
    snprintf(out, n, "%s", id ? id : "");
    brp_free(id);
}

static size_t count_at(brp_consumer_t *consumer, const char *topic, int32_t partition,
                       int64_t offset, int32_t max_wait_ms) {
    brp_record_t *records = NULL;
    size_t n = 0;
    if (brp_consumer_fetch(consumer, topic, partition, offset, max_wait_ms, &records, &n, NULL) !=
        BRP_OK)
        return (size_t)-1;
    brp_records_free(records, n);
    return n;
}

/* A second group member polled on a thread of its own (a group consumer is
 * single-threaded, so only that thread touches it); it publishes the
 * partitions it holds after each poll. One topic, so partition ids suffice. */
typedef struct second_member {
    brp_group_consumer_t *group;
    atomic_int stop;
    pthread_mutex_t mu;
    int32_t held[64];
    size_t held_n;
} second_member_t;

static void *second_member_main(void *arg) {
    second_member_t *s = arg;
    while (!atomic_load(&s->stop)) {
        poll_count(s->group, 200);
        brp_partition_offset_t *held = NULL;
        size_t n = 0;
        if (brp_group_consumer_assignment(s->group, &held, &n) != BRP_OK) n = 0;
        pthread_mutex_lock(&s->mu);
        s->held_n = n < 64 ? n : 64;
        for (size_t i = 0; i < s->held_n; i++) s->held[i] = held[i].partition;
        pthread_mutex_unlock(&s->mu);
        brp_offsets_free(held, n);
    }
    return NULL;
}

/* True when the two members' holdings are non-empty, disjoint and together
 * cover `partitions`. */
static bool split_between(brp_group_consumer_t *a, second_member_t *b, size_t partitions,
                          size_t *na_out, size_t *nb_out) {
    brp_partition_offset_t *ha = NULL;
    size_t na = 0;
    if (brp_group_consumer_assignment(a, &ha, &na) != BRP_OK) na = 0;
    pthread_mutex_lock(&b->mu);
    size_t nb = b->held_n;
    bool overlap = false;
    for (size_t i = 0; i < na; i++)
        for (size_t j = 0; j < nb; j++)
            if (ha[i].partition == b->held[j]) overlap = true;
    pthread_mutex_unlock(&b->mu);
    brp_offsets_free(ha, na);
    *na_out = na;
    *nb_out = nb;
    return na > 0 && nb > 0 && !overlap && na + nb == partitions;
}

static void run_checklist(const char *host, const char *port) {
    section("producer: batch.size and linger.ms");
    {
        char topic[128];
        unique(topic, sizeof topic, "c-batchsize");
        brp_producer_config_t config;
        brp_producer_config_init(&config);
        config.linger_ms = 60000; /* only batch.size can send anything here */
        config.batch_size = 1024;
        brp_producer_t *producer = producer_with(&config);
        brp_consumer_t *consumer = new_consumer();
        int32_t *partitions;
        size_t pcount;
        must(brp_client_partitions(brp_producer_client(producer), topic, &partitions, &pcount),
             "partitions");
        brp_free(partitions);
        uint8_t value[200];
        memset(value, 'b', sizeof value);
        for (int i = 0; i < 8; i++) send_to(producer, topic, 0, value, sizeof value, NULL);
        size_t early = count_at(consumer, topic, 0, 0, 0);
        check("a batch that reaches batch.size is sent before linger.ms", early >= 1 && early < 8,
              "%zu of 8 sent before any flush", early);
        must(brp_producer_flush(producer), "flush");
        size_t after = count_at(consumer, topic, 0, 0, 500);
        check("flush sends the partial batch that is left", after == 8, "got %zu", after);
        brp_consumer_close(consumer);
        must(brp_producer_close(producer), "close");

        unique(topic, sizeof topic, "c-linger");
        brp_producer_config_init(&config);
        config.linger_ms = 500;
        producer = producer_with(&config);
        consumer = new_consumer();
        must(brp_client_partitions(brp_producer_client(producer), topic, &partitions, &pcount),
             "partitions");
        brp_free(partitions);
        send_str(producer, topic, 0, "lingering", NULL);
        size_t immediate = count_at(consumer, topic, 0, 0, 0);
        sleep_ms(1500);
        size_t later = count_at(consumer, topic, 0, 0, 0);
        check("linger.ms holds a record back, then sends it without a flush",
              immediate == 0 && later == 1, "immediately %zu, after linger %zu", immediate,
              later);
        brp_consumer_close(consumer);
        must(brp_producer_close(producer), "close");
    }

    section("producer: partitioners");
    {
        char rr_topic[128], pin_topic[128];
        unique(rr_topic, sizeof rr_topic, "c-rr");
        unique(pin_topic, sizeof pin_topic, "c-pinned");
        brp_producer_t *producer = immediate_producer();
        int32_t *partitions;
        size_t pcount;
        must(brp_client_partitions(brp_producer_client(producer), rr_topic, &partitions, &pcount),
             "partitions");
        for (size_t i = 0; i < pcount * 2; i++) {
            char value[32];
            snprintf(value, sizeof value, "rr%zu", i);
            send_str(producer, rr_topic, BRP_PARTITION_ANY, value, NULL);
        }
        int32_t *pin_parts;
        size_t pin_count;
        must(brp_client_partitions(brp_producer_client(producer), pin_topic, &pin_parts,
                                   &pin_count),
             "partitions");
        brp_free(pin_parts);
        int32_t last = partitions[pcount - 1];
        send_str(producer, pin_topic, last, "pinned", NULL);
        must(brp_producer_close(producer), "close");
        brp_consumer_t *consumer = new_consumer();
        bool even = true;
        size_t pinned_there = 0, pinned_elsewhere = 0;
        char counts[256] = "";
        for (size_t i = 0; i < pcount; i++) {
            size_t n = count_at(consumer, rr_topic, partitions[i], 0, 0);
            size_t used = strlen(counts);
            snprintf(counts + used, sizeof counts - used, "%d=%zu ", (int)partitions[i], n);
            if (n != 2) even = false;
            size_t pinned = count_at(consumer, pin_topic, partitions[i], 0, 0);
            if (partitions[i] == last)
                pinned_there += pinned;
            else
                pinned_elsewhere += pinned;
        }
        check("a null key round-robins across every partition", even, "%s", counts);
        check("an explicit partition is honoured", pinned_there == 1 && pinned_elsewhere == 0,
              "%zu there, %zu elsewhere", pinned_there, pinned_elsewhere);
        brp_free(partitions);
        brp_consumer_close(consumer);
    }

    section("producer: record timestamps and send-and-wait");
    {
        char time_topic[128], sync_topic[128];
        unique(time_topic, sizeof time_topic, "c-timestamps");
        unique(sync_topic, sizeof sync_topic, "c-sync");
        int64_t base = now_ms() - 60000;
        int64_t before_send = now_ms();
        brp_producer_t *producer = immediate_producer();
        for (int i = 0; i < 3; i++) {
            char value[16];
            snprintf(value, sizeof value, "t%d", i);
            brp_message_t m;
            brp_message_init(&m);
            m.topic = time_topic;
            m.partition = 0;
            m.value = value;
            m.value_len = strlen(value);
            m.timestamp_ms = base + i * 1000;
            must(brp_producer_send(producer, &m), "send");
        }
        send_str(producer, time_topic, 0, "now", NULL);
        int64_t offsets[2] = {-1, -1};
        for (int i = 0; i < 2; i++) {
            brp_message_t m;
            brp_message_init(&m);
            m.topic = sync_topic;
            m.partition = 0;
            m.value = "s";
            m.value_len = 1;
            must(brp_producer_send_sync(producer, &m, &offsets[i]), "send_sync");
        }
        must(brp_producer_close(producer), "close");
        brp_consumer_t *consumer = new_consumer();
        brp_record_t *records = NULL;
        size_t n = 0;
        must(brp_consumer_fetch(consumer, time_topic, 0, 0, 500, &records, &n, NULL), "fetch");
        bool exact = n == 4;
        for (int i = 0; exact && i < 3; i++) exact = records[i].timestamp == base + i * 1000;
        check("an explicit record timestamp round-trips exactly", exact, "%zu records", n);
        check("a record without one is stamped with the wall clock",
              n == 4 && records[3].timestamp >= before_send - 1000 &&
                  records[3].timestamp <= now_ms() + 1000,
              "%lld", n == 4 ? (long long)records[3].timestamp : 0LL);
        brp_records_free(records, n);
        check("send-and-wait returns each record's offset", offsets[0] == 0 && offsets[1] == 1,
              "%lld, %lld", (long long)offsets[0], (long long)offsets[1]);
        int64_t at_half = -9, at_last = -9;
        must(brp_consumer_list_offsets(consumer, time_topic, 0, base + 500, &at_half), "offsets");
        must(brp_consumer_list_offsets(consumer, time_topic, 0, base + 2000, &at_last), "offsets");
        check("list offsets by timestamp finds the first record at or after it",
              at_half == 1 && at_last == 2, "%lld, %lld", (long long)at_half, (long long)at_last);
        brp_consumer_close(consumer);
    }

    section("producer: codec registration");
    {
        static atomic_int calls[2];
        must(brp_register_codec(BRP_COMPRESSION_LZ4, lz4_literals, lz4_decode, calls), "register");
        char topic[128];
        unique(topic, sizeof topic, "c-lz4");
        brp_producer_config_t config;
        brp_producer_config_init(&config);
        config.linger_ms = 0;
        config.compression_type = "lz4";
        brp_producer_t *producer = producer_with(&config);
        char sent[10][64];
        for (int i = 0; i < 10; i++) {
            snprintf(sent[i], sizeof sent[i], "lz4 record %d%40s", i, "");
            char key[16];
            snprintf(key, sizeof key, "k%d", i);
            send_str(producer, topic, 0, sent[i], key);
        }
        must(brp_producer_close(producer), "close");
        brp_consumer_t *consumer = new_consumer();
        brp_record_t *records = NULL;
        size_t n = 0;
        must(brp_consumer_fetch(consumer, topic, 0, 0, 500, &records, &n, NULL), "fetch");
        bool same = n == 10;
        for (size_t i = 0; same && i < n; i++) same = value_is(&records[i], sent[i]);
        brp_records_free(records, n);
        int compressed = atomic_load(&calls[0]);
        int decompressed = atomic_load(&calls[1]);
        check("a registered codec (lz4) compresses sends and decodes fetches",
              same && compressed >= 10 && decompressed >= 10,
              "%zu records, %d compressed, %d decompressed", n, compressed, decompressed);
        brp_consumer_close(consumer);

        brp_producer_config_init(&config);
        config.compression_type = "zstd";
        brp_producer_t *refused = NULL;
        brp_err_t err = brp_producer_new(address, &config, &refused);
        check("an unregistered codec is refused, not sent uncompressed",
              err == BRP_ERR_CODEC && refused == NULL, "%s", brp_err_name(err));
        if (!err) brp_producer_close(refused);
    }

    section("producer: retries, request.timeout.ms and delivery.timeout.ms");
    {
        fault_proxy_t *proxy = fault_proxy_new(host, port);
        char topic[128];
        unique(topic, sizeof topic, "c-retry");
        brp_producer_config_t config;
        brp_producer_config_init(&config);
        config.linger_ms = 0;
        config.acks = -1;
        config.request_timeout_ms = 1234;
        config.retries = 3;
        config.retry_backoff_ms = 150;

        fault_fail_produces(proxy, 2);
        brp_producer_t *producer;
        must(brp_producer_new(proxy->address, &config, &producer), "producer");
        int64_t started = now_ms();
        brp_message_t m;
        brp_message_init(&m);
        m.topic = topic;
        m.partition = 0;
        m.value = "retried";
        m.value_len = 7;
        brp_err_t err = brp_producer_send(producer, &m);
        int64_t elapsed = now_ms() - started;
        brp_producer_close(producer);
        pthread_mutex_lock(&proxy->mu);
        int attempts = proxy->produces;
        int32_t acks = proxy->last_acks, timeout = proxy->last_timeout_ms;
        pthread_mutex_unlock(&proxy->mu);
        check("request.timeout.ms and acks travel with every produce", timeout == 1234 && acks == -1,
              "timeout=%d acks=%d", (int)timeout, (int)acks);
        check("a retriable error is retried after retry.backoff.ms",
              err == BRP_OK && attempts == 3 && elapsed >= 300, "attempts=%d elapsed=%lld err=%s",
              attempts, (long long)elapsed, brp_err_name(err));
        brp_consumer_t *consumer = new_consumer();
        size_t stored = count_at(consumer, topic, 0, 0, 500);
        check("the retried record is stored exactly once", stored == 1, "stored %zu", stored);
        brp_consumer_close(consumer);

        fault_fail_produces(proxy, -1);
        config.retries = 2;
        must(brp_producer_new(proxy->address, &config, &producer), "producer");
        m.value = "never";
        m.value_len = 5;
        err = brp_producer_send(producer, &m);
        brp_producer_close(producer);
        pthread_mutex_lock(&proxy->mu);
        attempts = proxy->produces;
        pthread_mutex_unlock(&proxy->mu);
        check("retries bounds the attempts: the error surfaces after retries + 1",
              err != BRP_OK && attempts == 3, "attempts=%d err=%s", attempts, brp_err_name(err));

        fault_fail_produces(proxy, -1);
        config.retries = 1000000;
        config.retry_backoff_ms = 50;
        config.delivery_timeout_ms = 500;
        must(brp_producer_new(proxy->address, &config, &producer), "producer");
        m.value = "late";
        m.value_len = 4;
        started = now_ms();
        err = brp_producer_send(producer, &m);
        elapsed = now_ms() - started;
        brp_producer_close(producer);
        check("delivery.timeout.ms bounds the time spent retrying",
              err == BRP_ERR_DELIVERY_TIMEOUT && elapsed >= 400 && elapsed < 3000,
              "elapsed=%lld err=%s", (long long)elapsed, brp_err_name(err));

        fault_fail_produces(proxy, 0);
        for (int mode = 1; mode <= 2; mode++) {
            pthread_mutex_lock(&proxy->mu);
            proxy->corrupt_fetch = mode;
            pthread_mutex_unlock(&proxy->mu);
            brp_consumer_t *through;
            must(brp_consumer_new(proxy->address, NULL, &through), "consumer");
            brp_record_t *records = NULL;
            size_t n = 0;
            err = brp_consumer_fetch(through, topic, 0, 0, 100, &records, &n, NULL);
            if (!err) brp_records_free(records, n);
            brp_consumer_close(through);
            check(mode == 1 ? "a negative length on the wire is an error"
                            : "a length past the end of the data is an error",
                  err == BRP_ERR_CORRUPT || err == BRP_ERR_PROTOCOL, "%s", brp_err_name(err));
        }
        fault_proxy_close(proxy);
    }

    section("consumer: fetch limits, high watermark and metadata");
    {
        char topic[128];
        unique(topic, sizeof topic, "c-fetch");
        brp_producer_t *producer = immediate_producer();
        uint8_t value[1000];
        memset(value, 'f', sizeof value);
        for (int i = 0; i < 10; i++) send_to(producer, topic, 0, value, sizeof value, NULL);
        must(brp_producer_close(producer), "close");

        brp_consumer_config_t config;
        brp_consumer_config_init(&config);
        config.fetch_max_bytes = 2500;
        brp_consumer_t *small;
        must(brp_consumer_new(address, &config, &small), "consumer");
        size_t got = count_at(small, topic, 0, 0, 500);
        check("fetch.max.bytes caps what one fetch returns", got >= 1 && got < 10,
              "got %zu of 10", got);
        brp_consumer_close(small);

        brp_consumer_config_init(&config);
        config.max_poll_records = 4;
        brp_consumer_t *capped;
        must(brp_consumer_new(address, &config, &capped), "consumer");
        got = count_at(capped, topic, 0, 0, 500);
        check("max.poll.records caps one fetch", got == 4, "got %zu", got);
        brp_record_t *records = NULL;
        size_t n = 0;
        must(brp_consumer_fetch(capped, topic, 0, 4, 500, &records, &n, NULL), "fetch");
        check("the records a cap held back come on the next fetch",
              n == 4 && records[0].offset == 4, "got %zu", n);
        brp_records_free(records, n);
        brp_consumer_close(capped);

        brp_consumer_config_init(&config);
        config.fetch_min_bytes = 1000000;
        config.fetch_max_wait_ms = 600;
        brp_consumer_t *waiting, *eager;
        must(brp_consumer_new(address, &config, &waiting), "consumer");
        eager = new_consumer();
        int64_t started = now_ms();
        size_t waited_for = count_at(waiting, topic, 0, 0, 600);
        int64_t waited = now_ms() - started;
        started = now_ms();
        size_t eager_got = count_at(eager, topic, 0, 0, 600);
        int64_t quick = now_ms() - started;
        check("fetch.min.bytes holds a fetch open until fetch.max.wait.ms",
              waited >= 450 && quick < 400 && waited_for == 10 && eager_got == 10,
              "waited %lldms, eager %lldms", (long long)waited, (long long)quick);

        int64_t hwm = -1;
        must(brp_consumer_fetch(eager, topic, 0, 0, 500, &records, &n, &hwm), "fetch");
        brp_records_free(records, n);
        check("the high watermark is reported", hwm == 10, "%lld", (long long)hwm);

        brp_metadata_t *md;
        const char *topics[1] = {topic};
        must(brp_client_metadata(brp_consumer_client(eager), topics, 1, &md), "metadata");
        bool led = false;
        size_t partitions = 0;
        for (size_t t = 0; t < md->topic_count; t++) {
            if (strcmp(md->topics[t].name, topic) != 0) continue;
            partitions = md->topics[t].partition_count;
            led = partitions > 0;
            for (size_t p = 0; p < partitions; p++) {
                bool known = false;
                for (size_t b = 0; b < md->broker_count; b++)
                    if (md->brokers[b].node_id == md->topics[t].partitions[p].leader) known = true;
                if (!known) led = false;
            }
        }
        brp_metadata_free(md);
        check("metadata names a live leader for every partition", led, "%zu partitions",
              partitions);
        brp_consumer_close(waiting);
        brp_consumer_close(eager);
    }

    section("consumer group: several topics, auto-commit and max.poll.records");
    {
        char topic_a[128], topic_b[128], group_id[128];
        unique(topic_a, sizeof topic_a, "c-multi-a");
        unique(topic_b, sizeof topic_b, "c-multi-b");
        unique(group_id, sizeof group_id, "c-multi");
        brp_producer_t *producer = immediate_producer();
        for (int i = 0; i < 6; i++) {
            char value[16];
            snprintf(value, sizeof value, "a%d", i);
            send_str(producer, topic_a, BRP_PARTITION_ANY, value, NULL);
            snprintf(value, sizeof value, "b%d", i);
            send_str(producer, topic_b, BRP_PARTITION_ANY, value, NULL);
        }
        must(brp_producer_close(producer), "close");
        brp_group_config_t config;
        brp_group_config_init(&config);
        config.auto_commit_interval_ms = 200;
        config.max_poll_records = 5;
        brp_group_consumer_t *g;
        must(brp_group_consumer_new(address, group_id, &config, &g), "group");
        const char *topics[2] = {topic_a, topic_b};
        must(brp_group_consumer_subscribe(g, topics, 2), "subscribe");
        size_t seen = 0, largest = 0, from_a = 0, from_b = 0;
        int64_t deadline = now_ms() + 20000;
        while (seen < 12 && now_ms() < deadline) {
            brp_record_t *records = NULL;
            size_t n = 0;
            if (brp_group_consumer_poll(g, 500, &records, &n) != BRP_OK) continue;
            for (size_t i = 0; i < n; i++) {
                if (strcmp(records[i].topic, topic_a) == 0) from_a++;
                if (strcmp(records[i].topic, topic_b) == 0) from_b++;
            }
            if (n > largest) largest = n;
            seen += n;
            brp_records_free(records, n);
        }
        check("one member consumes every subscribed topic", seen == 12 && from_a == 6 && from_b == 6,
              "%zu records, %zu from a, %zu from b", seen, from_a, from_b);
        check("max.poll.records caps each poll", largest >= 1 && largest <= 5, "largest poll %zu",
              largest);
        /* Nothing calls commit: these polls are what auto-commit rides on. */
        int64_t until = now_ms() + 1000;
        while (now_ms() < until) poll_count(g, 100);
        brp_partition_offset_t *committed = NULL;
        size_t count = 0;
        int64_t total = 0;
        if (brp_group_consumer_committed(g, NULL, 0, &committed, &count) == BRP_OK) {
            for (size_t i = 0; i < count; i++)
                if (committed[i].offset > 0) total += committed[i].offset;
            brp_offsets_free(committed, count);
        }
        check("auto.commit.interval.ms commits delivered positions without a commit call",
              total == 12, "committed %lld", (long long)total);
        brp_group_consumer_close(g);
    }

    section("consumer group: heartbeats, session timeout and rejoin");
    {
        char topic[128], group_id[128];
        unique(topic, sizeof topic, "c-heartbeat");
        brp_producer_t *producer = immediate_producer();
        for (int i = 0; i < 4; i++) send_str(producer, topic, BRP_PARTITION_ANY, "h", NULL);
        must(brp_producer_close(producer), "close");

        brp_group_config_t config;
        group_config(&config);
        config.session_timeout_ms = 1500;
        config.heartbeat_interval_ms = 300;
        unique(group_id, sizeof group_id, "c-hb");
        brp_group_consumer_t *steady = new_group(group_id, &config, topic);
        size_t got = drain(steady, 4, 15000);
        char before[128], after[128];
        member_id_of(steady, before, sizeof before);
        sleep_ms(3500); /* over twice the session timeout, with no poll */
        brp_err_t err = brp_group_consumer_commit(steady);
        member_id_of(steady, after, sizeof after);
        check("heartbeats keep an idle member in its group past session.timeout.ms",
              got == 4 && err == BRP_OK && strcmp(before, after) == 0, "got=%zu commit=%s", got,
              brp_err_name(err));
        brp_group_consumer_close(steady);

        group_config(&config);
        config.session_timeout_ms = 1000;
        config.heartbeat_interval_ms = 20000; /* effectively never, within this check */
        unique(group_id, sizeof group_id, "c-evicted");
        brp_group_consumer_t *quiet = new_group(group_id, &config, topic);
        got = drain(quiet, 4, 15000);
        member_id_of(quiet, before, sizeof before);
        sleep_ms(2500);
        err = brp_group_consumer_commit(quiet);
        check("a member that stops heartbeating is evicted after session.timeout.ms",
              got == 4 && err == BRP_ERR_UNKNOWN_MEMBER_ID, "got=%zu commit=%s", got,
              brp_err_name(err));
        /* Only the join is checked: with no heartbeats this member is
         * evicted again one session timeout after it rejoins. */
        brp_record_t *records = NULL;
        size_t n = 0;
        err = brp_group_consumer_poll(quiet, 1000, &records, &n);
        if (!err) brp_records_free(records, n);
        member_id_of(quiet, after, sizeof after);
        check("an evicted member rejoins as a new member",
              err == BRP_OK && after[0] && strcmp(before, after) != 0, "%s -> %s err=%s", before,
              after, brp_err_name(err));
        brp_group_consumer_close(quiet);
    }

    section("consumer group: static membership, LeaveGroup and rebalances");
    {
        char topic[128], group_id[128], instance[128];
        unique(topic, sizeof topic, "c-static");
        brp_producer_t *producer = immediate_producer();
        int32_t *partitions;
        size_t pcount;
        must(brp_client_partitions(brp_producer_client(producer), topic, &partitions, &pcount),
             "partitions");
        brp_free(partitions);
        for (int i = 0; i < 4; i++) send_str(producer, topic, BRP_PARTITION_ANY, "st", NULL);
        must(brp_producer_close(producer), "close");

        brp_group_config_t config;
        group_config(&config);
        config.heartbeat_interval_ms = 300;
        unique(instance, sizeof instance, "c-instance");
        config.group_instance_id = instance;
        unique(group_id, sizeof group_id, "c-static-grp");
        brp_group_consumer_t *first = new_group(group_id, &config, topic);
        await_assignment(first, 15000);
        char first_member[128], returning_member[128];
        member_id_of(first, first_member, sizeof first_member);
        int32_t first_generation = brp_group_consumer_generation(first);
        brp_group_consumer_t *returning = new_group(group_id, &config, topic);
        await_assignment(returning, 15000);
        member_id_of(returning, returning_member, sizeof returning_member);
        int32_t returning_generation = brp_group_consumer_generation(returning);
        check("a returning group.instance.id reclaims its member id without a rebalance",
              first_member[0] && strcmp(first_member, returning_member) == 0 &&
                  first_generation == returning_generation,
              "%s/%d -> %s/%d", first_member, (int)first_generation, returning_member,
              (int)returning_generation);
        brp_group_consumer_close(returning);
        brp_group_consumer_close(first);

        /* LeaveGroup: with a 30 s session and a 10 s rebalance timeout, a
         * successor could only get the partitions quickly if the first
         * member told the coordinator it left. */
        group_config(&config);
        config.session_timeout_ms = 30000;
        config.rebalance_timeout_ms = 10000;
        unique(group_id, sizeof group_id, "c-leave-grp");
        brp_group_consumer_t *departing = new_group(group_id, &config, topic);
        await_assignment(departing, 15000);
        brp_group_consumer_close(departing);
        int64_t started = now_ms();
        brp_group_consumer_t *successor = new_group(group_id, &config, topic);
        await_assignment(successor, 15000);
        int64_t took = now_ms() - started;
        size_t held = assignment_count(successor);
        check("close sends LeaveGroup, so a successor is not kept waiting",
              held == pcount && took < 6000, "%zu partitions after %lldms", held, (long long)took);
        brp_group_consumer_close(successor);

        /* Two members: the second's join makes the coordinator fence the
         * first's generation; its heartbeat learns that, it rejoins, and the
         * partitions split. */
        group_config(&config);
        config.heartbeat_interval_ms = 200;
        unique(group_id, sizeof group_id, "c-share-grp");
        brp_group_consumer_t *one = new_group(group_id, &config, topic);
        await_assignment(one, 15000);
        int32_t before = brp_group_consumer_generation(one);
        second_member_t second;
        memset(&second, 0, sizeof second);
        second.group = new_group(group_id, &config, topic);
        atomic_init(&second.stop, 0);
        pthread_mutex_init(&second.mu, NULL);
        pthread_t other;
        pthread_create(&other, NULL, second_member_main, &second);
        bool split = false;
        size_t na = 0, nb = 0;
        int64_t deadline = now_ms() + 20000;
        while (!split && now_ms() < deadline) {
            poll_count(one, 200);
            split = split_between(one, &second, pcount, &na, &nb);
        }
        atomic_store(&second.stop, 1);
        pthread_join(other, NULL);
        pthread_mutex_destroy(&second.mu);
        check("a second member rebalances the group and the partitions split between them", split,
              "%zu / %zu", na, nb);
        int32_t after = brp_group_consumer_generation(one);
        check("the generation advances when the group rebalances", after > before, "%d -> %d",
              (int)before, (int)after);
        brp_group_consumer_close(second.group);
        brp_group_consumer_close(one);
    }
}

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1] : "127.0.0.1";
    const char *port = argc > 2 ? argv[2] : "9092";
    snprintf(address, sizeof address, "%s:%s", host, port);

    section("connection and metadata");
    {
        brp_consumer_t *consumer = new_consumer();
        brp_api_version_range_t *versions = NULL;
        size_t count = 0;
        char *broker_version = NULL;
        brp_err_t err = brp_client_api_versions(brp_consumer_client(consumer), &versions, &count,
                                                &broker_version);
        check("ApiVersions answers", err == BRP_OK && count > 0, "%s", brp_last_error());
        check("broker reports a version", broker_version && *broker_version, "%s",
              broker_version ? broker_version : "(null)");
        brp_metadata_t *metadata;
        must(brp_client_metadata(brp_consumer_client(consumer), NULL, 0, &metadata), "metadata");
        check("metadata lists brokers", metadata->broker_count >= 1, "%zu brokers",
              metadata->broker_count);
        brp_metadata_free(metadata);
        brp_free(versions);
        brp_free(broker_version);
        brp_consumer_close(consumer);
    }

    section("produce and consume round trip");
    char topic[128];
    unique(topic, sizeof topic, "c-roundtrip");
    {
        brp_producer_t *producer = immediate_producer();
        for (int i = 0; i < 50; i++) {
            char payload[32];
            snprintf(payload, sizeof payload, "record-%d", i);
            send_str(producer, topic, 0, payload, NULL);
        }
        must(brp_producer_flush(producer), "flush");
        must(brp_producer_close(producer), "close");
    }
    {
        brp_consumer_t *consumer = new_consumer();
        brp_record_t *got;
        size_t n;
        must(brp_consumer_fetch(consumer, topic, 0, 0, 500, &got, &n, NULL), "fetch");
        check("every record comes back", n == 50, "got %zu", n);
        bool identical = n == 50;
        for (size_t i = 0; identical && i < n; i++) {
            char payload[32];
            snprintf(payload, sizeof payload, "record-%zu", i);
            if (!value_is(&got[i], payload) || got[i].offset != (int64_t)i) identical = false;
        }
        check("values byte-identical and offsets contiguous", identical, NULL);
        brp_records_free(got, n);
        brp_consumer_close(consumer);
    }

    section("compression codecs");
    /* Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
     * brp_register_codec. */
    {
        const char *codecs[] = {"none", "gzip"};
        const char *line = "the same line over and over. ";
        size_t line_len = strlen(line);
        size_t body_len = line_len * 40;
        char *body = malloc(body_len + 1);
        for (int i = 0; i < 40; i++) memcpy(body + (size_t)i * line_len, line, line_len);
        for (size_t c = 0; c < 2; c++) {
            char codec_topic[128], prefix[32];
            snprintf(prefix, sizeof prefix, "c-%s", codecs[c]);
            unique(codec_topic, sizeof codec_topic, prefix);
            brp_producer_config_t config;
            brp_producer_config_init(&config);
            config.linger_ms = 0;
            config.compression_type = codecs[c];
            brp_producer_t *producer = producer_with(&config);
            for (int i = 0; i < 20; i++) {
                body[body_len] = (char)('0' + i % 10);
                send_to(producer, codec_topic, 0, body, body_len + 1, NULL);
            }
            must(brp_producer_flush(producer), "flush");
            must(brp_producer_close(producer), "close");

            brp_consumer_t *consumer = new_consumer();
            brp_record_t *got;
            size_t n;
            must(brp_consumer_fetch(consumer, codec_topic, 0, 0, 500, &got, &n, NULL), "fetch");
            char name[64];
            snprintf(name, sizeof name, "%s: round trips", codecs[c]);
            check(name,
                  n == 20 && got[0].value_len == body_len + 1 &&
                      memcmp(got[0].value, body, body_len) == 0,
                  "got %zu records", n);
            brp_records_free(got, n);
            brp_consumer_close(consumer);
        }
        free(body);
    }

    section("keys, partitioning and ordering");
    {
        char key_topic[128];
        unique(key_topic, sizeof key_topic, "c-keys");
        brp_producer_t *producer = immediate_producer();
        int32_t *partitions;
        size_t pcount;
        must(brp_client_partitions(brp_producer_client(producer), key_topic, &partitions, &pcount),
             "partitions");
        for (int i = 0; i < 30; i++) {
            char value[32];
            snprintf(value, sizeof value, "v%d", i);
            send_str(producer, key_topic, BRP_PARTITION_ANY, value, "user-7");
        }
        must(brp_producer_flush(producer), "flush");
        must(brp_producer_close(producer), "close");

        int32_t target = brp_partition_for_key("user-7", 6, partitions, pcount);
        brp_consumer_t *consumer = new_consumer();
        brp_record_t *on_target;
        size_t n;
        must(brp_consumer_fetch(consumer, key_topic, target, 0, 500, &on_target, &n, NULL),
             "fetch");
        check("a key pins every record to one partition", n == 30,
              "partition %d holds %zu of 30", (int)target, n);
        bool ordered = n == 30;
        for (size_t i = 0; ordered && i < n; i++) {
            char value[32];
            snprintf(value, sizeof value, "v%zu", i);
            if (!value_is(&on_target[i], value)) ordered = false;
        }
        check("per-key order is preserved", ordered, NULL);
        brp_records_free(on_target, n);

        size_t strays = 0;
        for (size_t i = 0; i < pcount; i++) {
            if (partitions[i] == target) continue;
            brp_record_t *other;
            size_t on;
            must(brp_consumer_fetch(consumer, key_topic, partitions[i], 0, 200, &other, &on, NULL),
                 "fetch");
            strays += on;
            brp_records_free(other, on);
        }
        check("no keyed record landed elsewhere", strays == 0, "%zu strays", strays);
        brp_free(partitions);
        brp_consumer_close(consumer);
    }

    section("murmur2 agrees with the broker's partitioner");
    check("murmur2(\"\") is stable", brp_murmur2(NULL, 0) == 275646681u, "%u",
          brp_murmur2(NULL, 0));
    check("murmur2 is deterministic", brp_murmur2("user-7", 6) == brp_murmur2("user-7", 6), NULL);
    check("different keys hash differently", brp_murmur2("user-7", 6) != brp_murmur2("user-8", 6),
          NULL);

    section("record headers and timestamps");
    {
        char header_topic[128];
        unique(header_topic, sizeof header_topic, "c-headers");
        int64_t before = now_ms() - 1000;
        brp_producer_t *producer = immediate_producer();
        brp_header_t headers[] = {
            {"trace-id", (const uint8_t *)"abc-123", 7},
            {"content-type", (const uint8_t *)"application/json", 16},
            {"tombstone-reason", NULL, 0},
        };
        brp_message_t m;
        brp_message_init(&m);
        m.topic = header_topic;
        m.partition = 0;
        m.value = "annotated";
        m.value_len = 9;
        m.headers = headers;
        m.header_count = 3;
        must(brp_producer_send(producer, &m), "send");
        send_str(producer, header_topic, 0, "plain", NULL);
        must(brp_producer_flush(producer), "flush");
        must(brp_producer_close(producer), "close");
        int64_t after = now_ms() + 1000;

        brp_consumer_t *consumer = new_consumer();
        brp_record_t *got;
        size_t n;
        must(brp_consumer_fetch(consumer, header_topic, 0, 0, 500, &got, &n, NULL), "fetch");
        check("both records arrive", n == 2, "got %zu", n);
        if (n == 2) {
            brp_record_t *annotated = &got[0], *plain = &got[1];
            check("headers survive the round trip", annotated->header_count == 3, "%zu headers",
                  annotated->header_count);
            size_t len;
            const uint8_t *trace = brp_record_header(annotated, "trace-id", &len);
            check("header values are exact", trace && len == 7 && memcmp(trace, "abc-123", 7) == 0,
                  NULL);
            check("a null header value stays null",
                  annotated->header_count == 3 && annotated->headers[2].value == NULL, NULL);
            check("a record with no headers gains none from its batch", plain->header_count == 0,
                  "%zu headers", plain->header_count);
            bool in_window = true;
            for (size_t i = 0; i < n; i++)
                if (got[i].timestamp < before || got[i].timestamp > after) in_window = false;
            check("timestamps are real wall-clock values", in_window,
                  "%lld,%lld outside %lld..%lld", (long long)got[0].timestamp,
                  (long long)got[1].timestamp, (long long)before, (long long)after);
        }
        brp_records_free(got, n);
        brp_consumer_close(consumer);
    }

    section("tombstones");
    {
        char tomb_topic[128];
        unique(tomb_topic, sizeof tomb_topic, "c-tombstones");
        brp_producer_t *producer = immediate_producer();
        send_to(producer, tomb_topic, 0, "set", 3, "k1");
        send_to(producer, tomb_topic, 0, "", 0, "k2");
        /* A NULL value is a deletion, and must stay distinguishable from
         * the empty value above all the way through the round trip. */
        send_to(producer, tomb_topic, 0, NULL, 0, "k3");
        must(brp_producer_flush(producer), "flush");
        must(brp_producer_close(producer), "close");

        brp_consumer_t *consumer = new_consumer();
        brp_record_t *got;
        size_t n;
        must(brp_consumer_fetch(consumer, tomb_topic, 0, 0, 500, &got, &n, NULL), "fetch");
        check("all three records arrive", n == 3, "got %zu", n);
        if (n == 3) {
            check("an ordinary value round-trips", value_is(&got[0], "set"), NULL);
            check("an empty value is empty, not null", got[1].value != NULL && got[1].value_len == 0,
                  "value=%p len=%zu", (void *)got[1].value, got[1].value_len);
            check("a tombstone arrives as a null value", got[2].value == NULL, "len=%zu",
                  got[2].value_len);
        }
        brp_records_free(got, n);
        brp_consumer_close(consumer);
    }

    section("offsets");
    {
        brp_consumer_t *consumer = new_consumer();
        int64_t earliest = -99, latest = -99;
        must(brp_consumer_list_offsets(consumer, topic, 0, BRP_OFFSET_EARLIEST, &earliest),
             "list earliest");
        must(brp_consumer_list_offsets(consumer, topic, 0, BRP_OFFSET_LATEST, &latest),
             "list latest");
        check("earliest is 0 on a fresh topic", earliest == 0, "%lld", (long long)earliest);
        check("latest equals the record count", latest == 50, "%lld", (long long)latest);
        brp_consumer_close(consumer);
    }

    section("acks");
    {
        const int32_t all_acks[] = {0, 1, -1};
        for (size_t a = 0; a < 3; a++) {
            char acks_topic[128], prefix[32];
            snprintf(prefix, sizeof prefix, "c-acks%d", (int)all_acks[a]);
            unique(acks_topic, sizeof acks_topic, prefix);
            brp_producer_config_t config;
            brp_producer_config_init(&config);
            config.linger_ms = 0;
            config.acks = all_acks[a];
            brp_producer_t *producer = producer_with(&config);
            send_str(producer, acks_topic, 0, "durable", NULL);
            must(brp_producer_flush(producer), "flush");
            must(brp_producer_close(producer), "close");
            sleep_ms(400);

            brp_consumer_t *consumer = new_consumer();
            brp_record_t *got;
            size_t n;
            must(brp_consumer_fetch(consumer, acks_topic, 0, 0, 500, &got, &n, NULL), "fetch");
            char name[64];
            snprintf(name, sizeof name, "acks=%d stores the record", (int)all_acks[a]);
            check(name, n == 1, "got %zu", n);
            brp_records_free(got, n);
            brp_consumer_close(consumer);
        }
    }

    section("consumer group: assignment, commit, resume");
    {
        char group_topic[128], group_id[128];
        unique(group_topic, sizeof group_topic, "c-group");
        unique(group_id, sizeof group_id, "c-billing");
        brp_producer_t *producer = immediate_producer();
        for (int i = 0; i < 40; i++) {
            char value[32];
            snprintf(value, sizeof value, "g%d", i);
            send_str(producer, group_topic, BRP_PARTITION_ANY, value, NULL);
        }
        must(brp_producer_flush(producer), "flush");
        must(brp_producer_close(producer), "close");

        brp_group_config_t config;
        group_config(&config);
        brp_group_consumer_t *consumer = new_group(group_id, &config, group_topic);
        seen_t seen = {0};
        int64_t deadline = now_ms() + 30000;
        while (seen.n < 40 && now_ms() < deadline) {
            brp_record_t *records;
            size_t n;
            must(brp_group_consumer_poll(consumer, 500, &records, &n), "poll");
            seen_add(&seen, records, n);
        }
        check("the group consumes every record", seen.n == 40, "got %zu", seen.n);
        bool distinct = true;
        for (size_t i = 0; i < seen.n && distinct; i++)
            for (size_t j = i + 1; j < seen.n; j++)
                if (seen.records[i].partition == seen.records[j].partition &&
                    seen.records[i].offset == seen.records[j].offset)
                    distinct = false;
        check("no record is delivered twice", distinct, NULL);
        seen_free(&seen);

        must(brp_group_consumer_commit(consumer), "commit");
        brp_partition_offset_t *committed;
        size_t cn;
        must(brp_group_consumer_committed(consumer, NULL, 0, &committed, &cn), "committed");
        int64_t total = 0;
        for (size_t i = 0; i < cn; i++) total += committed[i].offset;
        check("commit records a position", total == 40, "%lld", (long long)total);
        brp_offsets_free(committed, cn);
        must(brp_group_consumer_close(consumer), "group close");

        /* A second consumer in the same group must resume, not replay. */
        brp_group_consumer_t *rejoined = new_group(group_id, &config, group_topic);
        seen_t replayed = {0};
        int64_t until = now_ms() + 5000;
        while (now_ms() < until) {
            brp_record_t *records;
            size_t n;
            if (brp_group_consumer_poll(rejoined, 300, &records, &n) == BRP_OK)
                seen_add(&replayed, records, n);
        }
        check("a rejoining group resumes from its commit", replayed.n == 0,
              "replayed %zu records it had already committed", replayed.n);
        seen_free(&replayed);
        must(brp_group_consumer_close(rejoined), "group close");
    }

    section("auto.offset.reset");
    {
        char reset_topic[128], latest_group[128], none_group[128];
        unique(reset_topic, sizeof reset_topic, "c-reset");
        unique(latest_group, sizeof latest_group, "c-latest");
        unique(none_group, sizeof none_group, "c-none");
        brp_producer_t *producer = immediate_producer();
        for (int i = 0; i < 10; i++) {
            char value[32];
            snprintf(value, sizeof value, "r%d", i);
            send_str(producer, reset_topic, BRP_PARTITION_ANY, value, NULL);
        }
        must(brp_producer_flush(producer), "flush");
        must(brp_producer_close(producer), "close");

        brp_group_config_t latest_config;
        group_config(&latest_config);
        latest_config.auto_offset_reset = "latest";
        brp_group_consumer_t *consumer = new_group(latest_group, &latest_config, reset_topic);
        seen_t skipped = {0};
        int64_t until = now_ms() + 4000;
        while (now_ms() < until) {
            brp_record_t *records;
            size_t n;
            if (brp_group_consumer_poll(consumer, 300, &records, &n) == BRP_OK)
                seen_add(&skipped, records, n);
        }
        check("latest skips records produced before the group existed", skipped.n == 0,
              "saw %zu", skipped.n);
        seen_free(&skipped);
        must(brp_group_consumer_close(consumer), "group close");

        brp_group_config_t none_config;
        group_config(&none_config);
        none_config.auto_offset_reset = "none";
        brp_group_consumer_t *strict = new_group(none_group, &none_config, reset_topic);
        bool raised = false;
        until = now_ms() + 5000;
        while (now_ms() < until && !raised) {
            brp_record_t *records;
            size_t n;
            brp_err_t err = brp_group_consumer_poll(strict, 300, &records, &n);
            if (err == BRP_OK)
                brp_records_free(records, n);
            else
                raised = err == BRP_ERR_NO_OFFSET &&
                         strstr(brp_last_error(), "no committed offset") != NULL;
        }
        check("none refuses to guess a position", raised, "%s", brp_last_error());
        brp_group_consumer_close(strict);
    }

    section("assignors");
    {
        const char *assignors[] = {"range", "roundrobin", "sticky"};
        for (size_t a = 0; a < 3; a++) {
            char assignor_topic[128], assignor_group[128], prefix[48];
            snprintf(prefix, sizeof prefix, "c-%s", assignors[a]);
            unique(assignor_topic, sizeof assignor_topic, prefix);
            snprintf(prefix, sizeof prefix, "c-grp-%s", assignors[a]);
            unique(assignor_group, sizeof assignor_group, prefix);
            brp_producer_t *producer = immediate_producer();
            for (int i = 0; i < 20; i++) {
                char value[32];
                snprintf(value, sizeof value, "a%d", i);
                send_str(producer, assignor_topic, BRP_PARTITION_ANY, value, NULL);
            }
            must(brp_producer_flush(producer), "flush");
            must(brp_producer_close(producer), "close");

            brp_group_config_t config;
            group_config(&config);
            config.assignor = assignors[a];
            brp_group_consumer_t *consumer = new_group(assignor_group, &config, assignor_topic);
            seen_t collected = {0};
            int64_t deadline = now_ms() + 20000;
            while (collected.n < 20 && now_ms() < deadline) {
                brp_record_t *records;
                size_t n;
                if (brp_group_consumer_poll(consumer, 500, &records, &n) == BRP_OK)
                    seen_add(&collected, records, n);
            }
            char name[64];
            snprintf(name, sizeof name, "%s: consumes every record", assignors[a]);
            check(name, collected.n == 20, "got %zu", collected.n);
            seen_free(&collected);
            must(brp_group_consumer_close(consumer), "group close");
        }
    }

    section("bounded client buffer");
    {
        char buffer_topic[128];
        unique(buffer_topic, sizeof buffer_topic, "c-buffer");
        brp_producer_config_t config;
        brp_producer_config_init(&config);
        config.linger_ms = 10000; /* never flush on time during this check */
        config.buffer_memory = 2048;
        config.max_block_ms = 300;
        brp_producer_t *producer = producer_with(&config);
        char payload[256];
        memset(payload, 'x', sizeof payload);
        bool blocked = false;
        for (int i = 0; i < 500 && !blocked; i++) {
            brp_message_t m;
            brp_message_init(&m);
            m.topic = buffer_topic;
            m.partition = 0;
            m.value = payload;
            m.value_len = sizeof payload;
            brp_err_t err = brp_producer_send(producer, &m);
            if (err != BRP_OK)
                blocked = err == BRP_ERR_BUFFER_FULL &&
                          strstr(brp_last_error(), "buffer full") != NULL;
        }
        check("a full buffer blocks and then reports", blocked, NULL);
        brp_producer_close(producer);
    }

    section("wire edge cases");
    {
        char edge_topic[128];
        unique(edge_topic, sizeof edge_topic, "c-edge");
        brp_producer_t *producer = immediate_producer();
        size_t large_len = 1u << 20;
        uint8_t *large = malloc(large_len);
        for (size_t i = 0; i < large_len; i++) large[i] = (uint8_t)(i * 7);
        const char *unicode_key = "ключ-✓-🔑";
        const char *unicode_value = "значение — 数据 — 🚀";
        const char *unicode_header = "ünïcødé-🏷";

        brp_message_t m;
        brp_message_init(&m);
        m.topic = edge_topic;
        m.partition = 0;
        m.value = large;
        m.value_len = large_len;
        must(brp_producer_send(producer, &m), "send large");

        brp_header_t h1[] = {{unicode_header, (const uint8_t *)"✓", strlen("✓")}};
        brp_message_init(&m);
        m.topic = edge_topic;
        m.partition = 0;
        m.key = unicode_key;
        m.key_len = strlen(unicode_key);
        m.value = unicode_value;
        m.value_len = strlen(unicode_value);
        m.headers = h1;
        m.header_count = 1;
        must(brp_producer_send(producer, &m), "send unicode");

        /* An empty key and an empty header value are values, not nulls. */
        brp_header_t h2[] = {{"empty", (const uint8_t *)"", 0}, {"null", NULL, 0}};
        brp_message_init(&m);
        m.topic = edge_topic;
        m.partition = 0;
        m.key = "";
        m.key_len = 0;
        m.value = "empty-key";
        m.value_len = 9;
        m.headers = h2;
        m.header_count = 2;
        must(brp_producer_send(producer, &m), "send empty key");
        send_str(producer, edge_topic, 0, "null-key", NULL);
        must(brp_producer_close(producer), "close");

        brp_consumer_t *consumer = new_consumer();
        seen_t got = {0};
        fetch_all(consumer, edge_topic, 4, &got);
        check("edge records all arrive", got.n == 4, "got %zu", got.n);
        if (got.n == 4) {
            brp_record_t *r = got.records;
            check("a 1 MiB value round-trips byte-identical",
                  r[0].value_len == large_len && memcmp(r[0].value, large, large_len) == 0,
                  "%zu bytes", r[0].value_len);
            check("unicode key, value and header key round-trip",
                  r[1].key_len == strlen(unicode_key) &&
                      memcmp(r[1].key, unicode_key, r[1].key_len) == 0 &&
                      value_is(&r[1], unicode_value) && r[1].header_count == 1 &&
                      strcmp(r[1].headers[0].key, unicode_header) == 0,
                  NULL);
            check("an empty key stays empty, not null", r[2].key != NULL && r[2].key_len == 0,
                  "key=%p len=%zu", (void *)r[2].key, r[2].key_len);
            check("an empty header value stays empty, not null",
                  r[2].header_count == 2 && r[2].headers[0].value != NULL &&
                      r[2].headers[0].value_len == 0 && r[2].headers[1].value == NULL,
                  "%zu headers", r[2].header_count);
            check("a null key stays null", r[3].key == NULL, "len=%zu", r[3].key_len);
        }
        seen_free(&got);
        free(large);
        brp_consumer_close(consumer);
    }

    section("ordering under linger flushes");
    {
        char order_topic[128];
        unique(order_topic, sizeof order_topic, "c-order");
        brp_producer_config_t config;
        brp_producer_config_init(&config);
        config.linger_ms = 1;
        config.batch_size = 256;
        brp_producer_t *producer = producer_with(&config);
        const size_t total = 5000;
        for (size_t i = 0; i < total; i++) {
            char value[32];
            snprintf(value, sizeof value, "%zu", i);
            send_str(producer, order_topic, 0, value, NULL);
        }
        must(brp_producer_close(producer), "close");
        brp_consumer_t *consumer = new_consumer();
        seen_t got = {0};
        fetch_all(consumer, order_topic, total, &got);
        size_t inversions = 0;
        long previous = -1;
        for (size_t i = 0; i < got.n; i++) {
            char value[32] = "";
            size_t len = got.records[i].value_len < 31 ? got.records[i].value_len : 31;
            if (got.records[i].value) memcpy(value, got.records[i].value, len);
            long v = atol(value);
            if (v < previous) inversions++;
            previous = v;
        }
        check("every record of a partition arrives", got.n == total, "got %zu", got.n);
        check("a partition's records keep send order", inversions == 0, "%zu inversions",
              inversions);
        seen_free(&got);
        brp_consumer_close(consumer);
    }

    section("background flush failures are reported");
    {
        char bg_topic[128];
        unique(bg_topic, sizeof bg_topic, "c-bgfail");
        brp_producer_config_t config;
        brp_producer_config_init(&config);
        config.linger_ms = 20;
        brp_producer_t *producer = producer_with(&config);
        /* Partition 999 does not exist, so the linger thread's flush fails. */
        brp_message_t m;
        brp_message_init(&m);
        m.topic = bg_topic;
        m.partition = 999;
        m.value = "lost";
        m.value_len = 4;
        brp_err_t send_err = brp_producer_send(producer, &m);
        sleep_ms(300);
        brp_err_t flush_err = brp_producer_flush(producer);
        check("a failed linger flush surfaces on the next flush",
              send_err == BRP_OK && flush_err != BRP_OK, "send=%s flush=%s",
              brp_err_name(send_err), brp_err_name(flush_err));
        int64_t started = now_ms();
        brp_producer_close(producer);
        check("close returns after a failed flush", now_ms() - started < 5000, "took %lld ms",
              (long long)(now_ms() - started));
    }

    section("connection failures");
    {
        /* A broker that accepts and never answers must cost an error, not a
         * thread blocked forever. */
        silent_t silent = {0};
        silent.listen_fd = listen_local(&silent.port);
        if (silent.listen_fd >= 0) {
            pthread_create(&silent.thread, NULL, silent_main, &silent);
            char silent_address[64];
            snprintf(silent_address, sizeof silent_address, "127.0.0.1:%d", silent.port);
            brp_client_config_t cc;
            brp_client_config_init(&cc);
            cc.socket_connection_setup_timeout_ms = 1000;
            cc.request_timeout_ms = 300;
            brp_client_t *client;
            must(brp_client_new(silent_address, &cc, &client), "dial silent broker");
            int64_t started = now_ms();
            brp_api_version_range_t *versions = NULL;
            size_t count;
            brp_err_t err = brp_client_api_versions(client, &versions, &count, NULL);
            if (err == BRP_OK) brp_free(versions);
            check("a request to an unresponsive broker times out",
                  err != BRP_OK && now_ms() - started < 3000, "%s", brp_last_error());
            check("a timed-out connection is not reused", !brp_client_connected(client), NULL);
            brp_client_destroy(client);
            shutdown(silent.listen_fd, SHUT_RDWR);
            pthread_join(silent.thread, NULL);
            close(silent.listen_fd);
            for (int i = 0; i < silent.accepted_n; i++) close(silent.accepted[i]);
        }

        /* A connection the broker drops is redialled, not kept forever. */
        proxy_t *proxy = proxy_new(host, port);
        char drop_topic[128];
        unique(drop_topic, sizeof drop_topic, "c-drop");
        brp_producer_config_t config;
        brp_producer_config_init(&config);
        config.linger_ms = 0;
        brp_producer_t *producer;
        must(brp_producer_new(proxy->address, &config, &producer), "producer via proxy");
        send_str(producer, drop_topic, 0, "before", NULL);
        proxy_drop_all(proxy);
        brp_err_t recovered = BRP_ERR_STATE;
        for (int attempt = 0; attempt < 3 && recovered != BRP_OK; attempt++) {
            brp_message_t m;
            brp_message_init(&m);
            m.topic = drop_topic;
            m.partition = 0;
            m.value = "after";
            m.value_len = 5;
            recovered = brp_producer_send(producer, &m);
        }
        check("a producer recovers after its connection drops", recovered == BRP_OK, "%s",
              brp_last_error());
        brp_producer_close(producer);

        brp_consumer_t *consumer;
        must(brp_consumer_new(proxy->address, NULL, &consumer), "consumer via proxy");
        brp_record_t *records;
        size_t n;
        must(brp_consumer_fetch(consumer, drop_topic, 0, 0, 100, &records, &n, NULL), "fetch");
        brp_records_free(records, n);
        proxy_drop_all(proxy);
        brp_err_t fetch_err = BRP_ERR_STATE;
        n = 0;
        for (int attempt = 0; attempt < 3 && fetch_err != BRP_OK; attempt++) {
            fetch_err = brp_consumer_fetch(consumer, drop_topic, 0, 0, 100, &records, &n, NULL);
            if (fetch_err == BRP_OK) brp_records_free(records, n);
        }
        check("a consumer recovers after its connection drops", fetch_err == BRP_OK && n >= 1,
              "%s", brp_last_error());
        brp_consumer_close(consumer);
        proxy_close(proxy);
    }

    section("consumer group: max.poll.interval and rejoin");
    {
        char slow_topic[128], slow_group[128];
        unique(slow_topic, sizeof slow_topic, "c-slow");
        unique(slow_group, sizeof slow_group, "c-slow-grp");
        brp_producer_t *producer = immediate_producer();
        for (int i = 0; i < 10; i++) {
            char value[32];
            snprintf(value, sizeof value, "s%d", i);
            send_str(producer, slow_topic, BRP_PARTITION_ANY, value, NULL);
        }
        brp_group_config_t config;
        group_config(&config);
        config.max_poll_interval_ms = 1500;
        brp_group_consumer_t *consumer = new_group(slow_group, &config, slow_topic);
        seen_t first = {0}, second = {0};
        int64_t deadline = now_ms() + 15000;
        while (first.n < 10 && now_ms() < deadline) {
            brp_record_t *records;
            size_t n;
            if (brp_group_consumer_poll(consumer, 300, &records, &n) != BRP_OK) break;
            seen_add(&first, records, n);
        }
        must(brp_group_consumer_commit(consumer), "commit");
        /* Stall past max.poll.interval.ms: the member leaves the group. */
        sleep_ms(2500);
        for (int i = 10; i < 20; i++) {
            char value[32];
            snprintf(value, sizeof value, "s%d", i);
            send_str(producer, slow_topic, BRP_PARTITION_ANY, value, NULL);
        }
        must(brp_producer_close(producer), "close");
        brp_err_t poll_err = BRP_OK;
        char poll_msg[512] = "";
        deadline = now_ms() + 15000;
        while (second.n < 10 && now_ms() < deadline) {
            brp_record_t *records;
            size_t n;
            poll_err = brp_group_consumer_poll(consumer, 300, &records, &n);
            if (poll_err != BRP_OK) {
                snprintf(poll_msg, sizeof poll_msg, "%s", brp_last_error());
                break;
            }
            seen_add(&second, records, n);
        }
        check("a member that stalled rejoins on its next poll",
              first.n == 10 && second.n == 10 && poll_err == BRP_OK, "first=%zu second=%zu err=%s",
              first.n, second.n, poll_msg);
        seen_free(&first);
        seen_free(&second);
        must(brp_group_consumer_close(consumer), "group close");
    }

    section("consumer group: time inside poll does not count against max.poll.interval");
    {
        char join_topic[128], join_group[128];
        unique(join_topic, sizeof join_topic, "c-inpoll");
        unique(join_group, sizeof join_group, "c-inpoll-grp");
        brp_producer_t *producer = immediate_producer();
        int32_t *partitions;
        size_t pcount;
        must(brp_client_partitions(brp_producer_client(producer), join_topic, &partitions, &pcount),
             "partitions");
        brp_free(partitions);
        brp_group_config_t config;
        group_config(&config);
        /* Far shorter than the first poll below, which spends ~1s joining
         * (the broker's initial rebalance delay) and then waits for data. */
        config.max_poll_interval_ms = 600;
        brp_group_consumer_t *consumer = new_group(join_group, &config, join_topic);
        delayed_send_t delayed = {producer, join_topic};
        pthread_t sender;
        pthread_create(&sender, NULL, delayed_send_main, &delayed);
        /* One long poll: it joins, then waits for the records above. */
        brp_record_t *records = NULL;
        size_t n = 0;
        brp_err_t poll_err = brp_group_consumer_poll(consumer, 4000, &records, &n);
        char poll_msg[512] = "";
        if (poll_err) snprintf(poll_msg, sizeof poll_msg, "%s", brp_last_error());
        /* Committed straight away, before another poll could quietly
         * rejoin: this fails if the member left the group mid-poll. */
        brp_err_t commit_err = brp_group_consumer_commit(consumer);
        check("a member is still in its group after a long poll",
              poll_err == BRP_OK && n > 0 && commit_err == BRP_OK, "got=%zu poll=%s commit=%s (%s)",
              n, poll_msg, brp_err_name(commit_err), commit_err ? brp_last_error() : "");
        if (poll_err == BRP_OK) brp_records_free(records, n);
        pthread_join(sender, NULL);
        must(brp_group_consumer_close(consumer), "group close");
        must(brp_producer_close(producer), "close");
    }

    run_checklist(host, port);

    printf("\n%d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
