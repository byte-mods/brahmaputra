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

    printf("\n%d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
