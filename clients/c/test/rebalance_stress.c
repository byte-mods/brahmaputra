/* Repeats the two-member rebalance from the manual suite many times.
 *
 * A group of one settles; a second member joins; the coordinator bumps the
 * generation, the first member's heartbeat learns of it and rejoins, and the
 * four partitions split two and two. The manual suite checks this once. It
 * failed intermittently on CI runners and never locally, so this runs it N
 * times with fresh groups and prints each outcome. With BRP_DEBUG=1 the
 * driver traces every join, sync and heartbeat result to stderr.
 *
 *   make stress HOST=127.0.0.1 PORT=9092 ITERATIONS=100
 */
#define _POSIX_C_SOURCE 200809L

#include "brahmaputra.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static char address[128];

static int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void must(brp_err_t err, const char *what) {
    if (err != BRP_OK) {
        fprintf(stderr, "FATAL %s: %s\n", what, brp_last_error());
        exit(2);
    }
}

static size_t poll_count(brp_group_consumer_t *g, int timeout_ms) {
    brp_record_t *records = NULL;
    size_t n = 0;
    brp_err_t err = brp_group_consumer_poll(g, timeout_ms, &records, &n);
    if (err != BRP_OK) {
        fprintf(stderr, "[%lld poll error %d: %s]\n", (long long)now_ms(), (int)err,
                brp_last_error());
        return 0;
    }
    brp_records_free(records, n);
    return n;
}

static size_t assignment_count(brp_group_consumer_t *g) {
    brp_partition_offset_t *held = NULL;
    size_t n = 0;
    if (brp_group_consumer_assignment(g, &held, &n) != BRP_OK) return 0;
    brp_offsets_free(held, n);
    return n;
}

static brp_group_consumer_t *new_group(const char *group_id, const brp_group_config_t *config,
                                       const char *topic) {
    brp_group_consumer_t *g;
    must(brp_group_consumer_new(address, group_id, config, &g), "group consumer");
    must(brp_group_consumer_subscribe(g, &topic, 1), "subscribe");
    return g;
}

typedef struct {
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

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s HOST PORT [ITERATIONS] [TOPIC]\n", argv[0]);
        return 64;
    }
    snprintf(address, sizeof address, "%s:%s", argv[1], argv[2]);
    int iterations = argc > 3 ? atoi(argv[3]) : 20;
    const char *topic = argc > 4 ? argv[4] : "c-rebalance-stress";

    /* The topic must exist with its partitions before members subscribe. */
    brp_producer_config_t pconfig;
    brp_producer_config_init(&pconfig);
    pconfig.linger_ms = 0;
    brp_producer_t *producer;
    must(brp_producer_new(address, &pconfig, &producer), "producer");
    int32_t *partitions;
    size_t pcount;
    must(brp_client_partitions(brp_producer_client(producer), topic, &partitions, &pcount),
         "partitions");
    brp_free(partitions);
    must(brp_producer_close(producer), "close producer");

    int failures = 0;
    for (int it = 0; it < iterations; it++) {
        char group_id[96];
        snprintf(group_id, sizeof group_id, "c-stress-%lld-%d", (long long)now_ms(), it);
        brp_group_config_t config;
        brp_group_config_init(&config);
        config.auto_commit_interval_ms = 0;
        config.heartbeat_interval_ms = 200;

        brp_group_consumer_t *one = new_group(group_id, &config, topic);
        int64_t deadline = now_ms() + 15000;
        while (assignment_count(one) == 0 && now_ms() < deadline) poll_count(one, 200);
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
        int64_t started = now_ms();
        deadline = started + 20000;
        while (!split && now_ms() < deadline) {
            poll_count(one, 200);
            split = split_between(one, &second, pcount, &na, &nb);
        }
        atomic_store(&second.stop, 1);
        pthread_join(other, NULL);
        pthread_mutex_destroy(&second.mu);

        int32_t after = brp_group_consumer_generation(one);
        bool ok = split && after > before;
        if (!ok) failures++;
        printf("iteration %d %s: split %zu/%zu, first generation %d -> %d, second %d, %lld ms\n",
               it, ok ? "ok" : "FAIL", na, nb, (int)before, (int)after,
               (int)brp_group_consumer_generation(second.group),
               (long long)(now_ms() - started));
        fflush(stdout);
        brp_group_consumer_close(second.group);
        brp_group_consumer_close(one);
    }
    printf("%d failures of %d\n", failures, iterations);
    return failures ? 1 : 0;
}
