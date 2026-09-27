/*
 * Consumer groups: join/sync/heartbeat with generation fencing, range /
 * roundrobin / sticky assignment, offset commit and fetch, auto offset
 * reset, auto commit, max.poll.interval.ms and LeaveGroup on close.
 */
#define _POSIX_C_SOURCE 200809L
#include "internal.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Bounds retries after a coordinator move or load. */
#define COORDINATOR_ATTEMPTS 4
/* Bounds join+sync rounds for a group that will not settle. */
#define JOIN_ATTEMPTS 4

typedef brp_partition_offset_t tpo_t; /* topic, partition, offset */

struct brp_group_consumer {
    char *group_id;
    brp_group_config_t config;
    char *client_id, *auto_offset_reset, *assignor, *group_instance_id;
    brp_consumer_t *consumer;

    char **subscribed;
    size_t sub_count;

    /* mu guards the fields the heartbeat thread shares with poll. */
    pthread_mutex_t mu;
    pthread_cond_t cond;
    char *member_id;
    int32_t generation;
    bool joined;
    bool closed;
    int64_t last_poll_ms;
    /* True while poll runs. max.poll.interval.ms bounds the gap *between*
     * polls; time spent inside one (joining, long-polling) is the consumer
     * working normally and must not count against it. */
    bool in_poll;

    /* Poll-thread only. */
    tpo_t *assignment;
    size_t assign_n;
    /* positions: next offset to *deliver* (what gets committed); it only
     * advances over records handed to the caller. fetch_positions: next
     * offset to *fetch*; runs ahead by exactly the buffered records. */
    tpo_t *positions;
    size_t pos_n;
    tpo_t *fetch_positions;
    size_t fpos_n;
    brp_record_t *buffered;
    size_t buf_n, buf_cap;
    int64_t last_commit_ms;

    pthread_t heartbeat;
    bool heartbeat_started;
};

void brp_group_config_init(brp_group_config_t *config) {
    config->client_id = "brahmaputra-c";
    /* Kafka defaults to 45s; 10s as the Rust client does. */
    config->session_timeout_ms = 10000;
    config->heartbeat_interval_ms = 3000;
    config->rebalance_timeout_ms = 3000;
    config->max_poll_interval_ms = 300000;
    config->auto_commit_interval_ms = 5000;
    config->auto_offset_reset = "earliest";
    config->assignor = "range";
    config->group_instance_id = NULL;
    config->max_poll_records = 500;
    config->fetch_max_bytes = 8 * 1024 * 1024;
    config->socket_connection_setup_timeout_ms = 30000;
}

void brp_offsets_free(brp_partition_offset_t *offsets, size_t count) {
    if (!offsets) return;
    for (size_t i = 0; i < count; i++) free(offsets[i].topic);
    free(offsets);
}

/* ---- topic-partition lists -------------------------------------------- */

static int cmp_tp(const char *ta, int32_t pa, const char *tb, int32_t pb) {
    int c = strcmp(ta, tb);
    if (c) return c;
    return (pa > pb) - (pa < pb);
}

static int cmp_tpo(const void *a, const void *b) {
    const tpo_t *x = a, *y = b;
    return cmp_tp(x->topic, x->partition, y->topic, y->partition);
}

static long tpo_find(const tpo_t *list, size_t n, const char *topic, int32_t partition) {
    for (size_t i = 0; i < n; i++)
        if (list[i].partition == partition && strcmp(list[i].topic, topic) == 0) return (long)i;
    return -1;
}

static brp_err_t tpo_set(tpo_t **list, size_t *n, const char *topic, int32_t partition,
                         int64_t offset) {
    long at = tpo_find(*list, *n, topic, partition);
    if (at >= 0) {
        (*list)[at].offset = offset;
        return BRP_OK;
    }
    tpo_t *grown = realloc(*list, (*n + 1) * sizeof *grown);
    if (!grown) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    *list = grown;
    char *copy = brp_strdup(topic);
    if (!copy) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    grown[*n].topic = copy;
    grown[*n].partition = partition;
    grown[*n].offset = offset;
    (*n)++;
    return BRP_OK;
}

static brp_err_t tpo_copy(const tpo_t *src, size_t n, tpo_t **out) {
    *out = calloc(n ? n : 1, sizeof **out);
    if (!*out) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    for (size_t i = 0; i < n; i++) {
        (*out)[i] = src[i];
        (*out)[i].topic = brp_strdup(src[i].topic);
        if (!(*out)[i].topic) {
            brp_offsets_free(*out, i);
            *out = NULL;
            return brp_set_error(BRP_ERR_NOMEM, "out of memory");
        }
    }
    return BRP_OK;
}

/* ---- construction ------------------------------------------------------ */

static void *heartbeat_main(void *arg);

static void group_free(brp_group_consumer_t *g) {
    brp_consumer_close(g->consumer);
    for (size_t i = 0; i < g->sub_count; i++) free(g->subscribed[i]);
    free(g->subscribed);
    brp_offsets_free(g->assignment, g->assign_n);
    brp_offsets_free(g->positions, g->pos_n);
    brp_offsets_free(g->fetch_positions, g->fpos_n);
    brp_records_free(g->buffered, g->buf_n);
    pthread_mutex_destroy(&g->mu);
    pthread_cond_destroy(&g->cond);
    free(g->member_id);
    free(g->group_id);
    free(g->client_id);
    free(g->auto_offset_reset);
    free(g->assignor);
    free(g->group_instance_id);
    free(g);
}

brp_err_t brp_group_consumer_new(const char *bootstrap, const char *group_id,
                                 const brp_group_config_t *config,
                                 brp_group_consumer_t **out) {
    brp_group_config_t defaults;
    if (!config) {
        brp_group_config_init(&defaults);
        config = &defaults;
    }
    if (!group_id || !*group_id) return brp_set_error(BRP_ERR_INVALID_ARG, "empty group id");
    const char *reset = config->auto_offset_reset ? config->auto_offset_reset : "earliest";
    if (strcmp(reset, "earliest") && strcmp(reset, "latest") && strcmp(reset, "none"))
        return brp_set_error(BRP_ERR_INVALID_ARG,
                             "unknown auto.offset.reset \"%s\" (earliest, latest, none)", reset);
    const char *assignor = config->assignor ? config->assignor : "range";
    if (strcmp(assignor, "range") && strcmp(assignor, "roundrobin") && strcmp(assignor, "sticky"))
        return brp_set_error(BRP_ERR_INVALID_ARG,
                             "unknown assignor \"%s\" (range, roundrobin, sticky)", assignor);

    brp_group_consumer_t *g = calloc(1, sizeof *g);
    if (!g) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    pthread_mutex_init(&g->mu, NULL);
    pthread_cond_init(&g->cond, NULL);
    g->config = *config;
    g->group_id = brp_strdup(group_id);
    g->client_id = brp_strdup(config->client_id);
    g->auto_offset_reset = brp_strdup(reset);
    g->assignor = brp_strdup(assignor);
    g->group_instance_id = brp_strdup(config->group_instance_id);
    g->member_id = brp_strdup("");
    if (!g->group_id || !g->client_id || !g->auto_offset_reset || !g->assignor ||
        !g->group_instance_id || !g->member_id) {
        group_free(g);
        return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    }
    g->config.client_id = g->client_id;
    g->config.auto_offset_reset = g->auto_offset_reset;
    g->config.assignor = g->assignor;
    g->config.group_instance_id = g->group_instance_id;
    g->generation = -1;
    g->last_poll_ms = g->last_commit_ms = brp_now_ms();

    brp_consumer_config_t cc;
    brp_consumer_config_init(&cc);
    cc.client_id = g->client_id;
    cc.fetch_max_bytes = config->fetch_max_bytes;
    cc.max_poll_records = config->max_poll_records;
    cc.socket_connection_setup_timeout_ms = config->socket_connection_setup_timeout_ms;
    brp_err_t err = brp_consumer_new(bootstrap, &cc, &g->consumer);
    if (err) {
        group_free(g);
        return err;
    }
    if (pthread_create(&g->heartbeat, NULL, heartbeat_main, g) != 0) {
        group_free(g);
        return brp_set_error(BRP_ERR_STATE, "cannot start heartbeat thread");
    }
    g->heartbeat_started = true;
    *out = g;
    return BRP_OK;
}

brp_err_t brp_group_consumer_subscribe(brp_group_consumer_t *g, const char *const *topics,
                                       size_t count) {
    char **copy = calloc(count ? count : 1, sizeof *copy);
    if (!copy) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    for (size_t i = 0; i < count; i++) {
        if (!topics[i] || !*topics[i] || !(copy[i] = brp_strdup(topics[i]))) {
            for (size_t j = 0; j < i; j++) free(copy[j]);
            free(copy);
            return brp_set_error(BRP_ERR_INVALID_ARG, "bad topic at index %zu", i);
        }
    }
    for (size_t i = 0; i < g->sub_count; i++) free(g->subscribed[i]);
    free(g->subscribed);
    g->subscribed = copy;
    g->sub_count = count;
    pthread_mutex_lock(&g->mu);
    g->joined = false;
    pthread_mutex_unlock(&g->mu);
    return BRP_OK;
}

/* ---- coordinator routing ----------------------------------------------- */

static brp_err_t peek_code(const uint8_t *resp, size_t len, int32_t *code) {
    bpr_t r;
    brp_err_t err = bpr_init(&r, resp, len);
    if (err) return err;
    *code = bpr_i32(&r);
    if (r.err) return brp_set_error(BRP_ERR_PROTOCOL, "response too short for error code");
    return BRP_OK;
}

/* Sends to the group's coordinator — the leader of partition
 * crc32c(group_id) % partitions of __consumer_offsets — following moves
 * and waiting out loads. Every group response starts with an error code,
 * which is what makes this generic wrapper possible. */
static brp_err_t coordinator_request(brp_group_consumer_t *g, int16_t api, const buf_t *body,
                                     uint8_t **resp, size_t *resp_len) {
    brp_client_t *client = brp_consumer_client(g->consumer);
    for (int attempt = 0; attempt < COORDINATOR_ATTEMPTS; attempt++) {
        int32_t *partitions;
        size_t count;
        brp_err_t err = brp_client_partitions(client, BRP_OFFSETS_TOPIC, &partitions, &count);
        if (err) return err;
        free(partitions);
        int32_t partition =
            (int32_t)(brp_crc32c(g->group_id, strlen(g->group_id)) % (uint32_t)count);
        brp_conn_t *conn;
        if ((err = brp_client_conn_for(client, BRP_OFFSETS_TOPIC, partition, &conn)) != BRP_OK)
            return err;
        if ((err = brp_conn_request(conn, api, body, resp, resp_len)) != BRP_OK) return err;
        int32_t code;
        if ((err = peek_code(*resp, *resp_len, &code)) != BRP_OK) {
            free(*resp);
            return err;
        }
        if (code == BRP_ERR_COORDINATOR_LOAD_IN_PROGRESS) {
            free(*resp);
            brp_sleep_ms(100);
            continue;
        }
        if (code == BRP_ERR_NOT_COORDINATOR || code == BRP_ERR_NOT_LEADER_OR_FOLLOWER) {
            free(*resp);
            brp_client_refresh(client, BRP_OFFSETS_TOPIC);
            continue;
        }
        return BRP_OK;
    }
    return brp_set_error(BRP_ERR_NOT_COORDINATOR,
                         "group coordinator unavailable after %d attempts",
                         COORDINATOR_ATTEMPTS);
}

/* Sends a request whose response is just an error code. */
static brp_err_t simple_request(brp_group_consumer_t *g, int16_t api, buf_t *w,
                                const char *context, int32_t *code_out) {
    uint8_t *resp;
    size_t len;
    brp_err_t err = coordinator_request(g, api, w, &resp, &len);
    buf_free(w);
    if (err) return err;
    int32_t code;
    err = peek_code(resp, len, &code);
    free(resp);
    if (err) return err;
    if (code_out) *code_out = code;
    if (code != 0) return brp_server_error(code, context);
    return BRP_OK;
}

/* ---- offsets ----------------------------------------------------------- */

brp_err_t brp_group_consumer_committed(brp_group_consumer_t *g,
                                       const brp_topic_partition_t *partitions, size_t count,
                                       brp_partition_offset_t **out, size_t *out_count) {
    buf_t w;
    bp_init(&w);
    bp_str(&w, g->group_id);
    bp_i32(&w, (int32_t)count);
    for (size_t i = 0; i < count; i++) {
        bp_str(&w, partitions[i].topic);
        bp_i32(&w, partitions[i].partition);
    }
    uint8_t *resp;
    size_t len;
    brp_err_t err = coordinator_request(g, API_OFFSET_FETCH, &w, &resp, &len);
    buf_free(&w);
    if (err) return err;
    bpr_t r;
    tpo_t *list = NULL;
    size_t n = 0;
    if ((err = bpr_init(&r, resp, len)) != BRP_OK) goto done;
    int32_t code = bpr_i32(&r);
    if (!r.err && code != 0) {
        err = brp_server_error(code, "offset_fetch");
        goto done;
    }
    size_t entries = bpr_count(&r);
    list = calloc(entries ? entries : 1, sizeof *list);
    if (!list) {
        err = brp_set_error(BRP_ERR_NOMEM, "out of memory");
        goto done;
    }
    for (size_t i = 0; i < entries && !r.err; i++) {
        list[i].topic = bpr_str(&r);
        list[i].partition = bpr_i32(&r);
        list[i].offset = bpr_i64(&r);
        n = i + 1;
    }
    if (r.err) {
        err = brp_set_error(BRP_ERR_PROTOCOL, "malformed offset_fetch response");
        brp_offsets_free(list, n);
        goto done;
    }
    *out = list;
    *out_count = n;
done:
    free(resp);
    return err;
}

brp_err_t brp_group_consumer_commit(brp_group_consumer_t *g) {
    if (g->pos_n == 0) return BRP_OK;
    qsort(g->positions, g->pos_n, sizeof *g->positions, cmp_tpo);
    pthread_mutex_lock(&g->mu);
    int32_t generation = g->generation;
    char *member_id = brp_strdup(g->member_id);
    pthread_mutex_unlock(&g->mu);
    if (!member_id) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    buf_t w;
    bp_init(&w);
    bp_str(&w, g->group_id);
    bp_i32(&w, generation);
    bp_str(&w, member_id);
    free(member_id);
    bp_i32(&w, (int32_t)g->pos_n);
    for (size_t i = 0; i < g->pos_n; i++) {
        bp_str(&w, g->positions[i].topic);
        bp_i32(&w, g->positions[i].partition);
        bp_i64(&w, g->positions[i].offset);
    }
    int32_t code = 0;
    brp_err_t err = simple_request(g, API_OFFSET_COMMIT, &w, "offset_commit", &code);
    if (!err) g->last_commit_ms = brp_now_ms();
    if (code == BRP_ERR_UNKNOWN_MEMBER_ID || code == BRP_ERR_ILLEGAL_GENERATION ||
        code == BRP_ERR_REBALANCE_IN_PROGRESS) {
        /* Fenced: this member is no longer in the generation it committed
         * for. The next poll rejoins, as a new member when the coordinator
         * no longer knows this one, rather than committing into the same
         * wall again. */
        pthread_mutex_lock(&g->mu);
        if (code == BRP_ERR_UNKNOWN_MEMBER_ID) g->member_id[0] = 0;
        g->joined = false;
        pthread_mutex_unlock(&g->mu);
    }
    return err;
}

char *brp_group_consumer_member_id(brp_group_consumer_t *g) {
    pthread_mutex_lock(&g->mu);
    char *id = brp_strdup(g->member_id);
    pthread_mutex_unlock(&g->mu);
    return id;
}

int32_t brp_group_consumer_generation(brp_group_consumer_t *g) {
    pthread_mutex_lock(&g->mu);
    int32_t generation = g->generation;
    pthread_mutex_unlock(&g->mu);
    return generation;
}

static void maybe_auto_commit(brp_group_consumer_t *g) {
    int interval = g->config.auto_commit_interval_ms;
    if (interval <= 0 || g->pos_n == 0) return;
    if (brp_now_ms() - g->last_commit_ms < interval) return;
    /* A failed auto-commit is retried on the next poll; the explicit
     * commit is what a caller relies on. */
    brp_group_consumer_commit(g);
}

static brp_err_t reset_offset(brp_group_consumer_t *g, const char *topic, int32_t partition,
                              int64_t *offset) {
    if (strcmp(g->auto_offset_reset, "none") == 0)
        return brp_set_error(BRP_ERR_NO_OFFSET,
                             "no committed offset for partition %s-%d and "
                             "auto.offset.reset=none",
                             topic, (int)partition);
    int64_t which = strcmp(g->auto_offset_reset, "latest") == 0 ? BRP_OFFSET_LATEST
                                                                 : BRP_OFFSET_EARLIEST;
    return brp_consumer_list_offsets(g->consumer, topic, partition, which, offset);
}

/* ---- assignors --------------------------------------------------------- */

typedef struct member {
    char *id;
    char **topics;
    size_t topic_n;
    tpo_t *held;
    size_t held_n;
    tpo_t *assigned; /* topic pointers borrowed from topic_parts */
    size_t assigned_n, assigned_cap;
    int quota;
    bool eligible;
} member_t;

typedef struct topic_parts {
    char *topic;
    int32_t *parts;
    size_t n;
} topic_parts_t;

static void members_free(member_t *members, size_t n) {
    for (size_t i = 0; i < n; i++) {
        free(members[i].id);
        for (size_t t = 0; t < members[i].topic_n; t++) free(members[i].topics[t]);
        free(members[i].topics);
        brp_offsets_free(members[i].held, members[i].held_n);
        free(members[i].assigned);
    }
    free(members);
}

static bool subscribes(const member_t *m, const char *topic) {
    for (size_t i = 0; i < m->topic_n; i++)
        if (strcmp(m->topics[i], topic) == 0) return true;
    return false;
}

static bool assign_to(member_t *m, const char *topic, int32_t partition) {
    if (m->assigned_n == m->assigned_cap) {
        size_t next = m->assigned_cap ? m->assigned_cap * 2 : 8;
        tpo_t *grown = realloc(m->assigned, next * sizeof *grown);
        if (!grown) return false;
        m->assigned = grown;
        m->assigned_cap = next;
    }
    m->assigned[m->assigned_n].topic = (char *)(uintptr_t)topic;
    m->assigned[m->assigned_n].partition = partition;
    m->assigned[m->assigned_n].offset = 0;
    m->assigned_n++;
    return true;
}

static int cmp_member(const void *a, const void *b) {
    return strcmp(((const member_t *)a)->id, ((const member_t *)b)->id);
}

static int cmp_topic_parts(const void *a, const void *b) {
    return strcmp(((const topic_parts_t *)a)->topic, ((const topic_parts_t *)b)->topic);
}

/* Each subscribed member gets a contiguous range per topic; the first
 * (partitions % members) members take one extra. Members are sorted. */
static bool range_assign(member_t *members, size_t mn, topic_parts_t *tps, size_t tn) {
    size_t *subs = malloc((mn ? mn : 1) * sizeof *subs);
    if (!subs) return false;
    for (size_t t = 0; t < tn; t++) {
        size_t sn = 0;
        for (size_t i = 0; i < mn; i++)
            if (subscribes(&members[i], tps[t].topic)) subs[sn++] = i;
        if (!sn) continue;
        size_t base = tps[t].n / sn, extra = tps[t].n % sn, cursor = 0;
        for (size_t k = 0; k < sn; k++) {
            size_t count = base + (k < extra ? 1 : 0);
            for (size_t j = 0; j < count; j++)
                if (!assign_to(&members[subs[k]], tps[t].topic, tps[t].parts[cursor + j])) {
                    free(subs);
                    return false;
                }
            cursor += count;
        }
    }
    free(subs);
    return true;
}

/* Deals every partition around the circle of members sorted by id,
 * skipping members not subscribed to a partition's topic. */
static bool roundrobin_assign(member_t *members, size_t mn, topic_parts_t *tps, size_t tn) {
    if (!mn) return true;
    size_t cursor = 0;
    for (size_t t = 0; t < tn; t++) {
        for (size_t p = 0; p < tps[t].n; p++) {
            size_t start = cursor;
            for (;;) {
                member_t *m = &members[cursor % mn];
                cursor++;
                if (subscribes(m, tps[t].topic)) {
                    if (!assign_to(m, tps[t].topic, tps[t].parts[p])) return false;
                    break;
                }
                if (cursor - start >= mn) break; /* nobody subscribes */
            }
        }
    }
    return true;
}

typedef struct slot_ref {
    const char *topic;
    int32_t partition;
    size_t holder;
} slot_ref_t;

static int cmp_slot_ref(const void *a, const void *b) {
    const slot_ref_t *x = a, *y = b;
    return cmp_tp(x->topic, x->partition, y->topic, y->partition);
}

static int cmp_assigned(const void *a, const void *b) { return cmp_tpo(a, b); }

/* Keeps members on what they hold and moves only what balance requires.
 * Mirrors the Rust (and Go) implementation exactly, because members
 * computing the assignment independently must agree. */
static bool sticky_assign(member_t *members, size_t mn, topic_parts_t *tps, size_t tn) {
    if (!mn) return true;
    size_t total = 0;
    for (size_t t = 0; t < tn; t++) total += tps[t].n;
    slot_ref_t *claimed = malloc((total ? total : 1) * sizeof *claimed);
    slot_ref_t *unassigned = malloc((total ? total : 1) * sizeof *unassigned);
    if (!claimed || !unassigned) {
        free(claimed);
        free(unassigned);
        return false;
    }
    size_t cn = 0, un = 0;
    for (size_t t = 0; t < tn; t++) {
        for (size_t p = 0; p < tps[t].n; p++) {
            slot_ref_t slot = {tps[t].topic, tps[t].parts[p], (size_t)-1};
            for (size_t i = 0; i < mn && slot.holder == (size_t)-1; i++) {
                if (!subscribes(&members[i], slot.topic)) continue;
                if (tpo_find(members[i].held, members[i].held_n, slot.topic, slot.partition) >= 0)
                    slot.holder = i;
            }
            if (slot.holder == (size_t)-1)
                unassigned[un++] = slot;
            else
                claimed[cn++] = slot;
        }
    }

    size_t eligible = 0;
    for (size_t i = 0; i < mn; i++) {
        members[i].eligible = false;
        for (size_t t = 0; t < tn; t++)
            if (subscribes(&members[i], tps[t].topic)) members[i].eligible = true;
        if (members[i].eligible) eligible++;
    }
    if (!eligible) {
        free(claimed);
        free(unassigned);
        return true;
    }
    size_t base = total / eligible, extra = total % eligible, index = 0;
    for (size_t i = 0; i < mn; i++) {
        members[i].quota = 0;
        if (!members[i].eligible) continue;
        members[i].quota = (int)(base + (index < extra ? 1 : 0));
        index++;
    }

    qsort(claimed, cn, sizeof *claimed, cmp_slot_ref);
    bool ok = true;
    for (size_t k = 0; k < cn && ok; k++) {
        member_t *m = &members[claimed[k].holder];
        if ((int)m->assigned_n < m->quota)
            ok = assign_to(m, claimed[k].topic, claimed[k].partition);
        else
            unassigned[un++] = claimed[k];
    }

    qsort(unassigned, un, sizeof *unassigned, cmp_slot_ref);
    for (size_t k = 0; k < un && ok; k++) {
        member_t *taker = NULL;
        for (size_t i = 0; i < mn && !taker; i++)
            if (members[i].eligible && subscribes(&members[i], unassigned[k].topic) &&
                (int)members[i].assigned_n < members[i].quota)
                taker = &members[i];
        /* Quotas exhausted (possible with uneven subscriptions): an
         * unassigned partition is a stalled one, so fall back to any
         * subscribed member rather than dropping it. */
        for (size_t i = 0; i < mn && !taker; i++)
            if (members[i].eligible && subscribes(&members[i], unassigned[k].topic))
                taker = &members[i];
        if (taker) ok = assign_to(taker, unassigned[k].topic, unassigned[k].partition);
    }
    for (size_t i = 0; i < mn; i++)
        qsort(members[i].assigned, members[i].assigned_n, sizeof *members[i].assigned,
              cmp_assigned);
    free(claimed);
    free(unassigned);
    return ok;
}

/* ---- membership -------------------------------------------------------- */

static void clear_buffered(brp_group_consumer_t *g) {
    brp_records_free(g->buffered, g->buf_n);
    g->buffered = NULL;
    g->buf_n = g->buf_cap = 0;
}

static brp_err_t apply_assignment(brp_group_consumer_t *g, tpo_t *assignment, size_t n) {
    brp_offsets_free(g->assignment, g->assign_n);
    g->assignment = assignment;
    g->assign_n = n;
    /* Drop positions for partitions no longer owned. */
    for (size_t i = 0; i < g->pos_n;) {
        if (tpo_find(assignment, n, g->positions[i].topic, g->positions[i].partition) < 0) {
            free(g->positions[i].topic);
            g->positions[i] = g->positions[--g->pos_n];
        } else {
            i++;
        }
    }
    /* Buffered records sit ahead of the consumed position and were never
     * delivered, so a new assignment simply drops them. */
    clear_buffered(g);

    brp_err_t err = BRP_OK;
    brp_topic_partition_t *needed = malloc((n ? n : 1) * sizeof *needed);
    if (!needed) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    size_t nn = 0;
    for (size_t i = 0; i < n; i++)
        if (tpo_find(g->positions, g->pos_n, assignment[i].topic, assignment[i].partition) < 0) {
            needed[nn].topic = assignment[i].topic;
            needed[nn].partition = assignment[i].partition;
            nn++;
        }
    if (nn) {
        tpo_t *committed = NULL;
        size_t cn = 0;
        err = brp_group_consumer_committed(g, needed, nn, &committed, &cn);
        for (size_t i = 0; i < nn && !err; i++) {
            long at = tpo_find(committed, cn, needed[i].topic, needed[i].partition);
            int64_t offset = at >= 0 ? committed[at].offset : -1;
            if (offset < 0) err = reset_offset(g, needed[i].topic, needed[i].partition, &offset);
            if (!err) err = tpo_set(&g->positions, &g->pos_n, needed[i].topic,
                                    needed[i].partition, offset);
        }
        brp_offsets_free(committed, cn);
    }
    free(needed);
    brp_offsets_free(g->fetch_positions, g->fpos_n);
    g->fetch_positions = NULL;
    g->fpos_n = 0;
    brp_err_t copy_err = tpo_copy(g->positions, g->pos_n, &g->fetch_positions);
    if (!copy_err) g->fpos_n = g->pos_n;
    return err ? err : copy_err;
}

/* Returns BRP_OK with *settled=false when the group moved on underneath
 * this round (rebalance / illegal generation) and a rejoin is needed. */
static brp_err_t sync_group(brp_group_consumer_t *g, member_t *members, size_t mn,
                            bool *settled) {
    *settled = false;
    pthread_mutex_lock(&g->mu);
    int32_t generation = g->generation;
    char *member_id = brp_strdup(g->member_id);
    pthread_mutex_unlock(&g->mu);
    if (!member_id) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    buf_t w;
    bp_init(&w);
    bp_str(&w, g->group_id);
    bp_i32(&w, generation);
    bp_str(&w, member_id);
    free(member_id);
    bp_i32(&w, (int32_t)mn);
    for (size_t i = 0; i < mn; i++) {
        bp_str(&w, members[i].id);
        bp_i32(&w, (int32_t)members[i].assigned_n);
        for (size_t k = 0; k < members[i].assigned_n; k++) {
            bp_str(&w, members[i].assigned[k].topic);
            bp_i32(&w, members[i].assigned[k].partition);
        }
    }
    uint8_t *resp;
    size_t len;
    brp_err_t err = coordinator_request(g, API_SYNC_GROUP, &w, &resp, &len);
    buf_free(&w);
    if (err) return err;
    bpr_t r;
    tpo_t *assignment = NULL;
    size_t an = 0;
    if ((err = bpr_init(&r, resp, len)) != BRP_OK) goto done;
    int32_t code = bpr_i32(&r);
    if (code == BRP_ERR_REBALANCE_IN_PROGRESS || code == BRP_ERR_ILLEGAL_GENERATION) goto done;
    if (code != 0) {
        err = brp_server_error(code, "sync_group");
        goto done;
    }
    size_t count = bpr_count(&r);
    assignment = calloc(count ? count : 1, sizeof *assignment);
    if (!assignment) {
        err = brp_set_error(BRP_ERR_NOMEM, "out of memory");
        goto done;
    }
    for (size_t i = 0; i < count && !r.err; i++) {
        assignment[i].topic = bpr_str(&r);
        assignment[i].partition = bpr_i32(&r);
        an = i + 1;
    }
    if (r.err) {
        brp_offsets_free(assignment, an);
        err = brp_set_error(BRP_ERR_PROTOCOL, "malformed sync_group response");
        goto done;
    }
    *settled = true;
    err = apply_assignment(g, assignment, an);
done:
    free(resp);
    return err;
}

static brp_err_t compute_assignment(brp_group_consumer_t *g, member_t *members, size_t mn) {
    qsort(members, mn, sizeof *members, cmp_member);
    topic_parts_t *tps = NULL;
    size_t tn = 0;
    brp_err_t err = BRP_OK;
    for (size_t i = 0; i < mn && !err; i++) {
        for (size_t t = 0; t < members[i].topic_n && !err; t++) {
            bool seen = false;
            for (size_t k = 0; k < tn; k++)
                if (strcmp(tps[k].topic, members[i].topics[t]) == 0) seen = true;
            if (seen) continue;
            topic_parts_t *grown = realloc(tps, (tn + 1) * sizeof *grown);
            if (!grown) {
                err = brp_set_error(BRP_ERR_NOMEM, "out of memory");
                break;
            }
            tps = grown;
            tps[tn].topic = members[i].topics[t];
            err = brp_client_partitions(brp_consumer_client(g->consumer), tps[tn].topic,
                                        &tps[tn].parts, &tps[tn].n);
            if (!err) tn++;
        }
    }
    if (!err) {
        qsort(tps, tn, sizeof *tps, cmp_topic_parts);
        bool ok;
        if (strcmp(g->assignor, "roundrobin") == 0)
            ok = roundrobin_assign(members, mn, tps, tn);
        else if (strcmp(g->assignor, "sticky") == 0)
            ok = sticky_assign(members, mn, tps, tn);
        else
            ok = range_assign(members, mn, tps, tn);
        if (!ok) err = brp_set_error(BRP_ERR_NOMEM, "out of memory computing assignment");
    }
    /* Assigned entries borrow topic names from members[].topics, which
     * outlive this function; only the partition arrays are freed here. */
    for (size_t k = 0; k < tn; k++) free(tps[k].parts);
    free(tps);
    return err;
}

static brp_err_t join_group(brp_group_consumer_t *g) {
    for (int attempt = 0; attempt < JOIN_ATTEMPTS; attempt++) {
        pthread_mutex_lock(&g->mu);
        char *current = brp_strdup(g->member_id);
        pthread_mutex_unlock(&g->mu);
        if (!current) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
        buf_t w;
        bp_init(&w);
        bp_str(&w, g->group_id);
        bp_i32(&w, g->config.session_timeout_ms);
        bp_i32(&w, g->config.rebalance_timeout_ms);
        bp_str(&w, current);
        free(current);
        bp_i32(&w, (int32_t)g->sub_count);
        for (size_t i = 0; i < g->sub_count; i++) bp_str(&w, g->subscribed[i]);
        bp_str(&w, g->group_instance_id);

        uint8_t *resp;
        size_t len;
        brp_err_t err = coordinator_request(g, API_JOIN_GROUP, &w, &resp, &len);
        buf_free(&w);
        if (err) return err;
        bpr_t r;
        if ((err = bpr_init(&r, resp, len)) != BRP_OK) {
            free(resp);
            return err;
        }
        int32_t code = bpr_i32(&r);
        if (code == BRP_ERR_REBALANCE_IN_PROGRESS) {
            free(resp);
            brp_sleep_ms(100);
            continue;
        }
        if (code == BRP_ERR_UNKNOWN_MEMBER_ID) {
            /* Evicted (e.g. after leaving for a slow poll): rejoin fresh. */
            free(resp);
            pthread_mutex_lock(&g->mu);
            g->member_id[0] = 0;
            pthread_mutex_unlock(&g->mu);
            continue;
        }
        if (code != 0) {
            free(resp);
            return brp_server_error(code, "join_group");
        }
        int32_t generation = bpr_i32(&r);
        char *member_id = bpr_str(&r);
        char *leader_id = bpr_str(&r);
        size_t mn = bpr_count(&r);
        member_t *members = calloc(mn ? mn : 1, sizeof *members);
        size_t parsed = 0;
        for (size_t i = 0; members && i < mn && !r.err; i++) {
            member_t *m = &members[i];
            parsed = i + 1;
            m->id = bpr_str(&r);
            m->topic_n = bpr_count(&r);
            m->topics = calloc(m->topic_n ? m->topic_n : 1, sizeof *m->topics);
            if (!m->topics) {
                m->topic_n = 0;
                r.err = true;
                break;
            }
            for (size_t t = 0; t < m->topic_n; t++) m->topics[t] = bpr_str(&r);
            size_t hn = bpr_count(&r);
            m->held = calloc(hn ? hn : 1, sizeof *m->held);
            if (!m->held) {
                r.err = true;
                break;
            }
            for (size_t h = 0; h < hn && !r.err; h++) {
                m->held[h].topic = bpr_str(&r);
                m->held[h].partition = bpr_i32(&r);
                m->held_n = h + 1;
            }
        }
        free(resp);
        if (!members || r.err || !member_id || !leader_id) {
            members_free(members, parsed);
            free(member_id);
            free(leader_id);
            return brp_set_error(members ? BRP_ERR_PROTOCOL : BRP_ERR_NOMEM,
                                 "malformed join_group response");
        }
        pthread_mutex_lock(&g->mu);
        free(g->member_id);
        g->member_id = member_id;
        g->generation = generation;
        pthread_mutex_unlock(&g->mu);

        bool leader = strcmp(member_id, leader_id) == 0;
        free(leader_id);
        if (leader) {
            err = compute_assignment(g, members, mn);
        } else {
            /* Followers send an empty assignment list. */
            members_free(members, mn);
            members = NULL;
            mn = 0;
        }
        bool settled = false;
        if (!err) err = sync_group(g, members, mn, &settled);
        members_free(members, mn);
        if (err) return err;
        if (settled) {
            pthread_mutex_lock(&g->mu);
            g->joined = true;
            pthread_mutex_unlock(&g->mu);
            return BRP_OK;
        }
    }
    return brp_set_error(BRP_ERR_REBALANCE_FAILED,
                         "consumer group failed to stabilise after %d join attempts",
                         JOIN_ATTEMPTS);
}

static brp_err_t leave_group(brp_group_consumer_t *g, const char *member_id) {
    buf_t w;
    bp_init(&w);
    bp_str(&w, g->group_id);
    bp_str(&w, member_id);
    brp_err_t err = simple_request(g, API_LEAVE_GROUP, &w, "leave_group", NULL);
    pthread_mutex_lock(&g->mu);
    g->joined = false;
    pthread_mutex_unlock(&g->mu);
    return err;
}

static void *heartbeat_main(void *arg) {
    brp_group_consumer_t *g = arg;
    /* Two independent deadlines are enforced here, so wake often enough
     * for the shorter: deriving the tick from the session timeout alone
     * would leave a long session with a short poll interval unchecked. */
    int heartbeat_every = g->config.heartbeat_interval_ms > 0 ? g->config.heartbeat_interval_ms
                                                              : g->config.session_timeout_ms / 3;
    if (heartbeat_every < 1) heartbeat_every = 1;
    int poll_check_every = g->config.max_poll_interval_ms / 3;
    if (poll_check_every < 1) poll_check_every = 1;
    int interval = heartbeat_every < poll_check_every ? heartbeat_every : poll_check_every;
    bool left_for_slow_poll = false;

    pthread_mutex_lock(&g->mu);
    while (!g->closed) {
        struct timespec deadline;
        brp_deadline_ts(&deadline, interval);
        while (!g->closed && pthread_cond_timedwait(&g->cond, &g->mu, &deadline) != ETIMEDOUT) {
        }
        if (g->closed) break;
        int64_t idle = g->in_poll ? 0 : brp_now_ms() - g->last_poll_ms;
        bool joined = g->joined;
        int32_t generation = g->generation;
        char *member_id = brp_strdup(g->member_id);
        pthread_mutex_unlock(&g->mu);

        if (joined && member_id && *member_id) {
            if (idle >= g->config.max_poll_interval_ms) {
                /* The application stopped consuming though the process is
                 * alive; heartbeating on would hold its partitions away from
                 * a consumer that could make progress. */
                if (!left_for_slow_poll) {
                    leave_group(g, member_id);
                    left_for_slow_poll = true;
                }
            } else {
                left_for_slow_poll = false;
                buf_t w;
                bp_init(&w);
                bp_str(&w, g->group_id);
                bp_i32(&w, generation);
                bp_str(&w, member_id);
                int32_t code = 0;
                simple_request(g, API_HEARTBEAT, &w, "heartbeat", &code);
                if (code == BRP_ERR_REBALANCE_IN_PROGRESS || code == BRP_ERR_UNKNOWN_MEMBER_ID ||
                    code == BRP_ERR_ILLEGAL_GENERATION) {
                    pthread_mutex_lock(&g->mu);
                    /* Only if nothing changed since the snapshot: a late
                     * answer for an old generation must not send a member
                     * that already rejoined round again. */
                    if (g->generation == generation && strcmp(g->member_id, member_id) == 0)
                        g->joined = false;
                    pthread_mutex_unlock(&g->mu);
                }
            }
        }
        free(member_id);
        pthread_mutex_lock(&g->mu);
    }
    pthread_mutex_unlock(&g->mu);
    return NULL;
}

/* ---- polling ----------------------------------------------------------- */

static brp_err_t take_buffered(brp_group_consumer_t *g, brp_record_t **out, size_t *count) {
    size_t limit = g->config.max_poll_records > 0 ? (size_t)g->config.max_poll_records : g->buf_n;
    if (limit > g->buf_n) limit = g->buf_n;
    brp_record_t *delivered = malloc(limit * sizeof *delivered);
    if (!delivered) return brp_set_error(BRP_ERR_NOMEM, "out of memory");
    memcpy(delivered, g->buffered, limit * sizeof *delivered);
    memmove(g->buffered, g->buffered + limit, (g->buf_n - limit) * sizeof *g->buffered);
    g->buf_n -= limit;
    for (size_t i = 0; i < limit; i++) {
        /* The consumed position advances only over records actually handed
         * to the caller; committing what was merely fetched would skip
         * records nobody processed. */
        tpo_set(&g->positions, &g->pos_n, delivered[i].topic, delivered[i].partition,
                delivered[i].offset + 1);
    }
    *out = delivered;
    *count = limit;
    return BRP_OK;
}

static brp_err_t poll_inner(brp_group_consumer_t *g, int timeout_ms, brp_record_t **out,
                            size_t *count);

brp_err_t brp_group_consumer_poll(brp_group_consumer_t *g, int timeout_ms,
                                  brp_record_t **out, size_t *count) {
    *out = NULL;
    *count = 0;
    if (g->sub_count == 0)
        return brp_set_error(BRP_ERR_STATE, "subscribe to at least one topic before polling");
    /* Stamped on entry and on exit, and flagged in between: the interval
     * measures the application's time between polls, never the poll's own. */
    pthread_mutex_lock(&g->mu);
    g->last_poll_ms = brp_now_ms();
    g->in_poll = true;
    pthread_mutex_unlock(&g->mu);
    brp_err_t err = poll_inner(g, timeout_ms, out, count);
    pthread_mutex_lock(&g->mu);
    g->last_poll_ms = brp_now_ms();
    g->in_poll = false;
    pthread_mutex_unlock(&g->mu);
    return err;
}

static brp_err_t poll_inner(brp_group_consumer_t *g, int timeout_ms, brp_record_t **out,
                            size_t *count) {

    int64_t deadline = brp_now_ms() + (timeout_ms > 0 ? timeout_ms : 0);
    for (;;) {
        pthread_mutex_lock(&g->mu);
        bool joined = g->joined;
        pthread_mutex_unlock(&g->mu);
        if (!joined) {
            brp_err_t err = join_group(g);
            if (err) return err;
        }
        if (g->buf_n > 0) return take_buffered(g, out, count);
        if (g->assign_n == 0) {
            if (brp_now_ms() > deadline) return BRP_OK;
            brp_sleep_ms(50);
            continue;
        }

        bool got_any = false;
        for (size_t i = 0; i < g->assign_n; i++) {
            const char *topic = g->assignment[i].topic;
            int32_t partition = g->assignment[i].partition;
            int64_t remaining = deadline - brp_now_ms();
            if (remaining < 0) remaining = 0;
            if (remaining > 500) remaining = 500;
            long at = tpo_find(g->fetch_positions, g->fpos_n, topic, partition);
            if (at < 0) continue;
            brp_record_t *records;
            size_t n;
            brp_err_t err = brp_consumer_fetch(g->consumer, topic, partition,
                                               g->fetch_positions[at].offset,
                                               (int32_t)remaining, &records, &n, NULL);
            if (err == BRP_ERR_OFFSET_OUT_OF_RANGE) {
                /* The committed offset fell off the log; restart where the
                 * policy says. */
                int64_t reset;
                if ((err = reset_offset(g, topic, partition, &reset)) != BRP_OK) return err;
                g->fetch_positions[at].offset = reset;
                if ((err = tpo_set(&g->positions, &g->pos_n, topic, partition, reset)) != BRP_OK)
                    return err;
                continue;
            }
            if (err == BRP_ERR_NOT_LEADER_OR_FOLLOWER) {
                brp_client_refresh(brp_consumer_client(g->consumer), topic);
                continue;
            }
            if (err) return err;
            if (n == 0) {
                free(records);
                continue;
            }
            got_any = true;
            g->fetch_positions[at].offset = records[n - 1].offset + 1;
            if (g->buf_n + n > g->buf_cap) {
                size_t next = g->buf_cap ? g->buf_cap : 64;
                while (next < g->buf_n + n) next *= 2;
                brp_record_t *grown = realloc(g->buffered, next * sizeof *grown);
                if (!grown) {
                    brp_records_free(records, n);
                    return brp_set_error(BRP_ERR_NOMEM, "out of memory");
                }
                g->buffered = grown;
                g->buf_cap = next;
            }
            memcpy(g->buffered + g->buf_n, records, n * sizeof *records);
            g->buf_n += n;
            free(records); /* elements moved into the buffer */
        }

        maybe_auto_commit(g);
        if (g->buf_n > 0) return take_buffered(g, out, count);
        if (!got_any && brp_now_ms() > deadline) return BRP_OK;
    }
}

brp_err_t brp_group_consumer_assignment(brp_group_consumer_t *g, brp_partition_offset_t **out,
                                        size_t *out_count) {
    brp_err_t err = tpo_copy(g->assignment, g->assign_n, out);
    if (err) return err;
    for (size_t i = 0; i < g->assign_n; i++) {
        long at = tpo_find(g->positions, g->pos_n, (*out)[i].topic, (*out)[i].partition);
        (*out)[i].offset = at >= 0 ? g->positions[at].offset : -1;
    }
    *out_count = g->assign_n;
    return BRP_OK;
}

/* Leaving is what separates a clean shutdown from a crash: without it the
 * coordinator waits out session.timeout.ms before reassigning, so a
 * rolling restart of N instances costs N session timeouts. */
brp_err_t brp_group_consumer_close(brp_group_consumer_t *g) {
    if (!g) return BRP_OK;
    pthread_mutex_lock(&g->mu);
    g->closed = true;
    pthread_cond_broadcast(&g->cond);
    pthread_mutex_unlock(&g->mu);
    if (g->heartbeat_started) pthread_join(g->heartbeat, NULL);

    brp_err_t err = BRP_OK;
    char message[512] = "";
    if (g->joined) {
        err = brp_group_consumer_commit(g);
        if (err) snprintf(message, sizeof message, "%s", brp_last_error());
    }
    if (g->member_id && *g->member_id) {
        /* Best effort: failing costs only the session timeout it avoids. */
        char *member_id = brp_strdup(g->member_id);
        if (member_id) leave_group(g, member_id);
        free(member_id);
    }
    group_free(g);
    if (err) brp_set_error(err, "%s", message);
    return err;
}
