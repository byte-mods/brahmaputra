/*
 * brahmaputra.h - C client for the Brahmaputra log streaming platform.
 *
 * C11 + POSIX (pthreads, BSD sockets). Speaks Brahmaputra's own wire
 * protocol directly; it is not Kafka-compatible, but its configuration
 * names mirror Kafka's so that someone who knows Kafka does not have to
 * learn a new vocabulary.
 *
 * Conventions
 * -----------
 *  - Every handle is opaque and created by a *_new() function that returns
 *    a brp_err_t and writes the handle through an out-parameter.
 *  - Every config struct has a *_init() function that fills in defaults.
 *    Strings in a config are copied when the handle is created, so the
 *    config may be discarded afterwards.
 *  - Arrays handed back to the caller are caller-owned and released with
 *    the matching free function (brp_records_free, brp_metadata_free,
 *    brp_offsets_free) or, for flat arrays and strings, brp_free().
 *  - On failure a function returns a non-zero brp_err_t. Negative values
 *    are client-side errors; positive values are the broker's own error
 *    codes. brp_last_error() gives a human-readable detail message for the
 *    most recent failure on the calling thread.
 *  - A NULL pointer with length 0 is a *null* key/value/header value; a
 *    non-NULL pointer with length 0 is an *empty* one. A record with a null
 *    value is a tombstone.
 *
 * Thread safety: brp_client_t and brp_producer_t may be shared between
 * threads. brp_consumer_t and brp_group_consumer_t are single-threaded,
 * like Kafka's consumer: use one per thread.
 */
#ifndef BRAHMAPUTRA_H
#define BRAHMAPUTRA_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BRP_VERSION "0.1.0"
/* BitPacker schema version every request/response body starts with. */
#define BRP_SCHEMA_VERSION "1.0.0"
/* Wire api version this client speaks; the broker requires an exact match. */
#define BRP_API_VERSION 4

/* ------------------------------------------------------------------------ */
/* Errors                                                                   */
/* ------------------------------------------------------------------------ */

typedef enum brp_err {
    BRP_OK = 0,

    /* Client-side errors (negative). */
    BRP_ERR_INVALID_ARG = -1,       /* bad argument or configuration        */
    BRP_ERR_NOMEM = -2,             /* allocation failed                    */
    BRP_ERR_IO = -3,                /* socket error or connection closed    */
    BRP_ERR_TIMEOUT = -4,           /* a network operation timed out        */
    BRP_ERR_PROTOCOL = -5,          /* malformed or unexpected response     */
    BRP_ERR_CORRUPT = -6,           /* record batch failed CRC or decoding  */
    BRP_ERR_CODEC = -7,             /* compression codec missing or failed  */
    BRP_ERR_BUFFER_FULL = -8,       /* buffer.memory exhausted max.block.ms */
    BRP_ERR_NO_OFFSET = -9,         /* auto.offset.reset=none, no position  */
    BRP_ERR_STATE = -10,            /* call not valid in the current state  */
    BRP_ERR_DELIVERY_TIMEOUT = -11, /* delivery.timeout.ms expired          */
    BRP_ERR_UNKNOWN_TOPIC = -12,    /* topic has no partitions / no leader  */
    BRP_ERR_REBALANCE_FAILED = -13, /* group failed to stabilise            */

    /* Broker error codes (positive), as carried in error_code fields. */
    BRP_ERR_UNKNOWN_TOPIC_OR_PARTITION = 1,
    BRP_ERR_OFFSET_OUT_OF_RANGE = 2,
    BRP_ERR_INVALID_REQUEST = 3,
    BRP_ERR_UNSUPPORTED_VERSION = 4,
    BRP_ERR_INTERNAL = 5,
    BRP_ERR_NOT_LEADER_OR_FOLLOWER = 6,
    BRP_ERR_FENCED_BROKER_EPOCH = 7,
    BRP_ERR_FENCED_LEADER_EPOCH = 8,
    BRP_ERR_UNKNOWN_LEADER_EPOCH = 9,
    BRP_ERR_NOT_ENOUGH_REPLICAS = 10,
    BRP_ERR_FENCED_PRODUCER_EPOCH = 11,
    BRP_ERR_OUT_OF_ORDER_SEQUENCE = 12,
    BRP_ERR_UNKNOWN_MEMBER_ID = 13,
    BRP_ERR_REBALANCE_IN_PROGRESS = 14,
    BRP_ERR_NOT_COORDINATOR = 15,
    BRP_ERR_ILLEGAL_GENERATION = 16,
    BRP_ERR_COORDINATOR_LOAD_IN_PROGRESS = 17,
    BRP_ERR_SASL_AUTHENTICATION_FAILED = 18,
    BRP_ERR_AUTHORIZATION_FAILED = 19
} brp_err_t;

/* Symbolic name of an error code, e.g. "NOT_LEADER_OR_FOLLOWER". */
const char *brp_err_name(brp_err_t err);

/* Detail message for the most recent failure on the calling thread.
 * Never NULL; valid until the next failing call on this thread. */
const char *brp_last_error(void);

/* Releases a flat array or string returned by this library. */
void brp_free(void *ptr);

/* ------------------------------------------------------------------------ */
/* Compression                                                              */
/* ------------------------------------------------------------------------ */

typedef enum brp_compression {
    BRP_COMPRESSION_NONE = 0,
    BRP_COMPRESSION_LZ4 = 1,
    BRP_COMPRESSION_ZSTD = 2,
    BRP_COMPRESSION_SNAPPY = 3,
    BRP_COMPRESSION_GZIP = 4
} brp_compression_t;

/*
 * A codec transform. Writes a malloc()-allocated buffer to *out (freed by
 * the library with free()) and its length to *out_len. Returns 0 on
 * success, non-zero on failure.
 */
typedef int (*brp_codec_fn)(const uint8_t *in, size_t in_len, uint8_t **out,
                            size_t *out_len, void *opaque);

/*
 * Plugs in a codec this library does not carry itself. `none` and, when
 * built with zlib (BRP_WITH_ZLIB, the default), `gzip` are built in;
 * lz4, zstd and snappy are opt-in so an application that does not want
 * those dependencies does not acquire them. Registering gzip overrides the
 * built-in one (and supplies it when built without zlib).
 *
 * lz4 note: the broker uses lz4_flex::compress_prepend_size — a
 * little-endian u32 of the uncompressed length followed by a raw LZ4
 * *block*, not the LZ4 frame format.
 *
 * Passing NULL for both functions unregisters. Thread-safe.
 */
brp_err_t brp_register_codec(brp_compression_t codec, brp_codec_fn compress,
                             brp_codec_fn decompress, void *opaque);

/* 1 if the codec can currently be used for both directions. */
int brp_codec_available(brp_compression_t codec);

/* Maps Kafka's compression.type spelling ("none", "gzip", "lz4", "zstd",
 * "snappy") onto a codec. */
brp_err_t brp_compression_parse(const char *name, brp_compression_t *out);

/* ------------------------------------------------------------------------ */
/* Partitioning and checksums                                               */
/* ------------------------------------------------------------------------ */

/* Kafka's 32-bit murmur2. brp_murmur2(NULL, 0) == 275646681. */
uint32_t brp_murmur2(const void *data, size_t len);

/* Kafka's default partitioner: partitions[(murmur2(key) & 0x7fffffff) % n]. */
int32_t brp_partition_for_key(const void *key, size_t key_len,
                              const int32_t *partitions, size_t count);

/* CRC32C (Castagnoli), the checksum record batches carry. */
uint32_t brp_crc32c(const void *data, size_t len);

/* ------------------------------------------------------------------------ */
/* Records                                                                  */
/* ------------------------------------------------------------------------ */

/* A record header. value == NULL is a null value, distinct from empty. */
typedef struct brp_header {
    const char *key;
    const uint8_t *value;
    size_t value_len;
} brp_header_t;

/* A consumed record. Every pointer is owned by the enclosing array and
 * released by brp_records_free(). key/value are NULL when null. */
typedef struct brp_record {
    char *topic;
    int32_t partition;
    int64_t offset;
    uint8_t *key;
    size_t key_len;
    uint8_t *value; /* NULL = tombstone; non-NULL with len 0 = empty */
    size_t value_len;
    int64_t timestamp; /* absolute unix milliseconds */
    brp_header_t *headers;
    size_t header_count;
} brp_record_t;

void brp_records_free(brp_record_t *records, size_t count);

/* First value stored under `key`, or NULL (also NULL for a null value;
 * use brp_record_find_header to tell the two apart). */
const uint8_t *brp_record_header(const brp_record_t *record, const char *key,
                                 size_t *value_len);
const brp_header_t *brp_record_find_header(const brp_record_t *record,
                                           const char *key);

/* ------------------------------------------------------------------------ */
/* Client: connections, metadata and leader routing                         */
/* ------------------------------------------------------------------------ */

typedef struct brp_client brp_client_t;

typedef struct brp_client_config {
    const char *client_id;                  /* client.id                */
    int socket_connection_setup_timeout_ms; /* dial timeout             */
    int request_timeout_ms;                 /* socket I/O timeout       */
} brp_client_config_t;

void brp_client_config_init(brp_client_config_t *config);

/* `bootstrap` is "host:port". */
brp_err_t brp_client_new(const char *bootstrap,
                         const brp_client_config_t *config,
                         brp_client_t **out);
void brp_client_destroy(brp_client_t *client);

typedef struct brp_api_version_range {
    int32_t api_key;
    int32_t min_version;
    int32_t max_version;
} brp_api_version_range_t;

/* ApiVersions against the seed broker. *out and *broker_version are
 * released with brp_free(). broker_version may be NULL. */
brp_err_t brp_client_api_versions(brp_client_t *client,
                                  brp_api_version_range_t **out, size_t *count,
                                  char **broker_version);

typedef struct brp_broker_info {
    int32_t node_id;
    char *host;
    int32_t port;
    char *rack; /* "" when the broker has none */
} brp_broker_info_t;

typedef struct brp_partition_info {
    int32_t partition;
    int32_t leader;
    int32_t *replicas;
    size_t replica_count;
    int32_t *isr;
    size_t isr_count;
    int32_t leader_epoch;
} brp_partition_info_t;

typedef struct brp_topic_info {
    char *name;
    int32_t error_code;
    brp_partition_info_t *partitions;
    size_t partition_count;
} brp_topic_info_t;

typedef struct brp_metadata {
    brp_broker_info_t *brokers;
    size_t broker_count;
    int32_t controller_id;
    brp_topic_info_t *topics;
    size_t topic_count;
} brp_metadata_t;

/* Fetches fresh metadata for `topics` (count 0 = every topic) and updates
 * the routing cache. Release with brp_metadata_free(). */
brp_err_t brp_client_metadata(brp_client_t *client, const char *const *topics,
                              size_t topic_count, brp_metadata_t **out);
void brp_metadata_free(brp_metadata_t *metadata);

/* A topic's partition ids, ascending. Brokers that auto-create topics
 * create it on first reference. Release *out with brp_free(). */
brp_err_t brp_client_partitions(brp_client_t *client, const char *topic,
                                int32_t **out, size_t *count);

/* 1 while the seed connection is open. A connection that failed, timed
 * out or desynchronised is closed rather than reused (0 here) and is
 * redialled on its next request. */
int brp_client_connected(brp_client_t *client);

/* Forces the next routing decision for `topic` to use fresh metadata. */
brp_err_t brp_client_refresh(brp_client_t *client, const char *topic);

/* ------------------------------------------------------------------------ */
/* Producer                                                                 */
/* ------------------------------------------------------------------------ */

typedef struct brp_producer brp_producer_t;

typedef struct brp_producer_config {
    const char *client_id;           /* client.id                           */
    int32_t acks;                    /* acks: 0, 1, or -1 (all)             */
    size_t batch_size;               /* batch.size bytes per partition      */
    int linger_ms;                   /* linger.ms; 0 = send immediately     */
    const char *compression_type;    /* compression.type                    */
    int32_t request_timeout_ms;      /* request.timeout.ms                  */
    int retries;                     /* retries (retriable errors only)     */
    int retry_backoff_ms;            /* retry.backoff.ms                    */
    int delivery_timeout_ms;         /* delivery.timeout.ms                 */
    size_t buffer_memory;            /* buffer.memory; 0 = unbounded        */
    int max_block_ms;                /* max.block.ms on a full buffer       */
    int socket_connection_setup_timeout_ms;
} brp_producer_config_t;

/* acks=1, batch.size=16384, linger.ms=5, compression none,
 * request.timeout.ms=30000, retries=5, retry.backoff.ms=100,
 * delivery.timeout.ms=120000, buffer.memory=32MiB, max.block.ms=60000. */
void brp_producer_config_init(brp_producer_config_t *config);

#define BRP_PARTITION_ANY (-1)

/* One record to send. Everything is copied by brp_producer_send(). */
typedef struct brp_message {
    const char *topic;
    int32_t partition;   /* BRP_PARTITION_ANY: murmur2(key) or round-robin */
    const void *key;     /* NULL = null key                                */
    size_t key_len;
    const void *value;   /* NULL = tombstone                               */
    size_t value_len;
    const brp_header_t *headers;
    size_t header_count;
    int64_t timestamp_ms; /* <= 0: now                                     */
} brp_message_t;

/* Zeroes a message and sets partition = BRP_PARTITION_ANY. */
void brp_message_init(brp_message_t *message);

brp_err_t brp_producer_new(const char *bootstrap,
                           const brp_producer_config_t *config,
                           brp_producer_t **out);

/* Buffers one record; blocks up to max.block.ms when buffer.memory is
 * exhausted and then fails with BRP_ERR_BUFFER_FULL. Call
 * brp_producer_flush() to await delivery. */
brp_err_t brp_producer_send(brp_producer_t *producer,
                            const brp_message_t *message);

/* Sends one record on its own and returns its offset. A full round trip
 * per record: correct, and slow. offset may be NULL. */
brp_err_t brp_producer_send_sync(brp_producer_t *producer,
                                 const brp_message_t *message,
                                 int64_t *offset);

/* Sends every buffered record and waits for acknowledgement. Also reports
 * a failure from an earlier background (linger) flush, once. */
brp_err_t brp_producer_flush(brp_producer_t *producer);

/* Flushes, stops the linger thread and frees the producer. Always frees;
 * the return value is the flush result. */
brp_err_t brp_producer_close(brp_producer_t *producer);

/* The producer's routing client (owned by the producer). */
brp_client_t *brp_producer_client(brp_producer_t *producer);

/* ------------------------------------------------------------------------ */
/* Consumer (single partition reads, no group)                              */
/* ------------------------------------------------------------------------ */

typedef struct brp_consumer brp_consumer_t;

#define BRP_OFFSET_EARLIEST ((int64_t)-2)
#define BRP_OFFSET_LATEST ((int64_t)-1)

#define BRP_READ_UNCOMMITTED 0
#define BRP_READ_COMMITTED 1

typedef struct brp_consumer_config {
    const char *client_id;    /* client.id                                 */
    int32_t fetch_max_bytes;  /* fetch.max.bytes                           */
    int32_t fetch_min_bytes;  /* fetch.min.bytes                           */
    int32_t fetch_max_wait_ms;/* fetch.max.wait.ms (caps each fetch)       */
    int32_t isolation_level;  /* isolation.level                           */
    const char *client_rack;  /* client.rack, "" or NULL for none          */
    int max_poll_records;     /* max.poll.records (group consumer)         */
    int socket_connection_setup_timeout_ms;
} brp_consumer_config_t;

/* fetch.max.bytes=8MiB, fetch.min.bytes=1, fetch.max.wait.ms=500,
 * read_uncommitted, max.poll.records=500. */
void brp_consumer_config_init(brp_consumer_config_t *config);

brp_err_t brp_consumer_new(const char *bootstrap,
                           const brp_consumer_config_t *config,
                           brp_consumer_t **out);
void brp_consumer_close(brp_consumer_t *consumer);
brp_client_t *brp_consumer_client(brp_consumer_t *consumer);

/* Resolves BRP_OFFSET_EARLIEST, BRP_OFFSET_LATEST or a unix-ms timestamp
 * to an offset. */
brp_err_t brp_consumer_list_offsets(brp_consumer_t *consumer, const char *topic,
                                    int32_t partition, int64_t timestamp,
                                    int64_t *offset);

/* Reads one partition from `offset`, long-polling up to max_wait_ms
 * (capped by fetch.max.wait.ms). *records is caller-owned: release with
 * brp_records_free(). high_watermark may be NULL. */
brp_err_t brp_consumer_fetch(brp_consumer_t *consumer, const char *topic,
                             int32_t partition, int64_t offset,
                             int32_t max_wait_ms, brp_record_t **records,
                             size_t *count, int64_t *high_watermark);

/* ------------------------------------------------------------------------ */
/* Consumer groups                                                          */
/* ------------------------------------------------------------------------ */

typedef struct brp_group_consumer brp_group_consumer_t;

#define BRP_OFFSETS_TOPIC "__consumer_offsets"

typedef struct brp_group_config {
    const char *client_id;          /* client.id                           */
    int32_t session_timeout_ms;     /* session.timeout.ms (10000)          */
    int32_t rebalance_timeout_ms;   /* rebalance.timeout.ms (3000)         */
    int max_poll_interval_ms;       /* max.poll.interval.ms (300000)       */
    int auto_commit_interval_ms;    /* auto.commit.interval.ms; 0 = off    */
    const char *auto_offset_reset;  /* "earliest" | "latest" | "none"      */
    const char *assignor;           /* partition.assignment.strategy:
                                       "range" | "roundrobin" | "sticky"   */
    const char *group_instance_id;  /* group.instance.id; NULL = dynamic   */
    int max_poll_records;           /* max.poll.records (500)              */
    int32_t fetch_max_bytes;        /* fetch.max.bytes                     */
    int socket_connection_setup_timeout_ms;
} brp_group_config_t;

void brp_group_config_init(brp_group_config_t *config);

typedef struct brp_topic_partition {
    const char *topic;
    int32_t partition;
} brp_topic_partition_t;

typedef struct brp_partition_offset {
    char *topic;
    int32_t partition;
    int64_t offset; /* -1 = no committed offset */
} brp_partition_offset_t;

void brp_offsets_free(brp_partition_offset_t *offsets, size_t count);

/* Connects and starts the heartbeat thread. */
brp_err_t brp_group_consumer_new(const char *bootstrap, const char *group_id,
                                 const brp_group_config_t *config,
                                 brp_group_consumer_t **out);

/* Sets the topics this member wants a share of; takes effect on the next
 * poll (which rejoins the group). */
brp_err_t brp_group_consumer_subscribe(brp_group_consumer_t *group,
                                       const char *const *topics, size_t count);

/* Returns up to max.poll.records records, joining the group if needed.
 * *records is caller-owned: release with brp_records_free(). An empty
 * result (count 0) after timeout_ms is not an error. */
brp_err_t brp_group_consumer_poll(brp_group_consumer_t *group, int timeout_ms,
                                  brp_record_t **records, size_t *count);

/* Commits the positions of records delivered so far. At-least-once:
 * call it after processing, not before. */
brp_err_t brp_group_consumer_commit(brp_group_consumer_t *group);

/* Reads the group's committed offsets. count 0 asks for every partition
 * the group holds. Release with brp_offsets_free(). */
brp_err_t brp_group_consumer_committed(brp_group_consumer_t *group,
                                       const brp_topic_partition_t *partitions,
                                       size_t count,
                                       brp_partition_offset_t **out,
                                       size_t *out_count);

/* This member's current assignment. Release with brp_offsets_free()
 * (offset holds the next position to deliver). */
brp_err_t brp_group_consumer_assignment(brp_group_consumer_t *group,
                                        brp_partition_offset_t **out,
                                        size_t *out_count);

/* Commits, sends LeaveGroup so partitions move at once rather than after
 * session.timeout.ms, stops the heartbeat thread and frees. Always frees;
 * returns the commit result. */
brp_err_t brp_group_consumer_close(brp_group_consumer_t *group);

#ifdef __cplusplus
}
#endif

#endif /* BRAHMAPUTRA_H */
