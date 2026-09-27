%% Wire constants shared by every Brahmaputra module.
%%
%% Three encodings share one connection and none of them agrees with the
%% others: the frame header is fixed big-endian, request/response bodies
%% are BitPacker (zigzag varints, prefixed with ?SCHEMA_VERSION), and a
%% record batch is big-endian header fields followed by plain varints.

-ifndef(BRAHMAPUTRA_HRL).
-define(BRAHMAPUTRA_HRL, true).

-define(SCHEMA_VERSION, <<"1.0.0">>).
%% The broker requires an exact api version match.
-define(API_VERSION, 4).

-define(DEFAULT_CLIENT_ID, <<"brahmaputra-erlang">>).

%% Offset sentinels for list_offsets.
-define(EARLIEST, -2).
-define(LATEST, -1).

%% Isolation levels.
-define(READ_UNCOMMITTED, 0).
-define(READ_COMMITTED, 1).

%% API keys.
-define(API_PRODUCE, 0).
-define(API_FETCH, 1).
-define(API_LIST_OFFSETS, 2).
-define(API_METADATA, 3).
-define(API_REPLICA_FETCH, 4).
-define(API_OFFSETS_FOR_LEADER_EPOCH, 5).
-define(API_INIT_PRODUCER_ID, 6).
-define(API_JOIN_GROUP, 7).
-define(API_SYNC_GROUP, 8).
-define(API_HEARTBEAT, 9).
-define(API_OFFSET_COMMIT, 10).
-define(API_OFFSET_FETCH, 11).
-define(API_LIST_GROUPS, 12).
-define(API_DESCRIBE_GROUP, 13).
-define(API_API_VERSIONS, 14).
-define(API_PRODUCE_MULTI, 15).
-define(API_FETCH_MULTI, 16).
-define(API_AUTHENTICATE, 17).
-define(API_LEAVE_GROUP, 18).

%% Error codes.
-define(ERR_NONE, 0).
-define(ERR_UNKNOWN_TOPIC_OR_PARTITION, 1).
-define(ERR_OFFSET_OUT_OF_RANGE, 2).
-define(ERR_INVALID_REQUEST, 3).
-define(ERR_UNSUPPORTED_VERSION, 4).
-define(ERR_INTERNAL, 5).
-define(ERR_NOT_LEADER_OR_FOLLOWER, 6).
-define(ERR_FENCED_BROKER_EPOCH, 7).
-define(ERR_FENCED_LEADER_EPOCH, 8).
-define(ERR_UNKNOWN_LEADER_EPOCH, 9).
-define(ERR_NOT_ENOUGH_REPLICAS, 10).
-define(ERR_FENCED_PRODUCER_EPOCH, 11).
-define(ERR_OUT_OF_ORDER_SEQUENCE, 12).
-define(ERR_UNKNOWN_MEMBER_ID, 13).
-define(ERR_REBALANCE_IN_PROGRESS, 14).
-define(ERR_NOT_COORDINATOR, 15).
-define(ERR_ILLEGAL_GENERATION, 16).
-define(ERR_COORDINATOR_LOAD_IN_PROGRESS, 17).
-define(ERR_SASL_AUTHENTICATION_FAILED, 18).
-define(ERR_AUTHORIZATION_FAILED, 19).

%% Compression codec ids, as the broker's batch attributes spell them.
-define(COMPRESSION_NONE, 0).
-define(COMPRESSION_LZ4, 1).
-define(COMPRESSION_ZSTD, 2).
-define(COMPRESSION_SNAPPY, 3).
-define(COMPRESSION_GZIP, 4).

%% The internal topic whose partition leaders coordinate groups.
-define(OFFSETS_TOPIC, <<"__consumer_offsets">>).

-endif.
