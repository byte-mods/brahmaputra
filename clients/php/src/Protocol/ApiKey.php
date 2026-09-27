<?php

declare(strict_types=1);

namespace Brahmaputra\Protocol;

/** Request types, as the frame header's int16 api key. */
final class ApiKey
{
    public const PRODUCE = 0;
    public const FETCH = 1;
    public const LIST_OFFSETS = 2;
    public const METADATA = 3;
    public const REPLICA_FETCH = 4;
    public const OFFSETS_FOR_LEADER_EPOCH = 5;
    public const INIT_PRODUCER_ID = 6;
    public const JOIN_GROUP = 7;
    public const SYNC_GROUP = 8;
    public const HEARTBEAT = 9;
    public const OFFSET_COMMIT = 10;
    public const OFFSET_FETCH = 11;
    public const LIST_GROUPS = 12;
    public const DESCRIBE_GROUP = 13;
    public const API_VERSIONS = 14;
    public const PRODUCE_MULTI = 15;
    public const FETCH_MULTI = 16;
    public const AUTHENTICATE = 17;
    public const LEAVE_GROUP = 18;
}
