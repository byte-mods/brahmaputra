<?php

declare(strict_types=1);

namespace Brahmaputra\Protocol;

/** Broker error codes. */
final class ErrorCode
{
    public const NONE = 0;
    public const UNKNOWN_TOPIC_OR_PARTITION = 1;
    public const OFFSET_OUT_OF_RANGE = 2;
    public const INVALID_REQUEST = 3;
    public const UNSUPPORTED_VERSION = 4;
    public const INTERNAL = 5;
    public const NOT_LEADER_OR_FOLLOWER = 6;
    public const FENCED_BROKER_EPOCH = 7;
    public const FENCED_LEADER_EPOCH = 8;
    public const UNKNOWN_LEADER_EPOCH = 9;
    public const NOT_ENOUGH_REPLICAS = 10;
    public const FENCED_PRODUCER_EPOCH = 11;
    public const OUT_OF_ORDER_SEQUENCE = 12;
    public const UNKNOWN_MEMBER_ID = 13;
    public const REBALANCE_IN_PROGRESS = 14;
    public const NOT_COORDINATOR = 15;
    public const ILLEGAL_GENERATION = 16;
    public const COORDINATOR_LOAD_IN_PROGRESS = 17;
    public const SASL_AUTHENTICATION_FAILED = 18;
    public const AUTHORIZATION_FAILED = 19;

    /**
     * Codes the broker only ever returns *before* it appends anything, so a
     * retry cannot duplicate a record. Anything else fails identically
     * however often it is sent, or means state is already broken.
     */
    private const RETRIABLE = [
        self::NOT_LEADER_OR_FOLLOWER => true,
        self::FENCED_LEADER_EPOCH => true,
        self::UNKNOWN_LEADER_EPOCH => true,
        self::NOT_ENOUGH_REPLICAS => true,
        self::COORDINATOR_LOAD_IN_PROGRESS => true,
        self::INTERNAL => true,
    ];

    public static function isRetriable(int $code): bool
    {
        return isset(self::RETRIABLE[$code]);
    }

    /** True when the error means the cached leader route is stale. */
    public static function isStaleRoute(int $code): bool
    {
        return $code === self::NOT_LEADER_OR_FOLLOWER
            || $code === self::FENCED_LEADER_EPOCH
            || $code === self::UNKNOWN_LEADER_EPOCH;
    }

    public static function name(int $code): string
    {
        static $names = null;
        if ($names === null) {
            $names = array_flip(array_filter(
                (new \ReflectionClass(self::class))->getConstants(),
                'is_int',
            ));
        }
        return $names[$code] ?? 'UNKNOWN';
    }
}
