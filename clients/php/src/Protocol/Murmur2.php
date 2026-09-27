<?php

declare(strict_types=1);

namespace Brahmaputra\Protocol;

/**
 * Kafka's 32-bit murmur2, so a key lands on the same partition as it would
 * from any other Brahmaputra or Kafka client.
 *
 * Transcribed rather than imported: "some murmur2" is not good enough. PHP
 * ints are 64-bit, so every intermediate is kept as an unsigned 32-bit
 * value by masking. The product of two such values stays below 2^63
 * (0xFFFFFFFF * 0x5BD1E995 < 6.7e18), so multiplication never overflows
 * into a float before the mask truncates it, and `>>` on a masked
 * non-negative value is the logical shift Java's `>>>` performs.
 */
final class Murmur2
{
    private const SEED = 0x9747b28c;
    private const M = 0x5bd1e995;
    private const MASK = 0xffffffff;

    /** Hash as an unsigned 32-bit value. murmur2("") === 275646681. */
    public static function hash(string $data): int
    {
        $length = strlen($data);
        $h = (self::SEED ^ $length) & self::MASK;
        $chunks = intdiv($length, 4);

        for ($i = 0; $i < $chunks; $i++) {
            $k = unpack('V', $data, $i * 4)[1];
            $k = ($k * self::M) & self::MASK;
            $k ^= $k >> 24;
            $k = ($k * self::M) & self::MASK;
            $h = ($h * self::M) & self::MASK;
            $h ^= $k;
        }

        $tail = $chunks * 4;
        switch ($length - $tail) {
            case 3:
                $h ^= ord($data[$tail + 2]) << 16;
                // fall through
            case 2:
                $h ^= ord($data[$tail + 1]) << 8;
                // fall through
            case 1:
                $h ^= ord($data[$tail]);
                $h = ($h * self::M) & self::MASK;
        }

        $h ^= $h >> 13;
        $h = ($h * self::M) & self::MASK;
        $h ^= $h >> 15;
        return $h;
    }

    /** Kafka's default partitioner: positive(murmur2(key)) % count. */
    public static function partition(string $key, int $partitionCount): int
    {
        return (self::hash($key) & 0x7fffffff) % $partitionCount;
    }
}
