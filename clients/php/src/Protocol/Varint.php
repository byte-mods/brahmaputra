<?php

declare(strict_types=1);

namespace Brahmaputra\Protocol;

use Brahmaputra\Exception\ProtocolException;

/**
 * Varint and zigzag primitives over PHP's 64-bit signed int.
 *
 * PHP has no unsigned integer, so a uint64 lives in an int's bit pattern:
 * values >= 2^63 read as negative. Two consequences shape every function
 * here: `>>` is arithmetic, so a logical shift needs the sign bits masked
 * off afterwards, and "is there more to write" must test `< 0` as well as
 * `>= 0x80`. `<<` wraps (PHP shifts the unsigned representation), which is
 * exactly what zigzag needs.
 */
final class Varint
{
    /** Logical right shift of a 64-bit pattern. */
    public static function lsr(int $value, int $bits): int
    {
        if ($bits === 0) {
            return $value;
        }
        return ($value >> $bits) & (PHP_INT_MAX >> ($bits - 1));
    }

    /** Unsigned LEB128 of a 64-bit pattern. */
    public static function encodeUnsigned(int $value): string
    {
        $out = '';
        while ($value < 0 || $value >= 0x80) {
            $out .= chr(($value & 0x7f) | 0x80);
            $value = self::lsr($value, 7);
        }
        return $out . chr($value);
    }

    /**
     * Decode an unsigned varint at $pos, advancing it. The result is the
     * 64-bit pattern (may read negative when the top bit is set).
     */
    public static function decodeUnsigned(string $data, int &$pos): int
    {
        $result = 0;
        $shift = 0;
        $length = strlen($data);
        while (true) {
            if ($pos >= $length) {
                throw new ProtocolException('truncated varint');
            }
            $byte = ord($data[$pos++]);
            $result |= ($byte & 0x7f) << $shift;
            if (($byte & 0x80) === 0) {
                return $result;
            }
            $shift += 7;
            if ($shift > 63) {
                throw new ProtocolException('varint overflows 64 bits');
            }
        }
    }

    public static function zigzag64(int $value): int
    {
        return ($value << 1) ^ ($value >> 63);
    }

    public static function unzigzag64(int $raw): int
    {
        return self::lsr($raw, 1) ^ -($raw & 1);
    }

    public static function zigzag32(int $value): int
    {
        if ($value < -0x80000000 || $value > 0x7fffffff) {
            throw new \InvalidArgumentException("{$value} does not fit an int32");
        }
        return (($value << 1) ^ ($value >> 31)) & 0xffffffff;
    }

    public static function unzigzag32(int $raw): int
    {
        $raw &= 0xffffffff;
        return ($raw >> 1) ^ -($raw & 1);
    }
}
