<?php

declare(strict_types=1);

namespace Brahmaputra\Protocol;

/**
 * CRC32C (Castagnoli). Record batches use it rather than zlib's CRC32, so
 * PHP's crc32() is the wrong function.
 *
 * ext-hash ships `crc32c` since PHP 7.4 and it is used when present — it is
 * orders of magnitude faster than a PHP loop. The table implementation is
 * the fallback and the reference the two are checked against.
 */
final class Crc32c
{
    /** @var list<int>|null */
    private static ?array $table = null;
    private static ?bool $native = null;

    public static function checksum(string $data): int
    {
        if (self::$native ??= in_array('crc32c', hash_algos(), true)) {
            return unpack('N', hash('crc32c', $data, true))[1];
        }
        return self::portable($data);
    }

    /** Pure-PHP table-driven CRC32C, reflected polynomial 0x82F63B78. */
    public static function portable(string $data): int
    {
        $table = self::$table ??= self::buildTable();
        $crc = 0xffffffff;
        $length = strlen($data);
        for ($i = 0; $i < $length; $i++) {
            $crc = $table[($crc ^ ord($data[$i])) & 0xff] ^ ($crc >> 8);
        }
        return $crc ^ 0xffffffff;
    }

    /** @return list<int> */
    private static function buildTable(): array
    {
        $table = [];
        for ($index = 0; $index < 256; $index++) {
            $crc = $index;
            for ($bit = 0; $bit < 8; $bit++) {
                $crc = ($crc & 1) ? (($crc >> 1) ^ 0x82f63b78) : ($crc >> 1);
            }
            $table[] = $crc;
        }
        return $table;
    }
}
