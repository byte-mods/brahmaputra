<?php

declare(strict_types=1);

namespace Brahmaputra\Protocol;

use Brahmaputra\Exception\BrahmaputraException;
use Brahmaputra\Exception\ProtocolException;

/**
 * Batch compression codecs.
 *
 * `none` and `gzip` are built in (gzip via ext-zlib, in the RFC 1952 gzip
 * container the broker's flate2 GzEncoder/GzDecoder use — gzencode, not
 * gzcompress, which is the zlib container). lz4, zstd and snappy are
 * opt-in through register(), so an application that does not want those
 * extensions does not need them.
 *
 * If you register lz4, the broker expects lz4_flex's
 * `compress_prepend_size` layout: a little-endian uint32 of the
 * uncompressed length, then a raw LZ4 *block* — not the LZ4 frame format.
 */
final class Compression
{
    public const NONE = 0;
    public const LZ4 = 1;
    public const ZSTD = 2;
    public const SNAPPY = 3;
    public const GZIP = 4;

    /** Caps decompressed output so a corrupt batch cannot exhaust memory. */
    public const MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024;

    private const NAMES = [
        'none' => self::NONE,
        'lz4' => self::LZ4,
        'zstd' => self::ZSTD,
        'snappy' => self::SNAPPY,
        'gzip' => self::GZIP,
    ];

    /** @var array<int, array{0:callable(string):string, 1:callable(string):string}> */
    private static array $codecs = [];

    /**
     * Plug in a codec this driver does not carry.
     *
     * @param callable(string):string $compress
     * @param callable(string):string $decompress
     */
    public static function register(int $codec, callable $compress, callable $decompress): void
    {
        self::$codecs[$codec] = [$compress, $decompress];
    }

    public static function parse(string $name): int
    {
        $codec = self::NAMES[strtolower($name)] ?? null;
        if ($codec === null) {
            throw new BrahmaputraException(
                "unknown compression.type {$name} (" . implode(', ', array_keys(self::NAMES)) . ')'
            );
        }
        return $codec;
    }

    public static function name(int $codec): string
    {
        $name = array_search($codec, self::NAMES, true);
        return $name === false ? "unknown({$codec})" : $name;
    }

    public static function compress(int $codec, string $payload): string
    {
        if (isset(self::$codecs[$codec])) {
            return (self::$codecs[$codec][0])($payload);
        }
        switch ($codec) {
            case self::NONE:
                return $payload;
            case self::GZIP:
                $out = gzencode($payload, 6);
                if ($out === false) {
                    throw new BrahmaputraException('gzip compression failed');
                }
                return $out;
        }
        throw new BrahmaputraException(
            self::name($codec) . ' compression is not available; register it with '
            . 'Compression::register() or use none/gzip'
        );
    }

    public static function decompress(int $codec, string $payload): string
    {
        if (isset(self::$codecs[$codec])) {
            return (self::$codecs[$codec][1])($payload);
        }
        switch ($codec) {
            case self::NONE:
                return $payload;
            case self::GZIP:
                $out = @gzdecode($payload, self::MAX_DECOMPRESSED_BYTES);
                if ($out === false) {
                    throw new ProtocolException('gzip batch payload failed to decompress');
                }
                return $out;
        }
        throw new BrahmaputraException(
            self::name($codec) . ' decompression is not available; register it with Compression::register()'
        );
    }
}
