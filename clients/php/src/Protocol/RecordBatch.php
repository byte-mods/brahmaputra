<?php

declare(strict_types=1);

namespace Brahmaputra\Protocol;

use Brahmaputra\Exception\ProtocolException;
use Brahmaputra\RecordHeader;

/**
 * Record batch codec.
 *
 * The broker never re-encodes a batch: it validates the header, stamps
 * base_offset and leader_epoch in place (both precede the CRC, so it stays
 * valid) and writes these exact bytes to disk. An encoding slip here
 * corrupts the log rather than failing a request.
 *
 * Layout (big-endian): base_offset i64, batch_length i32, leader_epoch
 * i32, magic i8, crc u32 (CRC32C over everything after it), attributes
 * u16, last_offset_delta i32, max_timestamp i64, [v2: producer_id i64,
 * producer_epoch i16, base_sequence i32], then the (possibly compressed)
 * records. Inside each record varints are *plain*, not zigzag, except the
 * timestamp delta.
 */
final class RecordBatch
{
    public const HEADER_LEN = 12;
    public const MIN_BATCH_LENGTH = 4 + 1 + 4 + 2 + 4 + 8;
    private const PRODUCER_EXTENSION_LEN = 8 + 2 + 4;

    public const MAGIC_V1 = 1;
    public const MAGIC_V2 = 2;

    private const COMPRESSION_MASK = 0x0007;
    private const HEADERS_BIT = 0x0008;
    /**
     * Some record in the batch has a null value (a tombstone). Set only
     * when one is present, so a batch without one encodes exactly as it
     * always did; it widens value lengths to length+1 with 0 meaning null.
     */
    private const NULL_VALUE_BIT = 0x0040;

    /**
     * @param list<array{key:?string, value:?string, timestampDelta:int, headers:list<RecordHeader>}> $records
     */
    public static function encode(array $records, int $maxTimestamp, int $codec = Compression::NONE): string
    {
        $hasHeaders = false;
        $hasNullValues = false;
        foreach ($records as $record) {
            $hasHeaders = $hasHeaders || $record['headers'] !== [];
            $hasNullValues = $hasNullValues || $record['value'] === null;
        }

        $payload = '';
        foreach ($records as $record) {
            $rec = $record['key'] === null
                ? "\x00"
                : Varint::encodeUnsigned(strlen($record['key']) + 1) . $record['key'];
            $value = $record['value'];
            if ($hasNullValues) {
                $rec .= $value === null ? "\x00" : Varint::encodeUnsigned(strlen($value) + 1) . $value;
            } else {
                $rec .= Varint::encodeUnsigned(strlen($value)) . $value;
            }
            $rec .= Varint::encodeUnsigned(Varint::zigzag64($record['timestampDelta']));
            if ($hasHeaders) {
                $rec .= Varint::encodeUnsigned(count($record['headers']));
                foreach ($record['headers'] as $header) {
                    $rec .= Varint::encodeUnsigned(strlen($header->key)) . $header->key;
                    $rec .= $header->value === null
                        ? "\x00"
                        : Varint::encodeUnsigned(strlen($header->value) + 1) . $header->value;
                }
            }
            $payload .= Varint::encodeUnsigned(strlen($rec)) . $rec;
        }

        $compressed = Compression::compress($codec, $payload);
        $attributes = $codec & self::COMPRESSION_MASK;
        if ($hasHeaders) {
            $attributes |= self::HEADERS_BIT;
        }
        if ($hasNullValues) {
            $attributes |= self::NULL_VALUE_BIT;
        }

        $afterCrc = pack('nNJ', $attributes, max(count($records) - 1, 0), $maxTimestamp) . $compressed;
        return pack('J', 0)                                   // base_offset, stamped by the broker
            . pack('N', self::MIN_BATCH_LENGTH + strlen($compressed))
            . pack('N', 0)                                    // leader_epoch, likewise
            . chr(self::MAGIC_V1)
            . pack('N', Crc32c::checksum($afterCrc))
            . $afterCrc;
    }

    /**
     * Decode one batch starting at $offset.
     *
     * @return array{0: array{baseOffset:int, maxTimestamp:int, records:list<array{key:?string,value:?string,timestampDelta:int,headers:list<RecordHeader>}>}, 1:int}
     */
    public static function decode(string $data, int $offset): array
    {
        $length = strlen($data);
        if ($length - $offset < self::HEADER_LEN) {
            throw new ProtocolException('truncated batch header');
        }
        $baseOffset = unpack('J', $data, $offset)[1];
        $batchLength = Protocol::signed32(unpack('N', $data, $offset + 8)[1]);
        if ($batchLength < self::MIN_BATCH_LENGTH) {
            throw new ProtocolException('batch_length too small');
        }
        $bodyAt = $offset + self::HEADER_LEN;
        $end = $bodyAt + $batchLength;
        if ($end > $length) {
            throw new ProtocolException('truncated batch body');
        }

        $magic = ord($data[$bodyAt + 4]);
        if ($magic !== self::MAGIC_V1 && $magic !== self::MAGIC_V2) {
            throw new ProtocolException("unsupported magic {$magic}");
        }
        $crcAt = $bodyAt + 5;
        $stored = unpack('N', $data, $crcAt)[1];
        $computed = Crc32c::checksum(substr($data, $crcAt + 4, $end - $crcAt - 4));
        if ($stored !== $computed) {
            throw new ProtocolException(sprintf('crc mismatch: stored 0x%08x, computed 0x%08x', $stored, $computed));
        }

        $cursor = $crcAt + 4;
        $attributes = unpack('n', $data, $cursor)[1];
        $maxTimestamp = unpack('J', $data, $cursor + 6)[1];
        $cursor += 14;
        if ($magic === self::MAGIC_V2) {
            $cursor += self::PRODUCER_EXTENSION_LEN;
        }

        $payload = Compression::decompress($attributes & self::COMPRESSION_MASK, substr($data, $cursor, $end - $cursor));
        $records = self::decodeRecords(
            $payload,
            ($attributes & self::HEADERS_BIT) !== 0,
            ($attributes & self::NULL_VALUE_BIT) !== 0,
        );
        return [['baseOffset' => $baseOffset, 'maxTimestamp' => $maxTimestamp, 'records' => $records], $end];
    }

    /** @return list<array{key:?string,value:?string,timestampDelta:int,headers:list<RecordHeader>}> */
    private static function decodeRecords(string $payload, bool $hasHeaders, bool $hasNullValues): array
    {
        $records = [];
        $pos = 0;
        $total = strlen($payload);
        while ($pos < $total) {
            $size = Varint::decodeUnsigned($payload, $pos);
            if ($size < 0 || $pos + $size > $total) {
                throw new ProtocolException('truncated record');
            }
            $end = $pos + $size;

            $keyLenPlusOne = Varint::decodeUnsigned($payload, $pos);
            $key = null;
            if ($keyLenPlusOne !== 0) {
                $key = self::take($payload, $pos, $keyLenPlusOne - 1, $end);
            }

            $rawValueLen = Varint::decodeUnsigned($payload, $pos);
            if ($hasNullValues && $rawValueLen === 0) {
                // A tombstone: null, which is what distinguishes a deletion
                // from a record whose value is empty.
                $value = null;
            } else {
                $value = self::take($payload, $pos, $hasNullValues ? $rawValueLen - 1 : $rawValueLen, $end);
            }

            $timestampDelta = Varint::unzigzag64(Varint::decodeUnsigned($payload, $pos));

            $headers = [];
            if ($hasHeaders) {
                $count = Varint::decodeUnsigned($payload, $pos);
                // A count is a promise about bytes that follow; one larger
                // than what is left is corrupt, and allocating on it would
                // let a two-byte record ask for gigabytes.
                if ($count < 0 || $count > $end - $pos) {
                    throw new ProtocolException('record header count exceeds record');
                }
                for ($i = 0; $i < $count; $i++) {
                    $headerKey = self::take($payload, $pos, Varint::decodeUnsigned($payload, $pos), $end);
                    $valuePlusOne = Varint::decodeUnsigned($payload, $pos);
                    $headerValue = $valuePlusOne === 0 ? null : self::take($payload, $pos, $valuePlusOne - 1, $end);
                    $headers[] = new RecordHeader($headerKey, $headerValue);
                }
            }

            if ($pos !== $end) {
                throw new ProtocolException('trailing bytes in record');
            }
            $records[] = ['key' => $key, 'value' => $value, 'timestampDelta' => $timestampDelta, 'headers' => $headers];
        }
        return $records;
    }

    private static function take(string $data, int &$pos, int $length, int $end): string
    {
        if ($length < 0 || $pos + $length > $end) {
            throw new ProtocolException('record field runs past its record');
        }
        $value = substr($data, $pos, $length);
        $pos += $length;
        return $value;
    }
}
