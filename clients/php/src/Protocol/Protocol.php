<?php

declare(strict_types=1);

namespace Brahmaputra\Protocol;

use Brahmaputra\Exception\ProtocolException;

/**
 * Wire constants and the frame codec.
 *
 * Three encodings share one connection and none agrees with the others:
 *
 *  - the frame header is fixed big-endian (int32 length, int16 api key,
 *    int16 api version, int32 correlation id, int16-prefixed client id);
 *  - the request/response body is BitPacker (see Writer/Reader);
 *  - a record batch is big-endian header fields plus plain varints (see
 *    RecordBatch).
 */
final class Protocol
{
    /** The BitPacker schema version every body carries as its first field. */
    public const SCHEMA_VERSION = '1.0.0';
    /** Wire version this client speaks. The broker requires an exact match. */
    public const API_VERSION = 4;

    public const READ_UNCOMMITTED = 0;
    public const READ_COMMITTED = 1;

    /** One complete frame, length prefix included. */
    public static function encodeFrame(int $apiKey, int $correlationId, ?string $clientId, string $body): string
    {
        $client = $clientId === null
            ? pack('n', 0xffff)
            : pack('n', strlen($clientId)) . $clientId;
        $payload = pack('nnN', $apiKey, self::API_VERSION, $correlationId & 0xffffffff) . $client;
        return pack('N', strlen($payload) + strlen($body)) . $payload . $body;
    }

    /**
     * Split a frame payload (length prefix already stripped) into its
     * correlation id and body.
     *
     * @return array{0:int,1:string}
     */
    public static function decodeFramePayload(string $payload): array
    {
        if (strlen($payload) < 10) {
            throw new ProtocolException('frame payload shorter than its header');
        }
        $header = unpack('napi/nversion/Ncorrelation/nclientLen', $payload);
        $correlation = self::signed32($header['correlation']);
        $clientLen = $header['clientLen'] >= 0x8000 ? $header['clientLen'] - 0x10000 : $header['clientLen'];
        $offset = 10 + max(0, $clientLen);
        if ($offset > strlen($payload)) {
            throw new ProtocolException('frame client id runs past the payload');
        }
        return [$correlation, substr($payload, $offset)];
    }

    public static function signed32(int $value): int
    {
        return $value >= 0x80000000 ? $value - 0x100000000 : $value;
    }
}
