<?php

declare(strict_types=1);

namespace Brahmaputra;

use Brahmaputra\Exception\ConnectionException;
use Brahmaputra\Exception\ProtocolException;
use Brahmaputra\Exception\ServerException;
use Brahmaputra\Exception\TimeoutException;
use Brahmaputra\Protocol\ApiKey;
use Brahmaputra\Protocol\ErrorCode;
use Brahmaputra\Protocol\Protocol;
use Brahmaputra\Protocol\Reader;
use Brahmaputra\Protocol\Writer;

/**
 * One blocking TCP connection to one broker (stream_socket_client).
 *
 * PHP has no threads, so a request is written and its response read
 * before the call returns; there is at most one request in flight per
 * connection. Responses are still matched by correlation id, and any frame
 * with a different id (a late answer to a request that timed out) is
 * discarded. A socket error or timeout closes the connection, because
 * after a partial read its framing can no longer be trusted; the Router
 * reopens it on next use.
 */
final class Connection
{
    private const SCRAM_MECHANISM = 'SCRAM-SHA-256';
    private const MAX_FRAME = 512 * 1024 * 1024;

    /** @var resource|null */
    private $socket;
    private int $correlation = 0;

    /** @param resource $socket */
    private function __construct(
        $socket,
        public readonly string $host,
        public readonly int $port,
        private readonly string $clientId,
        private int $requestTimeoutMs,
    ) {
        $this->socket = $socket;
    }

    public static function open(
        string $host,
        int $port,
        string $clientId = 'brahmaputra-php',
        int $connectTimeoutMs = 10000,
        int $requestTimeoutMs = 30000,
    ): self {
        $context = stream_context_create(['socket' => ['tcp_nodelay' => true]]);
        $address = str_contains($host, ':') ? "tcp://[{$host}]:{$port}" : "tcp://{$host}:{$port}";
        $errno = 0;
        $errstr = '';
        $socket = @stream_socket_client(
            $address,
            $errno,
            $errstr,
            $connectTimeoutMs / 1000,
            STREAM_CLIENT_CONNECT,
            $context,
        );
        if ($socket === false) {
            throw new ConnectionException("connect to {$host}:{$port} failed: {$errstr} ({$errno})");
        }
        stream_set_blocking($socket, true);
        stream_set_write_buffer($socket, 0);
        return new self($socket, $host, $port, $clientId, $requestTimeoutMs);
    }

    public function isOpen(): bool
    {
        return $this->socket !== null && !feof($this->socket);
    }

    /**
     * True once a socket error or timeout has closed this connection. A
     * broken connection is never reused: the Router dials a fresh one.
     */
    public function isBroken(): bool
    {
        return $this->socket === null;
    }

    /** Change how long one round trip may take before it fails and breaks the connection. */
    public function setRequestTimeout(int $timeoutMs): void
    {
        $this->requestTimeoutMs = max(1, $timeoutMs);
    }

    public function close(): void
    {
        if ($this->socket !== null) {
            @fclose($this->socket);
            $this->socket = null;
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * Send one request and return the matching response body.
     *
     * @param int|null $timeoutMs how long to wait for the answer; defaults
     *                            to request.timeout.ms
     */
    public function request(int $apiKey, string $body, ?int $timeoutMs = null): string
    {
        $correlationId = $this->nextCorrelation();
        $timeoutMs ??= $this->requestTimeoutMs;
        $this->write(Protocol::encodeFrame($apiKey, $correlationId, $this->clientId, $body), $timeoutMs);
        $deadline = Config::nowMs() + $timeoutMs;
        while (true) {
            $payload = $this->readFrame($deadline);
            [$id, $responseBody] = Protocol::decodeFramePayload($payload);
            if ($id === $correlationId) {
                return $responseBody;
            }
            // A stale answer to an earlier request (acks=0, or one that
            // timed out on our side). Drop it and keep reading.
        }
    }

    /** Send without waiting for a response (acks=0). */
    public function sendOneway(int $apiKey, string $body): void
    {
        $this->write(Protocol::encodeFrame($apiKey, $this->nextCorrelation(), $this->clientId, $body), $this->requestTimeoutMs);
    }

    private function nextCorrelation(): int
    {
        $this->correlation = ($this->correlation + 1) & 0x7fffffff;
        return $this->correlation;
    }

    private function write(string $frame, int $timeoutMs): void
    {
        $socket = $this->requireSocket();
        $this->setTimeout($timeoutMs);
        $written = 0;
        $length = strlen($frame);
        while ($written < $length) {
            $n = @fwrite($socket, $written === 0 ? $frame : substr($frame, $written));
            if ($n === false || $n === 0) {
                $timedOut = stream_get_meta_data($socket)['timed_out'] ?? false;
                $this->close();
                if ($timedOut) {
                    throw new TimeoutException("write to {$this->host}:{$this->port} timed out");
                }
                throw new ConnectionException("write to {$this->host}:{$this->port} failed");
            }
            $written += $n;
        }
    }

    private function readFrame(int $deadlineMs): string
    {
        $prefix = $this->readExact(4, $deadlineMs);
        $length = Protocol::signed32(unpack('N', $prefix)[1]);
        if ($length < 0 || $length > self::MAX_FRAME) {
            $this->close();
            throw new ProtocolException("implausible frame length {$length}");
        }
        return $this->readExact($length, $deadlineMs);
    }

    private function readExact(int $length, int $deadlineMs): string
    {
        $socket = $this->requireSocket();
        $out = '';
        while (strlen($out) < $length) {
            $remaining = $deadlineMs - Config::nowMs();
            if ($remaining <= 0) {
                $this->close();
                throw new TimeoutException("request to {$this->host}:{$this->port} timed out");
            }
            $this->setTimeout($remaining);
            $chunk = @fread($socket, min($length - strlen($out), 1 << 20));
            if ($chunk === false || $chunk === '') {
                $meta = stream_get_meta_data($socket);
                if ($meta['timed_out'] ?? false) {
                    continue; // re-checked against the deadline above
                }
                if ($meta['eof'] ?? feof($socket)) {
                    $this->close();
                    throw new ConnectionException("connection to {$this->host}:{$this->port} closed by broker");
                }
                if ($chunk === false) {
                    $this->close();
                    throw new ConnectionException("read from {$this->host}:{$this->port} failed");
                }
                continue;
            }
            $out .= $chunk;
        }
        return $out;
    }

    private function setTimeout(int $ms): void
    {
        $ms = max(1, $ms);
        stream_set_timeout($this->requireSocket(), intdiv($ms, 1000), ($ms % 1000) * 1000);
    }

    /** @return resource */
    private function requireSocket()
    {
        if ($this->socket === null) {
            throw new ConnectionException("connection to {$this->host}:{$this->port} is closed");
        }
        return $this->socket;
    }

    /**
     * Ask the broker what it speaks. This is the one call that works
     * across a version mismatch.
     *
     * @return array{versions: list<array{apiKey:int,minVersion:int,maxVersion:int}>, brokerVersion: string}
     */
    public function apiVersions(): array
    {
        $body = Writer::body()->string('brahmaputra-php')->string('0.1.0')->bytes();
        $reader = Reader::body($this->request(ApiKey::API_VERSIONS, $body));
        $code = $reader->int32();
        if ($code !== ErrorCode::NONE) {
            throw new ServerException($code, 'api_versions');
        }
        $versions = [];
        for ($count = $reader->count(); $count > 0; $count--) {
            $versions[] = [
                'apiKey' => $reader->int32(),
                'minVersion' => $reader->int32(),
                'maxVersion' => $reader->int32(),
            ];
        }
        return ['versions' => $versions, 'brokerVersion' => $reader->string()];
    }

    /**
     * Bind a principal to this connection with SCRAM-SHA-256 (RFC 5802).
     * The password never crosses the wire, only a proof derived from it.
     *
     * @return array{principal:string, role:string}
     */
    public function authenticate(string $username, string $password): array
    {
        $clientNonce = str_replace(',', '.', base64_encode(random_bytes(18)));
        $bare = "n={$username},r={$clientNonce}";
        $first = $this->authenticateStep($username, '', self::SCRAM_MECHANISM, "n,,{$bare}");
        if ($first['done']) {
            throw new ProtocolException('broker ended the SCRAM exchange before it began');
        }
        $serverFirst = $first['payload'];
        $nonce = self::scramField($serverFirst, 'r');
        $salt = self::scramField($serverFirst, 's');
        $iterations = (int) (self::scramField($serverFirst, 'i') ?? '0');
        if ($nonce === null || $salt === null || $iterations <= 0) {
            throw new ProtocolException('malformed SCRAM server-first message');
        }
        // The server must extend this client's nonce, which is what makes
        // the exchange this one rather than a replay.
        if (!str_starts_with($nonce, $clientNonce)) {
            throw new ProtocolException('SCRAM server nonce does not extend the client nonce');
        }
        $withoutProof = "c=biws,r={$nonce}";
        $authMessage = "{$bare},{$serverFirst},{$withoutProof}";
        $salted = hash_pbkdf2('sha256', $password, (string) base64_decode($salt, true), $iterations, 32, true);
        $clientKey = hash_hmac('sha256', 'Client Key', $salted, true);
        $storedKey = hash('sha256', $clientKey, true);
        $signature = hash_hmac('sha256', $authMessage, $storedKey, true);
        $proof = base64_encode($clientKey ^ $signature);
        $final = $this->authenticateStep($username, '', self::SCRAM_MECHANISM, "{$withoutProof},p={$proof}");
        return ['principal' => $final['principal'], 'role' => $final['role']];
    }

    /**
     * SASL/PLAIN: sends the password itself. The broker refuses it on a
     * plaintext listener.
     *
     * @return array{principal:string, role:string}
     */
    public function authenticatePlain(string $username, string $password): array
    {
        $result = $this->authenticateStep($username, $password, 'PLAIN', '');
        return ['principal' => $result['principal'], 'role' => $result['role']];
    }

    /** @return array{principal:string, role:string, payload:string, done:bool} */
    private function authenticateStep(string $username, string $password, string $mechanism, string $payload): array
    {
        $body = Writer::body()->string($username)->string($password)->string($mechanism)->string($payload)->bytes();
        $reader = Reader::body($this->request(ApiKey::AUTHENTICATE, $body));
        $code = $reader->int32();
        $principal = $reader->string();
        $role = $reader->string();
        $responsePayload = $reader->string();
        $done = $reader->bool();
        if ($code !== ErrorCode::NONE) {
            throw new ServerException($code, 'authenticate');
        }
        return ['principal' => $principal, 'role' => $role, 'payload' => $responsePayload, 'done' => $done];
    }

    private static function scramField(string $message, string $key): ?string
    {
        foreach (explode(',', $message) as $part) {
            if (str_starts_with($part, "{$key}=")) {
                return substr($part, strlen($key) + 1);
            }
        }
        return null;
    }
}
