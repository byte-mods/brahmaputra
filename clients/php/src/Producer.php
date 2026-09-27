<?php

declare(strict_types=1);

namespace Brahmaputra;

use Brahmaputra\Exception\BrahmaputraException;
use Brahmaputra\Exception\BufferFullException;
use Brahmaputra\Exception\ConnectionException;
use Brahmaputra\Exception\ServerException;
use Brahmaputra\Exception\TimeoutException;
use Brahmaputra\Protocol\ApiKey;
use Brahmaputra\Protocol\Compression;
use Brahmaputra\Protocol\ErrorCode;
use Brahmaputra\Protocol\Murmur2;
use Brahmaputra\Protocol\Reader;
use Brahmaputra\Protocol\RecordBatch;
use Brahmaputra\Protocol\Writer;

/**
 * A batching producer.
 *
 * PHP has no background threads, so there is no sender thread either:
 * batching happens in-process and batches go out from inside your calls.
 * A partition's batch is sent
 *
 *  - by send() when it reaches `batch.size` bytes, or immediately when
 *    `linger.ms` is 0;
 *  - by send(), poll() or flush() once its oldest record has waited
 *    `linger.ms` — whichever of those you call first after the deadline;
 *  - by flush() and close() unconditionally.
 *
 * So a long-lived worker should call poll(0) from its loop (as with
 * php-rdkafka), and every script must call flush() or close() before it
 * ends; the destructor flushes as a last resort. Sends are synchronous
 * round trips, which also means a batch's delivery outcome is known when
 * the call that sent it returns: failures are thrown from that call, or
 * handed to `delivery.report.callback` if you configure one.
 *
 * Retries: a batch the broker refuses with a retriable code (returned
 * before it appends, so no duplicate is possible) is retried up to
 * `retries` times, `retry.backoff.ms` apart, within `delivery.timeout.ms`
 * of its oldest record. A connection failure mid-request is retried too,
 * as Kafka's non-idempotent producer does: the broker may have appended
 * the batch before the connection dropped, so that case is at-least-once.
 */
final class Producer
{
    /** @return array<string, mixed> */
    public static function defaults(): array
    {
        return [
            'bootstrap.servers' => null,
            'client.id' => 'brahmaputra-php',
            /** 0 fire-and-forget, 1 leader append, -1 / "all" every in-sync replica. */
            'acks' => 1,
            'batch.size' => 16384,
            /**
             * Kafka's default is 0; this is 5 because an unbatched producer
             * is slow enough to look broken.
             */
            'linger.ms' => 5,
            'compression.type' => 'none',
            'request.timeout.ms' => 30000,
            'retries' => 5,
            'retry.backoff.ms' => 100,
            'delivery.timeout.ms' => 120000,
            'buffer.memory' => 32 * 1024 * 1024,
            'max.block.ms' => 60000,
            'socket.connection.setup.timeout.ms' => 10000,
            /** callable(DeliveryReport): void, or null to throw failures instead. */
            'delivery.report.callback' => null,
        ];
    }

    /** @var array<string, mixed> */
    private array $config;
    private Router $router;
    private int $codec;
    private int $acks;
    /** @var array<string, array{topic:string, partition:int, records:list<array{key:?string,value:?string,headers:list<RecordHeader>,timestamp:int,createdMs:int}>, bytes:int, firstMs:int}> */
    private array $slots = [];
    private int $bufferedBytes = 0;
    private int $roundRobin = 0;
    private bool $closed = false;

    /** @param array<string, mixed> $config Kafka-style keys; see defaults() */
    public function __construct(array $config)
    {
        $this->config = Config::resolve(self::defaults(), $config, 'producer');
        $acks = $this->config['acks'];
        $this->acks = $acks === 'all' ? -1 : (int) $acks;
        if (!in_array($this->acks, [0, 1, -1], true)) {
            throw new \InvalidArgumentException("acks must be 0, 1, -1 or \"all\", got {$acks}");
        }
        $this->codec = Compression::parse((string) $this->config['compression.type']);
        $this->router = new Router(
            $this->config['bootstrap.servers'],
            $this->config['client.id'],
            (int) $this->config['request.timeout.ms'],
            (int) $this->config['socket.connection.setup.timeout.ms'],
        );
    }

    public function router(): Router
    {
        return $this->router;
    }

    /**
     * Buffer one record for delivery.
     *
     * A null $value is a tombstone (deletes $key on a compacted topic);
     * "" is an ordinary empty value. Without $partition a keyed record goes
     * to murmur2(key) % partitions and a keyless one round-robins.
     *
     * @param list<RecordHeader> $headers
     * @param int|null $timestampMs record time; defaults to now
     */
    public function send(
        string $topic,
        ?string $value,
        ?string $key = null,
        array $headers = [],
        ?int $partition = null,
        ?int $timestampMs = null,
    ): void {
        $this->ensureOpen();
        // Deliver whatever has lingered long enough before adding more.
        $this->sendExpired();

        $target = $partition ?? $this->choosePartition($topic, $key);
        $size = self::estimate($value, $key, $headers);
        $this->reserve($size);

        $slot = $topic . "\0" . $target;
        $now = Config::nowMs();
        if (!isset($this->slots[$slot])) {
            $this->slots[$slot] = ['topic' => $topic, 'partition' => $target, 'records' => [], 'bytes' => 0, 'firstMs' => $now];
        }
        if ($this->slots[$slot]['records'] === []) {
            $this->slots[$slot]['firstMs'] = $now;
        }
        $this->slots[$slot]['records'][] = [
            'key' => $key,
            'value' => $value,
            'headers' => array_values($headers),
            'timestamp' => $timestampMs ?? Config::wallMs(),
            'createdMs' => $now,
        ];
        $this->slots[$slot]['bytes'] += $size;

        if ((int) $this->config['linger.ms'] <= 0 || $this->slots[$slot]['bytes'] >= (int) $this->config['batch.size']) {
            $this->flushSlots([$slot]);
        }
    }

    /**
     * Send one record on its own, bypassing the buffer, and return its
     * offset (-1 with acks=0). A full round trip per record: correct, and slow.
     *
     * @param list<RecordHeader> $headers
     */
    public function sendSync(
        string $topic,
        ?string $value,
        ?string $key = null,
        array $headers = [],
        ?int $partition = null,
        ?int $timestampMs = null,
    ): int {
        $this->ensureOpen();
        $target = $partition ?? $this->choosePartition($topic, $key);
        $record = [
            'key' => $key,
            'value' => $value,
            'headers' => array_values($headers),
            'timestamp' => $timestampMs ?? Config::wallMs(),
            'createdMs' => Config::nowMs(),
        ];
        return $this->produce($topic, $target, [$record]);
    }

    /**
     * Send every batch whose linger.ms has elapsed. With $timeoutMs > 0,
     * waits up to that long for the next batch to become due and sends it
     * too. Returns the number of batches sent.
     *
     * Call it from a long-running loop so lingering batches do not wait
     * for the next send().
     */
    public function poll(int $timeoutMs = 0): int
    {
        $this->ensureOpen();
        $sent = $this->sendExpired();
        if ($timeoutMs > 0) {
            $deadline = Config::nowMs() + $timeoutMs;
            while ($this->hasBuffered() && Config::nowMs() < $deadline) {
                $wait = min($this->nextDueMs() - Config::nowMs(), $deadline - Config::nowMs());
                Config::sleepMs(max(1, $wait));
                $sent += $this->sendExpired();
            }
        }
        return $sent;
    }

    /** Send every buffered record now and wait for the broker to acknowledge them. */
    public function flush(): void
    {
        $this->flushSlots(array_keys($this->slots));
    }

    /** Flush, then close every connection. */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        try {
            $this->flush();
        } finally {
            $this->closed = true;
            $this->router->close();
        }
    }

    /** Unflushed record bytes currently held client-side. */
    public function bufferedBytes(): int
    {
        return $this->bufferedBytes;
    }

    public function __destruct()
    {
        if ($this->closed) {
            return;
        }
        try {
            $this->close();
        } catch (\Throwable $error) {
            // A destructor cannot usefully throw; say so instead of losing
            // the records silently.
            trigger_error('brahmaputra producer lost buffered records at shutdown: ' . $error->getMessage(), E_USER_WARNING);
        }
    }

    private function choosePartition(string $topic, ?string $key): int
    {
        $partitions = $this->router->partitions($topic);
        if ($key === null) {
            return $partitions[$this->roundRobin++ % count($partitions)];
        }
        return $partitions[Murmur2::partition($key, count($partitions))];
    }

    /** @param list<RecordHeader> $headers */
    private static function estimate(?string $value, ?string $key, array $headers): int
    {
        $size = strlen($value ?? '') + strlen($key ?? '') + 16;
        foreach ($headers as $header) {
            $size += strlen($header->key) + strlen($header->value ?? '') + 4;
        }
        return $size;
    }

    /**
     * Wait until $size more bytes may be buffered.
     *
     * This is what makes buffer.memory real: a producer faster than its
     * broker is held here instead of growing without limit. With no
     * background sender the only thing that can drain the buffer while we
     * wait is a batch whose linger.ms falls due, so that is what the loop
     * sends; if nothing does within max.block.ms, the send fails.
     */
    private function reserve(int $size): void
    {
        $limit = (int) $this->config['buffer.memory'];
        if ($limit <= 0 || $size >= $limit) {
            // A record larger than the whole budget is admitted rather than
            // waiting on a condition that can never hold; refusing
            // oversized records is the broker's job.
            $this->bufferedBytes += $size;
            return;
        }
        $maxBlock = (int) $this->config['max.block.ms'];
        $deadline = Config::nowMs() + $maxBlock;
        while ($this->bufferedBytes + $size > $limit) {
            $this->sendExpired();
            if ($this->bufferedBytes + $size <= $limit) {
                break;
            }
            $now = Config::nowMs();
            if ($now >= $deadline) {
                throw new BufferFullException(sprintf(
                    'producer buffer full: %d of %d bytes unflushed after max.block.ms=%d',
                    $this->bufferedBytes,
                    $limit,
                    $maxBlock,
                ));
            }
            Config::sleepMs(max(1, min(20, $deadline - $now, $this->nextDueMs() - $now)));
        }
        $this->bufferedBytes += $size;
    }

    private function hasBuffered(): bool
    {
        foreach ($this->slots as $slot) {
            if ($slot['records'] !== []) {
                return true;
            }
        }
        return false;
    }

    private function nextDueMs(): int
    {
        $due = PHP_INT_MAX;
        $linger = (int) $this->config['linger.ms'];
        foreach ($this->slots as $slot) {
            if ($slot['records'] !== []) {
                $due = min($due, $slot['firstMs'] + $linger);
            }
        }
        return $due;
    }

    private function sendExpired(): int
    {
        $now = Config::nowMs();
        $linger = (int) $this->config['linger.ms'];
        $due = [];
        foreach ($this->slots as $name => $slot) {
            if ($slot['records'] !== [] && $now - $slot['firstMs'] >= $linger) {
                $due[] = $name;
            }
        }
        if ($due !== []) {
            $this->flushSlots($due);
        }
        return count($due);
    }

    /**
     * Send the named slots. Every slot is attempted even if one fails; the
     * first failure is then thrown (or each is reported to the callback).
     *
     * @param list<string> $names
     */
    private function flushSlots(array $names): void
    {
        $firstError = null;
        $failures = 0;
        foreach ($names as $name) {
            $slot = $this->slots[$name] ?? null;
            if ($slot === null || $slot['records'] === []) {
                continue;
            }
            unset($this->slots[$name]);
            $this->bufferedBytes = max(0, $this->bufferedBytes - $slot['bytes']);
            try {
                $offset = $this->produce($slot['topic'], $slot['partition'], $slot['records']);
                $this->report(new DeliveryReport($slot['topic'], $slot['partition'], $offset, count($slot['records'])));
            } catch (\Throwable $error) {
                $failures++;
                if (!$this->report(new DeliveryReport($slot['topic'], $slot['partition'], -1, count($slot['records']), $error))) {
                    $firstError ??= $error;
                }
            }
        }
        if ($firstError !== null) {
            if ($failures > 1) {
                throw new BrahmaputraException(
                    "{$failures} batches failed to deliver; first: " . $firstError->getMessage(),
                    0,
                    $firstError,
                );
            }
            throw $firstError;
        }
    }

    /** Returns true when a callback consumed the report. */
    private function report(DeliveryReport $report): bool
    {
        $callback = $this->config['delivery.report.callback'];
        if ($callback === null) {
            return false;
        }
        $callback($report);
        return true;
    }

    /**
     * Encode and deliver one batch; returns its base offset (-1 for acks=0).
     *
     * @param list<array{key:?string,value:?string,headers:list<RecordHeader>,timestamp:int,createdMs:int}> $buffered
     */
    private function produce(string $topic, int $partition, array $buffered): int
    {
        // One base timestamp per batch plus a delta per record; the base is
        // the newest record's time, so max_timestamp truthfully answers
        // "how recent is this batch".
        $maxTimestamp = max(array_column($buffered, 'timestamp'));
        $oldestCreated = min(array_column($buffered, 'createdMs'));
        $records = [];
        foreach ($buffered as $item) {
            $records[] = [
                'key' => $item['key'],
                'value' => $item['value'],
                'timestampDelta' => $item['timestamp'] - $maxTimestamp,
                'headers' => $item['headers'],
            ];
        }
        $encoded = RecordBatch::encode($records, $maxTimestamp, $this->codec);
        $requestTimeout = (int) $this->config['request.timeout.ms'];
        $body = Writer::body()
            ->string($topic)
            ->int32($partition)
            ->int32($this->acks)
            ->int32($requestTimeout)
            ->int64(strlen($encoded))
            ->raw($encoded)
            ->bytes();

        $deadline = $oldestCreated + (int) $this->config['delivery.timeout.ms'];
        $attemptsLeft = (int) $this->config['retries'];
        while (true) {
            if (Config::nowMs() >= $deadline) {
                throw new TimeoutException("delivery.timeout.ms expired for {$topic}-{$partition}");
            }
            try {
                $connection = $this->router->connectionFor($topic, $partition);
                if ($this->acks === 0) {
                    $connection->sendOneway(ApiKey::PRODUCE, $body);
                    return -1;
                }
                $reader = Reader::body($connection->request(ApiKey::PRODUCE, $body, $requestTimeout));
            } catch (ConnectionException $error) {
                if ($attemptsLeft-- <= 0 || Config::nowMs() >= $deadline) {
                    throw $error;
                }
                Config::sleepMs((int) $this->config['retry.backoff.ms']);
                continue;
            }
            $reader->string(); // topic
            $reader->int32();  // partition
            $code = $reader->int32();
            $baseOffset = $reader->int64();
            $reader->int64();  // log_append_time_ms
            if ($code === ErrorCode::NONE) {
                return $baseOffset;
            }
            if (!ErrorCode::isRetriable($code) || $attemptsLeft-- <= 0 || Config::nowMs() >= $deadline) {
                throw new ServerException($code, "produce to {$topic}-{$partition}");
            }
            if (ErrorCode::isStaleRoute($code)) {
                // Resending to the same broker would repeat the error.
                $this->router->refresh($topic);
            }
            Config::sleepMs((int) $this->config['retry.backoff.ms']);
        }
    }

    private function ensureOpen(): void
    {
        if ($this->closed) {
            throw new BrahmaputraException('producer is closed');
        }
    }
}
