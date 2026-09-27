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
use Brahmaputra\Protocol\RecordBatch;
use Brahmaputra\Protocol\Writer;

/** Reads partitions directly, with no group coordination. */
final class Consumer
{
    /** @return array<string, mixed> */
    public static function defaults(): array
    {
        return [
            'bootstrap.servers' => null,
            'client.id' => 'brahmaputra-php',
            'fetch.max.bytes' => 8 * 1024 * 1024,
            'fetch.min.bytes' => 1,
            'fetch.max.wait.ms' => 500,
            'max.poll.records' => 500,
            /** read_uncommitted or read_committed (stops at the last stable offset). */
            'isolation.level' => 'read_uncommitted',
            /** This consumer's failure domain; with it set the leader may point reads at a same-rack replica. */
            'client.rack' => '',
            'request.timeout.ms' => 30000,
            'socket.connection.setup.timeout.ms' => 10000,
        ];
    }

    /** @var array<string, mixed> */
    private array $config;
    private Router $router;
    private int $isolation;

    /** @param array<string, mixed> $config Kafka-style keys; see defaults() */
    public function __construct(array $config)
    {
        $this->config = Config::resolve(self::defaults(), $config, 'consumer');
        $this->isolation = match ($this->config['isolation.level']) {
            'read_uncommitted', Protocol::READ_UNCOMMITTED => Protocol::READ_UNCOMMITTED,
            'read_committed', Protocol::READ_COMMITTED => Protocol::READ_COMMITTED,
            default => throw new \InvalidArgumentException('isolation.level must be read_uncommitted or read_committed'),
        };
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

    /** @return array<string, mixed> */
    public function config(): array
    {
        return $this->config;
    }

    public function close(): void
    {
        $this->router->close();
    }

    /** @return list<int> */
    public function partitions(string $topic): array
    {
        return $this->router->partitions($topic);
    }

    /** Resolve Offset::EARLIEST, Offset::LATEST or a unix-ms timestamp to an offset. */
    public function listOffsets(string $topic, int $partition, int $timestamp): int
    {
        $body = Writer::body()->string($topic)->int32($partition)->int64($timestamp)->bytes();
        $reader = Reader::body($this->requestLeader($topic, $partition, ApiKey::LIST_OFFSETS, $body));
        $reader->string(); // topic
        $reader->int32();  // partition
        $code = $reader->int32();
        $offset = $reader->int64();
        $reader->int64();  // timestamp
        if ($code !== ErrorCode::NONE) {
            throw new ServerException($code, "list_offsets {$topic}-{$partition}");
        }
        return $offset;
    }

    /** The partition's high watermark: the offset the next committed record will get. */
    public function highWatermark(string $topic, int $partition): int
    {
        return $this->listOffsets($topic, $partition, Offset::LATEST);
    }

    /**
     * Records from $offset on, waiting up to $maxWaitMs (capped by
     * fetch.max.wait.ms) for fetch.min.bytes to accumulate.
     *
     * @return list<ConsumedRecord>
     */
    public function fetch(string $topic, int $partition, int $offset, ?int $maxWaitMs = null): array
    {
        return $this->fetchVerbose($topic, $partition, $offset, $maxWaitMs)->records;
    }

    /** Fetch, also returning the partition's high watermark. */
    public function fetchVerbose(string $topic, int $partition, int $offset, ?int $maxWaitMs = null): FetchResult
    {
        $configured = (int) $this->config['fetch.max.wait.ms'];
        $wait = max(0, min($maxWaitMs ?? $configured, $configured));
        $body = Writer::body()
            ->string($topic)
            ->int32($partition)
            ->int64($offset)
            ->int32((int) $this->config['fetch.max.bytes'])
            ->int32($wait)
            ->int32((int) $this->config['fetch.min.bytes'])
            ->int32($this->isolation)
            ->string((string) $this->config['client.rack'])
            ->bytes();
        // The broker may hold the request for $wait before answering.
        $timeout = (int) $this->config['request.timeout.ms'] + $wait;

        $result = $this->decodeFetch($this->requestLeader($topic, $partition, ApiKey::FETCH, $body, $timeout));
        if ($result[0] === ErrorCode::NOT_LEADER_OR_FOLLOWER) {
            $this->router->refresh($topic);
            $result = $this->decodeFetch($this->requestLeader($topic, $partition, ApiKey::FETCH, $body, $timeout));
        }
        [$code, $highWatermark, $batches] = $result;
        if ($code !== ErrorCode::NONE) {
            throw new ServerException($code, "fetch {$topic}-{$partition}");
        }

        $records = [];
        foreach ($batches as $batch) {
            foreach ($batch['records'] as $index => $record) {
                $recordOffset = $batch['baseOffset'] + $index;
                // A batch can start before the requested offset; skip what
                // the caller has already seen.
                if ($recordOffset < $offset) {
                    continue;
                }
                $records[] = new ConsumedRecord(
                    $topic,
                    $partition,
                    $recordOffset,
                    $record['key'],
                    $record['value'],
                    $batch['maxTimestamp'] + $record['timestampDelta'],
                    $record['headers'],
                );
            }
        }
        return new FetchResult($records, $highWatermark);
    }

    /**
     * Send a read to the partition leader. Reads are idempotent, so a
     * connection that dropped (broker restart, idle timeout) is redialled
     * and the request sent once more before the error reaches the caller.
     */
    private function requestLeader(string $topic, int $partition, int $apiKey, string $body, ?int $timeoutMs = null): string
    {
        try {
            return $this->router->connectionFor($topic, $partition)->request($apiKey, $body, $timeoutMs);
        } catch (ConnectionException $error) {
            if ($error instanceof TimeoutException) {
                throw $error;
            }
            return $this->router->connectionFor($topic, $partition)->request($apiKey, $body, $timeoutMs);
        }
    }

    /** @return array{0:int, 1:int, 2:list<array{baseOffset:int,maxTimestamp:int,records:list<array>}>} */
    private function decodeFetch(string $body): array
    {
        $reader = Reader::body($body);
        $reader->string(); // topic
        $reader->int32();  // partition
        $code = $reader->int32();
        $highWatermark = $reader->int64();
        $reader->int64();  // last_stable_offset
        $batchesLength = $reader->int64();
        // Read even though unused: the batches trail the whole struct, so
        // skipping a field would take them from the wrong offset.
        $reader->int32();  // preferred_read_replica
        $trailing = $reader->rest();
        if ($batchesLength < 0 || $batchesLength > strlen($trailing)) {
            throw new ProtocolException('fetch response claims more batch bytes than it carries');
        }
        $raw = substr($trailing, 0, $batchesLength);
        $batches = [];
        $pos = 0;
        while ($pos < strlen($raw)) {
            [$batch, $pos] = RecordBatch::decode($raw, $pos);
            $batches[] = $batch;
        }
        return [$code, $highWatermark, $batches];
    }
}
