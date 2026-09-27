<?php

declare(strict_types=1);

namespace Brahmaputra;

use Brahmaputra\Exception\BrahmaputraException;
use Brahmaputra\Exception\ConnectionException;
use Brahmaputra\Exception\ServerException;
use Brahmaputra\Protocol\ApiKey;
use Brahmaputra\Protocol\ErrorCode;
use Brahmaputra\Protocol\Reader;
use Brahmaputra\Protocol\Writer;

/**
 * Keeps one connection per broker and routes each request to its
 * partition's leader.
 *
 * Metadata is cached and refreshed only when a request says the route was
 * stale (or the topic is not yet known), because refreshing per request
 * would put the control plane on the data path.
 */
final class Router
{
    private ?Connection $seed = null;
    /** @var array<int, Connection> */
    private array $connections = [];
    private ?ClusterMetadata $metadata = null;
    /** @var list<array{0:string,1:int}> */
    private array $bootstrap;

    public function __construct(
        string $bootstrapServers,
        private readonly string $clientId = 'brahmaputra-php',
        private readonly int $requestTimeoutMs = 30000,
        private readonly int $connectTimeoutMs = 10000,
    ) {
        $this->bootstrap = Config::parseBootstrap($bootstrapServers);
        $this->seed();
    }

    /** The bootstrap connection, reopened (trying each bootstrap server) if it dropped. */
    public function seed(): Connection
    {
        if ($this->seed !== null && $this->seed->isOpen()) {
            return $this->seed;
        }
        $last = null;
        foreach ($this->bootstrap as [$host, $port]) {
            try {
                $this->seed = Connection::open($host, $port, $this->clientId, $this->connectTimeoutMs, $this->requestTimeoutMs);
                return $this->seed;
            } catch (ConnectionException $error) {
                $last = $error;
            }
        }
        throw $last ?? new ConnectionException('no bootstrap server reachable');
    }

    public function close(): void
    {
        foreach ($this->connections as $connection) {
            $connection->close();
        }
        $this->connections = [];
        $this->seed?->close();
        $this->seed = null;
    }

    /**
     * Cluster metadata. Topics named here are merged into the cached image;
     * an empty list asks the broker for every topic.
     *
     * @param list<string> $topics
     */
    public function metadata(array $topics = [], bool $refresh = false): ClusterMetadata
    {
        if (!$refresh && $this->metadata !== null) {
            $missing = array_filter($topics, fn (string $t) => !isset($this->metadata->topics[$t]));
            if ($missing === []) {
                return $this->metadata;
            }
        }
        $body = Writer::body()->stringArray(array_values($topics))->bytes();
        try {
            $response = $this->seed()->request(ApiKey::METADATA, $body);
        } catch (ConnectionException) {
            // One reconnect: a broker restart should not fail the caller.
            $response = $this->seed()->request(ApiKey::METADATA, $body);
        }
        $fresh = self::decodeMetadata(Reader::body($response));
        if ($this->metadata !== null && $topics !== []) {
            $fresh->topics = array_replace($this->metadata->topics, $fresh->topics);
        }
        $this->metadata = $fresh;
        return $fresh;
    }

    public function refresh(string $topic): ClusterMetadata
    {
        return $this->metadata([$topic], true);
    }

    /**
     * The topic's partition ids, ascending.
     *
     * @return list<int>
     */
    public function partitions(string $topic): array
    {
        $metadata = $this->metadata([$topic]);
        if (($metadata->topics[$topic] ?? []) === []) {
            // A topic auto-created on first use is not in the cached image
            // yet; one refresh distinguishes "new" from "absent".
            $metadata = $this->refresh($topic);
        }
        $infos = $metadata->topics[$topic] ?? [];
        if ($infos === []) {
            throw new BrahmaputraException("topic {$topic} has no partitions");
        }
        $ids = array_map(fn (array $info) => $info['partition'], $infos);
        sort($ids);
        return $ids;
    }

    /** The connection to the leader of $topic-$partition. */
    public function connectionFor(string $topic, int $partition): Connection
    {
        $metadata = $this->metadata([$topic]);
        $leader = $metadata->leaderOf($topic, $partition);
        if ($leader < 0) {
            $metadata = $this->refresh($topic);
            $leader = $metadata->leaderOf($topic, $partition);
        }
        if ($leader < 0) {
            throw new BrahmaputraException("no leader for {$topic}-{$partition}");
        }
        return $this->connectionTo($leader, $metadata);
    }

    private function connectionTo(int $nodeId, ClusterMetadata $metadata): Connection
    {
        $existing = $this->connections[$nodeId] ?? null;
        if ($existing !== null && $existing->isOpen()) {
            return $existing;
        }
        // A single-broker cluster advertises the address it was configured
        // with, which may not be the one we dialled; reuse the seed rather
        // than opening a second connection to ourselves.
        if (count($metadata->brokers) === 1) {
            return $this->connections[$nodeId] = $this->seed();
        }
        $broker = $metadata->broker($nodeId);
        if ($broker === null) {
            throw new BrahmaputraException("broker {$nodeId} is not in the metadata");
        }
        return $this->connections[$nodeId] = Connection::open(
            $broker['host'],
            $broker['port'],
            $this->clientId,
            $this->connectTimeoutMs,
            $this->requestTimeoutMs,
        );
    }

    private static function decodeMetadata(Reader $reader): ClusterMetadata
    {
        // Schema order: error_code, brokers, controller_id, topics. The
        // leading code is request-level (an authorization denial, say) and
        // distinct from the per-topic one "no such topic" uses.
        $requestError = $reader->int32();
        if ($requestError !== ErrorCode::NONE) {
            throw new ServerException($requestError, 'metadata');
        }
        $brokers = [];
        for ($count = $reader->count(); $count > 0; $count--) {
            $brokers[] = [
                'nodeId' => $reader->int32(),
                'host' => $reader->string(),
                'port' => $reader->int32(),
                'rack' => $reader->string(), // empty without --rack
            ];
        }
        $controllerId = $reader->int32();
        $topics = [];
        for ($count = $reader->count(); $count > 0; $count--) {
            $name = $reader->string();
            $topicError = $reader->int32();
            $partitions = [];
            for ($pcount = $reader->count(); $pcount > 0; $pcount--) {
                $partition = $reader->int32();
                $leader = $reader->int32();
                $replicas = [];
                for ($rc = $reader->count(); $rc > 0; $rc--) {
                    $replicas[] = $reader->int32();
                }
                $isr = [];
                for ($ic = $reader->count(); $ic > 0; $ic--) {
                    $isr[] = $reader->int32();
                }
                $partitions[] = [
                    'partition' => $partition,
                    'leader' => $leader,
                    'replicas' => $replicas,
                    'isr' => $isr,
                    'leaderEpoch' => $reader->int32(),
                ];
            }
            if ($topicError !== ErrorCode::NONE && $topicError !== ErrorCode::UNKNOWN_TOPIC_OR_PARTITION) {
                throw new ServerException($topicError, "metadata for {$name}");
            }
            $topics[$name] = $partitions;
        }
        return new ClusterMetadata($brokers, $controllerId, $topics);
    }
}
