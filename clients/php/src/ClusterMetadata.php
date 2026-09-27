<?php

declare(strict_types=1);

namespace Brahmaputra;

/**
 * A metadata snapshot.
 *
 * $brokers: list of ['nodeId' => int, 'host' => string, 'port' => int, 'rack' => string]
 * $topics:  topic => list of ['partition' => int, 'leader' => int,
 *           'replicas' => list<int>, 'isr' => list<int>, 'leaderEpoch' => int]
 */
final class ClusterMetadata
{
    /**
     * @param list<array{nodeId:int,host:string,port:int,rack:string}> $brokers
     * @param array<string, list<array{partition:int,leader:int,replicas:list<int>,isr:list<int>,leaderEpoch:int}>> $topics
     */
    public function __construct(
        public array $brokers,
        public int $controllerId,
        public array $topics,
    ) {
    }

    /** @return array{nodeId:int,host:string,port:int,rack:string}|null */
    public function broker(int $nodeId): ?array
    {
        foreach ($this->brokers as $broker) {
            if ($broker['nodeId'] === $nodeId) {
                return $broker;
            }
        }
        return null;
    }

    public function leaderOf(string $topic, int $partition): int
    {
        foreach ($this->topics[$topic] ?? [] as $info) {
            if ($info['partition'] === $partition) {
                return $info['leader'];
            }
        }
        return -1;
    }
}
