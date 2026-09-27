<?php

declare(strict_types=1);

namespace Brahmaputra;

use Brahmaputra\Exception\BrahmaputraException;
use Brahmaputra\Exception\ConnectionException;
use Brahmaputra\Exception\NoOffsetForPartitionException;
use Brahmaputra\Exception\ServerException;
use Brahmaputra\Protocol\ApiKey;
use Brahmaputra\Protocol\Crc32c;
use Brahmaputra\Protocol\ErrorCode;
use Brahmaputra\Protocol\Reader;
use Brahmaputra\Protocol\Writer;

/**
 * A consumer that shares its topics' partitions with the rest of its group.
 *
 * The group's coordinator is the leader of `__consumer_offsets` partition
 * `crc32c(group.id) % partitions`; every group request goes there.
 *
 * **Heartbeats run inside poll().** PHP has no threads, so there is no
 * background heartbeat thread as in the Java client. poll() heartbeats
 * every `heartbeat.interval.ms` while it waits (and commit() heartbeats
 * too), which means your processing between two poll() calls must stay
 * under `session.timeout.ms` or the coordinator evicts this member and
 * rebalances. For longer processing call heartbeat() periodically from
 * your own loop, or raise session.timeout.ms.
 *
 * `max.poll.interval.ms` is enforced at the next poll(): if the gap since
 * the previous poll exceeded it, this member leaves the group (as the Java
 * client's heartbeat thread would have done when the deadline passed),
 * drops its uncommitted positions, and rejoins. A heartbeat() call made
 * after the interval has passed does the same, so a stuck application does
 * not keep asserting a liveness it no longer has.
 *
 * Single-instance by design: one GroupConsumer per worker process.
 */
final class GroupConsumer
{
    private const OFFSETS_TOPIC = '__consumer_offsets';
    private const COORDINATOR_ATTEMPTS = 4;
    private const JOIN_ATTEMPTS = 4;

    /** @return array<string, mixed> */
    public static function defaults(): array
    {
        return [
            'bootstrap.servers' => null,
            'group.id' => null,
            'client.id' => 'brahmaputra-php',
            /** Kafka's default is 45s; 10s matches the Rust client. */
            'session.timeout.ms' => 10000,
            /** 0 means session.timeout.ms / 3. */
            'heartbeat.interval.ms' => 0,
            'rebalance.timeout.ms' => 3000,
            'max.poll.interval.ms' => 300000,
            'enable.auto.commit' => true,
            'auto.commit.interval.ms' => 5000,
            /** earliest, latest or none. */
            'auto.offset.reset' => 'earliest',
            /** range, roundrobin or sticky. */
            'partition.assignment.strategy' => Assignor::RANGE,
            /** Static membership (KIP-345); empty for a dynamic member. */
            'group.instance.id' => '',
            'max.poll.records' => 500,
            'fetch.max.bytes' => 8 * 1024 * 1024,
            'fetch.min.bytes' => 1,
            'fetch.max.wait.ms' => 500,
            'isolation.level' => 'read_uncommitted',
            'client.rack' => '',
            'request.timeout.ms' => 30000,
            'socket.connection.setup.timeout.ms' => 10000,
        ];
    }

    /** @var array<string, mixed> */
    private array $config;
    private Consumer $consumer;
    private string $groupId;

    /** @var list<string> */
    private array $subscribed = [];
    private string $memberId = '';
    private int $generation = -1;
    private bool $joined = false;
    /** @var list<TopicPartition> */
    private array $assignment = [];
    /** Next offset to *deliver* — what gets committed. @var array<string, array{0:TopicPartition,1:int}> */
    private array $positions = [];
    /** Next offset to *fetch*; runs ahead of positions by the buffer. @var array<string,int> */
    private array $fetchPositions = [];
    /** @var list<ConsumedRecord> */
    private array $buffered = [];
    private ?int $lastPollMs = null;
    private int $lastCommitMs;
    private int $lastHeartbeatMs = 0;
    private bool $closed = false;

    /** @param array<string, mixed> $config Kafka-style keys; see defaults() */
    public function __construct(array $config)
    {
        $this->config = Config::resolve(self::defaults(), $config, 'group consumer');
        if (!is_string($this->config['group.id']) || $this->config['group.id'] === '') {
            throw new \InvalidArgumentException('group consumer config needs group.id');
        }
        if (!in_array($this->config['auto.offset.reset'], ['earliest', 'latest', 'none'], true)) {
            throw new \InvalidArgumentException('auto.offset.reset must be earliest, latest or none');
        }
        if (!in_array($this->config['partition.assignment.strategy'], Assignor::ALL, true)) {
            throw new \InvalidArgumentException('partition.assignment.strategy must be one of ' . implode(', ', Assignor::ALL));
        }
        $this->groupId = $this->config['group.id'];
        $consumerConfig = array_intersect_key($this->config, Consumer::defaults());
        $this->consumer = new Consumer($consumerConfig);
        $this->lastCommitMs = Config::nowMs();
    }

    /** @param list<string> $topics */
    public function subscribe(array $topics): void
    {
        $this->subscribed = array_values(array_unique($topics));
        $this->joined = false;
    }

    /** @return list<TopicPartition> */
    public function assignment(): array
    {
        return $this->assignment;
    }

    public function memberId(): string
    {
        return $this->memberId;
    }

    public function generation(): int
    {
        return $this->generation;
    }

    public function consumer(): Consumer
    {
        return $this->consumer;
    }

    /**
     * Up to max.poll.records records, waiting up to $timeoutMs for some to
     * arrive. Joins (or rejoins) the group, heartbeats and auto-commits as
     * needed while it waits.
     *
     * @return list<ConsumedRecord>
     */
    public function poll(int $timeoutMs = 1000): array
    {
        if ($this->closed) {
            throw new BrahmaputraException('group consumer is closed');
        }
        if ($this->subscribed === []) {
            throw new BrahmaputraException('subscribe to at least one topic before polling');
        }
        $this->enforcePollInterval();
        // Stamped on entry: the interval bounds how long the *application*
        // goes without asking for records; a poll that blocks for its full
        // timeout is the consumer working normally.
        $this->lastPollMs = Config::nowMs();
        $deadline = $this->lastPollMs + $timeoutMs;

        while (true) {
            $this->maybeHeartbeat();
            if (!$this->joined) {
                $this->join();
            }
            if ($this->buffered !== []) {
                return $this->takeBuffered();
            }
            if ($this->assignment === []) {
                if (Config::nowMs() >= $deadline) {
                    return [];
                }
                Config::sleepMs(min(50, max(1, $deadline - Config::nowMs())));
                continue;
            }

            $gotAny = false;
            foreach ($this->assignment as $tp) {
                if (!$this->joined) {
                    break; // a heartbeat below saw a rebalance
                }
                // Once something is buffered, do not sit in a long poll on
                // the remaining partitions.
                $wait = $gotAny ? 0 : min(max(0, $deadline - Config::nowMs()), $this->heartbeatIntervalMs(), 500);
                $offset = $this->fetchPositions[$tp->key()] ?? 0;
                try {
                    $records = $this->consumer->fetch($tp->topic, $tp->partition, $offset, $wait);
                } catch (ServerException $error) {
                    if ($error->errorCode === ErrorCode::OFFSET_OUT_OF_RANGE) {
                        // The position fell off the log (retention); restart
                        // where auto.offset.reset says.
                        $reset = $this->resetOffset($tp);
                        $this->fetchPositions[$tp->key()] = $reset;
                        $this->positions[$tp->key()] = [$tp, $reset];
                        $this->buffered = array_values(array_filter(
                            $this->buffered,
                            fn (ConsumedRecord $r) => $r->topic !== $tp->topic || $r->partition !== $tp->partition,
                        ));
                        continue;
                    }
                    if (ErrorCode::isStaleRoute($error->errorCode)) {
                        $this->consumer->router()->refresh($tp->topic);
                        continue;
                    }
                    throw $error;
                }
                if ($records !== []) {
                    $gotAny = true;
                    $this->fetchPositions[$tp->key()] = $records[count($records) - 1]->offset + 1;
                    array_push($this->buffered, ...$records);
                }
                $this->maybeHeartbeat();
            }

            $this->maybeAutoCommit();
            if ($this->buffered !== []) {
                return $this->takeBuffered();
            }
            if (Config::nowMs() >= $deadline) {
                return [];
            }
        }
    }

    /**
     * Commit the positions of records poll() has returned (at-least-once:
     * call it after processing them).
     */
    public function commit(): void
    {
        if ($this->positions === []) {
            return;
        }
        $entries = array_values($this->positions);
        usort($entries, fn (array $a, array $b) => TopicPartition::compare($a[0], $b[0]));
        $writer = Writer::body()
            ->string($this->groupId)
            ->int32($this->generation)
            ->string($this->memberId)
            ->int32(count($entries));
        foreach ($entries as [$tp, $offset]) {
            $writer->string($tp->topic)->int32($tp->partition)->int64($offset);
        }
        $reader = Reader::body($this->coordinatorRequest(ApiKey::OFFSET_COMMIT, $writer->bytes()));
        $code = $reader->int32();
        if ($code !== ErrorCode::NONE) {
            if ($this->isFencing($code)) {
                // Generation fencing: this member's view is stale, so its
                // commit is refused. Rejoin on the next poll.
                $this->joined = false;
            }
            throw new ServerException($code, 'offset_commit');
        }
        $this->lastCommitMs = Config::nowMs();
        $this->maybeHeartbeat();
    }

    /**
     * The group's committed offsets. An empty list asks for every partition
     * the group has committed.
     *
     * @param list<TopicPartition> $partitions
     * @return list<TopicPartition> each carrying its committed ->offset
     */
    public function committed(array $partitions = []): array
    {
        $writer = Writer::body()->string($this->groupId)->int32(count($partitions));
        foreach ($partitions as $tp) {
            $writer->string($tp->topic)->int32($tp->partition);
        }
        $reader = Reader::body($this->coordinatorRequest(ApiKey::OFFSET_FETCH, $writer->bytes()));
        $code = $reader->int32();
        if ($code !== ErrorCode::NONE) {
            throw new ServerException($code, 'offset_fetch');
        }
        $out = [];
        for ($count = $reader->count(); $count > 0; $count--) {
            $topic = $reader->string();
            $partition = $reader->int32();
            $out[] = new TopicPartition($topic, $partition, $reader->int64());
        }
        return $out;
    }

    /**
     * Heartbeat now. poll() does this itself; call it from a long
     * processing loop to stay in the group without polling. Returns false
     * when the coordinator reports a rebalance (the next poll() rejoins).
     */
    public function heartbeat(): bool
    {
        if (!$this->enforcePollInterval()) {
            return false;
        }
        if (!$this->joined || $this->memberId === '') {
            return false;
        }
        $body = Writer::body()->string($this->groupId)->int32($this->generation)->string($this->memberId)->bytes();
        $reader = Reader::body($this->coordinatorRequest(ApiKey::HEARTBEAT, $body));
        $code = $reader->int32();
        $this->lastHeartbeatMs = Config::nowMs();
        if ($code === ErrorCode::NONE) {
            return true;
        }
        if ($this->isFencing($code)) {
            if ($code === ErrorCode::UNKNOWN_MEMBER_ID) {
                // Evicted: the next join gets a fresh member id.
                $this->memberId = '';
            }
            $this->joined = false;
            return false;
        }
        throw new ServerException($code, 'heartbeat');
    }

    /**
     * Commit, leave the group, and close connections.
     *
     * Leaving is what separates a clean shutdown from a crash: without it
     * the coordinator must wait out session.timeout.ms before reassigning.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        try {
            if ($this->joined) {
                $this->commit();
            }
        } catch (\Throwable) {
            // A failed final commit shows up as the next member resuming
            // from an older position, not as a crash on the shutdown path.
        }
        try {
            if ($this->memberId !== '') {
                $this->leave();
            }
        } catch (\Throwable) {
            // Best effort: failing costs only the session timeout.
        }
        $this->consumer->close();
    }

    public function __destruct()
    {
        $this->close();
    }

    private function isFencing(int $code): bool
    {
        return $code === ErrorCode::REBALANCE_IN_PROGRESS
            || $code === ErrorCode::UNKNOWN_MEMBER_ID
            || $code === ErrorCode::ILLEGAL_GENERATION;
    }

    private function heartbeatIntervalMs(): int
    {
        $interval = (int) $this->config['heartbeat.interval.ms'];
        return $interval > 0 ? $interval : max(1, intdiv((int) $this->config['session.timeout.ms'], 3));
    }

    private function maybeHeartbeat(): void
    {
        if ($this->joined && Config::nowMs() - $this->lastHeartbeatMs >= $this->heartbeatIntervalMs()) {
            $this->heartbeat();
        }
    }

    /**
     * Leave if the application went longer than max.poll.interval.ms
     * between polls. Returns false when it did.
     */
    private function enforcePollInterval(): bool
    {
        if ($this->lastPollMs === null || !$this->joined) {
            return true;
        }
        $idle = Config::nowMs() - $this->lastPollMs;
        if ($idle < (int) $this->config['max.poll.interval.ms']) {
            return true;
        }
        try {
            $this->leave();
        } catch (\Throwable) {
            // The coordinator will evict us after the session timeout anyway.
        }
        // What was delivered but not committed is abandoned, exactly as
        // when Kafka's heartbeat thread leaves on this deadline: another
        // member may already own these partitions.
        $this->positions = [];
        $this->fetchPositions = [];
        $this->buffered = [];
        $this->assignment = [];
        $this->memberId = '';
        $this->lastPollMs = null;
        return false;
    }

    /** @return list<ConsumedRecord> */
    private function takeBuffered(): array
    {
        $limit = max(1, (int) $this->config['max.poll.records']);
        $delivered = array_slice($this->buffered, 0, $limit);
        $this->buffered = array_slice($this->buffered, $limit);
        foreach ($delivered as $record) {
            // The committed position advances only over records actually
            // handed to the caller; committing what was merely fetched
            // would skip records nobody processed.
            $tp = new TopicPartition($record->topic, $record->partition);
            $this->positions[$tp->key()] = [$tp, $record->offset + 1];
        }
        return $delivered;
    }

    private function maybeAutoCommit(): void
    {
        if (!$this->config['enable.auto.commit'] || (int) $this->config['auto.commit.interval.ms'] <= 0) {
            return;
        }
        if ($this->positions === [] || Config::nowMs() - $this->lastCommitMs < (int) $this->config['auto.commit.interval.ms']) {
            return;
        }
        try {
            $this->commit();
        } catch (ServerException | ConnectionException) {
            // Retried on the next poll; an explicit commit() is what a
            // caller relies on.
        }
    }

    private function resetOffset(TopicPartition $tp): int
    {
        return match ($this->config['auto.offset.reset']) {
            'earliest' => $this->consumer->listOffsets($tp->topic, $tp->partition, Offset::EARLIEST),
            'latest' => $this->consumer->listOffsets($tp->topic, $tp->partition, Offset::LATEST),
            default => throw new NoOffsetForPartitionException("no committed offset for {$tp} and auto.offset.reset=none"),
        };
    }

    private function join(): void
    {
        // A member that rejoins after a rebalance commits what it has
        // delivered first, while its generation may still be accepted.
        if ($this->generation >= 0 && $this->positions !== [] && $this->config['enable.auto.commit']) {
            try {
                $this->commit();
            } catch (\Throwable) {
            }
        }
        for ($attempt = 0; $attempt < self::JOIN_ATTEMPTS; $attempt++) {
            $body = Writer::body()
                ->string($this->groupId)
                ->int32((int) $this->config['session.timeout.ms'])
                ->int32((int) $this->config['rebalance.timeout.ms'])
                ->string($this->memberId)
                ->stringArray($this->subscribed)
                ->string((string) $this->config['group.instance.id'])
                ->bytes();
            $reader = Reader::body($this->coordinatorRequest(ApiKey::JOIN_GROUP, $body, (int) $this->config['rebalance.timeout.ms']));
            $code = $reader->int32();
            if ($code === ErrorCode::REBALANCE_IN_PROGRESS) {
                Config::sleepMs(100);
                continue;
            }
            if ($code === ErrorCode::UNKNOWN_MEMBER_ID) {
                $this->memberId = '';
                continue;
            }
            if ($code !== ErrorCode::NONE) {
                throw new ServerException($code, 'join_group');
            }

            $generation = $reader->int32();
            $memberId = $reader->string();
            $leaderId = $reader->string();
            $members = [];
            $previous = [];
            for ($count = $reader->count(); $count > 0; $count--) {
                $id = $reader->string();
                $topics = $reader->stringArray();
                $held = [];
                for ($h = $reader->count(); $h > 0; $h--) {
                    $held[] = new TopicPartition($reader->string(), $reader->int32());
                }
                $members[] = ['id' => $id, 'topics' => $topics];
                $previous[$id] = $held;
            }

            $this->memberId = $memberId;
            $this->generation = $generation;
            $this->lastHeartbeatMs = Config::nowMs();

            $assignments = $memberId === $leaderId ? $this->computeAssignments($members, $previous) : [];
            if ($this->sync($assignments)) {
                $this->joined = true;
                return;
            }
        }
        throw new BrahmaputraException('consumer group failed to stabilise after ' . self::JOIN_ATTEMPTS . ' join attempts');
    }

    /** @param array<string, list<TopicPartition>> $assignments */
    private function sync(array $assignments): bool
    {
        ksort($assignments, SORT_STRING);
        $writer = Writer::body()
            ->string($this->groupId)
            ->int32($this->generation)
            ->string($this->memberId)
            ->int32(count($assignments));
        foreach ($assignments as $memberId => $partitions) {
            $writer->string((string) $memberId)->int32(count($partitions));
            foreach ($partitions as $tp) {
                $writer->string($tp->topic)->int32($tp->partition);
            }
        }
        $reader = Reader::body($this->coordinatorRequest(ApiKey::SYNC_GROUP, $writer->bytes(), (int) $this->config['rebalance.timeout.ms']));
        $code = $reader->int32();
        if ($code === ErrorCode::REBALANCE_IN_PROGRESS || $code === ErrorCode::ILLEGAL_GENERATION) {
            return false;
        }
        if ($code !== ErrorCode::NONE) {
            throw new ServerException($code, 'sync_group');
        }
        $assignment = [];
        for ($count = $reader->count(); $count > 0; $count--) {
            $assignment[] = new TopicPartition($reader->string(), $reader->int32());
        }
        $this->applyAssignment($assignment);
        return true;
    }

    /** @param list<TopicPartition> $assignment */
    private function applyAssignment(array $assignment): void
    {
        $this->assignment = $assignment;
        $owned = [];
        foreach ($assignment as $tp) {
            $owned[$tp->key()] = true;
        }
        $this->positions = array_intersect_key($this->positions, $owned);
        // Buffered records sit ahead of the delivered position and were
        // never handed out, so a new assignment simply drops them.
        $this->buffered = [];

        $needed = array_values(array_filter($assignment, fn (TopicPartition $tp) => !isset($this->positions[$tp->key()])));
        if ($needed !== []) {
            $committed = [];
            foreach ($this->committed($needed) as $tp) {
                $committed[$tp->key()] = $tp->offset;
            }
            foreach ($needed as $tp) {
                $offset = $committed[$tp->key()] ?? -1;
                if ($offset < 0) {
                    $offset = $this->resetOffset($tp);
                }
                $this->positions[$tp->key()] = [$tp, $offset];
            }
        }
        $this->fetchPositions = array_map(fn (array $entry) => $entry[1], $this->positions);
    }

    /**
     * @param list<array{id:string, topics:list<string>}> $members
     * @param array<string, list<TopicPartition>> $previous
     * @return array<string, list<TopicPartition>>
     */
    private function computeAssignments(array $members, array $previous): array
    {
        $topicPartitions = [];
        foreach ($members as $member) {
            foreach ($member['topics'] as $topic) {
                $topicPartitions[$topic] ??= $this->consumer->partitions($topic);
            }
        }
        return Assignor::assign((string) $this->config['partition.assignment.strategy'], $members, $topicPartitions, $previous);
    }

    private function leave(): void
    {
        $body = Writer::body()->string($this->groupId)->string($this->memberId)->bytes();
        $this->joined = false;
        $reader = Reader::body($this->coordinatorRequest(ApiKey::LEAVE_GROUP, $body));
        $code = $reader->int32();
        if ($code !== ErrorCode::NONE && $code !== ErrorCode::UNKNOWN_MEMBER_ID) {
            throw new ServerException($code, 'leave_group');
        }
    }

    private function coordinatorPartition(): int
    {
        $partitions = $this->consumer->partitions(self::OFFSETS_TOPIC);
        return Crc32c::checksum($this->groupId) % count($partitions);
    }

    /** Send to the group's coordinator, following moves and waiting out loads. */
    private function coordinatorRequest(int $apiKey, string $body, int $extraWaitMs = 0): string
    {
        $timeout = (int) $this->config['request.timeout.ms'] + $extraWaitMs;
        $lastError = null;
        for ($attempt = 0; $attempt < self::COORDINATOR_ATTEMPTS; $attempt++) {
            try {
                $partition = $this->coordinatorPartition();
                $connection = $this->consumer->router()->connectionFor(self::OFFSETS_TOPIC, $partition);
                $response = $connection->request($apiKey, $body, $timeout);
            } catch (ConnectionException $error) {
                $lastError = $error;
                Config::sleepMs(100);
                continue;
            }
            $code = self::peekErrorCode($response);
            if ($code === ErrorCode::COORDINATOR_LOAD_IN_PROGRESS) {
                Config::sleepMs(100);
                continue;
            }
            if ($code === ErrorCode::NOT_COORDINATOR || $code === ErrorCode::NOT_LEADER_OR_FOLLOWER) {
                $this->consumer->router()->refresh(self::OFFSETS_TOPIC);
                continue;
            }
            return $response;
        }
        throw new BrahmaputraException(
            'group coordinator unavailable after ' . self::COORDINATOR_ATTEMPTS . ' attempts',
            0,
            $lastError,
        );
    }

    /** Every group response starts with an error code; read it without consuming the body. */
    private static function peekErrorCode(string $body): int
    {
        try {
            return Reader::body($body)->int32();
        } catch (\Throwable) {
            return ErrorCode::NONE;
        }
    }
}
