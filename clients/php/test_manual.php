<?php

/*
 * End-to-end suite for the PHP driver against a live broker. A port of
 * clients/go/cmd/manualtest/main.go with the same sections and checks.
 *
 *   brahmaputra-server --data-dir ./data --default-partitions 4
 *   php test_manual.php 127.0.0.1 9092
 *
 * Every check asserts a property of the system, not that a function ran:
 * records come back byte-identical, keys pin partitions, headers survive,
 * offsets are contiguous. Exits non-zero on any failure.
 */

declare(strict_types=1);

require __DIR__ . '/autoload.php';

use Brahmaputra\Assignor;
use Brahmaputra\ConsumedRecord;
use Brahmaputra\Consumer;
use Brahmaputra\Exception\NoOffsetForPartitionException;
use Brahmaputra\GroupConsumer;
use Brahmaputra\Offset;
use Brahmaputra\Producer;
use Brahmaputra\Protocol\Murmur2;
use Brahmaputra\RecordHeader;

$passed = 0;
$failed = 0;

function check(string $name, bool $ok, string $detail = ''): void
{
    global $passed, $failed;
    if ($ok) {
        $passed++;
        echo "  ok   {$name}\n";
        return;
    }
    $failed++;
    echo $detail !== '' ? "  FAIL {$name}: {$detail}\n" : "  FAIL {$name}\n";
}

function section(string $title): void
{
    echo "\n{$title}\n";
}

function unique(string $prefix): string
{
    return sprintf('%s-%d', $prefix, hrtime(true) % 1_000_000_000);
}

function nowMs(): int
{
    return (int) floor(microtime(true) * 1000);
}

set_exception_handler(static function (\Throwable $error): void {
    echo '  FATAL ' . get_class($error) . ': ' . $error->getMessage() . "\n";
    exit(2);
});

$host = $argv[1] ?? '127.0.0.1';
$port = $argv[2] ?? '9092';
$bootstrap = "{$host}:{$port}";

// array + keeps the left side's keys, so overrides must come first.
$producerConfig = static fn (array $overrides = []): array => $overrides + ['bootstrap.servers' => $bootstrap, 'linger.ms' => 0];
$consumerConfig = static fn (): array => ['bootstrap.servers' => $bootstrap];
$groupConfig = static fn (string $groupId, array $overrides = []): array =>
    $overrides + ['bootstrap.servers' => $bootstrap, 'group.id' => $groupId, 'enable.auto.commit' => false];

section('connection and metadata');
{
    $consumer = new Consumer($consumerConfig());
    $answer = null;
    $error = '';
    try {
        $answer = $consumer->router()->seed()->apiVersions();
    } catch (\Throwable $e) {
        $error = $e->getMessage();
    }
    check('ApiVersions answers', $answer !== null && count($answer['versions']) > 0, $error);
    check('broker reports a version', ($answer['brokerVersion'] ?? '') !== '', (string) ($answer['brokerVersion'] ?? ''));
    $metadata = $consumer->router()->metadata([], true);
    check('metadata lists brokers', count($metadata->brokers) >= 1, count($metadata->brokers) . ' brokers');
    $consumer->close();
}

section('produce and consume round trip');
$topic = unique('php-roundtrip');
$payloads = [];
for ($i = 0; $i < 50; $i++) {
    $payloads[] = "record-{$i}";
}
{
    $producer = new Producer($producerConfig());
    foreach ($payloads as $payload) {
        $producer->send($topic, $payload, null, [], 0);
    }
    $producer->flush();
    $producer->close();
}
{
    $consumer = new Consumer($consumerConfig());
    $got = $consumer->fetch($topic, 0, 0, 500);
    check('every record comes back', count($got) === count($payloads), 'got ' . count($got));
    $identical = count($got) === count($payloads);
    for ($i = 0; $identical && $i < count($got); $i++) {
        if ($got[$i]->value !== $payloads[$i] || $got[$i]->offset !== $i) {
            $identical = false;
        }
    }
    check('values byte-identical and offsets contiguous', $identical);
    $consumer->close();
}

section('compression codecs');
// Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
// Compression::register().
foreach (['none', 'gzip'] as $codec) {
    $codecTopic = unique("php-{$codec}");
    $body = str_repeat('the same line over and over. ', 40);
    $producer = new Producer($producerConfig(['compression.type' => $codec]));
    for ($i = 0; $i < 20; $i++) {
        $producer->send($codecTopic, $body . chr(ord('0') + $i % 10), null, [], 0);
    }
    $producer->flush();
    $producer->close();

    $consumer = new Consumer($consumerConfig());
    $got = $consumer->fetch($codecTopic, 0, 0, 500);
    check("{$codec}: round trips", count($got) === 20 && str_starts_with((string) $got[0]->value, $body), 'got ' . count($got) . ' records');
    $consumer->close();
}

section('keys, partitioning and ordering');
{
    $keyTopic = unique('php-keys');
    $producer = new Producer($producerConfig());
    $partitions = $producer->router()->partitions($keyTopic);
    for ($i = 0; $i < 30; $i++) {
        $producer->send($keyTopic, "v{$i}", 'user-7');
    }
    $producer->flush();
    $producer->close();

    $target = $partitions[Murmur2::partition('user-7', count($partitions))];
    $consumer = new Consumer($consumerConfig());
    $onTarget = $consumer->fetch($keyTopic, $target, 0, 500);
    check('a key pins every record to one partition', count($onTarget) === 30, "partition {$target} holds " . count($onTarget) . ' of 30');

    $ordered = count($onTarget) === 30;
    for ($i = 0; $ordered && $i < count($onTarget); $i++) {
        if ($onTarget[$i]->value !== "v{$i}") {
            $ordered = false;
        }
    }
    check('per-key order is preserved', $ordered);

    $strays = 0;
    foreach ($partitions as $partition) {
        if ($partition !== $target) {
            $strays += count($consumer->fetch($keyTopic, $partition, 0, 200));
        }
    }
    check('no keyed record landed elsewhere', $strays === 0, "{$strays} strays");
    $consumer->close();
}

section("murmur2 agrees with the broker's partitioner");
check('murmur2("") is stable', Murmur2::hash('') === 275646681, (string) Murmur2::hash(''));
check('murmur2 is deterministic', Murmur2::hash('user-7') === Murmur2::hash('user-7'));
check('different keys hash differently', Murmur2::hash('user-7') !== Murmur2::hash('user-8'));

section('record headers and timestamps');
{
    $headerTopic = unique('php-headers');
    $before = nowMs() - 1000;
    $producer = new Producer($producerConfig());
    $producer->send($headerTopic, 'annotated', null, [
        new RecordHeader('trace-id', 'abc-123'),
        new RecordHeader('content-type', 'application/json'),
        new RecordHeader('tombstone-reason', null),
    ], 0);
    $producer->send($headerTopic, 'plain', null, [], 0);
    $producer->flush();
    $producer->close();
    $after = nowMs() + 1000;

    $consumer = new Consumer($consumerConfig());
    $got = $consumer->fetch($headerTopic, 0, 0, 500);
    check('both records arrive', count($got) === 2, 'got ' . count($got));
    if (count($got) === 2) {
        [$annotated, $plain] = $got;
        check('headers survive the round trip', count($annotated->headers) === 3, count($annotated->headers) . ' headers');
        check('header values are exact', $annotated->header('trace-id') === 'abc-123');
        check('a null header value stays null', count($annotated->headers) === 3 && $annotated->headers[2]->value === null);
        check('a record with no headers gains none from its batch', count($plain->headers) === 0, count($plain->headers) . ' headers');
        $inWindow = true;
        foreach ($got as $record) {
            if ($record->timestamp < $before || $record->timestamp > $after) {
                $inWindow = false;
            }
        }
        check('timestamps are real wall-clock values', $inWindow, "{$got[0]->timestamp},{$got[1]->timestamp} outside {$before}..{$after}");
    }
    $consumer->close();
}

section('tombstones');
{
    $tombTopic = unique('php-tombstones');
    $producer = new Producer($producerConfig());
    $producer->send($tombTopic, 'set', 'k1', [], 0);
    $producer->send($tombTopic, '', 'k2', [], 0);
    // A null value is a deletion, and must stay distinguishable from the
    // empty value above all the way through the round trip.
    $producer->send($tombTopic, null, 'k3', [], 0);
    $producer->flush();
    $producer->close();

    $consumer = new Consumer($consumerConfig());
    $got = $consumer->fetch($tombTopic, 0, 0, 500);
    check('all three records arrive', count($got) === 3, 'got ' . count($got));
    if (count($got) === 3) {
        check('an ordinary value round-trips', $got[0]->value === 'set');
        check('an empty value is empty, not null', $got[1]->value === '', var_export($got[1]->value, true));
        check('a tombstone arrives as a null value', $got[2]->value === null, var_export($got[2]->value, true));
    }
    $consumer->close();
}

section('offsets');
{
    $consumer = new Consumer($consumerConfig());
    $earliest = $consumer->listOffsets($topic, 0, Offset::EARLIEST);
    $latest = $consumer->listOffsets($topic, 0, Offset::LATEST);
    check('earliest is 0 on a fresh topic', $earliest === 0, (string) $earliest);
    check('latest equals the record count', $latest === 50, (string) $latest);
    $consumer->close();
}

section('acks');
foreach ([0, 1, -1] as $acks) {
    $acksTopic = unique("php-acks{$acks}");
    $producer = new Producer($producerConfig(['acks' => $acks]));
    $producer->send($acksTopic, 'durable', null, [], 0);
    $producer->flush();
    $producer->close();
    usleep(400_000);

    $consumer = new Consumer($consumerConfig());
    $got = $consumer->fetch($acksTopic, 0, 0, 500);
    check("acks={$acks} stores the record", count($got) === 1, 'got ' . count($got));
    $consumer->close();
}

section('consumer group: assignment, commit, resume');
{
    $groupTopic = unique('php-group');
    $groupId = unique('php-billing');
    $producer = new Producer($producerConfig());
    for ($i = 0; $i < 40; $i++) {
        $producer->send($groupTopic, "g{$i}");
    }
    $producer->flush();
    $producer->close();

    $consumer = new GroupConsumer($groupConfig($groupId));
    $consumer->subscribe([$groupTopic]);
    /** @var list<ConsumedRecord> $seen */
    $seen = [];
    $deadline = nowMs() + 30_000;
    while (count($seen) < 40 && nowMs() < $deadline) {
        array_push($seen, ...$consumer->poll(500));
    }
    check('the group consumes every record', count($seen) === 40, 'got ' . count($seen));

    $distinct = [];
    foreach ($seen as $record) {
        $distinct["{$record->partition}-{$record->offset}"] = true;
    }
    check('no record is delivered twice', count($distinct) === count($seen));

    $consumer->commit();
    $total = 0;
    foreach ($consumer->committed() as $tp) {
        $total += $tp->offset;
    }
    check('commit records a position', $total === 40, (string) $total);
    $consumer->close();

    // A second consumer in the same group must resume, not replay.
    $rejoined = new GroupConsumer($groupConfig($groupId));
    $rejoined->subscribe([$groupTopic]);
    $replayed = [];
    $until = nowMs() + 5_000;
    while (nowMs() < $until) {
        array_push($replayed, ...$rejoined->poll(300));
    }
    check('a rejoining group resumes from its commit', count($replayed) === 0, 'replayed ' . count($replayed) . ' records it had already committed');
    $rejoined->close();
}

section('auto.offset.reset');
{
    $resetTopic = unique('php-reset');
    $producer = new Producer($producerConfig());
    for ($i = 0; $i < 10; $i++) {
        $producer->send($resetTopic, "r{$i}");
    }
    $producer->flush();
    $producer->close();

    $consumer = new GroupConsumer($groupConfig(unique('php-latest'), ['auto.offset.reset' => 'latest']));
    $consumer->subscribe([$resetTopic]);
    $skipped = [];
    $until = nowMs() + 4_000;
    while (nowMs() < $until) {
        array_push($skipped, ...$consumer->poll(300));
    }
    check('latest skips records produced before the group existed', count($skipped) === 0, 'saw ' . count($skipped));
    $consumer->close();

    $strict = new GroupConsumer($groupConfig(unique('php-none'), ['auto.offset.reset' => 'none']));
    $strict->subscribe([$resetTopic]);
    $raised = false;
    $until = nowMs() + 5_000;
    while (nowMs() < $until && !$raised) {
        try {
            $strict->poll(300);
        } catch (NoOffsetForPartitionException) {
            $raised = true;
        }
    }
    check('none refuses to guess a position', $raised);
    $strict->close();
}

section('assignors');
foreach ([Assignor::RANGE, Assignor::ROUNDROBIN, Assignor::STICKY] as $assignor) {
    $assignorTopic = unique("php-{$assignor}");
    $producer = new Producer($producerConfig());
    for ($i = 0; $i < 20; $i++) {
        $producer->send($assignorTopic, "a{$i}");
    }
    $producer->flush();
    $producer->close();

    $consumer = new GroupConsumer($groupConfig(unique("php-grp-{$assignor}"), ['partition.assignment.strategy' => $assignor]));
    $consumer->subscribe([$assignorTopic]);
    $collected = [];
    $deadline = nowMs() + 20_000;
    while (count($collected) < 20 && nowMs() < $deadline) {
        array_push($collected, ...$consumer->poll(500));
    }
    check("{$assignor}: consumes every record", count($collected) === 20, 'got ' . count($collected));
    $consumer->close();
}

section('bounded client buffer');
{
    $bufferTopic = unique('php-buffer');
    $producer = new Producer([
        'bootstrap.servers' => $bootstrap,
        'linger.ms' => 10_000, // never flush on time during this check
        'buffer.memory' => 2048,
        'max.block.ms' => 300,
    ]);
    $blocked = false;
    for ($i = 0; $i < 500 && !$blocked; $i++) {
        try {
            $producer->send($bufferTopic, str_repeat('x', 256), null, [], 0);
        } catch (\Brahmaputra\Exception\BufferFullException $error) {
            $blocked = str_contains($error->getMessage(), 'buffer full');
        }
    }
    check('a full buffer blocks and then reports', $blocked);
}

section('wire edge cases');
{
    $edgeTopic = unique('php-edge');
    $producer = new Producer($producerConfig());
    $large = '';
    for ($i = 0; $i < (1 << 20); $i++) {
        $large .= chr(($i * 7) & 0xff);
    }
    $unicodeKey = 'ключ-✓-🔑';
    $unicodeValue = 'значение — 数据 — 🚀';
    $producer->send($edgeTopic, $large, null, [], 0);
    $producer->send($edgeTopic, $unicodeValue, $unicodeKey, [new RecordHeader('ünïcødé-🏷', '✓')], 0);
    // An empty key and an empty header value are values, not nulls.
    $producer->send($edgeTopic, 'empty-key', '', [new RecordHeader('empty', ''), new RecordHeader('null', null)], 0);
    $producer->send($edgeTopic, 'null-key', null, [], 0);
    $producer->close();

    $consumer = new Consumer($consumerConfig());
    $got = [];
    for ($offset = 0; count($got) < 4;) {
        try {
            $batch = $consumer->fetch($edgeTopic, 0, $offset, 500);
        } catch (\Throwable) {
            break;
        }
        if ($batch === []) {
            break;
        }
        array_push($got, ...$batch);
        $offset = $batch[count($batch) - 1]->offset + 1;
    }
    check('edge records all arrive', count($got) === 4, 'got ' . count($got));
    if (count($got) === 4) {
        check('a 1 MiB value round-trips byte-identical', $got[0]->value === $large, strlen((string) $got[0]->value) . ' bytes');
        check(
            'unicode key, value and header key round-trip',
            $got[1]->key === $unicodeKey && $got[1]->value === $unicodeValue
                && count($got[1]->headers) === 1 && $got[1]->headers[0]->key === 'ünïcødé-🏷',
        );
        check('an empty key stays empty, not null', $got[2]->key === '', var_export($got[2]->key, true));
        check(
            'an empty header value stays empty, not null',
            count($got[2]->headers) === 2 && $got[2]->headers[0]->value === '' && $got[2]->headers[1]->value === null,
            var_export($got[2]->headers, true),
        );
        check('a null key stays null', $got[3]->key === null, var_export($got[3]->key, true));
    }
    $consumer->close();
}

section('ordering under linger flushes');
{
    $orderTopic = unique('php-order');
    $producer = new Producer(['bootstrap.servers' => $bootstrap, 'linger.ms' => 1, 'batch.size' => 256]);
    $total = 5000;
    for ($i = 0; $i < $total; $i++) {
        $producer->send($orderTopic, (string) $i, null, [], 0);
    }
    $producer->close();
    $consumer = new Consumer($consumerConfig());
    $values = [];
    for ($offset = 0; count($values) < $total;) {
        try {
            $batch = $consumer->fetch($orderTopic, 0, $offset, 500);
        } catch (\Throwable) {
            break;
        }
        if ($batch === []) {
            break;
        }
        foreach ($batch as $record) {
            $values[] = (int) $record->value;
        }
        $offset = $batch[count($batch) - 1]->offset + 1;
    }
    $inversions = 0;
    for ($i = 1; $i < count($values); $i++) {
        if ($values[$i] < $values[$i - 1]) {
            $inversions++;
        }
    }
    check("every record of a partition arrives", count($values) === $total, 'got ' . count($values));
    check("a partition's records keep send order", $inversions === 0, "{$inversions} inversions");
    $consumer->close();
}

section('background flush failures are reported');
{
    $producer = new Producer(['bootstrap.servers' => $bootstrap, 'linger.ms' => 20]);
    // Partition 999 does not exist. PHP has no ticker thread, so the
    // "background" flush is the linger-expired one poll() performs; its
    // failure must not vanish, and must not be thrown from poll() either.
    $sendError = null;
    $pollError = null;
    $flushError = null;
    try {
        $producer->send(unique('php-bgfail'), 'lost', null, [], 999);
    } catch (\Throwable $e) {
        $sendError = $e;
    }
    usleep(300_000);
    try {
        $producer->poll(0);
    } catch (\Throwable $e) {
        $pollError = $e;
    }
    try {
        $producer->flush();
    } catch (\Throwable $e) {
        $flushError = $e;
    }
    check(
        'a failed linger flush surfaces on the next Flush',
        $sendError === null && $pollError === null && $flushError !== null,
        sprintf('send=%s poll=%s flush=%s', $sendError?->getMessage() ?? 'nil', $pollError?->getMessage() ?? 'nil', $flushError?->getMessage() ?? 'nil'),
    );
    $started = nowMs();
    try {
        $producer->close();
    } catch (\Throwable) {
    }
    check('Close returns after a failed flush', nowMs() - $started < 5000, 'hung');
}

section('connection failures');
{
    // A broker that accepts and never answers must cost an error, not a
    // process blocked forever. The kernel completes the handshake from the
    // listen backlog, so this socket never needs to accept().
    $silent = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($silent !== false) {
        [$silentHost, $silentPort] = explode(':', stream_socket_get_name($silent, false));
        $conn = \Brahmaputra\Connection::open($silentHost, (int) $silentPort, 'php-test', 1000);
        $conn->setRequestTimeout(300);
        $started = nowMs();
        $requestError = null;
        try {
            $conn->apiVersions();
        } catch (\Throwable $e) {
            $requestError = $e;
        }
        check(
            'a request to an unresponsive broker times out',
            $requestError !== null && nowMs() - $started < 3000,
            $requestError?->getMessage() ?? 'no error',
        );
        check('a timed-out connection is not reused', $conn->isBroken());
        $conn->close();
        fclose($silent);
    }

    // A connection the broker drops is redialled, not kept forever.
    $proxy = TestProxy::start($host, (int) $port);
    $dropTopic = unique('php-drop');
    $producer = new Producer(['bootstrap.servers' => $proxy->address, 'linger.ms' => 0]);
    $producer->send($dropTopic, 'before', null, [], 0);
    $proxy->dropAll();
    $recovered = 'not attempted';
    for ($attempt = 0; $attempt < 3 && $recovered !== null; $attempt++) {
        try {
            $producer->send($dropTopic, 'after', null, [], 0);
            $recovered = null;
        } catch (\Throwable $e) {
            $recovered = $e->getMessage();
        }
    }
    check('a producer recovers after its connection drops', $recovered === null, (string) $recovered);
    try {
        $producer->close();
    } catch (\Throwable) {
    }
    $consumer = new Consumer(['bootstrap.servers' => $proxy->address]);
    $consumer->fetch($dropTopic, 0, 0, 100);
    $proxy->dropAll();
    $fetchError = 'not attempted';
    $fetched = [];
    for ($attempt = 0; $attempt < 3 && $fetchError !== null; $attempt++) {
        try {
            $fetched = $consumer->fetch($dropTopic, 0, 0, 100);
            $fetchError = null;
        } catch (\Throwable $e) {
            $fetchError = $e->getMessage();
        }
    }
    check('a consumer recovers after its connection drops', $fetchError === null && count($fetched) >= 1, (string) $fetchError);
    $consumer->close();
    $proxy->close();
}

section('consumer group: max.poll.interval and rejoin');
{
    $slowTopic = unique('php-slow');
    $producer = new Producer($producerConfig());
    for ($i = 0; $i < 10; $i++) {
        $producer->send($slowTopic, "s{$i}");
    }
    $consumer = new GroupConsumer($groupConfig(unique('php-slow-grp'), ['max.poll.interval.ms' => 1500]));
    $consumer->subscribe([$slowTopic]);
    $first = [];
    $deadline = nowMs() + 15_000;
    while (count($first) < 10 && nowMs() < $deadline) {
        try {
            array_push($first, ...$consumer->poll(300));
        } catch (\Throwable) {
            break;
        }
    }
    $consumer->commit();
    // Stall past max.poll.interval.ms: the member leaves the group.
    usleep(2_500_000);
    for ($i = 10; $i < 20; $i++) {
        $producer->send($slowTopic, "s{$i}");
    }
    $producer->close();
    $second = [];
    $pollError = null;
    $deadline = nowMs() + 15_000;
    while (count($second) < 10 && nowMs() < $deadline) {
        try {
            array_push($second, ...$consumer->poll(300));
        } catch (\Throwable $e) {
            $pollError = $e;
            break;
        }
    }
    check(
        'a member that stalled rejoins on its next poll',
        count($first) === 10 && count($second) === 10 && $pollError === null,
        sprintf('first=%d second=%d err=%s', count($first), count($second), $pollError?->getMessage() ?? 'nil'),
    );
    $consumer->close();
}

section('consumer group: time inside poll does not count against max.poll.interval');
{
    $joinTopic = unique('php-inpoll');
    $producer = new Producer($producerConfig());
    $producer->router()->partitions($joinTopic);
    $producer->close();
    // Far shorter than the poll below, which spends ~1s joining (the
    // broker's initial rebalance delay) and then waits for data.
    $consumer = new GroupConsumer($groupConfig(unique('php-inpoll-grp'), ['max.poll.interval.ms' => 600]));
    $consumer->subscribe([$joinTopic]);
    // PHP has no threads: a forked child produces while the parent polls.
    $child = runInChild(static function () use ($bootstrap, $joinTopic): void {
        usleep(2_000_000);
        $late = new Producer(['bootstrap.servers' => $bootstrap, 'linger.ms' => 0]);
        for ($i = 0; $i < 10; $i++) {
            $late->send($joinTopic, "j{$i}");
        }
        $late->close();
    });
    $got = [];
    $pollError = null;
    $commitError = null;
    try {
        // One long poll: it joins, then waits for the records above.
        $got = $consumer->poll(4000);
    } catch (\Throwable $e) {
        $pollError = $e;
    }
    // Committed straight away, before another poll could quietly rejoin:
    // this fails if the member left the group mid-poll.
    try {
        $consumer->commit();
    } catch (\Throwable $e) {
        $commitError = $e;
    }
    check(
        'a member is still in its group after a long poll',
        $pollError === null && count($got) > 0 && $commitError === null,
        sprintf('got=%d poll=%s commit=%s', count($got), $pollError?->getMessage() ?? 'nil', $commitError?->getMessage() ?? 'nil'),
    );
    pcntl_waitpid($child, $status);
    $consumer->close();
}

echo "\n{$passed} passed, {$failed} failed\n";
exit($failed > 0 ? 1 : 0);

/**
 * Run $work in a forked child. The child ends with SIGKILL rather than
 * exit() so it never runs destructors on the parent's copies of open
 * clients (which would send LeaveGroup or commits on shared sockets).
 */
function runInChild(callable $work): int
{
    $pid = pcntl_fork();
    if ($pid < 0) {
        throw new \RuntimeException('fork failed');
    }
    if ($pid === 0) {
        try {
            $work();
        } catch (\Throwable $e) {
            fwrite(STDERR, 'child failed: ' . $e->getMessage() . "\n");
        }
        posix_kill(posix_getpid(), SIGKILL);
    }
    return $pid;
}

/**
 * Forwards TCP to the broker from a forked child and can sever every live
 * connection (SIGUSR1), which is how a broker restart or an idle timeout
 * looks to a client.
 */
final class TestProxy
{
    private function __construct(public readonly string $address, private readonly int $pid)
    {
    }

    public static function start(string $host, int $port): self
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($server === false) {
            throw new \RuntimeException("proxy: {$errstr}");
        }
        $address = stream_socket_get_name($server, false);
        $pid = runInChild(static function () use ($server, $host, $port): void {
            $drop = false;
            pcntl_async_signals(true);
            pcntl_signal(SIGUSR1, function () use (&$drop): void {
                $drop = true;
            });
            $pairs = []; // id => [client, upstream]
            while (true) {
                if ($drop) {
                    foreach ($pairs as [$a, $b]) {
                        @fclose($a);
                        @fclose($b);
                    }
                    $pairs = [];
                    $drop = false;
                }
                $read = [$server];
                foreach ($pairs as [$a, $b]) {
                    $read[] = $a;
                    $read[] = $b;
                }
                $write = null;
                $except = null;
                if (@stream_select($read, $write, $except, 0, 50_000) === false) {
                    continue; // interrupted by the signal
                }
                foreach ($read as $socket) {
                    if ($socket === $server) {
                        $client = @stream_socket_accept($server, 1);
                        $upstream = @stream_socket_client("tcp://{$host}:{$port}", $errno, $errstr, 2);
                        if ($client !== false && $upstream !== false) {
                            $pairs[] = [$client, $upstream];
                        } elseif ($client !== false) {
                            fclose($client);
                        }
                        continue;
                    }
                    foreach ($pairs as $id => [$a, $b]) {
                        if ($socket !== $a && $socket !== $b) {
                            continue;
                        }
                        $data = @fread($socket, 1 << 16);
                        if ($data === '' || $data === false) {
                            @fclose($a);
                            @fclose($b);
                            unset($pairs[$id]);
                        } else {
                            $peer = $socket === $a ? $b : $a;
                            for ($off = 0; $off < strlen($data);) {
                                $n = @fwrite($peer, substr($data, $off));
                                if ($n === false || $n === 0) {
                                    break;
                                }
                                $off += $n;
                            }
                        }
                        break;
                    }
                }
            }
        });
        fclose($server);
        return new self($address, $pid);
    }

    public function dropAll(): void
    {
        posix_kill($this->pid, SIGUSR1);
        usleep(100_000);
    }

    public function close(): void
    {
        posix_kill($this->pid, SIGKILL);
        pcntl_waitpid($this->pid, $status);
    }
}
