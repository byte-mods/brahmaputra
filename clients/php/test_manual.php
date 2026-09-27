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
use Brahmaputra\Exception\ProtocolException;
use Brahmaputra\Protocol\ApiKey;
use Brahmaputra\Protocol\Compression;
use Brahmaputra\Protocol\ErrorCode;
use Brahmaputra\Protocol\Murmur2;
use Brahmaputra\Protocol\Reader;
use Brahmaputra\Protocol\RecordBatch;
use Brahmaputra\Protocol\Writer;
use Brahmaputra\TopicPartition;
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

// ---------------------------------------------------------------------------
// The client feature checklist, item by item: every setting is shown to
// change behaviour, not merely to be accepted.
// ---------------------------------------------------------------------------

$fetchAll = static function (string $topic, int $partition) use ($consumerConfig): array {
    $consumer = new Consumer($consumerConfig());
    $out = [];
    for ($offset = 0;;) {
        $batch = $consumer->fetch($topic, $partition, $offset, 100);
        if ($batch === []) {
            break;
        }
        array_push($out, ...$batch);
        $offset = $batch[count($batch) - 1]->offset + 1;
    }
    $consumer->close();
    return $out;
};
$failure = static function (callable $work): ?\Throwable {
    try {
        $work();
        return null;
    } catch (\Throwable $error) {
        return $error;
    }
};

section('producer settings');
{
    $topic = unique('php-linger');
    $producer = new Producer($producerConfig(['linger.ms' => 50]));
    $producer->send($topic, 'lingered', null, [], 0);
    usleep(600_000);
    // No thread: the lingering batch goes out on the next producer call.
    $producer->poll(0);
    $got = $fetchAll($topic, 0);
    check('linger.ms sends a batch without an explicit flush', count($got) === 1, 'got ' . count($got) . ' before any flush');
    $producer->close();
}
{
    $topic = unique('php-batchsize');
    $producer = new Producer($producerConfig(['linger.ms' => 60_000, 'batch.size' => 200]));
    for ($i = 0; $i < 10; $i++) {
        $producer->send($topic, str_repeat('b', 50), null, [], 0);
    }
    $got = $fetchAll($topic, 0);
    check('batch.size sends a full batch before linger expires', count($got) >= 3, 'got ' . count($got) . ' of 10 with linger 60s');
    $producer->close();
}
{
    $topic = unique('php-closeflush');
    $producer = new Producer($producerConfig(['linger.ms' => 60_000]));
    for ($i = 0; $i < 5; $i++) {
        $producer->send($topic, "c{$i}", null, [], 0);
    }
    $producer->close();
    $got = $fetchAll($topic, 0);
    check('close flushes buffered records', count($got) === 5, 'got ' . count($got));
}
{
    $topic = unique('php-sync');
    $producer = new Producer($producerConfig());
    $first = $producer->sendSync($topic, 's0', null, [], 1);
    $second = $producer->sendSync($topic, 's1', null, [], 1);
    $keyed = $producer->sendSync($topic, 's2', 'k');
    $producer->close();
    $got = $fetchAll($topic, 1);
    check(
        "send-and-wait returns the record's offset",
        $first === 0 && $second === 1 && $keyed >= 0 && count($got) >= 2 && $got[1]->value === 's1',
        "offsets {$first} {$second} {$keyed}",
    );
}
{
    $topic = unique('php-roundrobin');
    $producer = new Producer($producerConfig());
    $partitions = $producer->router()->partitions($topic);
    for ($i = 0; $i < 2 * count($partitions); $i++) {
        $producer->send($topic, "rr{$i}");
    }
    $producer->close();
    $counts = array_map(static fn (int $p): int => count($fetchAll($topic, $p)), $partitions);
    check('null keys are spread round-robin', count($partitions) > 1 && array_unique($counts) === [2], json_encode($counts));
}
{
    $topic = unique('php-timestamp');
    $producer = new Producer($producerConfig());
    $producer->send($topic, 't1', null, [], 0, 1_600_000_001_000);
    $producer->send($topic, 't2', null, [], 0, 1_600_000_002_000);
    $producer->sendSync($topic, 't3', null, [], 0, 1_600_000_003_000);
    $producer->close();
    $got = $fetchAll($topic, 0);
    check(
        'an explicit record timestamp is kept',
        count($got) === 3 && $got[0]->timestamp === 1_600_000_001_000 && $got[2]->timestamp === 1_600_000_003_000,
        json_encode(array_map(static fn ($r) => $r->timestamp, $got)),
    );
    $consumer = new Consumer($consumerConfig());
    $byTime = $consumer->listOffsets($topic, 0, 1_600_000_001_500);
    $pastEnd = $consumer->listOffsets($topic, 0, 1_700_000_000_000);
    check('list offsets by timestamp finds the first record at or after it', $byTime === 1 && $pastEnd === 3, "byTime={$byTime} pastEnd={$pastEnd}");
    $consumer->close();
}
{
    $producer = new Producer($producerConfig(['acks' => 'all', 'request.timeout.ms' => 1500]));
    $error = $failure(static fn () => $producer->sendSync(unique('php-acksall'), 'durable'));
    check('acks=all with request.timeout.ms is acknowledged', $error === null, (string) $error?->getMessage());
    $producer->close();
}
{
    $calls = ['compress' => 0, 'decompress' => 0];
    Compression::register(
        Compression::LZ4,
        static function (string $payload) use (&$calls): string {
            $calls['compress']++;
            return lz4Encode($payload);
        },
        static function (string $payload) use (&$calls): string {
            $calls['decompress']++;
            return lz4Decode($payload);
        },
    );
    $topic = unique('php-lz4');
    $producer = new Producer($producerConfig(['compression.type' => 'lz4']));
    $body = str_repeat('registered codec ', 30);
    for ($i = 0; $i < 5; $i++) {
        $producer->send($topic, $body, (string) $i, [], 0);
    }
    $producer->close();
    $got = $fetchAll($topic, 0);
    $ok = count($got) === 5;
    foreach ($got as $record) {
        $ok = $ok && $record->value === $body;
    }
    check('a registered lz4 codec round-trips', $ok && $calls['compress'] > 0 && $calls['decompress'] > 0, 'got ' . count($got) . ' ' . json_encode($calls));
}

section('retries (fault-injecting proxy)');
{
    $proxy = FaultProxy::start($host, (int) $port);
    $topic = unique('php-retry');
    $viaProxy = static function (array $overrides) use ($proxy, $topic): Producer {
        $producer = new Producer($overrides + ['bootstrap.servers' => $proxy->address, 'linger.ms' => 0]);
        $producer->router()->partitions($topic);
        return $producer;
    };

    $producer = $viaProxy(['retries' => 5, 'retry.backoff.ms' => 50]);
    $proxy->inject(ErrorCode::NOT_LEADER_OR_FOLLOWER, 2);
    $offset = null;
    $error = $failure(static function () use ($producer, $topic, &$offset): void {
        $offset = $producer->sendSync($topic, 'eventually', null, [], 0);
    });
    check(
        'a retriable produce error is retried until it succeeds',
        $error === null && $offset === 0 && $proxy->produces() === 3,
        sprintf('err=%s offset=%s attempts=%d', $error?->getMessage() ?? 'nil', var_export($offset, true), $proxy->produces()),
    );
    $producer->close();

    $producer = $viaProxy(['retries' => 2, 'retry.backoff.ms' => 200]);
    $proxy->inject(ErrorCode::NOT_LEADER_OR_FOLLOWER, -1);
    $started = nowMs();
    $error = $failure(static fn () => $producer->sendSync($topic, 'never', null, [], 0));
    $elapsed = nowMs() - $started;
    check('retry.backoff.ms spaces the retries', $error !== null && $proxy->produces() === 3 && $elapsed >= 400, "attempts={$proxy->produces()} elapsed={$elapsed}ms");
    $failure(static fn () => $producer->close());

    $producer = $viaProxy(['retries' => 5, 'retry.backoff.ms' => 1000]);
    $proxy->inject(ErrorCode::INVALID_REQUEST, -1);
    $started = nowMs();
    $error = $failure(static fn () => $producer->sendSync($topic, 'rejected', null, [], 0));
    check('a non-retriable produce error is not retried', $error !== null && $proxy->produces() === 1 && nowMs() - $started < 1000, "attempts={$proxy->produces()}");
    $failure(static fn () => $producer->close());

    $producer = $viaProxy(['retries' => 1_000_000, 'retry.backoff.ms' => 50, 'delivery.timeout.ms' => 600]);
    $proxy->inject(ErrorCode::NOT_LEADER_OR_FOLLOWER, -1);
    $started = nowMs();
    $error = $failure(static fn () => $producer->sendSync($topic, 'late', null, [], 0));
    $elapsed = nowMs() - $started;
    check('delivery.timeout.ms bounds the retries', $error !== null && $elapsed < 3000 && $proxy->produces() > 2, "attempts={$proxy->produces()} elapsed={$elapsed}ms");
    $failure(static fn () => $producer->close());
    $proxy->close();
}

section('consumer settings');
{
    $topic = unique('php-fetch');
    $producer = new Producer($producerConfig());
    for ($i = 0; $i < 10; $i++) {
        $producer->send($topic, str_repeat(chr(97 + $i), 1000), null, [], 0);
    }
    $producer->close();

    $consumer = new Consumer($consumerConfig());
    $result = $consumer->fetchVerbose($topic, 0, 0, 100);
    check('fetch reports the high watermark', $result->highWatermark === 10 && count($result->records) === 10, "hw={$result->highWatermark}");
    $metadata = $consumer->router()->metadata([$topic], true);
    $partitions = array_map(static fn (array $info): int => $info['partition'], $metadata->topics[$topic] ?? []);
    $led = count($partitions) === 4;
    foreach ($partitions as $partition) {
        $led = $led && $metadata->leaderOf($topic, $partition) >= 0;
    }
    check('metadata lists every partition with a leader', $led, json_encode($partitions));
    $consumer->close();

    $capped = new Consumer($consumerConfig() + ['fetch.max.bytes' => 2500]);
    $got = $capped->fetch($topic, 0, 0, 100);
    check('fetch.max.bytes caps a response', count($got) >= 1 && count($got) < 10, 'got ' . count($got) . ' of 10');
    $capped->close();

    $patient = new Consumer($consumerConfig() + ['fetch.min.bytes' => 1 << 20, 'fetch.max.wait.ms' => 400]);
    $started = nowMs();
    $got = $patient->fetch($topic, 0, 0, 400);
    $elapsed = nowMs() - $started;
    check('fetch.min.bytes waits up to fetch.max.wait.ms for more data', count($got) === 10 && $elapsed >= 300 && $elapsed < 3000, 'got ' . count($got) . " after {$elapsed}ms");
    $patient->close();
}
{
    // A length prefix larger than what follows, or negative, is an error —
    // never a read past the end or a huge allocation.
    $batch = RecordBatch::encode([['key' => null, 'value' => 'x', 'timestampDelta' => 0, 'headers' => []]], 0);
    $oversized = substr($batch, 0, 8) . pack('N', 0x7fffffff) . substr($batch, 12);
    $negative = substr($batch, 0, 8) . pack('N', 0xfffffff0) . substr($batch, 12);
    $errors = [
        $failure(static fn () => RecordBatch::decode($oversized, 0)),
        $failure(static fn () => RecordBatch::decode($negative, 0)),
        $failure(static fn () => Reader::body("\x7e1")),
    ];
    $allProtocol = true;
    foreach ($errors as $error) {
        $allProtocol = $allProtocol && $error instanceof ProtocolException;
    }
    check('a truncated or oversized length is an error, not a crash', $allProtocol, implode(' / ', array_map(static fn ($e) => $e ? get_class($e) : 'none', $errors)));
}

section('consumer group settings');
$produceN = static function (string $topic, int $n) use ($producerConfig): void {
    $producer = new Producer($producerConfig());
    for ($i = 0; $i < $n; $i++) {
        $producer->send($topic, "m{$i}");
    }
    $producer->close();
};
$pollUntil = static function (GroupConsumer $consumer, int $want, int $limitMs): array {
    $got = [];
    $largest = 0;
    $deadline = nowMs() + $limitMs;
    while (count($got) < $want && nowMs() < $deadline) {
        try {
            $records = $consumer->poll(300);
        } catch (\Throwable) {
            break;
        }
        $largest = max($largest, count($records));
        array_push($got, ...$records);
    }
    return [$got, $largest];
};
$partitionsOf = static function (array $assignment, string $topic): array {
    $out = [];
    foreach ($assignment as $tp) {
        if ($tp->topic === $topic) {
            $out[] = $tp->partition;
        }
    }
    sort($out);
    return $out;
};
{
    $topic = unique('php-maxpoll');
    $produceN($topic, 20);
    $consumer = new GroupConsumer($groupConfig(unique('php-maxpoll-grp'), ['max.poll.records' => 5]));
    $consumer->subscribe([$topic]);
    [$got, $largest] = $pollUntil($consumer, 20, 20_000);
    check('max.poll.records caps one poll', count($got) === 20 && $largest <= 5, 'got ' . count($got) . ", largest poll {$largest}");
    $consumer->close();
}
{
    $topic = unique('php-autocommit');
    $produceN($topic, 12);
    $consumer = new GroupConsumer($groupConfig(unique('php-autocommit-grp'), ['enable.auto.commit' => true, 'auto.commit.interval.ms' => 200]));
    $consumer->subscribe([$topic]);
    $pollUntil($consumer, 12, 20_000);
    usleep(300_000);
    $consumer->poll(300);
    $total = 0;
    foreach ($consumer->committed() as $tp) {
        $total += $tp->offset;
    }
    check('auto-commit commits delivered positions', $total === 12, (string) $total);
    $consumer->close();
}
{
    $topic = unique('php-heartbeat');
    $produceN($topic, 4);
    $consumer = new GroupConsumer($groupConfig(unique('php-heartbeat-grp'), ['session.timeout.ms' => 1500, 'heartbeat.interval.ms' => 300]));
    $consumer->subscribe([$topic]);
    $pollUntil($consumer, 4, 20_000);
    $generation = $consumer->generation();
    // Four seconds of "processing" without a poll — well past
    // session.timeout.ms. PHP has no heartbeat thread, so the processing
    // loop heartbeats explicitly, as the README says to.
    $until = nowMs() + 4000;
    $alive = true;
    while (nowMs() < $until) {
        $alive = $consumer->heartbeat() && $alive;
        usleep(300_000);
    }
    $error = $failure(static fn () => $consumer->commit());
    check('heartbeats keep an idle member in its group', $alive && $error === null && $consumer->generation() === $generation, (string) $error?->getMessage());
    $consumer->close();
}
{
    $first = unique('php-multi-a');
    $second = unique('php-multi-b');
    $produceN($first, 6);
    $produceN($second, 7);
    $consumer = new GroupConsumer($groupConfig(unique('php-multi-grp')));
    $consumer->subscribe([$first, $second]);
    [$got] = $pollUntil($consumer, 13, 20_000);
    $a = count(array_filter($got, static fn ($r) => $r->topic === $first));
    $b = count(array_filter($got, static fn ($r) => $r->topic === $second));
    check('a member subscribed to two topics consumes both', $a === 6 && $b === 7, "{$a}/{$b}");
    $consumer->close();
}
{
    $topic = unique('php-static');
    $produceN($topic, 4);
    $groupId = unique('php-static-grp');
    $original = new GroupConsumer($groupConfig($groupId, ['group.instance.id' => 'instance-1']));
    $original->subscribe([$topic]);
    $pollUntil($original, 4, 20_000);
    $memberId = $original->memberId();
    // The same instance comes back (a restart) before the old session has
    // expired: it must reclaim the slot, not join as a stranger.
    $returning = new GroupConsumer($groupConfig($groupId, ['group.instance.id' => 'instance-1']));
    $returning->subscribe([$topic]);
    $failure(static fn () => $returning->poll(2000));
    check('a returning static member reclaims its member id', $memberId !== '' && $returning->memberId() === $memberId, "{$memberId} then {$returning->memberId()}");
    $returning->close();
    $failure(static fn () => $original->close());
}
{
    $topic = unique('php-leave');
    $produceN($topic, 8);
    $groupId = unique('php-leave-grp');
    $settings = ['session.timeout.ms' => 30_000, 'rebalance.timeout.ms' => 10_000];
    $leaving = new GroupConsumer($groupConfig($groupId, $settings));
    $leaving->subscribe([$topic]);
    $pollUntil($leaving, 8, 20_000);
    $leaving->close();
    $successor = new GroupConsumer($groupConfig($groupId, $settings));
    $successor->subscribe([$topic]);
    $started = nowMs();
    while (count($partitionsOf($successor->assignment(), $topic)) < 4 && nowMs() - $started < 15_000) {
        $failure(static fn () => $successor->poll(200));
    }
    $elapsed = nowMs() - $started;
    check('close leaves the group so the next member is assigned at once', count($partitionsOf($successor->assignment(), $topic)) === 4 && $elapsed < 6000, "assigned after {$elapsed}ms");
    $successor->close();
}
{
    $topic = unique('php-fence');
    $produceN($topic, 8);
    $groupId = unique('php-fence-grp');
    // Each member heartbeats (inside poll) often enough to answer a
    // rebalance well within the other's rebalance timeout.
    $settings = ['rebalance.timeout.ms' => 2000, 'heartbeat.interval.ms' => 200];
    $first = new GroupConsumer($groupConfig($groupId, $settings));
    $first->subscribe([$topic]);
    $pollUntil($first, 8, 20_000);
    $oldGeneration = $first->generation();
    // A second member, in a forked child (PHP has no threads), joins while
    // the first stops polling: the group moves on without it. The child
    // reports its latest assignment through a file.
    $report = tempnam(sys_get_temp_dir(), 'php-fence');
    $child = runInChild(static function () use ($groupConfig, $groupId, $settings, $topic, $report): void {
        $second = new GroupConsumer($groupConfig($groupId, $settings));
        $second->subscribe([$topic]);
        $until = nowMs() + 16_000;
        while (nowMs() < $until) {
            try {
                $second->poll(200);
            } catch (\Throwable) {
            }
            $parts = [];
            foreach ($second->assignment() as $tp) {
                $parts[] = $tp->partition;
            }
            file_put_contents($report, json_encode(['generation' => $second->generation(), 'partitions' => $parts]), LOCK_EX);
        }
    });
    usleep(6_000_000);
    $error = $failure(static fn () => $first->commit());
    $seen = json_decode((string) @file_get_contents($report), true) ?: [];
    check('a commit from a superseded generation is fenced', $error !== null, "old={$oldGeneration} new=" . ($seen['generation'] ?? '?'));

    // Both members polling settle on a split of the partitions.
    $until = nowMs() + 8000;
    while (nowMs() < $until) {
        $failure(static fn () => $first->poll(200));
    }
    $a = $partitionsOf($first->assignment(), $topic);
    $seen = json_decode((string) @file_get_contents($report), true) ?: [];
    $b = $seen['partitions'] ?? [];
    $union = array_merge($a, $b);
    sort($union);
    check('two members share the partitions without overlap', $union === [0, 1, 2, 3] && $a !== [] && $b !== [], json_encode($a) . ' / ' . json_encode($b));
    pcntl_waitpid($child, $status);
    @unlink($report);
    $failure(static fn () => $first->close());
}
{
    // Partitions compare as integers: 2 sorts before 10, and the member
    // over quota keeps its lowest-numbered partitions.
    $held = array_map(static fn (int $p) => new TopicPartition('t', $p), [10, 2, 11, 3, 0, 1, 9]);
    $assignment = Assignor::assign(Assignor::STICKY, [['id' => 'a', 'topics' => ['t']], ['id' => 'b', 'topics' => ['t']]], ['t' => range(0, 11)], ['a' => $held]);
    $kept = array_map(static fn ($tp) => $tp->partition, $assignment['a'] ?? []);
    check('sticky compares partitions as integers', $kept === [0, 1, 2, 3, 9, 10] && count($assignment['b'] ?? []) === 6, json_encode($kept));
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

/** A size-prefixed LZ4 block holding one literal-only sequence: valid LZ4, just uncompressed. */
function lz4Encode(string $src): string
{
    $n = strlen($src);
    if ($n < 15) {
        $head = chr($n << 4);
    } else {
        $rest = $n - 15;
        $head = "\xf0" . str_repeat("\xff", intdiv($rest, 255)) . chr($rest % 255);
    }
    return pack('V', $n) . $head . $src;
}

/** Decode any size-prefixed LZ4 block. */
function lz4Decode(string $src): string
{
    $size = unpack('V', $src)[1];
    $out = '';
    $pos = 4;
    $end = strlen($src);
    $length = static function (int $base) use ($src, &$pos): int {
        $n = $base;
        if ($base === 15) {
            do {
                $b = ord($src[$pos++]);
                $n += $b;
            } while ($b === 255);
        }
        return $n;
    };
    while ($pos < $end) {
        $token = ord($src[$pos++]);
        $literals = $length($token >> 4);
        $out .= substr($src, $pos, $literals);
        $pos += $literals;
        if ($pos >= $end) {
            break;
        }
        $offset = unpack('v', $src, $pos)[1];
        $pos += 2;
        $match = $length($token & 15) + 4;
        if ($offset === 0 || $offset > strlen($out)) {
            throw new \RuntimeException('lz4: bad offset');
        }
        for ($i = 0; $i < $match; $i++) {
            $out .= $out[strlen($out) - $offset];
        }
    }
    if (strlen($out) !== $size) {
        throw new \RuntimeException('lz4: size mismatch');
    }
    return $out;
}

/**
 * Forwards frames to the broker one request at a time from a forked child,
 * but answers Produce requests itself with an injected error code — the
 * only way to make a healthy single broker return a retriable error on
 * demand. The parent steers it through a control file and reads back how
 * many Produce requests it saw.
 */
final class FaultProxy
{
    private int $epoch = 0;

    private function __construct(
        public readonly string $address,
        private readonly int $pid,
        private readonly string $control,
        private readonly string $counter,
    ) {
    }

    public static function start(string $host, int $port): self
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($server === false) {
            throw new \RuntimeException("fault proxy: {$errstr}");
        }
        $address = stream_socket_get_name($server, false);
        $control = tempnam(sys_get_temp_dir(), 'fp-ctl');
        $counter = tempnam(sys_get_temp_dir(), 'fp-cnt');
        file_put_contents($control, json_encode(['epoch' => 0, 'code' => 0, 'failures' => 0]));
        file_put_contents($counter, '0');
        $pid = runInChild(static function () use ($server, $host, $port, $control, $counter): void {
            $readExact = static function ($socket, int $length): ?string {
                $data = '';
                while (strlen($data) < $length) {
                    $chunk = @fread($socket, $length - strlen($data));
                    if ($chunk === '' || $chunk === false) {
                        return null;
                    }
                    $data .= $chunk;
                }
                return $data;
            };
            $readFrame = static function ($socket) use ($readExact): ?string {
                $head = $readExact($socket, 4);
                if ($head === null) {
                    return null;
                }
                $body = $readExact($socket, unpack('N', $head)[1]);
                return $body === null ? null : $head . $body;
            };
            $epoch = -1;
            $failures = 0;
            $code = 0;
            $seen = 0;
            $pairs = [];
            while (true) {
                $read = [$server];
                foreach ($pairs as [$client]) {
                    $read[] = $client;
                }
                $write = null;
                $except = null;
                if (@stream_select($read, $write, $except, 1) === false) {
                    continue;
                }
                foreach ($read as $socket) {
                    if ($socket === $server) {
                        $client = @stream_socket_accept($server, 1);
                        $upstream = @stream_socket_client("tcp://{$host}:{$port}", $errno, $errstr, 2);
                        if ($client !== false && $upstream !== false) {
                            $pairs[(int) $client] = [$client, $upstream];
                        }
                        continue;
                    }
                    [$client, $upstream] = $pairs[(int) $socket];
                    $frame = $readFrame($client);
                    if ($frame === null) {
                        @fclose($client);
                        @fclose($upstream);
                        unset($pairs[(int) $socket]);
                        continue;
                    }
                    if (unpack('n', $frame, 4)[1] === ApiKey::PRODUCE) {
                        $state = json_decode((string) file_get_contents($control), true);
                        if ($state['epoch'] !== $epoch) {
                            $epoch = $state['epoch'];
                            $failures = $state['failures'];
                            $code = $state['code'];
                            $seen = 0;
                        }
                        $seen++;
                        file_put_contents($counter, (string) $seen);
                        if ($failures !== 0) {
                            if ($failures > 0) {
                                $failures--;
                            }
                            $clientLen = unpack('n', $frame, 12)[1];
                            $header = substr($frame, 4, 10 + $clientLen);
                            $body = $header . Writer::body()->string('')->int32(0)->int32($code)->int64(-1)->int64(-1)->bytes();
                            fwrite($client, pack('N', strlen($body)) . $body);
                            continue;
                        }
                    }
                    fwrite($upstream, $frame);
                    $response = $readFrame($upstream);
                    if ($response === null) {
                        @fclose($client);
                        @fclose($upstream);
                        unset($pairs[(int) $socket]);
                        continue;
                    }
                    fwrite($client, $response);
                }
            }
        });
        fclose($server);
        return new self($address, $pid, $control, $counter);
    }

    public function inject(int $code, int $failures): void
    {
        $this->epoch++;
        file_put_contents($this->counter, '0');
        file_put_contents($this->control, json_encode(['epoch' => $this->epoch, 'code' => $code, 'failures' => $failures]));
    }

    public function produces(): int
    {
        return (int) file_get_contents($this->counter);
    }

    public function close(): void
    {
        posix_kill($this->pid, SIGKILL);
        pcntl_waitpid($this->pid, $status);
        @unlink($this->control);
        @unlink($this->counter);
    }
}
