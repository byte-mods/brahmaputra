<?php

declare(strict_types=1);

namespace Brahmaputra;

/**
 * Partition assignment strategies (`partition.assignment.strategy`).
 *
 * The group leader computes the assignment client-side and hands it to the
 * coordinator in SyncGroup. Each strategy mirrors the Rust client's, so a
 * PHP leader and a Rust leader produce the same assignment for the same
 * group. Members are passed as list<array{id:string, topics:list<string>}>,
 * partitions as topic => sorted list<int>; the result is
 * memberId => list<TopicPartition>.
 */
final class Assignor
{
    public const RANGE = 'range';
    public const ROUNDROBIN = 'roundrobin';
    /**
     * Keeps members on the partitions they already hold; prefer it when
     * consumers carry per-partition state.
     */
    public const STICKY = 'sticky';

    public const ALL = [self::RANGE, self::ROUNDROBIN, self::STICKY];

    /**
     * @param list<array{id:string, topics:list<string>}> $members
     * @param array<string, list<int>> $topicPartitions
     * @param array<string, list<TopicPartition>> $previous
     * @return array<string, list<TopicPartition>>
     */
    public static function assign(string $strategy, array $members, array $topicPartitions, array $previous = []): array
    {
        return match ($strategy) {
            self::RANGE => self::range($members, $topicPartitions),
            self::ROUNDROBIN => self::roundRobin($members, $topicPartitions),
            self::STICKY => self::sticky($members, $topicPartitions, $previous),
            default => throw new \InvalidArgumentException("unknown partition.assignment.strategy {$strategy}"),
        };
    }

    /** Contiguous ranges per topic; the first (n % members) take one extra. */
    public static function range(array $members, array $topicPartitions): array
    {
        $assignment = self::empty($members);
        ksort($topicPartitions, SORT_STRING);
        foreach ($topicPartitions as $topic => $partitions) {
            $subscribers = [];
            foreach ($members as $member) {
                if (in_array($topic, $member['topics'], true)) {
                    $subscribers[] = $member['id'];
                }
            }
            if ($subscribers === []) {
                continue;
            }
            sort($subscribers, SORT_STRING);
            $base = intdiv(count($partitions), count($subscribers));
            $extra = count($partitions) % count($subscribers);
            $cursor = 0;
            foreach ($subscribers as $index => $memberId) {
                $take = $base + ($index < $extra ? 1 : 0);
                foreach (array_slice($partitions, $cursor, $take) as $partition) {
                    $assignment[$memberId][] = new TopicPartition((string) $topic, $partition);
                }
                $cursor += $take;
            }
        }
        return $assignment;
    }

    /** Deal every partition around the circle of members sorted by id. */
    public static function roundRobin(array $members, array $topicPartitions): array
    {
        $assignment = self::empty($members);
        $circle = $members;
        usort($circle, fn (array $a, array $b) => strcmp($a['id'], $b['id']));
        if ($circle === []) {
            return $assignment;
        }
        ksort($topicPartitions, SORT_STRING);
        $cursor = 0;
        foreach ($topicPartitions as $topic => $partitions) {
            foreach ($partitions as $partition) {
                $start = $cursor;
                while (true) {
                    $member = $circle[$cursor % count($circle)];
                    $cursor++;
                    if (in_array($topic, $member['topics'], true)) {
                        $assignment[$member['id']][] = new TopicPartition((string) $topic, $partition);
                        break;
                    }
                    if ($cursor - $start >= count($circle)) {
                        break; // nobody subscribes
                    }
                }
            }
        }
        return $assignment;
    }

    /** Keep members on what they hold; move only what balance requires. */
    public static function sticky(array $members, array $topicPartitions, array $previous): array
    {
        $assignment = self::empty($members);
        if ($members === []) {
            return $assignment;
        }
        $topicsOf = [];
        foreach ($members as $member) {
            $topicsOf[$member['id']] = $member['topics'];
        }
        $subscribes = fn (string $memberId, string $topic): bool =>
            in_array($topic, $topicsOf[$memberId] ?? [], true);

        ksort($topicPartitions, SORT_STRING);
        ksort($previous, SORT_STRING);

        // Every partition that needs an owner, and who has a valid claim on it.
        $unassigned = [];
        $claimed = []; // list of [TopicPartition, memberId], in (topic, partition) order
        foreach ($topicPartitions as $topic => $partitions) {
            foreach ($partitions as $partition) {
                $tp = new TopicPartition((string) $topic, $partition);
                $holder = null;
                foreach ($previous as $memberId => $held) {
                    foreach ($held as $h) {
                        if ($h->topic === $tp->topic && $h->partition === $partition && $subscribes((string) $memberId, $tp->topic)) {
                            $holder = (string) $memberId;
                            break 2;
                        }
                    }
                }
                if ($holder === null) {
                    $unassigned[] = $tp;
                } else {
                    $claimed[] = [$tp, $holder];
                }
            }
        }

        // Fair share among members subscribed to at least one live topic.
        $eligible = [];
        foreach ($members as $member) {
            foreach ($member['topics'] as $topic) {
                if (isset($topicPartitions[$topic])) {
                    $eligible[] = $member['id'];
                    break;
                }
            }
        }
        if ($eligible === []) {
            return $assignment;
        }
        sort($eligible, SORT_STRING);
        $total = array_sum(array_map('count', $topicPartitions));
        $base = intdiv($total, count($eligible));
        $extra = $total % count($eligible);
        $quota = [];
        foreach ($eligible as $index => $memberId) {
            $quota[$memberId] = $base + ($index < $extra ? 1 : 0);
        }

        // Honour claims up to quota; the overflow joins the pool.
        $kept = [];
        foreach ($claimed as [$tp, $memberId]) {
            $kept[$memberId] ??= [];
            if (count($kept[$memberId]) < ($quota[$memberId] ?? 0)) {
                $kept[$memberId][] = $tp;
            } else {
                $unassigned[] = $tp;
            }
        }
        foreach ($kept as $memberId => $held) {
            if (isset($assignment[$memberId])) {
                $assignment[$memberId] = $held;
            }
        }

        usort($unassigned, [TopicPartition::class, 'compare']);
        foreach ($unassigned as $tp) {
            $taker = null;
            foreach ($eligible as $memberId) {
                if ($subscribes($memberId, $tp->topic) && count($assignment[$memberId]) < ($quota[$memberId] ?? 0)) {
                    $taker = $memberId;
                    break;
                }
            }
            if ($taker === null) {
                // Quotas exhausted (uneven subscriptions): an unassigned
                // partition is a stalled one, so fall back to any subscriber.
                foreach ($eligible as $memberId) {
                    if ($subscribes($memberId, $tp->topic)) {
                        $taker = $memberId;
                        break;
                    }
                }
            }
            if ($taker !== null) {
                $assignment[$taker][] = $tp;
            }
        }
        foreach ($assignment as &$held) {
            usort($held, [TopicPartition::class, 'compare']);
        }
        unset($held);
        return $assignment;
    }

    /** @return array<string, list<TopicPartition>> */
    private static function empty(array $members): array
    {
        $out = [];
        foreach ($members as $member) {
            $out[$member['id']] = [];
        }
        return $out;
    }
}
