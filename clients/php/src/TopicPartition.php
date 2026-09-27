<?php

declare(strict_types=1);

namespace Brahmaputra;

/** A topic partition, optionally carrying an offset (as committed() returns it). */
final class TopicPartition
{
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
        public readonly ?int $offset = null,
    ) {
    }

    public function key(): string
    {
        return $this->topic . "\0" . $this->partition;
    }

    /** Orders by topic, then partition number — the Rust assignors' (String, i32) tuple order. */
    public static function compare(self $a, self $b): int
    {
        return strcmp($a->topic, $b->topic) ?: ($a->partition <=> $b->partition);
    }

    public function __toString(): string
    {
        return "{$this->topic}-{$this->partition}";
    }
}
