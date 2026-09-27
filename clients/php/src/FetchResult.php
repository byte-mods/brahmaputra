<?php

declare(strict_types=1);

namespace Brahmaputra;

/** A fetch's records together with the partition's high watermark. */
final class FetchResult
{
    /** @param list<ConsumedRecord> $records */
    public function __construct(
        public readonly array $records,
        public readonly int $highWatermark,
    ) {
    }
}
