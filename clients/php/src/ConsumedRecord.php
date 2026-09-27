<?php

declare(strict_types=1);

namespace Brahmaputra;

/** One record as a consumer sees it. A null $value is a tombstone; "" is an empty value. */
final class ConsumedRecord
{
    /** @param list<RecordHeader> $headers */
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
        public readonly int $offset,
        public readonly ?string $key,
        public readonly ?string $value,
        public readonly int $timestamp,
        public readonly array $headers,
    ) {
    }

    /** The value of the first header named $name, or null (absent or a null value). */
    public function header(string $name): ?string
    {
        foreach ($this->headers as $header) {
            if ($header->key === $name) {
                return $header->value;
            }
        }
        return null;
    }
}
