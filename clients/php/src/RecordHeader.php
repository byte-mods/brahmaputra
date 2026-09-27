<?php

declare(strict_types=1);

namespace Brahmaputra;

/** An ordered, possibly repeating annotation on a record. A null value is kept distinct from "". */
final class RecordHeader
{
    public function __construct(
        public readonly string $key,
        public readonly ?string $value = null,
    ) {
    }
}
