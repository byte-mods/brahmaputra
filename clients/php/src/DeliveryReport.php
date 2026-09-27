<?php

declare(strict_types=1);

namespace Brahmaputra;

/**
 * The outcome of one produced batch, handed to `delivery.report.callback`.
 * $baseOffset is -1 when acks=0 (the broker does not answer) or on error.
 */
final class DeliveryReport
{
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
        public readonly int $baseOffset,
        public readonly int $recordCount,
        public readonly ?\Throwable $error = null,
    ) {
    }

    public function ok(): bool
    {
        return $this->error === null;
    }
}
