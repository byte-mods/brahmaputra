<?php

declare(strict_types=1);

namespace Brahmaputra;

/** Sentinels for Consumer::listOffsets(); any other value is a unix-ms timestamp. */
final class Offset
{
    /** The oldest retained offset. */
    public const EARLIEST = -2;
    /** The next offset to be written (the log end). */
    public const LATEST = -1;
}
