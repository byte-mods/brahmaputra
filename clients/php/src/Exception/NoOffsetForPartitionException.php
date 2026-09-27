<?php

declare(strict_types=1);

namespace Brahmaputra\Exception;

/** auto.offset.reset=none and a partition has no committed position. */
class NoOffsetForPartitionException extends BrahmaputraException
{
}
