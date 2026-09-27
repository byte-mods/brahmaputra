<?php

declare(strict_types=1);

namespace Brahmaputra\Exception;

/** A request did not complete within request.timeout.ms. */
class TimeoutException extends ConnectionException
{
}
