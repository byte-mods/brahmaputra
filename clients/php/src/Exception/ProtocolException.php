<?php

declare(strict_types=1);

namespace Brahmaputra\Exception;

/** The broker sent bytes this client cannot decode, or a frame was malformed. */
class ProtocolException extends BrahmaputraException
{
}
