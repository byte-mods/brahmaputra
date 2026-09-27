<?php

declare(strict_types=1);

namespace Brahmaputra\Exception;

use Brahmaputra\Protocol\ErrorCode;

/** The broker answered with a non-zero error code. */
class ServerException extends BrahmaputraException
{
    public function __construct(public readonly int $errorCode, string $context = '')
    {
        $name = ErrorCode::name($errorCode);
        parent::__construct(
            "broker returned {$name}[{$errorCode}]" . ($context !== '' ? " ({$context})" : ''),
            $errorCode,
        );
    }

    /** True for codes the broker returns before appending, so a retry cannot duplicate. */
    public function isRetriable(): bool
    {
        return ErrorCode::isRetriable($this->errorCode);
    }
}
