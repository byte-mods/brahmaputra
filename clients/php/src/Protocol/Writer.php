<?php

declare(strict_types=1);

namespace Brahmaputra\Protocol;

/**
 * Builds a BitPacker request body. Every integer goes out as a zigzag
 * varint, every string and array as a varint count then its contents —
 * which is why this cannot share code with the record-batch encoder.
 */
final class Writer
{
    private string $buffer = '';

    /** A writer already carrying the schema version every body starts with. */
    public static function body(): self
    {
        return (new self())->string(Protocol::SCHEMA_VERSION);
    }

    public function raw(string $bytes): self
    {
        $this->buffer .= $bytes;
        return $this;
    }

    public function int32(int $value): self
    {
        $this->buffer .= Varint::encodeUnsigned(Varint::zigzag32($value));
        return $this;
    }

    public function int64(int $value): self
    {
        $this->buffer .= Varint::encodeUnsigned(Varint::zigzag64($value));
        return $this;
    }

    public function bool(bool $value): self
    {
        $this->buffer .= $value ? "\x01" : "\x00";
        return $this;
    }

    public function string(string $value): self
    {
        $this->int32(strlen($value));
        $this->buffer .= $value;
        return $this;
    }

    /** @param list<string> $values */
    public function stringArray(array $values): self
    {
        $this->int32(count($values));
        foreach ($values as $value) {
            $this->string($value);
        }
        return $this;
    }

    public function bytes(): string
    {
        return $this->buffer;
    }
}
