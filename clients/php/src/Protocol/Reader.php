<?php

declare(strict_types=1);

namespace Brahmaputra\Protocol;

use Brahmaputra\Exception\ProtocolException;

/**
 * Reads a BitPacker response body. The encoding is positional, so a field
 * this client ignores must still be *read* or everything after misaligns.
 */
final class Reader
{
    private int $pos = 0;

    public function __construct(private readonly string $data)
    {
    }

    /**
     * A reader positioned past the schema version, which is verified: a
     * mismatch means broker and client disagree about the message shapes
     * themselves, and decoding garbage into plausible fields is worse than
     * failing loudly.
     */
    public static function body(string $data): self
    {
        $reader = new self($data);
        $version = $reader->string();
        if ($version !== Protocol::SCHEMA_VERSION) {
            throw new ProtocolException(sprintf(
                'schema version mismatch: broker speaks %s, this client speaks %s',
                $version,
                Protocol::SCHEMA_VERSION,
            ));
        }
        return $reader;
    }

    public function remaining(): int
    {
        return strlen($this->data) - $this->pos;
    }

    public function int32(): int
    {
        return Varint::unzigzag32(Varint::decodeUnsigned($this->data, $this->pos));
    }

    public function int64(): int
    {
        return Varint::unzigzag64(Varint::decodeUnsigned($this->data, $this->pos));
    }

    public function bool(): bool
    {
        if ($this->pos >= strlen($this->data)) {
            throw new ProtocolException('truncated bool');
        }
        return $this->data[$this->pos++] !== "\x00";
    }

    public function string(): string
    {
        $length = $this->int32();
        if ($length < 0 || $this->pos + $length > strlen($this->data)) {
            throw new ProtocolException('truncated string');
        }
        $value = substr($this->data, $this->pos, $length);
        $this->pos += $length;
        return $value;
    }

    /** @return list<string> */
    public function stringArray(): array
    {
        $out = [];
        for ($count = $this->count(); $count > 0; $count--) {
            $out[] = $this->string();
        }
        return $out;
    }

    /** An array length, bounded by the bytes left so garbage cannot allocate gigabytes. */
    public function count(): int
    {
        $count = $this->int32();
        if ($count < 0 || $count > $this->remaining()) {
            throw new ProtocolException("implausible array count {$count}");
        }
        return $count;
    }

    public function rest(): string
    {
        $value = substr($this->data, $this->pos);
        $this->pos = strlen($this->data);
        return $value;
    }
}
