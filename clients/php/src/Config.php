<?php

declare(strict_types=1);

namespace Brahmaputra;

/**
 * Kafka-style configuration: a flat array of dotted keys, as
 * php-rdkafka's `RdKafka\Conf::set()` and the Java client take them.
 *
 * Unknown keys are rejected rather than ignored, because a misspelled
 * `linger.ms` that silently falls back to the default is the kind of
 * mistake that is only found in production.
 */
final class Config
{
    /**
     * @param array<string, mixed> $defaults
     * @param array<string, mixed> $given
     * @return array<string, mixed>
     */
    public static function resolve(array $defaults, array $given, string $what): array
    {
        $unknown = array_diff_key($given, $defaults);
        if ($unknown !== []) {
            throw new \InvalidArgumentException(sprintf(
                'unknown %s config %s; known keys: %s',
                $what,
                implode(', ', array_keys($unknown)),
                implode(', ', array_keys($defaults)),
            ));
        }
        $config = array_replace($defaults, $given);
        if (!is_string($config['bootstrap.servers'] ?? null) || $config['bootstrap.servers'] === '') {
            throw new \InvalidArgumentException("{$what} config needs bootstrap.servers (\"host:port[,host:port]\")");
        }
        return $config;
    }

    /**
     * Parse "host:port,host:port" into [[host, port], ...].
     *
     * @return list<array{0:string,1:int}>
     */
    public static function parseBootstrap(string $servers): array
    {
        $out = [];
        foreach (explode(',', $servers) as $server) {
            $server = trim($server);
            if ($server === '') {
                continue;
            }
            $colon = strrpos($server, ':');
            if ($colon === false) {
                $out[] = [$server, 9092];
                continue;
            }
            $host = trim(substr($server, 0, $colon), '[]');
            $out[] = [$host, (int) substr($server, $colon + 1)];
        }
        if ($out === []) {
            throw new \InvalidArgumentException("bootstrap.servers is empty");
        }
        return $out;
    }

    public static function nowMs(): int
    {
        return intdiv(hrtime(true), 1_000_000) + self::epochOffsetMs();
    }

    /** Wall clock, for record timestamps. */
    public static function wallMs(): int
    {
        return (int) floor(microtime(true) * 1000);
    }

    public static function sleepMs(int $ms): void
    {
        if ($ms > 0) {
            usleep($ms * 1000);
        }
    }

    /** Anchors the monotonic clock to wall time once, so nowMs() is monotonic but epoch-like. */
    private static function epochOffsetMs(): int
    {
        static $offset = null;
        return $offset ??= self::wallMs() - intdiv(hrtime(true), 1_000_000);
    }
}
