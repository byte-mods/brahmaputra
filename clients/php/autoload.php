<?php

declare(strict_types=1);

/*
 * PSR-4 autoloader for use without Composer.
 *
 *   require __DIR__ . '/path/to/clients/php/autoload.php';
 *
 * With Composer, `vendor/autoload.php` does the same job via the
 * `Brahmaputra\\` => `src/` mapping in composer.json; this file exists so
 * the driver and its test suite run on a bare PHP install.
 */
spl_autoload_register(static function (string $class): void {
    $prefix = 'Brahmaputra\\';
    if (strncmp($class, $prefix, strlen($prefix)) !== 0) {
        return;
    }
    $relative = substr($class, strlen($prefix));
    $file = __DIR__ . '/src/' . str_replace('\\', '/', $relative) . '.php';
    if (is_file($file)) {
        require $file;
    }
});
