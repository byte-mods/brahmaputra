<?php
// Check helpers shared by bench_test.php and edge_test.php.
$GLOBALS['passed'] = 0;
$GLOBALS['failed'] = 0;

function check(string $name, bool $ok, string $detail = ""): void {
    if ($ok) { $GLOBALS['passed']++; echo "  ok   $name\n"; }
    else { $GLOBALS['failed']++; echo "  FAIL $name ($detail)\n"; }
}

function rejects(callable $decode, string $b): bool {
    try { $decode($b); return false; } catch (Throwable $e) { return true; }
}

function truncations(string $name, string $data, callable $decode): void {
    for ($cut = 0; $cut < strlen($data); $cut++) {
        if (!rejects($decode, substr($data, 0, $cut))) {
            check($name, false, "prefix of $cut/" . strlen($data) . " bytes decoded");
            return;
        }
    }
    check($name, true);
}

function badVersion(string $b): string { $b[1] = chr(ord($b[1]) ^ 1); return $b; }

function finish(): int { return $GLOBALS['failed'] > 0 ? 1 : 0; }
