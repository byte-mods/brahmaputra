<?php
// PHP target, edge.buff. usage: php edge_test.php <cross_lang_test dir> <gen dir>
require __DIR__ . '/check.php';
require $argv[2] . '/edge/php/Inner.php';
$root = $argv[1];

function inner(int $big, string $label): Inner { $i = new Inner(); $i->big = $big; $i->label = $label; return $i; }

function makeEdge(): Edge {
    $e = new Edge();
    $e->i_min = -2147483648; $e->i_max = 2147483647; $e->i_zero = 0; $e->i_neg = -1;
    $e->l_min = PHP_INT_MIN; $e->l_max = PHP_INT_MAX; $e->l_neg = -300;
    $e->f = -1.25; $e->d = 1234.5625; $e->d_neg = -0.5;
    $e->yes = true; $e->no = false;
    $e->empty = ""; $e->unicode = "h\u{e9}llo w\u{f6}rld \u{2713} \u{65e5}\u{672c} \u{1F680}";
    $e->ints = [0, -1, 1, -64, 64, -2147483648, 2147483647];
    $e->longs = [0, -1, PHP_INT_MAX, PHP_INT_MIN, 4294967296];
    $e->floats = [0.0, 0.5, -2.25];
    $e->doubles = [0.0, 3.5, -1000000.25];
    $e->bools = [true, false, true];
    $e->strings = ["", "a", "\u{65e5}\u{672c}\u{8a9e}"];
    $e->no_ints = [];
    $e->inner = inner(1099511627776, "inner");
    $e->inners = [inner(-1, ""), inner(0, "x")];
    $e->no_inners = [];
    return $e;
}

$ref = file_get_contents("$root/edge/edge_ref.bin");
$enc = makeEdge()->encode();
check("edge encode == edge_ref.bin", $enc === $ref, bin2hex($enc));
try {
    $e = Edge::decode($ref);
    $w = makeEdge();
    foreach (get_object_vars($w) as $field => $want) {
        $got = $e->$field;
        // === on objects is identity, so compare nested classes by value;
        // everything else must match in type and value exactly.
        $ok = is_object($want) || (is_array($want) && $want && is_object($want[0])) ? $got == $want : $got === $want;
        check("edge decode $field", $ok, "got " . var_export($got, true));
    }
    check("edge re-encode == edge_ref.bin", $e->encode() === $ref, "bytes differ");
} catch (Throwable $ex) { check("edge decode edge_ref.bin", false, $ex->getMessage()); }
$dec = fn(string $b) => Edge::decode($b);
check("edge wrong version rejected", rejects($dec, badVersion($ref)), "decoded");
truncations("edge every truncation rejected", $ref, $dec);
// float32 variant: x10000 must be computed in single precision
$f32ref = file_get_contents("$root/edge/edge_float32_ref.bin");
$fv = makeEdge(); $fv->f = 0.29; $fv->floats = [0.7, 16777.217, -0.29];
check("edge float32 encode == edge_float32_ref.bin", $fv->encode() === $f32ref, bin2hex($fv->encode()));
try {
    $e = Edge::decode($f32ref);
    $f32 = fn(float $v) => unpack("g", pack("g", $v))[1];
    check("edge float32 decode", $e->f === $f32(0.29) && $e->floats === array_map($f32, $fv->floats), var_export([$e->f, $e->floats], true));
} catch (Throwable $ex) { check("edge float32 decode", false, $ex->getMessage()); }
// trailing bytes after the root class are ignored
try { check("edge trailing bytes ignored", Edge::decode($ref . "\x00\xff")->encode() === $ref, "decoded value differs"); }
catch (Throwable $ex) { check("edge trailing bytes ignored", false, $ex->getMessage()); }
// unencodable floats are errors, not saturated
$nan = makeEdge(); $nan->d = NAN;
check("edge encode NaN double rejected", rejects(fn($_) => $nan->encode(), ""), "encoded");
$big = makeEdge(); $big->f = 1e30;
check("edge encode out-of-range float rejected", rejects(fn($_) => $big->encode(), ""), "encoded");

// hostile input: bogus lengths must fail fast, not allocate or crash
check("edge hostile huge string length rejected", rejects(fn(string $b) => Inner::decode($b), hex2bin("0a322e312e30008080808010")), "decoded");
check("edge hostile negative string length rejected", rejects(fn(string $b) => Inner::decode($b), hex2bin("0a322e312e300001")), "decoded");
check("edge hostile endless varint rejected", rejects(fn(string $b) => Inner::decode($b), hex2bin("0a322e312e30ffffffffffffffffffffff")), "decoded");
check("edge hostile invalid UTF-8 string rejected", rejects(fn(string $b) => Inner::decode($b), hex2bin("0a322e312e300002ff")), "decoded");
exit(finish());
