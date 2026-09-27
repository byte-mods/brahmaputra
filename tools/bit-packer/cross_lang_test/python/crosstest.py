#!/usr/bin/env python3
"""Cross-language conformance test for the Python target.

usage: crosstest.py <cross_lang_test dir> <gen dir> pure|cext

`pure` runs with only the generated modules on sys.path (the pure-Python
ZeroCopyByteBuff); `cext` also puts the built _bitpacker extension on the
path and asserts it is the one in use.
"""
import os
import struct
import sys

root, gen, mode = sys.argv[1], sys.argv[2], sys.argv[3]
sys.path[:0] = [os.path.join(gen, "bench", "python"), os.path.join(gen, "edge", "python"), root]
if mode == "cext":
    sys.path.insert(0, os.path.join(gen, "cext"))

import bench_complex  # noqa: E402
import edge as edge_mod  # noqa: E402
from test_python import create_test_data, verify  # noqa: E402

passed = failed = 0


def check(name, ok, detail=""):
    global passed, failed
    name = f"{mode}: {name}"
    if ok:
        passed += 1
        print(f"  ok   {name}")
    else:
        failed += 1
        print(f"  FAIL {name} ({detail})")


def rejects(decode, data):
    try:
        decode(data)
    except Exception:
        return True
    return False


def truncations(name, data, decode):
    for cut in range(len(data)):
        if not rejects(decode, data[:cut]):
            check(name, False, f"prefix of {cut}/{len(data)} bytes decoded")
            return
    check(name, True)


def bad_version(data):
    b = bytearray(data)
    b[1] ^= 1
    return bytes(b)


def f32(v):
    return struct.unpack("<f", struct.pack("<f", v))[0]


def make_inner(big, label):
    i = edge_mod.Inner()
    i.big, i.label = big, label
    return i


def make_edge():
    e = edge_mod.Edge()
    e.i_min, e.i_max, e.i_zero, e.i_neg = -2147483648, 2147483647, 0, -1
    e.l_min, e.l_max, e.l_neg = -9223372036854775808, 9223372036854775807, -300
    e.f, e.d, e.d_neg = -1.25, 1234.5625, -0.5
    e.yes, e.no = True, False
    e.empty, e.unicode = "", "héllo wörld ✓ 日本 \U0001F680"
    e.ints = [0, -1, 1, -64, 64, -2147483648, 2147483647]
    e.longs = [0, -1, 9223372036854775807, -9223372036854775808, 4294967296]
    e.floats = [0.0, 0.5, -2.25]
    e.doubles = [0.0, 3.5, -1000000.25]
    e.bools = [True, False, True]
    e.strings = ["", "a", "日本語"]
    e.no_ints = []
    e.inner = make_inner(1099511627776, "inner")
    e.inners = [make_inner(-1, ""), make_inner(0, "x")]
    e.no_inners = []
    return e


def plain(v):
    if isinstance(v, list):
        return [plain(x) for x in v]
    if isinstance(v, edge_mod.Inner):
        return ("Inner", v.big, v.label)
    return (type(v).__name__, v)


def main():
    using_c = bench_complex._USING_C_EXT
    check("expected runtime in use", using_c == (mode == "cext"), f"_USING_C_EXT={using_c}")

    with open(os.path.join(root, "test_data.bin"), "rb") as f:
        ref = f.read()
    with open(os.path.join(root, "edge", "edge_ref.bin"), "rb") as f:
        edge_ref = f.read()

    enc = create_test_data().encode()
    if mode == "pure":
        with open(os.path.join(root, "test_data_python.bin"), "wb") as f:
            f.write(enc)
    check("bench encode == test_data.bin", enc == ref, f"{len(enc)} vs {len(ref)} bytes")
    for label, data in (("bench decode test_data.bin", ref), ("bench round-trip", enc)):
        try:
            w = bench_complex.WorldState.decode(data)
            verify(w, label)
            check(label, True)
            if data is enc:
                check("bench re-encode == encode", w.encode() == enc, "bytes differ")
        except Exception as ex:  # noqa: BLE001
            check(label, False, repr(ex))
    check("bench wrong version rejected", rejects(bench_complex.WorldState.decode, bad_version(ref)), "decoded")
    truncations("bench every truncation rejected", ref, bench_complex.WorldState.decode)

    eenc = make_edge().encode()
    check("edge encode == edge_ref.bin", eenc == edge_ref, eenc.hex())
    try:
        e = edge_mod.Edge.decode(edge_ref)
        want = make_edge()
        for field in edge_mod.Edge.__slots__:
            got_v, want_v = plain(getattr(e, field)), plain(getattr(want, field))
            check(f"edge decode {field}", got_v == want_v, f"got {got_v!r}, want {want_v!r}")
        check("edge re-encode == edge_ref.bin", e.encode() == edge_ref, "bytes differ")
    except Exception as ex:  # noqa: BLE001
        check("edge decode edge_ref.bin", False, repr(ex))
    check("edge wrong version rejected", rejects(edge_mod.Edge.decode, bad_version(edge_ref)), "decoded")
    truncations("edge every truncation rejected", edge_ref, edge_mod.Edge.decode)
    # float32 variant: x10000 must be computed in single precision
    with open(os.path.join(root, "edge", "edge_float32_ref.bin"), "rb") as f:
        f32ref = f.read()
    fv = make_edge()
    fv.f, fv.floats = 0.29, [0.7, 16777.217, -0.29]
    check("edge float32 encode == edge_float32_ref.bin", fv.encode() == f32ref, fv.encode().hex())
    try:
        e = edge_mod.Edge.decode(f32ref)
        want = [f32(x) for x in fv.floats]
        check("edge float32 decode", e.f == f32(0.29) and e.floats == want, f"{e.f!r} {e.floats!r}")
    except Exception as ex:  # noqa: BLE001
        check("edge float32 decode", False, repr(ex))
    # trailing bytes after the root class are ignored
    try:
        check("edge trailing bytes ignored",
              edge_mod.Edge.decode(edge_ref + b"\x00\xff").encode() == edge_ref, "decoded value differs")
    except Exception as ex:  # noqa: BLE001
        check("edge trailing bytes ignored", False, repr(ex))
    # unencodable floats are errors, not saturated
    nan = make_edge()
    nan.d = float("nan")
    check("edge encode NaN double rejected", rejects(lambda _: nan.encode(), b""), "encoded")
    big = make_edge()
    big.f = 1e30
    check("edge encode out-of-range float rejected", rejects(lambda _: big.encode(), b""), "encoded")

    # hostile input: bogus lengths must fail fast, not allocate or crash
    check("bench hostile huge array length rejected", rejects(bench_complex.WorldState.decode, bytes.fromhex("0a312e302e300000feffffff0f")), "decoded")
    check("bench hostile negative array length rejected", rejects(bench_complex.WorldState.decode, bytes.fromhex("0a312e302e30000001")), "decoded")
    check("edge hostile huge string length rejected", rejects(edge_mod.Inner.decode, bytes.fromhex("0a322e312e30008080808010")), "decoded")
    check("edge hostile negative string length rejected", rejects(edge_mod.Inner.decode, bytes.fromhex("0a322e312e300001")), "decoded")
    check("edge hostile endless varint rejected", rejects(edge_mod.Inner.decode, bytes.fromhex("0a322e312e30ffffffffffffffffffffff")), "decoded")
    check("edge hostile invalid UTF-8 string rejected", rejects(edge_mod.Inner.decode, bytes.fromhex("0a322e312e300002ff")), "decoded")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
