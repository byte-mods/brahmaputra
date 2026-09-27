#!/usr/bin/env python3
"""Write edge_ref.bin: the canonical Edge value, encoded by hand.

This encoder is deliberately independent of every BitPacker generator, so
a generator bug cannot hide by agreeing with itself. It follows the wire
rules in README.md and nothing else.
"""
import os

VERSION = "2.1.0"


def uvarint(n):
    out = bytearray()
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def zz32(v):
    assert -(1 << 31) <= v < (1 << 31)
    return uvarint(((v << 1) ^ (v >> 31)) & 0xFFFFFFFF)


def zz64(v):
    assert -(1 << 63) <= v < (1 << 63)
    return uvarint(((v << 1) ^ (v >> 63)) & 0xFFFFFFFFFFFFFFFF)


def fixed(v):
    # float and double: value * 10000, truncated toward zero, as zigzag int64.
    # Every canonical value is chosen so that product is exact in float32 and
    # float64, so no target can legitimately round differently.
    scaled = v * 10000
    assert scaled == int(scaled), v
    return zz64(int(scaled))


def string(s):
    b = s.encode("utf-8")
    return zz64(len(b)) + b


def boolean(b):
    return b"\x01" if b else b"\x00"


def array(items, enc):
    return zz32(len(items)) + b"".join(enc(i) for i in items)


def inner(o):
    return zz64(o["big"]) + string(o["label"])


EDGE = {
    "i_min": -2147483648, "i_max": 2147483647, "i_zero": 0, "i_neg": -1,
    "l_min": -9223372036854775808, "l_max": 9223372036854775807, "l_neg": -300,
    "f": -1.25, "d": 1234.5625, "d_neg": -0.5,
    "yes": True, "no": False,
    "empty": "", "unicode": "héllo wörld ✓ 日本 \U0001F680",
    "ints": [0, -1, 1, -64, 64, -2147483648, 2147483647],
    "longs": [0, -1, 9223372036854775807, -9223372036854775808, 4294967296],
    "floats": [0.0, 0.5, -2.25],
    "doubles": [0.0, 3.5, -1000000.25],
    "bools": [True, False, True],
    "strings": ["", "a", "日本語"],
    "no_ints": [],
    "inner": {"big": 1099511627776, "label": "inner"},
    "inners": [{"big": -1, "label": ""}, {"big": 0, "label": "x"}],
    "no_inners": [],
}


def encode(e):
    return b"".join([
        string(VERSION),
        zz32(e["i_min"]), zz32(e["i_max"]), zz32(e["i_zero"]), zz32(e["i_neg"]),
        zz64(e["l_min"]), zz64(e["l_max"]), zz64(e["l_neg"]),
        fixed(e["f"]), fixed(e["d"]), fixed(e["d_neg"]),
        boolean(e["yes"]), boolean(e["no"]),
        string(e["empty"]), string(e["unicode"]),
        array(e["ints"], zz32), array(e["longs"], zz64),
        array(e["floats"], fixed), array(e["doubles"], fixed),
        array(e["bools"], boolean), array(e["strings"], string),
        array(e["no_ints"], zz32),
        inner(e["inner"]),
        array(e["inners"], inner), array(e["no_inners"], inner),
    ])


if __name__ == "__main__":
    data = encode(EDGE)
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "edge_ref.bin")
    with open(path, "wb") as f:
        f.write(data)
    print(f"wrote {path} ({len(data)} bytes)")
