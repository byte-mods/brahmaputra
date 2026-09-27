#!/usr/bin/env python3
"""Write edge_ref.bin (the canonical Edge value) and edge_float32_ref.bin
(the same value with float fields that single precision rounds), encoded by hand.

This encoder is deliberately independent of every BitPacker generator, so
a generator bug cannot hide by agreeing with itself. It follows the wire
rules in README.md and nothing else.
"""
import os
import struct

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


def f32(v):
    """Round a Python float (a double) to the nearest float32."""
    return struct.unpack("<f", struct.pack("<f", v))[0]


def fixed_f32(v):
    # float fields: round v to float32, multiply by 10000 in float32 (the
    # float32 product is the double product rounded to float32, since both
    # factors have <= 24 significant bits), truncate toward zero.
    return zz64(int(f32(f32(v) * 10000.0)))


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


# The canonical value with float fields whose x10000 product is NOT exact in
# float32: 0.29 -> 2900, 0.7 -> 7000, 16777.217 -> 167772160, -0.29 -> -2900.
# A target that computes a float field's x10000 in double precision sends
# 2899, 6999, 167772167, -2899 instead and fails this fixture.
EDGE_F32 = dict(EDGE, f=0.29, floats=[0.7, 16777.217, -0.29])


def encode(e, float_enc=fixed):
    return b"".join([
        string(VERSION),
        zz32(e["i_min"]), zz32(e["i_max"]), zz32(e["i_zero"]), zz32(e["i_neg"]),
        zz64(e["l_min"]), zz64(e["l_max"]), zz64(e["l_neg"]),
        float_enc(e["f"]), fixed(e["d"]), fixed(e["d_neg"]),
        boolean(e["yes"]), boolean(e["no"]),
        string(e["empty"]), string(e["unicode"]),
        array(e["ints"], zz32), array(e["longs"], zz64),
        array(e["floats"], float_enc), array(e["doubles"], fixed),
        array(e["bools"], boolean), array(e["strings"], string),
        array(e["no_ints"], zz32),
        inner(e["inner"]),
        array(e["inners"], inner), array(e["no_inners"], inner),
    ])


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    # For the canonical EDGE, fixed_f32 and fixed agree (asserted here).
    assert encode(EDGE) == encode(EDGE, fixed_f32)
    for name, data in (("edge_ref.bin", encode(EDGE)),
                       ("edge_float32_ref.bin", encode(EDGE_F32, fixed_f32))):
        path = os.path.join(here, name)
        with open(path, "wb") as f:
            f.write(data)
        print(f"wrote {path} ({len(data)} bytes)")
