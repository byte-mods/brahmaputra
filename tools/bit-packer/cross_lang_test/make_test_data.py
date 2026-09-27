#!/usr/bin/env python3
"""Write test_data.bin: the canonical bench_complex WorldState, encoded by hand.

Like edge/make_ref.py, this shares no code with any generator, so a
generator bug cannot pass by agreeing with itself. The values are the ones
in test_python.py create_test_data(). test_data.bin is committed; every
target must encode the canonical value to exactly these bytes. Run with
--check to verify the committed file instead of rewriting it.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "edge"))
from make_ref import zz32, string, boolean, array  # noqa: E402  (pure wire helpers)

VERSION = "1.0.0"


def vec3(x, y, z):
    return zz32(x) + zz32(y) + zz32(z)


def item(id_, name, value, weight, rarity):
    return zz32(id_) + string(name) + zz32(value) + zz32(weight) + string(rarity)


def encode():
    sword = item(1, "Excalibur", 9999, 15, "Legendary")
    hero = b"".join([
        string("TestHero"), zz32(99), zz32(1000), zz32(500), boolean(True),
        vec3(10, -20, 30),
        array([1, 2, 3, 100], zz32),
        zz32(1) + sword,
    ])
    guild = string("TestGuild") + string("A test guild for cross-language") + zz32(1) + hero
    potion = item(2, "HealthPotion", 50, 1, "Common")
    return b"".join([
        string(VERSION),
        zz32(42), string("cross_lang_test"),
        zz32(1) + guild,
        zz32(1) + potion,
    ])


if __name__ == "__main__":
    data = encode()
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data.bin")
    if "--check" in sys.argv:
        with open(path, "rb") as f:
            same = f.read() == data
        print(f"{path}: {'matches' if same else 'DIFFERS from'} the hand-written encoding")
        sys.exit(0 if same else 1)
    with open(path, "wb") as f:
        f.write(data)
    print(f"wrote {path} ({len(data)} bytes)")
