module test;

import std.conv : to;
import std.file : read, write;
import std.format : format;
import std.path : buildPath;
import std.stdio : writeln;

static import bench_complex;
static import edge;
import bench_complex : WorldState, Guild, Character, Item, Vec3;
import edge : Edge, Inner;

int passed, failed;

void check(string name, bool ok, lazy string detail = "")
{
    if (ok)
    {
        ++passed;
        writeln("  ok   ", name);
    }
    else
    {
        ++failed;
        writeln("  FAIL ", name, " (", detail, ")");
    }
}

bool rejects(scope void delegate() dg)
{
    try
        dg();
    catch (edge.BitPackerException)
        return true;
    catch (bench_complex.BitPackerException)
        return true;
    return false;
}

// ---------------------------------------------------------------- bench

WorldState makeWorld()
{
    auto sword = Item(1, "Excalibur", 9999, 15, "Legendary");
    Character hero = {
        name: "TestHero", level: 99, hp: 1000, mp: 500, isAlive: true,
        position: Vec3(10, -20, 30), skills: [1, 2, 3, 100], inventory: [sword],
    };
    Guild guild = {name: "TestGuild", description: "A test guild for cross-language", members: [hero]};
    auto potion = Item(2, "HealthPotion", 50, 1, "Common");
    WorldState w = {worldId: 42, seed: "cross_lang_test", guilds: [guild], lootTable: [potion]};
    return w;
}

string verifyWorld(const WorldState w)
{
    if (w.worldId != 42) return "world_id";
    if (w.seed != "cross_lang_test") return "seed";
    if (w.guilds.length != 1) return "guilds length";
    auto g = &w.guilds[0];
    if (g.name != "TestGuild") return "guild name";
    if (g.description != "A test guild for cross-language") return "guild description";
    if (g.members.length != 1) return "members length";
    auto h = &g.members[0];
    if (h.name != "TestHero") return "hero name";
    if (h.level != 99 || h.hp != 1000 || h.mp != 500) return "hero stats";
    if (!h.isAlive) return "hero is_alive";
    if (h.position.x != 10 || h.position.y != -20 || h.position.z != 30) return "position";
    if (h.skills != [1, 2, 3, 100]) return "skills " ~ h.skills.to!string;
    if (h.inventory.length != 1) return "inventory length";
    auto s = &h.inventory[0];
    if (s.name != "Excalibur" || s.value != 9999 || s.rarity != "Legendary") return "sword";
    if (w.lootTable.length != 1) return "loot length";
    auto p = &w.lootTable[0];
    if (p.name != "HealthPotion" || p.rarity != "Common") return "potion";
    return null;
}

// ---------------------------------------------------------------- edge

Edge makeEdge()
{
    Edge e = {
        iMin: int.min, iMax: int.max, iZero: 0, iNeg: -1,
        lMin: long.min, lMax: long.max, lNeg: -300,
        f: -1.25f, d: 1234.5625, dNeg: -0.5,
        yes: true, no: false,
        empty: "", unicode: "héllo wörld ✓ 日本 \U0001F680",
        ints: [0, -1, 1, -64, 64, int.min, int.max],
        longs: [0, -1, long.max, long.min, 4294967296L],
        floats: [0.0f, 0.5f, -2.25f],
        doubles: [0.0, 3.5, -1000000.25],
        bools: [true, false, true],
        strings: ["", "a", "日本語"],
        noInts: [],
        inner: Inner(1099511627776L, "inner"),
        inners: [Inner(-1, ""), Inner(0, "x")],
        noInners: [],
    };
    return e;
}

long readZigzag(const(ubyte)[] b, size_t pos)
{
    ulong u;
    for (uint shift = 0;; shift += 7)
    {
        immutable x = b[pos++];
        u |= cast(ulong)(x & 0x7F) << shift;
        if (x < 0x80) break;
    }
    return cast(long)((u >> 1) ^ (0UL - (u & 1)));
}

int main(string[] args)
{
    immutable dir = args.length > 1 ? args[1] : "..";

    auto benchRef = cast(const(ubyte)[]) read(buildPath(dir, "test_data.bin"));
    auto enc = makeWorld().encode();
    write(buildPath(dir, "test_data_d.bin"), enc);
    check("bench encode == test_data.bin", enc == benchRef, format("%s vs %s bytes", enc.length, benchRef.length));
    try
    {
        auto dec = WorldState.decode(benchRef);
        auto err = verifyWorld(dec);
        check("bench decode test_data.bin", err is null, err);
        check("bench round-trip", dec.encode() == benchRef);
        check("bench decoded == built value", dec == makeWorld());
    }
    catch (bench_complex.BitPackerException ex)
        check("bench decode test_data.bin", false, ex.msg);

    auto edgeRef = cast(const(ubyte)[]) read(buildPath(dir, "edge", "edge_ref.bin"));
    auto eenc = makeEdge().encode();
    check("edge encode == edge_ref.bin", eenc == edgeRef, format("%s vs %s bytes", eenc.length, edgeRef.length));

    try
    {
        auto e = Edge.decode(edgeRef);
        auto x = makeEdge();
        static foreach (i, field; Edge.tupleof)
            check("edge field " ~ __traits(identifier, field), e.tupleof[i] == x.tupleof[i],
                  e.tupleof[i].to!string);
        check("edge float values exact", e.f == -1.25f && e.floats[2] == -2.25f && e.doubles[2] == -1000000.25);
        check("edge round-trip", e.encode() == edgeRef);
    }
    catch (edge.BitPackerException ex)
        check("edge decode edge_ref.bin", false, ex.msg);

    // float fields: x10000 must happen in float32, exactly as Go/Java do.
    // (float32 bits, Go's int64(v * 10000.0) for v float32, bits of Go's float32(n) / 10000.0)
    static immutable uint[3][] floatCases = [
        [0x3dcccccd, 1000, 0x3dcccccd],             // 0.1
        [0x3f333333, 7000, 0x3f333333],             // 0.7   (float64 math gives 6999)
        [0x40490fd0, 31415, 0x40490e56],            // 3.14159
        [0x4683126f, 167772160, 0x4683126f],        // 16777.217 (float64: 167772167)
        [0x4851b717, 2147483648u, 0x4851b717],      // 214748.36 (float64: 2147483593)
        [0x391d4952, 1, 0x38d1b717],                // 0.00015
    ];
    foreach (c; floatCases)
    {
        uint bits = c[0];
        immutable float v = *cast(float*)&bits;
        Edge m;
        m.f = v;
        auto msg = m.encode();
        immutable got = readZigzag(msg, 13); // 6 bytes version + 4 ints + 3 longs, one byte each
        float back = Edge.decode(msg).f;
        immutable backBits = *cast(uint*)&back;
        check(format("float32 fixed-point %s -> %s", v, c[1]), got == c[1] && backBits == c[2],
              format("encoded %s, decoded bits 0x%08x", got, backBits));
    }

    auto bad = edgeRef.dup;
    bad[5] = '9'; // "2.1.0" -> "2.1.9"
    check("edge wrong version rejected", rejects({ Edge.decode(bad); }));
    check("edge other schema's message rejected", rejects({ Edge.decode(benchRef); }));

    long firstBad = -1;
    foreach (n; 0 .. edgeRef.length)
    {
        if (!rejects({ Edge.decode(edgeRef[0 .. n]); }))
        {
            firstBad = n;
            break;
        }
    }
    check(format("edge all %s truncations rejected", edgeRef.length), firstBad == -1,
          format("prefix of %s bytes decoded", firstBad));

    // a bogus huge length must fail fast, not allocate
    ubyte[] huge = [5, '2', '.', '1', '.', '0'];
    huge ~= new ubyte[](12); // 4 ints, 3 longs, 3 floats, 2 bools
    huge ~= [0xfe, 0xff, 0xff, 0xff, 0x0f]; // `empty` claims ~2^31 bytes
    check("edge huge length rejected", rejects({ Edge.decode(huge); }));

    // random byte mutations: must decode or throw BitPackerException, never anything else
    import std.random : Random, uniform;
    auto rng = Random(12345);
    bool fuzzOk = true;
    string fuzzDetail;
    foreach (_; 0 .. 3000)
    {
        auto m = edgeRef.dup;
        foreach (__; 0 .. uniform(1, 5, rng))
            m[uniform(0, m.length, rng)] = cast(ubyte) uniform(0, 256, rng);
        try
            Edge.decode(m);
        catch (edge.BitPackerException)
        {
        }
        catch (Throwable t)
        {
            fuzzOk = false;
            fuzzDetail = t.toString();
            break;
        }
    }
    check("edge 3000 random mutations: value or BitPackerException", fuzzOk, fuzzDetail);

    writeln("d: ", passed, " passed, ", failed, " failed");
    return failed == 0 ? 0 : 1;
}
