// Cross-language conformance test for the C# target. run.sh generates
// namespaces BenchGen (bench_complex.buff) and EdgeGen (edge.buff) first.
using System;
using System.IO;
using System.Linq;
using B = BenchGen;
using E = EdgeGen;

static class CrossTest {
    static int passed = 0, failed = 0;

    static void Check(string name, bool ok, string detail) {
        if (ok) { passed++; Console.WriteLine("  ok   " + name); }
        else { failed++; Console.WriteLine("  FAIL " + name + " (" + detail + ")"); }
    }

    static string Show(object o) {
        if (o is System.Collections.IEnumerable en && !(o is string))
            return "[" + string.Join(",", en.Cast<object>().Select(Show)) + "]";
        if (o is E.Inner i) return "Inner{" + i.big + "," + i.label + "}";
        if (o is double d) return d.ToString("R");
        if (o is float f) return f.ToString("R");
        return o == null ? "null" : o.ToString();
    }

    static void Eq(string name, object got, object want) {
        string g = Show(got), w = Show(want);
        Check(name, g == w, "got " + g + ", want " + w);
    }

    static byte[] BadVersion(byte[] data) { var b = (byte[])data.Clone(); b[1] ^= 1; return b; }

    static bool Rejects(Action<byte[]> decode, byte[] b) {
        try { decode(b); return false; } catch (Exception) { return true; }
    }

    static void Truncations(string name, byte[] data, Action<byte[]> decode) {
        for (int cut = 0; cut < data.Length; cut++) {
            if (!Rejects(decode, data.Take(cut).ToArray())) {
                Check(name, false, $"prefix of {cut}/{data.Length} bytes decoded");
                return;
            }
        }
        Check(name, true, "");
    }

    static B.WorldState MakeWorld() {
        var sword = new B.Item { id = 1, name = "Excalibur", value = 9999, weight = 15, rarity = "Legendary" };
        var hero = new B.Character {
            name = "TestHero", level = 99, hp = 1000, mp = 500, is_alive = true,
            position = new B.Vec3 { x = 10, y = -20, z = 30 },
            skills = new[] { 1, 2, 3, 100 }, inventory = new[] { sword },
        };
        var guild = new B.Guild { name = "TestGuild", description = "A test guild for cross-language", members = new[] { hero } };
        var potion = new B.Item { id = 2, name = "HealthPotion", value = 50, weight = 1, rarity = "Common" };
        return new B.WorldState { world_id = 42, seed = "cross_lang_test", guilds = new[] { guild }, loot_table = new[] { potion } };
    }

    static bool VerifyWorld(B.WorldState w) {
        if (w.world_id != 42 || w.seed != "cross_lang_test" || w.guilds.Length != 1) return false;
        var g = w.guilds[0];
        if (g.name != "TestGuild" || g.description != "A test guild for cross-language" || g.members.Length != 1) return false;
        var h = g.members[0];
        return h.name == "TestHero" && h.level == 99 && h.hp == 1000 && h.mp == 500 && h.is_alive
            && h.position.x == 10 && h.position.y == -20 && h.position.z == 30
            && h.skills.SequenceEqual(new[] { 1, 2, 3, 100 })
            && h.inventory.Length == 1 && h.inventory[0].name == "Excalibur"
            && h.inventory[0].value == 9999 && h.inventory[0].rarity == "Legendary"
            && w.loot_table.Length == 1 && w.loot_table[0].name == "HealthPotion" && w.loot_table[0].rarity == "Common";
    }

    static E.Edge MakeEdge() => new E.Edge {
        i_min = -2147483648, i_max = 2147483647, i_zero = 0, i_neg = -1,
        l_min = long.MinValue, l_max = 9223372036854775807L, l_neg = -300,
        f = -1.25f, d = 1234.5625, d_neg = -0.5,
        yes = true, no = false,
        empty = "", unicode = "héllo wörld ✓ 日本 🚀",
        ints = new[] { 0, -1, 1, -64, 64, -2147483648, 2147483647 },
        longs = new[] { 0L, -1L, 9223372036854775807L, long.MinValue, 4294967296L },
        floats = new[] { 0.0f, 0.5f, -2.25f },
        doubles = new[] { 0.0, 3.5, -1000000.25 },
        bools = new[] { true, false, true },
        strings = new[] { "", "a", "日本語" },
        no_ints = new int[0],
        inner = new E.Inner { big = 1099511627776L, label = "inner" },
        inners = new[] { new E.Inner { big = -1, label = "" }, new E.Inner { big = 0, label = "x" } },
        no_inners = new E.Inner[0],
    };

    static void VerifyEdge(E.Edge e) {
        var w = MakeEdge();
        Eq("edge decode i_min", e.i_min, w.i_min);
        Eq("edge decode i_max", e.i_max, w.i_max);
        Eq("edge decode i_zero", e.i_zero, w.i_zero);
        Eq("edge decode i_neg", e.i_neg, w.i_neg);
        Eq("edge decode l_min", e.l_min, w.l_min);
        Eq("edge decode l_max", e.l_max, w.l_max);
        Eq("edge decode l_neg", e.l_neg, w.l_neg);
        Check("edge decode f", e.f == w.f, Show(e.f));
        Check("edge decode d", e.d == w.d, Show(e.d));
        Check("edge decode d_neg", e.d_neg == w.d_neg, Show(e.d_neg));
        Eq("edge decode yes", e.yes, w.yes);
        Eq("edge decode no", e.no, w.no);
        Eq("edge decode empty", e.empty, w.empty);
        Eq("edge decode unicode", e.unicode, w.unicode);
        Eq("edge decode ints", e.ints, w.ints);
        Eq("edge decode longs", e.longs, w.longs);
        Check("edge decode floats", e.floats.SequenceEqual(w.floats), Show(e.floats));
        Check("edge decode doubles", e.doubles.SequenceEqual(w.doubles), Show(e.doubles));
        Eq("edge decode bools", e.bools, w.bools);
        Eq("edge decode strings", e.strings, w.strings);
        Eq("edge decode no_ints", e.no_ints, w.no_ints);
        Eq("edge decode inner", e.inner, w.inner);
        Eq("edge decode inners", e.inners, w.inners);
        Eq("edge decode no_inners", e.no_inners, w.no_inners);
    }

    static int Main(string[] args) {
        string root = args[0];
        byte[] reference = File.ReadAllBytes(Path.Combine(root, "test_data.bin"));
        byte[] edgeRef = File.ReadAllBytes(Path.Combine(root, "edge", "edge_ref.bin"));

        byte[] enc = MakeWorld().Encode();
        File.WriteAllBytes(Path.Combine(root, "test_data_csharp.bin"), enc);
        Check("bench encode == test_data.bin", enc.SequenceEqual(reference), $"{enc.Length} vs {reference.Length} bytes");
        try { Check("bench decode test_data.bin", VerifyWorld(B.WorldState.Decode(reference)), "field mismatch"); }
        catch (Exception ex) { Check("bench decode test_data.bin", false, ex.Message); }
        try {
            var w = B.WorldState.Decode(enc);
            Check("bench round-trip", VerifyWorld(w), "field mismatch");
            Check("bench re-encode == encode", w.Encode().SequenceEqual(enc), "bytes differ");
        } catch (Exception ex) { Check("bench round-trip", false, ex.Message); }
        Check("bench wrong version rejected", Rejects(b => B.WorldState.Decode(b), BadVersion(reference)), "decoded");
        Truncations("bench every truncation rejected", reference, b => B.WorldState.Decode(b));

        byte[] eenc = MakeEdge().Encode();
        Check("edge encode == edge_ref.bin", eenc.SequenceEqual(edgeRef), BitConverter.ToString(eenc));
        try {
            var e = E.Edge.Decode(edgeRef);
            VerifyEdge(e);
            Check("edge re-encode == edge_ref.bin", e.Encode().SequenceEqual(edgeRef), "bytes differ");
        } catch (Exception ex) { Check("edge decode edge_ref.bin", false, ex.Message); }
        Check("edge wrong version rejected", Rejects(b => E.Edge.Decode(b), BadVersion(edgeRef)), "decoded");
        Truncations("edge every truncation rejected", edgeRef, b => E.Edge.Decode(b));
        // float32 variant: x10000 must be computed in single precision
        byte[] f32ref = File.ReadAllBytes(Path.Combine(root, "edge", "edge_float32_ref.bin"));
        var fv = MakeEdge();
        fv.f = 0.29f; fv.floats = new[] { 0.7f, 16777.217f, -0.29f };
        Check("edge float32 encode == edge_float32_ref.bin", fv.Encode().SequenceEqual(f32ref), BitConverter.ToString(fv.Encode()));
        try {
            var e = E.Edge.Decode(f32ref);
            Check("edge float32 decode", e.f == fv.f && e.floats.SequenceEqual(fv.floats), Show(e.f) + " " + Show(e.floats));
        } catch (Exception ex) { Check("edge float32 decode", false, ex.Message); }
        // trailing bytes after the root class are ignored
        try {
            var trailing = edgeRef.Concat(new byte[] { 0x00, 0xff }).ToArray();
            Check("edge trailing bytes ignored", E.Edge.Decode(trailing).Encode().SequenceEqual(edgeRef), "decoded value differs");
        } catch (Exception ex) { Check("edge trailing bytes ignored", false, ex.Message); }
        // unencodable floats are errors, not saturated
        var nan = MakeEdge(); nan.d = double.NaN;
        Check("edge encode NaN double rejected", Rejects(_ => nan.Encode(), new byte[0]), "encoded");
        var big = MakeEdge(); big.f = 1e30f;
        Check("edge encode out-of-range float rejected", Rejects(_ => big.Encode(), new byte[0]), "encoded");

        // hostile input: bogus lengths must fail fast, not allocate or crash
        Check("bench hostile huge array length rejected", Rejects(b => B.WorldState.Decode(b), Convert.FromHexString("0a312e302e300000feffffff0f")), "decoded");
        Check("bench hostile negative array length rejected", Rejects(b => B.WorldState.Decode(b), Convert.FromHexString("0a312e302e30000001")), "decoded");
        Check("edge hostile huge string length rejected", Rejects(b => E.Inner.Decode(b), Convert.FromHexString("0a322e312e30008080808010")), "decoded");
        Check("edge hostile negative string length rejected", Rejects(b => E.Inner.Decode(b), Convert.FromHexString("0a322e312e300001")), "decoded");
        Check("edge hostile endless varint rejected", Rejects(b => E.Inner.Decode(b), Convert.FromHexString("0a322e312e30ffffffffffffffffffffff")), "decoded");
        Check("edge hostile invalid UTF-8 string rejected", Rejects(b => E.Inner.Decode(b), Convert.FromHexString("0a322e312e300002ff")), "decoded");
        return failed > 0 ? 1 : 0;
    }
}
