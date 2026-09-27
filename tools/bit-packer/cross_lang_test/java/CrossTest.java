// Cross-language conformance test for the Java target. run.sh generates
// packages `bench` (bench_complex.buff) and `edge` (edge.buff) first.
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Predicate;

public class CrossTest {
    static int passed = 0, failed = 0;

    static void check(String name, boolean ok, String detail) {
        if (ok) { passed++; System.out.println("  ok   " + name); }
        else { failed++; System.out.println("  FAIL " + name + " (" + detail + ")"); }
    }

    static void eq(String name, Object got, Object want) {
        boolean ok = Arrays.deepEquals(new Object[]{got}, new Object[]{want});
        check(name, ok, "got " + Arrays.deepToString(new Object[]{got}) + ", want " + Arrays.deepToString(new Object[]{want}));
    }

    static byte[] unhex(String s) { return java.util.HexFormat.of().parseHex(s); }

    static byte[] badVersion(byte[] data) {
        byte[] b = data.clone();
        b[1] ^= 1;
        return b;
    }

    interface Decoder { void decode(byte[] b) throws Exception; }

    static boolean rejects(Decoder d, byte[] b) {
        try { d.decode(b); return false; } catch (Exception e) { return true; }
    }

    static void truncations(String name, byte[] data, Decoder d) {
        for (int cut = 0; cut < data.length; cut++) {
            if (!rejects(d, Arrays.copyOf(data, cut))) {
                check(name, false, "prefix of " + cut + "/" + data.length + " bytes decoded");
                return;
            }
        }
        check(name, true, "");
    }

    // --- bench ---
    static bench.Vec3Gen.WorldState makeWorld() {
        bench.Vec3Gen.Vec3 pos = new bench.Vec3Gen.Vec3();
        pos.x = 10; pos.y = -20; pos.z = 30;
        bench.Vec3Gen.Item sword = new bench.Vec3Gen.Item();
        sword.id = 1; sword.name = "Excalibur"; sword.value = 9999; sword.weight = 15; sword.rarity = "Legendary";
        bench.Vec3Gen.Character hero = new bench.Vec3Gen.Character();
        hero.name = "TestHero"; hero.level = 99; hero.hp = 1000; hero.mp = 500; hero.is_alive = true;
        hero.position = pos; hero.skills = new int[]{1, 2, 3, 100};
        hero.inventory = new bench.Vec3Gen.Item[]{sword};
        bench.Vec3Gen.Guild guild = new bench.Vec3Gen.Guild();
        guild.name = "TestGuild"; guild.description = "A test guild for cross-language";
        guild.members = new bench.Vec3Gen.Character[]{hero};
        bench.Vec3Gen.Item potion = new bench.Vec3Gen.Item();
        potion.id = 2; potion.name = "HealthPotion"; potion.value = 50; potion.weight = 1; potion.rarity = "Common";
        bench.Vec3Gen.WorldState w = new bench.Vec3Gen.WorldState();
        w.world_id = 42; w.seed = "cross_lang_test";
        w.guilds = new bench.Vec3Gen.Guild[]{guild};
        w.loot_table = new bench.Vec3Gen.Item[]{potion};
        return w;
    }

    static boolean verifyWorld(bench.Vec3Gen.WorldState w) {
        if (w.world_id != 42 || !w.seed.equals("cross_lang_test") || w.guilds.length != 1) return false;
        bench.Vec3Gen.Guild g = w.guilds[0];
        if (!g.name.equals("TestGuild") || !g.description.equals("A test guild for cross-language") || g.members.length != 1) return false;
        bench.Vec3Gen.Character h = g.members[0];
        return h.name.equals("TestHero") && h.level == 99 && h.hp == 1000 && h.mp == 500 && h.is_alive
            && h.position.x == 10 && h.position.y == -20 && h.position.z == 30
            && Arrays.equals(h.skills, new int[]{1, 2, 3, 100})
            && h.inventory.length == 1 && h.inventory[0].name.equals("Excalibur")
            && h.inventory[0].value == 9999 && h.inventory[0].rarity.equals("Legendary")
            && w.loot_table.length == 1 && w.loot_table[0].name.equals("HealthPotion")
            && w.loot_table[0].rarity.equals("Common");
    }

    // --- edge ---
    static edge.InnerGen.Inner inner(long big, String label) {
        edge.InnerGen.Inner i = new edge.InnerGen.Inner();
        i.big = big; i.label = label;
        return i;
    }

    static edge.InnerGen.Edge makeEdge() {
        edge.InnerGen.Edge e = new edge.InnerGen.Edge();
        e.i_min = -2147483648; e.i_max = 2147483647; e.i_zero = 0; e.i_neg = -1;
        e.l_min = -9223372036854775808L; e.l_max = 9223372036854775807L; e.l_neg = -300;
        e.f = -1.25f; e.d = 1234.5625; e.d_neg = -0.5;
        e.yes = true; e.no = false;
        e.empty = ""; e.unicode = "héllo wörld ✓ 日本 🚀";
        e.ints = new int[]{0, -1, 1, -64, 64, -2147483648, 2147483647};
        e.longs = new long[]{0, -1, 9223372036854775807L, -9223372036854775808L, 4294967296L};
        e.floats = new float[]{0.0f, 0.5f, -2.25f};
        e.doubles = new double[]{0.0, 3.5, -1000000.25};
        e.bools = new boolean[]{true, false, true};
        e.strings = new String[]{"", "a", "日本語"};
        e.no_ints = new int[]{};
        e.inner = inner(1099511627776L, "inner");
        e.inners = new edge.InnerGen.Inner[]{inner(-1, ""), inner(0, "x")};
        e.no_inners = new edge.InnerGen.Inner[]{};
        return e;
    }

    static String innerStr(edge.InnerGen.Inner i) { return i == null ? "null" : "Inner{" + i.big + "," + i.label + "}"; }
    static String innersStr(edge.InnerGen.Inner[] a) {
        StringBuilder sb = new StringBuilder("[");
        for (edge.InnerGen.Inner i : a) sb.append(innerStr(i)).append(";");
        return sb.append("]").toString();
    }

    static void verifyEdge(edge.InnerGen.Edge e) {
        edge.InnerGen.Edge w = makeEdge();
        eq("edge decode i_min", e.i_min, w.i_min);
        eq("edge decode i_max", e.i_max, w.i_max);
        eq("edge decode i_zero", e.i_zero, w.i_zero);
        eq("edge decode i_neg", e.i_neg, w.i_neg);
        eq("edge decode l_min", e.l_min, w.l_min);
        eq("edge decode l_max", e.l_max, w.l_max);
        eq("edge decode l_neg", e.l_neg, w.l_neg);
        eq("edge decode f", e.f, w.f);
        eq("edge decode d", e.d, w.d);
        eq("edge decode d_neg", e.d_neg, w.d_neg);
        eq("edge decode yes", e.yes, w.yes);
        eq("edge decode no", e.no, w.no);
        eq("edge decode empty", e.empty, w.empty);
        eq("edge decode unicode", e.unicode, w.unicode);
        eq("edge decode ints", e.ints, w.ints);
        eq("edge decode longs", e.longs, w.longs);
        eq("edge decode floats", e.floats, w.floats);
        eq("edge decode doubles", e.doubles, w.doubles);
        eq("edge decode bools", e.bools, w.bools);
        eq("edge decode strings", e.strings, w.strings);
        eq("edge decode no_ints", e.no_ints, w.no_ints);
        eq("edge decode inner", innerStr(e.inner), innerStr(w.inner));
        eq("edge decode inners", innersStr(e.inners), innersStr(w.inners));
        eq("edge decode no_inners", innersStr(e.no_inners), innersStr(w.no_inners));
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        byte[] ref = Files.readAllBytes(root.resolve("test_data.bin"));
        byte[] edgeRef = Files.readAllBytes(root.resolve("edge").resolve("edge_ref.bin"));

        byte[] enc = makeWorld().encode();
        Files.write(root.resolve("test_data_java.bin"), enc);
        check("bench encode == test_data.bin", Arrays.equals(enc, ref), enc.length + " vs " + ref.length + " bytes");
        try {
            check("bench decode test_data.bin", verifyWorld(bench.Vec3Gen.WorldState.decode(ref)), "field mismatch");
        } catch (Exception ex) { check("bench decode test_data.bin", false, ex.toString()); }
        try {
            bench.Vec3Gen.WorldState w = bench.Vec3Gen.WorldState.decode(enc);
            check("bench round-trip", verifyWorld(w), "field mismatch");
            check("bench re-encode == encode", Arrays.equals(w.encode(), enc), "bytes differ");
        } catch (Exception ex) { check("bench round-trip", false, ex.toString()); }
        check("bench wrong version rejected", rejects(b -> bench.Vec3Gen.WorldState.decode(b), badVersion(ref)), "decoded");
        truncations("bench every truncation rejected", ref, b -> bench.Vec3Gen.WorldState.decode(b));

        byte[] eenc = makeEdge().encode();
        check("edge encode == edge_ref.bin", Arrays.equals(eenc, edgeRef), "bytes differ");
        try {
            edge.InnerGen.Edge e = edge.InnerGen.Edge.decode(edgeRef);
            verifyEdge(e);
            check("edge re-encode == edge_ref.bin", Arrays.equals(e.encode(), edgeRef), "bytes differ");
        } catch (Exception ex) { check("edge decode edge_ref.bin", false, ex.toString()); }
        check("edge wrong version rejected", rejects(b -> edge.InnerGen.Edge.decode(b), badVersion(edgeRef)), "decoded");
        truncations("edge every truncation rejected", edgeRef, b -> edge.InnerGen.Edge.decode(b));

        // float32 variant: x10000 must be computed in single precision
        byte[] f32ref = Files.readAllBytes(root.resolve("edge").resolve("edge_float32_ref.bin"));
        edge.InnerGen.Edge fv = makeEdge();
        fv.f = 0.29f; fv.floats = new float[]{0.7f, 16777.217f, -0.29f};
        check("edge float32 encode == edge_float32_ref.bin", Arrays.equals(fv.encode(), f32ref), "bytes differ");
        try {
            edge.InnerGen.Edge e = edge.InnerGen.Edge.decode(f32ref);
            check("edge float32 decode", e.f == fv.f && Arrays.equals(e.floats, fv.floats), e.f + " " + Arrays.toString(e.floats));
        } catch (Exception ex) { check("edge float32 decode", false, ex.toString()); }
        // trailing bytes after the root class are ignored
        try {
            byte[] trailing = Arrays.copyOf(edgeRef, edgeRef.length + 2);
            trailing[edgeRef.length + 1] = (byte) 0xff;
            check("edge trailing bytes ignored", Arrays.equals(edge.InnerGen.Edge.decode(trailing).encode(), edgeRef), "decoded value differs");
        } catch (Exception ex) { check("edge trailing bytes ignored", false, ex.toString()); }
        // unencodable floats are errors, not saturated
        edge.InnerGen.Edge nan = makeEdge(); nan.d = Double.NaN;
        check("edge encode NaN double rejected", rejects(b -> nan.encode(), new byte[0]), "encoded");
        edge.InnerGen.Edge big = makeEdge(); big.f = 1e30f;
        check("edge encode out-of-range float rejected", rejects(b -> big.encode(), new byte[0]), "encoded");

        // hostile input: bogus lengths must fail fast, not allocate or crash
        check("bench hostile huge array length rejected", rejects(b -> bench.Vec3Gen.WorldState.decode(b), unhex("0a312e302e300000feffffff0f")), "decoded");
        check("bench hostile negative array length rejected", rejects(b -> bench.Vec3Gen.WorldState.decode(b), unhex("0a312e302e30000001")), "decoded");
        check("edge hostile huge string length rejected", rejects(b -> edge.InnerGen.Inner.decode(b), unhex("0a322e312e30008080808010")), "decoded");
        check("edge hostile negative string length rejected", rejects(b -> edge.InnerGen.Inner.decode(b), unhex("0a322e312e300001")), "decoded");
        check("edge hostile endless varint rejected", rejects(b -> edge.InnerGen.Inner.decode(b), unhex("0a322e312e30ffffffffffffffffffffff")), "decoded");
        check("edge hostile invalid UTF-8 string rejected", rejects(b -> edge.InnerGen.Inner.decode(b), unhex("0a322e312e300002ff")), "decoded");

        System.exit(failed > 0 ? 1 : 0);
    }
}
