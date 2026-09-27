// Cross-language conformance test for the BitPacker Kotlin target.
import bench.Character
import bench.Guild
import bench.Item
import bench.Vec3
import bench.WorldState
import edge.Edge
import edge.Inner
import java.io.File
import kotlin.system.exitProcess
import edge.BitPackerException as EdgeError

var passed = 0
var failed = 0

fun check(name: String, ok: Boolean, detail: String = "") {
    if (ok) {
        passed++
        println("  ok   $name")
    } else {
        failed++
        println("  FAIL $name" + if (detail.isEmpty()) "" else " ($detail)")
    }
}

fun checkEq(name: String, got: Any?, want: Any?) = check(name, got == want, "got $got, want $want")

fun checkBytes(name: String, got: ByteArray, want: ByteArray) =
    check(name, got.contentEquals(want), "got ${got.size} bytes, want ${want.size}")

fun benchWorld(): WorldState {
    val sword = Item(id = 1, name = "Excalibur", value = 9999, weight = 15, rarity = "Legendary")
    val hero = Character(
        name = "TestHero", level = 99, hp = 1000, mp = 500, isAlive = true,
        position = Vec3(10, -20, 30), skills = listOf(1, 2, 3, 100), inventory = listOf(sword),
    )
    val guild = Guild(name = "TestGuild", description = "A test guild for cross-language", members = listOf(hero))
    val potion = Item(id = 2, name = "HealthPotion", value = 50, weight = 1, rarity = "Common")
    return WorldState(worldId = 42, seed = "cross_lang_test", guilds = listOf(guild), lootTable = listOf(potion))
}

fun verifyBench(label: String, w: WorldState) {
    checkEq("$label world_id", w.worldId, 42)
    checkEq("$label seed", w.seed, "cross_lang_test")
    checkEq("$label guilds length", w.guilds.size, 1)
    val g = w.guilds.firstOrNull() ?: return
    checkEq("$label guild name", g.name, "TestGuild")
    checkEq("$label guild description", g.description, "A test guild for cross-language")
    checkEq("$label members length", g.members.size, 1)
    val h = g.members.firstOrNull() ?: return
    checkEq("$label hero name", h.name, "TestHero")
    checkEq("$label hero level", h.level, 99)
    checkEq("$label hero hp", h.hp, 1000)
    checkEq("$label hero mp", h.mp, 500)
    checkEq("$label hero is_alive", h.isAlive, true)
    checkEq("$label position", h.position, Vec3(10, -20, 30))
    checkEq("$label skills", h.skills, listOf(1, 2, 3, 100))
    checkEq("$label inventory length", h.inventory.size, 1)
    h.inventory.firstOrNull()?.let {
        checkEq("$label sword name", it.name, "Excalibur")
        checkEq("$label sword value", it.value, 9999)
        checkEq("$label sword rarity", it.rarity, "Legendary")
    }
    checkEq("$label loot length", w.lootTable.size, 1)
    w.lootTable.firstOrNull()?.let {
        checkEq("$label potion name", it.name, "HealthPotion")
        checkEq("$label potion rarity", it.rarity, "Common")
    }
}

fun canonicalEdge() = Edge(
    iMin = Int.MIN_VALUE, iMax = Int.MAX_VALUE, iZero = 0, iNeg = -1,
    lMin = Long.MIN_VALUE, lMax = Long.MAX_VALUE, lNeg = -300L,
    f = -1.25f, d = 1234.5625, dNeg = -0.5,
    yes = true, no = false,
    empty = "", unicode = "héllo wörld ✓ 日本 🚀",
    ints = listOf(0, -1, 1, -64, 64, Int.MIN_VALUE, Int.MAX_VALUE),
    longs = listOf(0L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 4294967296L),
    floats = listOf(0.0f, 0.5f, -2.25f),
    doubles = listOf(0.0, 3.5, -1000000.25),
    bools = listOf(true, false, true),
    strings = listOf("", "a", "日本語"),
    noInts = emptyList(),
    inner = Inner(big = 1099511627776L, label = "inner"),
    inners = listOf(Inner(big = -1L, label = ""), Inner(big = 0L, label = "x")),
    noInners = emptyList(),
)

fun rejects(data: ByteArray): String = try {
    Edge.decode(data)
    "no error"
} catch (e: EdgeError) {
    ""
} catch (e: Throwable) {
    "wrong error: $e"
}

fun main(args: Array<String>) {
    val dir = File(args.getOrElse(0) { ".." })

    println("bench_complex")
    val benchRef = File(dir, "test_data.bin").readBytes()
    val benchEnc = benchWorld().encode()
    File(dir, "test_data_kotlin.bin").writeBytes(benchEnc)
    checkBytes("bench encode == test_data.bin", benchEnc, benchRef)
    val benchDec = WorldState.decode(benchRef)
    verifyBench("bench decode", benchDec)
    checkEq("bench decode == canonical value", benchDec, benchWorld())
    checkBytes("bench re-encode == test_data.bin", benchDec.encode(), benchRef)
    verifyBench("bench roundtrip", WorldState.decode(benchEnc))

    println("edge")
    val ref = File(dir, "edge/edge_ref.bin").readBytes()
    val want = canonicalEdge()
    checkBytes("edge encode == edge_ref.bin", want.encode(), ref)
    val dec = try { Edge.decode(ref) } catch (e: Exception) { check("edge decode edge_ref.bin", false, e.toString()); null }
    if (dec != null) {
        checkEq("edge field i_min", dec.iMin, Int.MIN_VALUE)
        checkEq("edge field i_max", dec.iMax, Int.MAX_VALUE)
        checkEq("edge field i_zero", dec.iZero, 0)
        checkEq("edge field i_neg", dec.iNeg, -1)
        checkEq("edge field l_min", dec.lMin, Long.MIN_VALUE)
        checkEq("edge field l_max", dec.lMax, Long.MAX_VALUE)
        checkEq("edge field l_neg", dec.lNeg, -300L)
        checkEq("edge field f", dec.f, -1.25f)
        checkEq("edge field d", dec.d, 1234.5625)
        checkEq("edge field d_neg", dec.dNeg, -0.5)
        checkEq("edge field yes", dec.yes, true)
        checkEq("edge field no", dec.no, false)
        checkEq("edge field empty", dec.empty, "")
        checkEq("edge field unicode", dec.unicode, want.unicode)
        checkEq("edge field ints", dec.ints, want.ints)
        checkEq("edge field longs", dec.longs, want.longs)
        checkEq("edge field floats", dec.floats, want.floats)
        checkEq("edge field doubles", dec.doubles, want.doubles)
        checkEq("edge field bools", dec.bools, want.bools)
        checkEq("edge field strings", dec.strings, want.strings)
        checkEq("edge field no_ints", dec.noInts, emptyList<Int>())
        checkEq("edge field inner", dec.inner, want.inner)
        checkEq("edge field inners", dec.inners, want.inners)
        checkEq("edge field no_inners", dec.noInners, emptyList<Inner>())
        checkEq("edge decode == canonical value", dec, want)
        checkBytes("edge re-encode == edge_ref.bin", dec.encode(), ref)
    }

    val bad = ref.copyOf()
    bad[5] = '9'.code.toByte() // "2.1.0" -> "2.1.9"
    val badResult = rejects(bad)
    check("edge wrong version rejected", badResult.isEmpty(), badResult)

    var truncFail = ""
    for (n in 0 until ref.size) {
        val r = rejects(ref.copyOf(n))
        if (r.isNotEmpty()) {
            truncFail = "prefix $n: $r"
            break
        }
    }
    check("edge every truncation (${ref.size} prefixes) rejected", truncFail.isEmpty(), truncFail)

    // float fields are scaled in single precision, like the Go and Java
    // targets: 1.0005f * 10000f = 10005 in float32 (10004 in
    // float64).
    val back = Edge.decode(Edge(f = 1.0005f, floats = listOf(1.0013f)).encode())
    checkEq("float field scaled in float32", back.f, 10005L.toFloat() / 10000.0f)
    checkEq("float[] element scaled in float32", back.floats, listOf(10013L.toFloat() / 10000.0f))

    val bom = Inner(big = 7L, label = "\uFEFF\uFEFFbom")
    checkEq("string with leading U+FEFF round-trips", Inner.decode(bom.encode()).label, bom.label)
    val overlong = byteArrayOf(10, 50, 46, 49, 46, 48, 0, 4, 0xC0.toByte(), 0x80.toByte())
    val ovr = try { Inner.decode(overlong); "no error" } catch (e: EdgeError) { "" } catch (e: Throwable) { "wrong error: $e" }
    check("invalid UTF-8 rejected", ovr.isEmpty(), ovr)

    // edge_float32_ref.bin: f and floats whose x10000 is inexact in float32.
    val f32ref = File(dir, "edge/edge_float32_ref.bin").readBytes()
    val f32want = canonicalEdge().copy(f = 0.29f, floats = listOf(0.7f, 16777.217f, -0.29f))
    checkBytes("float32 fixture: encode == edge_float32_ref.bin", f32want.encode(), f32ref)
    try {
        val f32dec = Edge.decode(f32ref)
        checkEq("float32 fixture: decoded f == 0.29f", f32dec.f, 0.29f)
        checkEq("float32 fixture: decoded floats", f32dec.floats, listOf(0.7f, 16777.217f, -0.29f))
        checkEq("float32 fixture: decode == variant", f32dec, f32want)
    } catch (e: Exception) {
        check("float32 fixture: decode", false, e.toString())
    }

    val trailing = ref + byteArrayOf(-1, 0, 127)
    val trailingResult = try { Edge.decode(trailing) == want } catch (e: Exception) { false }
    check("trailing bytes ignored", trailingResult)

    fun encodeRejects(name: String, e: Edge) {
        val r = try { e.encode(); "no error" } catch (x: IllegalArgumentException) { "" } catch (x: Throwable) { "wrong error: $x" }
        check(name, r.isEmpty(), r)
    }
    encodeRejects("NaN float rejected on encode", Edge(f = Float.NaN))
    encodeRejects("infinite float rejected on encode", Edge(floats = listOf(Float.POSITIVE_INFINITY)))
    encodeRejects("out-of-range float rejected on encode", Edge(f = 1e15f))
    encodeRejects("NaN double rejected on encode", Edge(d = Double.NaN))
    encodeRejects("infinite double rejected on encode", Edge(dNeg = Double.NEGATIVE_INFINITY))
    encodeRejects("out-of-range double rejected on encode", Edge(doubles = listOf(1e300)))

    fun decodeRejects(name: String, data: ByteArray, f: (ByteArray) -> Any) {
        val r = try { f(data); "no error" } catch (x: EdgeError) { "" } catch (x: Throwable) { "wrong error: $x" }
        check(name, r.isEmpty(), r)
    }
    fun bytesOf(vararg xs: Int) = ByteArray(xs.size) { xs[it].toByte() }
    val dflt = Edge().encode() // ints count at offset 20
    fun withCount(vararg count: Int) = dflt.copyOfRange(0, 20) + bytesOf(*count) + dflt.copyOfRange(21, dflt.size)
    decodeRejects("negative array length rejected", withCount(0x01)) { Edge.decode(it) }
    decodeRejects("oversized array length rejected", withCount(0xFE, 0xFF, 0xFF, 0xFF, 0x0F)) { Edge.decode(it) }
    val innerHead = bytesOf(10, 50, 46, 49, 46, 48, 0)
    decodeRejects("negative string length rejected", innerHead + bytesOf(0x01, 0x61)) { Inner.decode(it) }
    decodeRejects("oversized string length rejected",
        innerHead + bytesOf(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x40, 0x61)) { Inner.decode(it) }
    decodeRejects("varint longer than 10 bytes rejected",
        innerHead.copyOf(6) + bytesOf(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00)) { Inner.decode(it) }

    println("kotlin: $passed passed, $failed failed")
    if (failed > 0) exitProcess(1)
}
