// Cross-language conformance test for the BitPacker Scala target.
import java.nio.file.{Files, Paths}

import bench.{Character, Guild, Item, Vec3, WorldState}
import edge.{Edge, Inner}

object Test {
  private var passed = 0
  private var failed = 0

  def check(name: String, ok: Boolean, detail: => String = ""): Unit =
    if (ok) {
      passed += 1
      println(s"  ok   $name")
    } else {
      failed += 1
      val d = detail
      println(s"  FAIL $name" + (if (d.isEmpty) "" else s" ($d)"))
    }

  def checkEq(name: String, got: Any, want: Any): Unit = check(name, got == want, s"got $got, want $want")

  def checkBytes(name: String, got: Array[Byte], want: Array[Byte]): Unit =
    check(name, java.util.Arrays.equals(got, want), s"got ${got.length} bytes, want ${want.length}")

  def benchWorld(): WorldState = {
    val sword = Item(id = 1, name = "Excalibur", value = 9999, weight = 15, rarity = "Legendary")
    val hero = Character(
      name = "TestHero", level = 99, hp = 1000, mp = 500, isAlive = true,
      position = Vec3(10, -20, 30), skills = Vector(1, 2, 3, 100), inventory = Vector(sword)
    )
    val guild = Guild(name = "TestGuild", description = "A test guild for cross-language", members = Vector(hero))
    val potion = Item(id = 2, name = "HealthPotion", value = 50, weight = 1, rarity = "Common")
    WorldState(worldId = 42, seed = "cross_lang_test", guilds = Vector(guild), lootTable = Vector(potion))
  }

  def verifyBench(label: String, w: WorldState): Unit = {
    checkEq(s"$label world_id", w.worldId, 42)
    checkEq(s"$label seed", w.seed, "cross_lang_test")
    checkEq(s"$label guilds length", w.guilds.length, 1)
    w.guilds.headOption.foreach { g =>
      checkEq(s"$label guild name", g.name, "TestGuild")
      checkEq(s"$label guild description", g.description, "A test guild for cross-language")
      checkEq(s"$label members length", g.members.length, 1)
      g.members.headOption.foreach { h =>
        checkEq(s"$label hero name", h.name, "TestHero")
        checkEq(s"$label hero level", h.level, 99)
        checkEq(s"$label hero hp", h.hp, 1000)
        checkEq(s"$label hero mp", h.mp, 500)
        checkEq(s"$label hero is_alive", h.isAlive, true)
        checkEq(s"$label position", h.position, Vec3(10, -20, 30))
        checkEq(s"$label skills", h.skills, Vector(1, 2, 3, 100))
        checkEq(s"$label inventory length", h.inventory.length, 1)
        h.inventory.headOption.foreach { s =>
          checkEq(s"$label sword name", s.name, "Excalibur")
          checkEq(s"$label sword value", s.value, 9999)
          checkEq(s"$label sword rarity", s.rarity, "Legendary")
        }
      }
    }
    checkEq(s"$label loot length", w.lootTable.length, 1)
    w.lootTable.headOption.foreach { p =>
      checkEq(s"$label potion name", p.name, "HealthPotion")
      checkEq(s"$label potion rarity", p.rarity, "Common")
    }
  }

  def canonicalEdge(): Edge = Edge(
    iMin = Int.MinValue, iMax = Int.MaxValue, iZero = 0, iNeg = -1,
    lMin = Long.MinValue, lMax = Long.MaxValue, lNeg = -300L,
    f = -1.25f, d = 1234.5625, dNeg = -0.5,
    yes = true, no = false,
    empty = "", unicode = "héllo wörld ✓ 日本 🚀",
    ints = Vector(0, -1, 1, -64, 64, Int.MinValue, Int.MaxValue),
    longs = Vector(0L, -1L, Long.MaxValue, Long.MinValue, 4294967296L),
    floats = Vector(0.0f, 0.5f, -2.25f),
    doubles = Vector(0.0, 3.5, -1000000.25),
    bools = Vector(true, false, true),
    strings = Vector("", "a", "日本語"),
    noInts = Vector.empty,
    inner = Inner(big = 1099511627776L, label = "inner"),
    inners = Vector(Inner(big = -1L, label = ""), Inner(big = 0L, label = "x")),
    noInners = Vector.empty
  )

  /** "" when decode returns a Left, else what went wrong. */
  def rejects(data: Array[Byte]): String =
    try {
      Edge.decode(data) match {
        case Left(_)  => ""
        case Right(_) => "no error"
      }
    } catch { case e: Throwable => s"threw $e" }

  def main(args: Array[String]): Unit = {
    val dir = Paths.get(if (args.nonEmpty) args(0) else "..")

    println("bench_complex")
    val benchRef = Files.readAllBytes(dir.resolve("test_data.bin"))
    val benchEnc = benchWorld().encode()
    Files.write(dir.resolve("test_data_scala.bin"), benchEnc)
    checkBytes("bench encode == test_data.bin", benchEnc, benchRef)
    WorldState.decode(benchRef) match {
      case Left(err) => check("bench decode test_data.bin", ok = false, err)
      case Right(w) =>
        verifyBench("bench decode", w)
        checkEq("bench decode == canonical value", w, benchWorld())
        checkBytes("bench re-encode == test_data.bin", w.encode(), benchRef)
    }
    WorldState.decode(benchEnc).foreach(w => verifyBench("bench roundtrip", w))

    println("edge")
    val ref = Files.readAllBytes(dir.resolve("edge/edge_ref.bin"))
    val want = canonicalEdge()
    checkBytes("edge encode == edge_ref.bin", want.encode(), ref)
    Edge.decode(ref) match {
      case Left(err) => check("edge decode edge_ref.bin", ok = false, err)
      case Right(d) =>
        checkEq("edge field i_min", d.iMin, Int.MinValue)
        checkEq("edge field i_max", d.iMax, Int.MaxValue)
        checkEq("edge field i_zero", d.iZero, 0)
        checkEq("edge field i_neg", d.iNeg, -1)
        checkEq("edge field l_min", d.lMin, Long.MinValue)
        checkEq("edge field l_max", d.lMax, Long.MaxValue)
        checkEq("edge field l_neg", d.lNeg, -300L)
        checkEq("edge field f", d.f, -1.25f)
        checkEq("edge field d", d.d, 1234.5625)
        checkEq("edge field d_neg", d.dNeg, -0.5)
        checkEq("edge field yes", d.yes, true)
        checkEq("edge field no", d.no, false)
        checkEq("edge field empty", d.empty, "")
        checkEq("edge field unicode", d.unicode, want.unicode)
        checkEq("edge field ints", d.ints, want.ints)
        checkEq("edge field longs", d.longs, want.longs)
        checkEq("edge field floats", d.floats, want.floats)
        checkEq("edge field doubles", d.doubles, want.doubles)
        checkEq("edge field bools", d.bools, want.bools)
        checkEq("edge field strings", d.strings, want.strings)
        checkEq("edge field no_ints", d.noInts, Vector.empty[Int])
        checkEq("edge field inner", d.inner, want.inner)
        checkEq("edge field inners", d.inners, want.inners)
        checkEq("edge field no_inners", d.noInners, Vector.empty[Inner])
        checkEq("edge decode == canonical value", d, want)
        checkBytes("edge re-encode == edge_ref.bin", d.encode(), ref)
    }

    val bad = ref.clone()
    bad(5) = '9'.toByte // "2.1.0" -> "2.1.9"
    val badResult = rejects(bad)
    check("edge wrong version rejected", badResult.isEmpty, badResult)

    val truncFail = (0 until ref.length).iterator
      .map(n => (n, rejects(java.util.Arrays.copyOf(ref, n))))
      .collectFirst { case (n, r) if r.nonEmpty => s"prefix $n: $r" }
      .getOrElse("")
    check(s"edge every truncation (${ref.length} prefixes) rejected", truncFail.isEmpty, truncFail)

    // float fields are scaled in single precision, like the Go and Java
    // targets: 1.0005f * 10000f = 10005 in float32 (10004 in
    // float64).
    Edge.decode(Edge(f = 1.0005f, floats = Vector(1.0013f)).encode()) match {
      case Left(err) => check("float round trip", ok = false, err)
      case Right(back) =>
        checkEq("float field scaled in float32", back.f, 10005L.toFloat / 10000.0f)
        checkEq("float[] element scaled in float32", back.floats, Vector(10013L.toFloat / 10000.0f))
    }

    val bom = Inner(big = 7L, label = "\uFEFF\uFEFFbom")
    checkEq("string with leading U+FEFF round-trips", Inner.decode(bom.encode()).map(_.label), Right(bom.label))
    val overlong = Array[Byte](10, 50, 46, 49, 46, 48, 0, 4, 0xC0.toByte, 0x80.toByte)
    check("invalid UTF-8 rejected", Inner.decode(overlong).isLeft, Inner.decode(overlong).toString)

    // edge_float32_ref.bin: f and floats whose x10000 is inexact in float32.
    val f32ref = Files.readAllBytes(dir.resolve("edge/edge_float32_ref.bin"))
    val f32want = canonicalEdge().copy(f = 0.29f, floats = Vector(0.7f, 16777.217f, -0.29f))
    checkBytes("float32 fixture: encode == edge_float32_ref.bin", f32want.encode(), f32ref)
    Edge.decode(f32ref) match {
      case Left(err) => check("float32 fixture: decode", ok = false, err)
      case Right(f32dec) =>
        checkEq("float32 fixture: decoded f == 0.29f", f32dec.f, 0.29f)
        checkEq("float32 fixture: decoded floats", f32dec.floats, Vector(0.7f, 16777.217f, -0.29f))
        checkEq("float32 fixture: decode == variant", f32dec, f32want)
    }

    checkEq("trailing bytes ignored", Edge.decode(ref ++ Array[Byte](-1, 0, 127)), Right(want))

    def encodeRejects(name: String, e: Edge): Unit = {
      val r =
        try { e.encode(); "no error" }
        catch { case _: IllegalArgumentException => ""; case x: Throwable => s"wrong error: $x" }
      check(name, r.isEmpty, r)
    }
    encodeRejects("NaN float rejected on encode", Edge(f = Float.NaN))
    encodeRejects("infinite float rejected on encode", Edge(floats = Vector(Float.PositiveInfinity)))
    encodeRejects("out-of-range float rejected on encode", Edge(f = 1e15f))
    encodeRejects("NaN double rejected on encode", Edge(d = Double.NaN))
    encodeRejects("infinite double rejected on encode", Edge(dNeg = Double.NegativeInfinity))
    encodeRejects("out-of-range double rejected on encode", Edge(doubles = Vector(1e300)))

    def decodeRejects(name: String, data: Array[Byte], f: Array[Byte] => Either[String, Any]): Unit = {
      val r =
        try { if (f(data).isLeft) "" else "no error" }
        catch { case x: Throwable => s"threw $x" }
      check(name, r.isEmpty, r)
    }
    def bytesOf(xs: Int*): Array[Byte] = xs.map(_.toByte).toArray
    val dflt = Edge().encode() // ints count at offset 20
    def withCount(count: Int*): Array[Byte] = dflt.take(20) ++ bytesOf(count: _*) ++ dflt.drop(21)
    decodeRejects("negative array length rejected", withCount(0x01), Edge.decode)
    decodeRejects("oversized array length rejected", withCount(0xFE, 0xFF, 0xFF, 0xFF, 0x0F), Edge.decode)
    val innerHead = bytesOf(10, 50, 46, 49, 46, 48, 0)
    decodeRejects("negative string length rejected", innerHead ++ bytesOf(0x01, 0x61), Inner.decode)
    decodeRejects("oversized string length rejected",
      innerHead ++ bytesOf(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x40, 0x61), Inner.decode)
    decodeRejects("varint longer than 10 bytes rejected",
      innerHead.take(6) ++ bytesOf(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00), Inner.decode)

    println(s"scala: $passed passed, $failed failed")
    if (failed > 0) sys.exit(1)
  }
}
