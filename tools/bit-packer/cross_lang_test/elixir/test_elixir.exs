# Cross-language conformance test for the BitPacker Elixir target.
# Run through run.sh, which generates and compiles the modules first.
defmodule T do
  def start do
    Process.put(:pass, 0)
    Process.put(:fail, 0)
  end

  def check(name, cond, detail \\ "") do
    if cond == true do
      Process.put(:pass, Process.get(:pass) + 1)
      IO.puts("  ok   #{name}")
    else
      Process.put(:fail, Process.get(:fail) + 1)
      IO.puts("  FAIL #{name} (#{detail})")
    end
  end

  def eq(name, got, want),
    do: check(name, got === want, "got #{inspect(got)}, want #{inspect(want)}")

  def error?({:error, _}), do: true
  def error?(_), do: false

  def uv(n) when n < 128, do: <<n>>
  def uv(n), do: <<Bitwise.bor(Bitwise.band(n, 127), 128), uv(Bitwise.bsr(n, 7))::binary>>
end

alias BenchComplex.{WorldState, Guild, Character, Item, Vec3}
alias Edge.Inner

[dir] = System.argv()
parent = Path.dirname(dir)
T.start()

try do
  # ------------------------------------------------------------ bench
  world = %WorldState{
    world_id: 42,
    seed: "cross_lang_test",
    guilds: [
      %Guild{
        name: "TestGuild",
        description: "A test guild for cross-language",
        members: [
          %Character{
            name: "TestHero",
            level: 99,
            hp: 1000,
            mp: 500,
            is_alive: true,
            position: %Vec3{x: 10, y: -20, z: 30},
            skills: [1, 2, 3, 100],
            inventory: [
              %Item{id: 1, name: "Excalibur", value: 9999, weight: 15, rarity: "Legendary"}
            ]
          }
        ]
      }
    ],
    loot_table: [%Item{id: 2, name: "HealthPotion", value: 50, weight: 1, rarity: "Common"}]
  }

  ref = File.read!(Path.join(parent, "test_data.bin"))
  enc = WorldState.encode(world)
  File.write!(Path.join(parent, "test_data_elixir.bin"), enc)
  T.check("bench: encode == test_data.bin", enc == ref, "#{byte_size(enc)} vs #{byte_size(ref)} bytes")

  case WorldState.decode(ref) do
    {:ok, w} ->
      T.check("bench: decode test_data.bin", true)
      T.eq("bench: world_id", w.world_id, 42)
      T.eq("bench: seed", w.seed, "cross_lang_test")
      T.eq("bench: guilds length", length(w.guilds), 1)
      [g] = w.guilds
      T.eq("bench: guild name", g.name, "TestGuild")
      T.eq("bench: guild description", g.description, "A test guild for cross-language")
      T.eq("bench: members length", length(g.members), 1)
      [h] = g.members
      T.eq("bench: hero name", h.name, "TestHero")
      T.eq("bench: hero level", h.level, 99)
      T.eq("bench: hero hp", h.hp, 1000)
      T.eq("bench: hero mp", h.mp, 500)
      T.eq("bench: hero is_alive", h.is_alive, true)
      T.eq("bench: position", h.position, %Vec3{x: 10, y: -20, z: 30})
      T.eq("bench: skills", h.skills, [1, 2, 3, 100])
      T.eq("bench: inventory length", length(h.inventory), 1)
      [s] = h.inventory
      T.eq("bench: sword name", s.name, "Excalibur")
      T.eq("bench: sword value", s.value, 9999)
      T.eq("bench: sword rarity", s.rarity, "Legendary")
      T.eq("bench: loot length", length(w.loot_table), 1)
      [p] = w.loot_table
      T.eq("bench: potion name", p.name, "HealthPotion")
      T.eq("bench: potion rarity", p.rarity, "Common")
      T.eq("bench: re-encode decoded == test_data.bin", WorldState.encode(w), ref)

    other ->
      T.check("bench: decode test_data.bin", false, inspect(other))
  end

  T.eq("bench: round-trip", WorldState.decode(enc), {:ok, world})

  # ------------------------------------------------------------ edge
  canonical = %Edge.Edge{
    i_min: -2_147_483_648,
    i_max: 2_147_483_647,
    i_zero: 0,
    i_neg: -1,
    l_min: -9_223_372_036_854_775_808,
    l_max: 9_223_372_036_854_775_807,
    l_neg: -300,
    f: -1.25,
    d: 1234.5625,
    d_neg: -0.5,
    yes: true,
    no: false,
    empty: "",
    unicode: "héllo wörld ✓ 日本 \u{1F680}",
    ints: [0, -1, 1, -64, 64, -2_147_483_648, 2_147_483_647],
    longs: [0, -1, 9_223_372_036_854_775_807, -9_223_372_036_854_775_808, 4_294_967_296],
    floats: [0.0, 0.5, -2.25],
    doubles: [0.0, 3.5, -1_000_000.25],
    bools: [true, false, true],
    strings: ["", "a", "日本語"],
    no_ints: [],
    inner: %Inner{big: 1_099_511_627_776, label: "inner"},
    inners: [%Inner{big: -1, label: ""}, %Inner{big: 0, label: "x"}],
    no_inners: []
  }

  eref = File.read!(Path.join([parent, "edge", "edge_ref.bin"]))
  eenc = Edge.Edge.encode(canonical)
  T.check("edge: encode == edge_ref.bin", eenc == eref, "#{byte_size(eenc)} vs #{byte_size(eref)} bytes")

  case Edge.Edge.decode(eref) do
    {:ok, d} ->
      T.check("edge: decode edge_ref.bin", true)

      for {k, want} <- Map.from_struct(canonical) do
        T.eq("edge: field #{k}", Map.fetch!(d, k), want)
      end

      T.eq("edge: decoded == canonical", d, canonical)
      T.eq("edge: re-encode decoded == edge_ref.bin", Edge.Edge.encode(d), eref)

    other ->
      T.check("edge: decode edge_ref.bin", false, inspect(other))
  end

  <<pre::binary-size(5), last, rest::binary>> = eref
  bad = <<pre::binary, Bitwise.bxor(last, 1), rest::binary>>

  T.check(
    "edge: wrong version rejected",
    match?({:error, {:version_mismatch, _, _}}, Edge.Edge.decode(bad)),
    inspect(Edge.Edge.decode(bad))
  )

  accepted =
    for l <- 0..(byte_size(eref) - 1),
        not T.error?(Edge.Edge.decode(binary_part(eref, 0, l))),
        do: l

  T.check("edge: all #{byte_size(eref)} truncations rejected", accepted == [],
    "accepted prefixes #{inspect(accepted)}")

  # ------------------------------------------------------------ extras
  T.eq("extra: int wraps to 32 bits",
    Edge.Edge.encode(%{canonical | i_zero: Bitwise.bsl(1, 32)}), Edge.Edge.encode(canonical))
  T.eq("extra: long wraps to 64 bits",
    Inner.encode(%Inner{big: Bitwise.bsl(1, 64) - 1}), Inner.encode(%Inner{big: -1}))
  T.check("extra: garbage input is an error", T.error?(Edge.Edge.decode(<<255, 255, 255>>)))
  huge = <<10, "1.0.0", 84, 0, T.uv(4_000_000_000)::binary>>
  T.check("extra: huge array count is an error", T.error?(WorldState.decode(huge)))
  T.check("extra: non-binary input is an error", T.error?(Edge.Edge.decode(:nope)))

  # float32(0.29) * 10000 is 2899.9999... in double but 2900 in single
  # precision, which is what the float32 targets put on the wire.
  {:ok, d29} = Edge.Edge.decode(Edge.Edge.encode(%{canonical | f: 0.29}))
  <<f29::float-32>> = <<0.29::float-32>>
  T.eq("extra: float field x10000 in single precision (0.29)", d29.f, f29)

  T.check(
    "extra: out-of-float32-range value raises",
    try do
      Edge.Edge.encode(%{canonical | f: 1.0e300})
      false
    rescue
      ArgumentError -> true
    end
  )
rescue
  e -> T.check("no crash", false, Exception.format(:error, e, __STACKTRACE__))
end

p = Process.get(:pass)
f = Process.get(:fail)
IO.puts("elixir: #{p} passed, #{f} failed")
System.halt(if f == 0, do: 0, else: 1)
