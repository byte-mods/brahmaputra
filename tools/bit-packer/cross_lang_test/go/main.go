// Cross-language conformance test for the Go target. run.sh generates
// gen/bench/go and gen/edge/go from the current generator first.
package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"

	bench "gotest/gen/bench/go"
	edge "gotest/gen/edge/go"
)

var passed, failed int

func check(name string, ok bool, detail string) {
	if ok {
		passed++
		fmt.Printf("  ok   %s\n", name)
	} else {
		failed++
		fmt.Printf("  FAIL %s (%s)\n", name, detail)
	}
}

func eq(name string, got, want interface{}) {
	check(name, reflect.DeepEqual(got, want), fmt.Sprintf("got %#v, want %#v", got, want))
}

// --- bench ---

func makeWorld() *bench.WorldState {
	hero := bench.Character{
		Name: "TestHero", Level: 99, Hp: 1000, Mp: 500, Is_alive: true,
		Position:  bench.Vec3{X: 10, Y: -20, Z: 30},
		Skills:    []int32{1, 2, 3, 100},
		Inventory: []bench.Item{{Id: 1, Name: "Excalibur", Value: 9999, Weight: 15, Rarity: "Legendary"}},
	}
	return &bench.WorldState{
		World_id: 42,
		Seed:     "cross_lang_test",
		Guilds: []bench.Guild{{
			Name: "TestGuild", Description: "A test guild for cross-language",
			Members: []bench.Character{hero},
		}},
		Loot_table: []bench.Item{{Id: 2, Name: "HealthPotion", Value: 50, Weight: 1, Rarity: "Common"}},
	}
}

func verifyWorld(label string, w *bench.WorldState) {
	ok := w.World_id == 42 && w.Seed == "cross_lang_test" && len(w.Guilds) == 1 &&
		len(w.Loot_table) == 1 && w.Loot_table[0].Name == "HealthPotion" && w.Loot_table[0].Rarity == "Common"
	if ok {
		g := w.Guilds[0]
		ok = g.Name == "TestGuild" && g.Description == "A test guild for cross-language" && len(g.Members) == 1
		if ok {
			h := g.Members[0]
			ok = h.Name == "TestHero" && h.Level == 99 && h.Hp == 1000 && h.Mp == 500 && h.Is_alive &&
				h.Position == bench.Vec3{X: 10, Y: -20, Z: 30} &&
				reflect.DeepEqual(h.Skills, []int32{1, 2, 3, 100}) && len(h.Inventory) == 1 &&
				h.Inventory[0].Name == "Excalibur" && h.Inventory[0].Value == 9999 && h.Inventory[0].Rarity == "Legendary"
		}
	}
	check(label, ok, fmt.Sprintf("%+v", w))
}

// --- edge ---

func makeEdge() *edge.Edge {
	return &edge.Edge{
		I_min: -2147483648, I_max: 2147483647, I_zero: 0, I_neg: -1,
		L_min: -9223372036854775808, L_max: 9223372036854775807, L_neg: -300,
		F: -1.25, D: 1234.5625, D_neg: -0.5,
		Yes: true, No: false,
		Empty: "", Unicode: "héllo wörld ✓ 日本 🚀",
		Ints:      []int32{0, -1, 1, -64, 64, -2147483648, 2147483647},
		Longs:     []int64{0, -1, 9223372036854775807, -9223372036854775808, 4294967296},
		Floats:    []float32{0.0, 0.5, -2.25},
		Doubles:   []float64{0.0, 3.5, -1000000.25},
		Bools:     []bool{true, false, true},
		Strings:   []string{"", "a", "日本語"},
		No_ints:   []int32{},
		Inner:     edge.Inner{Big: 1099511627776, Label: "inner"},
		Inners:    []edge.Inner{{Big: -1, Label: ""}, {Big: 0, Label: "x"}},
		No_inners: []edge.Inner{},
	}
}

func verifyEdge(e *edge.Edge) {
	w := makeEdge()
	eq("edge decode i_min", e.I_min, w.I_min)
	eq("edge decode i_max", e.I_max, w.I_max)
	eq("edge decode i_zero", e.I_zero, w.I_zero)
	eq("edge decode i_neg", e.I_neg, w.I_neg)
	eq("edge decode l_min", e.L_min, w.L_min)
	eq("edge decode l_max", e.L_max, w.L_max)
	eq("edge decode l_neg", e.L_neg, w.L_neg)
	eq("edge decode f", e.F, w.F)
	eq("edge decode d", e.D, w.D)
	eq("edge decode d_neg", e.D_neg, w.D_neg)
	eq("edge decode yes", e.Yes, w.Yes)
	eq("edge decode no", e.No, w.No)
	eq("edge decode empty", e.Empty, w.Empty)
	eq("edge decode unicode", e.Unicode, w.Unicode)
	eq("edge decode ints", e.Ints, w.Ints)
	eq("edge decode longs", e.Longs, w.Longs)
	eq("edge decode floats", e.Floats, w.Floats)
	eq("edge decode doubles", e.Doubles, w.Doubles)
	eq("edge decode bools", e.Bools, w.Bools)
	eq("edge decode strings", e.Strings, w.Strings)
	eq("edge decode no_ints", e.No_ints, w.No_ints)
	eq("edge decode inner", e.Inner, w.Inner)
	eq("edge decode inners", e.Inners, w.Inners)
	eq("edge decode no_inners", e.No_inners, w.No_inners)
}

func badVersion(data []byte) []byte {
	b := append([]byte(nil), data...)
	b[1] ^= 0x01              // first byte of the version string
	return b
}

func truncations(name string, data []byte, decode func([]byte) error) {
	for cut := 0; cut < len(data); cut++ {
		if decode(data[:cut]) == nil {
			check(name, false, fmt.Sprintf("prefix of %d/%d bytes decoded without error", cut, len(data)))
			return
		}
	}
	check(name, true, "")
}

// encodeRejected reports whether Encode panics with ErrFloatRange.
func encodeRejected(mutate func(*edge.Edge)) (rejected bool) {
	e := makeEdge()
	mutate(e)
	defer func() {
		if r := recover(); r != nil {
			rejected = r == edge.ErrFloatRange
		}
	}()
	e.Encode()
	return false
}

func unhex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func main() {
	root := os.Args[1]
	ref, err := os.ReadFile(filepath.Join(root, "test_data.bin"))
	if err != nil {
		panic(err)
	}
	edgeRef, err := os.ReadFile(filepath.Join(root, "edge", "edge_ref.bin"))
	if err != nil {
		panic(err)
	}

	// bench
	enc := makeWorld().Encode()
	_ = os.WriteFile(filepath.Join(root, "test_data_go.bin"), enc, 0o644)
	check("bench encode == test_data.bin", bytes.Equal(enc, ref), fmt.Sprintf("%d vs %d bytes", len(enc), len(ref)))
	if w, err := bench.DecodeWorldState(ref); err != nil {
		check("bench decode test_data.bin", false, err.Error())
	} else {
		verifyWorld("bench decode test_data.bin", w)
	}
	if w, err := bench.DecodeWorldState(enc); err != nil {
		check("bench round-trip", false, err.Error())
	} else {
		verifyWorld("bench round-trip", w)
		check("bench re-encode == encode", bytes.Equal(w.Encode(), enc), "bytes differ")
	}
	_, err = bench.DecodeWorldState(badVersion(ref))
	check("bench wrong version rejected", err != nil, "decoded without error")
	truncations("bench every truncation rejected", ref, func(b []byte) error { _, err := bench.DecodeWorldState(b); return err })

	// edge
	eenc := makeEdge().Encode()
	check("edge encode == edge_ref.bin", bytes.Equal(eenc, edgeRef), fmt.Sprintf("got %x", eenc))
	if e, err := edge.DecodeEdge(edgeRef); err != nil {
		check("edge decode edge_ref.bin", false, err.Error())
	} else {
		verifyEdge(e)
		check("edge re-encode == edge_ref.bin", bytes.Equal(e.Encode(), edgeRef), "bytes differ")
	}
	_, err = edge.DecodeEdge(badVersion(edgeRef))
	check("edge wrong version rejected", err != nil, "decoded without error")
	truncations("edge every truncation rejected", edgeRef, func(b []byte) error { _, err := edge.DecodeEdge(b); return err })

	// float32 variant: x10000 must be computed in single precision
	f32ref, err := os.ReadFile(filepath.Join(root, "edge", "edge_float32_ref.bin"))
	if err != nil {
		panic(err)
	}
	fv := makeEdge()
	fv.F, fv.Floats = 0.29, []float32{0.7, 16777.217, -0.29}
	check("edge float32 encode == edge_float32_ref.bin", bytes.Equal(fv.Encode(), f32ref), fmt.Sprintf("got %x", fv.Encode()))
	if e, err := edge.DecodeEdge(f32ref); err != nil {
		check("edge float32 decode", false, err.Error())
	} else {
		check("edge float32 decode", e.F == fv.F && reflect.DeepEqual(e.Floats, fv.Floats), fmt.Sprintf("%v %v", e.F, e.Floats))
	}
	// trailing bytes after the root class are ignored
	if e, err := edge.DecodeEdge(append(append([]byte(nil), edgeRef...), 0x00, 0xff)); err != nil {
		check("edge trailing bytes ignored", false, err.Error())
	} else {
		check("edge trailing bytes ignored", bytes.Equal(e.Encode(), edgeRef), "decoded value differs")
	}
	// unencodable floats: Encode has no error return, so it panics
	check("edge encode NaN double rejected", encodeRejected(func(e *edge.Edge) { e.D = math.NaN() }), "encoded")
	check("edge encode out-of-range float rejected", encodeRejected(func(e *edge.Edge) { e.F = 1e30 }), "encoded")

	// hostile input: bogus lengths must fail fast, not allocate or crash
	_, err = bench.DecodeWorldState(unhex("0a312e302e300000feffffff0f"))
	check("bench hostile huge array length rejected", err != nil, "decoded without error")
	_, err = bench.DecodeWorldState(unhex("0a312e302e30000001"))
	check("bench hostile negative array length rejected", err != nil, "decoded without error")
	_, err = edge.DecodeInner(unhex("0a322e312e30008080808010"))
	check("edge hostile huge string length rejected", err != nil, "decoded without error")
	_, err = edge.DecodeInner(unhex("0a322e312e300001"))
	check("edge hostile negative string length rejected", err != nil, "decoded without error")
	_, err = edge.DecodeInner(unhex("0a322e312e30ffffffffffffffffffffff"))
	check("edge hostile endless varint rejected", err != nil, "decoded without error")
	_, err = edge.DecodeInner(unhex("0a322e312e300002ff"))
	check("edge hostile invalid UTF-8 string rejected", err != nil, "decoded without error")

	if failed > 0 {
		os.Exit(1)
	}
}
