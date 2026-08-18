// Go roundtrip test + cross-language decode test
package main

import (
	"fmt"
	"os"
	"path/filepath"

	bp "crosstest/generated/go"
)

func createTestData() *bp.WorldState {
	w := &bp.WorldState{
		World_id: 42,
		Seed:     "cross_lang_test",
		Guilds: []bp.Guild{
			{
				Name:        "TestGuild",
				Description: "A test guild for cross-language",
				Members: []bp.Character{
					{
						Name:     "TestHero",
						Level:    99,
						Hp:       1000,
						Mp:       500,
						Is_alive: true,
						Position: bp.Vec3{X: 10, Y: -20, Z: 30},
						Skills:   []int32{1, 2, 3, 100},
						Inventory: []bp.Item{
							{Id: 1, Name: "Excalibur", Value: 9999, Weight: 15, Rarity: "Legendary"},
						},
					},
				},
			},
		},
		Loot_table: []bp.Item{
			{Id: 2, Name: "HealthPotion", Value: 50, Weight: 1, Rarity: "Common"},
		},
	}
	return w
}

func verify(d *bp.WorldState, label string) error {
	if d.World_id != 42 {
		return fmt.Errorf("%s: world_id=%d", label, d.World_id)
	}
	if d.Seed != "cross_lang_test" {
		return fmt.Errorf("%s: seed=%s", label, d.Seed)
	}
	if len(d.Guilds) != 1 {
		return fmt.Errorf("%s: guilds len=%d", label, len(d.Guilds))
	}
	g := d.Guilds[0]
	if g.Name != "TestGuild" {
		return fmt.Errorf("%s: guild=%s", label, g.Name)
	}
	if len(g.Members) != 1 {
		return fmt.Errorf("%s: members=%d", label, len(g.Members))
	}
	h := g.Members[0]
	if h.Name != "TestHero" {
		return fmt.Errorf("%s: hero=%s", label, h.Name)
	}
	if h.Level != 99 {
		return fmt.Errorf("%s: level=%d", label, h.Level)
	}
	if h.Hp != 1000 {
		return fmt.Errorf("%s: hp=%d", label, h.Hp)
	}
	if h.Position.X != 10 {
		return fmt.Errorf("%s: x=%d", label, h.Position.X)
	}
	if h.Position.Y != -20 {
		return fmt.Errorf("%s: y=%d", label, h.Position.Y)
	}
	if h.Position.Z != 30 {
		return fmt.Errorf("%s: z=%d", label, h.Position.Z)
	}
	if len(h.Skills) != 4 {
		return fmt.Errorf("%s: skills=%d", label, len(h.Skills))
	}
	if h.Skills[3] != 100 {
		return fmt.Errorf("%s: skill[3]=%d", label, h.Skills[3])
	}
	if h.Inventory[0].Name != "Excalibur" {
		return fmt.Errorf("%s: sword=%s", label, h.Inventory[0].Name)
	}
	if h.Inventory[0].Value != 9999 {
		return fmt.Errorf("%s: sword_val=%d", label, h.Inventory[0].Value)
	}
	if d.Loot_table[0].Name != "HealthPotion" {
		return fmt.Errorf("%s: potion=%s", label, d.Loot_table[0].Name)
	}
	if d.Loot_table[0].Rarity != "Common" {
		return fmt.Errorf("%s: rarity=%s", label, d.Loot_table[0].Rarity)
	}
	return nil
}

func main() {
	fmt.Println("🔵 Go")

	// 1. Roundtrip
	w := createTestData()
	encoded := w.Encode()
	fmt.Printf("   Encoded: %d bytes\n", len(encoded))

	decoded, err := bp.DecodeWorldState(encoded)
	if err != nil {
		fmt.Printf("   ❌ Decode error: %v\n", err)
		os.Exit(1)
	}
	if err := verify(decoded, "Go roundtrip"); err != nil {
		fmt.Printf("   ❌ %v\n", err)
		os.Exit(1)
	}
	fmt.Println("   ✅ Roundtrip PASS")

	// 2. Write to file
	// Use the cross_lang_test dir
	outFile := filepath.Join(filepath.Dir(os.Args[0]), "test_data_go.bin")
	if len(os.Args) > 1 {
		outFile = os.Args[1]
	}
	os.WriteFile(outFile, encoded, 0644)
	fmt.Printf("   📁 Written to %s\n", outFile)

	// 3. Cross-language: decode Python's encoded data
	pyFile := filepath.Join(filepath.Dir(os.Args[0]), "test_data.bin")
	if len(os.Args) > 2 {
		pyFile = os.Args[2]
	}
	pyData, err := os.ReadFile(pyFile)
	if err != nil {
		fmt.Printf("   ⚠️  No Python data: %v\n", err)
		return
	}
	pyDecoded, err := bp.DecodeWorldState(pyData)
	if err != nil {
		fmt.Printf("   ❌ Cross-lang decode error: %v\n", err)
		os.Exit(1)
	}
	if err := verify(pyDecoded, "Go←Python cross-lang"); err != nil {
		fmt.Printf("   ❌ %v\n", err)
		os.Exit(1)
	}
	fmt.Println("   ✅ Cross-language decode (Python→Go) PASS")
}
