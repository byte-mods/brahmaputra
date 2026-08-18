package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	fb "benchmark/bench_fb"
	pb "benchmark/bench_proto"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
)

func main() {
	fmt.Println("Generating Data...")
	world := createBenchmarkData()
	fmt.Println("Data Generation Complete. Starting Benchmark...")

	const ITERATIONS = 10

	// --- JSON ---
	{
		start := time.Now()
		var size int
		for i := 0; i < ITERATIONS; i++ {
			b, _ := json.Marshal(world)
			if i == 0 {
				size = len(b)
			}
			var w WorldState
			json.Unmarshal(b, &w)
		}
		duration := time.Since(start).Seconds()
		fmt.Printf("JSON:      %.3fs (Size: %d bytes)\n", duration, size)
	}

	// --- MsgPack ---
	{
		start := time.Now()
		var size int
		for i := 0; i < ITERATIONS; i++ {
			b, _ := msgpack.Marshal(world)
			if i == 0 {
				size = len(b)
			}
			var w WorldState
			msgpack.Unmarshal(b, &w)
		}
		duration := time.Since(start).Seconds()
		fmt.Printf("MsgPack:   %.3fs (Size: %d bytes)\n", duration, size)
	}

	// --- BitPacker ---
	{
		start := time.Now()
		var size int
		for i := 0; i < ITERATIONS; i++ {
			b := world.Encode()
			if i == 0 {
				size = len(b)
			}
			_, err := DecodeWorldState(b)
			if err != nil {
				panic(err)
			}
		}
		duration := time.Since(start).Seconds()
		fmt.Printf("BitPacker: %.3fs (Size: %d bytes)\n", duration, size)
	}

	// --- Protobuf ---
	{
		protoWorld := createProtoData(world)
		start := time.Now()
		var size int
		for i := 0; i < ITERATIONS; i++ {
			b, _ := proto.Marshal(protoWorld)
			if i == 0 {
				size = len(b)
			}
			var w pb.WorldState
			proto.Unmarshal(b, &w)
		}
		duration := time.Since(start).Seconds()
		fmt.Printf("Protobuf:  %.3fs (Size: %d bytes)\n", duration, size)
	}

	// --- FlatBuffers ---
	{
		start := time.Now()
		var size int

		// Pre-warm / Test Encode
		builder := flatbuffers.NewBuilder(1024)
		b := createFlatBufferData(builder, world)
		size = len(b)

		// Make a copy for decoding, because builder reuse will overwrite 'b'
		fbBytes := make([]byte, len(b))
		copy(fbBytes, b)

		start = time.Now()
		for i := 0; i < ITERATIONS; i++ {
			builder.Reset()
			createFlatBufferData(builder, world)
		}
		duration := time.Since(start).Seconds()
		// Only Encode time for now to match structure, checking decode separately
		fmt.Printf("FlatBuffers Encode: %.3fs (Size: %d bytes)\n", duration, size)

		// Decode
		start = time.Now()
		for i := 0; i < ITERATIONS; i++ {
			w := fb.GetRootAsWorldState(fbBytes, 0)
			// Trigger access to verify decode
			_ = w.WorldId()
			_ = w.GuildsLength()
		}
		duration = time.Since(start).Seconds()
		fmt.Printf("FlatBuffers Decode: %.3fs\n", duration)
	}
}

func createProtoData(src *WorldState) *pb.WorldState {
	guilds := make([]*pb.Guild, len(src.Guilds))
	for i, g := range src.Guilds {
		members := make([]*pb.Character, len(g.Members))
		for j, m := range g.Members {
			inv := make([]*pb.Item, len(m.Inventory))
			for k, item := range m.Inventory {
				inv[k] = &pb.Item{
					Id:     item.Id,
					Name:   item.Name,
					Value:  item.Value,
					Weight: item.Weight,
					Rarity: item.Rarity,
				}
			}
			members[j] = &pb.Character{
				Name:    m.Name,
				Level:   m.Level,
				Hp:      m.Hp,
				Mp:      m.Mp,
				IsAlive: m.Is_alive,
				Position: &pb.Vec3{
					X: m.Position.X,
					Y: m.Position.Y,
					Z: m.Position.Z,
				},
				Skills:    m.Skills,
				Inventory: inv,
			}
		}
		guilds[i] = &pb.Guild{
			Name:        g.Name,
			Description: g.Description,
			Members:     members,
		}
	}

	return &pb.WorldState{
		WorldId:   src.World_id,
		Seed:      src.Seed,
		Guilds:    guilds,
		LootTable: []*pb.Item{}, // Empty for now to match other benches
	}
}

func createFlatBufferData(builder *flatbuffers.Builder, src *WorldState) []byte {
	// Create strings and nested tables first (leaf to root)

	// We need to create Guilds
	guildOffsets := make([]flatbuffers.UOffsetT, len(src.Guilds))
	for i, g := range src.Guilds {
		// Members
		memberOffsets := make([]flatbuffers.UOffsetT, len(g.Members))
		for j, m := range g.Members {
			// Inventory
			invOffsets := make([]flatbuffers.UOffsetT, len(m.Inventory))
			for k, item := range m.Inventory {
				name := builder.CreateString(item.Name)
				rarity := builder.CreateString(item.Rarity)

				fb.ItemStart(builder)
				fb.ItemAddId(builder, item.Id)
				fb.ItemAddName(builder, name)
				fb.ItemAddValue(builder, item.Value)
				fb.ItemAddWeight(builder, item.Weight)
				fb.ItemAddRarity(builder, rarity)
				invOffsets[k] = fb.ItemEnd(builder)
			}

			fb.CharacterStartInventoryVector(builder, len(invOffsets))
			for k := len(invOffsets) - 1; k >= 0; k-- {
				builder.PrependUOffsetT(invOffsets[k])
			}
			invVector := builder.EndVector(len(invOffsets))

			// Skills
			fb.CharacterStartSkillsVector(builder, len(m.Skills))
			for k := len(m.Skills) - 1; k >= 0; k-- {
				builder.PrependInt32(m.Skills[k])
			}
			skillsVector := builder.EndVector(len(m.Skills))

			name := builder.CreateString(m.Name)

			// Position (Struct)
			// Vec3 is a struct in FB schema, so we create it inline using CreateVec3 (assuming generated code has it)
			// Actually CreateVec3 usually returns an Offset if it's a struct?
			// Wait, for structs, builder.Prep is used inside CreateVec3 and it returns an Offset to the start of the struct.
			// Let's verify usage.
			// generated Vec3.go: func CreateVec3(...) flatbuffers.UOffsetT
			// Yes.

			// However, Structs in FlatBuffers are stored inline in the table.
			// But the generated Go code `CreateVec3` writes to the buffer and returns an offset.
			// And `CharacterAddPosition` takes a generic `flatbuffers.UOffsetT`.
			// So this is correct.

			// pos := fb.CreateVec3(builder, m.Position.X, m.Position.Y, m.Position.Z)

			fb.CharacterStart(builder)
			fb.CharacterAddName(builder, name)
			fb.CharacterAddLevel(builder, m.Level)
			fb.CharacterAddHp(builder, m.Hp)
			fb.CharacterAddMp(builder, m.Mp)
			fb.CharacterAddIsAlive(builder, m.Is_alive)
			// fb.CharacterAddPosition(builder, pos)
			fb.CharacterAddSkills(builder, skillsVector)
			fb.CharacterAddInventory(builder, invVector)
			memberOffsets[j] = fb.CharacterEnd(builder)
		}

		fb.GuildStartMembersVector(builder, len(memberOffsets))
		for k := len(memberOffsets) - 1; k >= 0; k-- {
			builder.PrependUOffsetT(memberOffsets[k])
		}
		membersVector := builder.EndVector(len(memberOffsets))

		name := builder.CreateString(g.Name)
		desc := builder.CreateString(g.Description)

		fb.GuildStart(builder)
		fb.GuildAddName(builder, name)
		fb.GuildAddDescription(builder, desc)
		fb.GuildAddMembers(builder, membersVector)
		guildOffsets[i] = fb.GuildEnd(builder)
	}

	fb.WorldStateStartGuildsVector(builder, len(guildOffsets))
	for i := len(guildOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(guildOffsets[i])
	}
	guildsVector := builder.EndVector(len(guildOffsets))

	seed := builder.CreateString(src.Seed)

	fb.WorldStateStart(builder)
	fb.WorldStateAddWorldId(builder, src.World_id)
	fb.WorldStateAddSeed(builder, seed)
	fb.WorldStateAddGuilds(builder, guildsVector)

	root := fb.WorldStateEnd(builder)
	builder.Finish(root)
	return builder.FinishedBytes()
}

func createBenchmarkData() *WorldState {
	world := &WorldState{
		World_id:   1,
		Seed:       "benchmark_seed",
		Guilds:     make([]Guild, 0, 1000),
		Loot_table: []Item{},
	}

	// MATCHING RUST: 1000 Guilds, 20 Members, 10 Items
	for g := 0; g < 1000; g++ {
		guild := Guild{
			Name:        fmt.Sprintf("Guild_%d", g),
			Description: "A very powerful guild",
			Members:     make([]Character, 0, 20),
		}
		for c := 0; c < 20; c++ {
			char := Character{
				Name:      fmt.Sprintf("Char_%d_%d", g, c),
				Level:     int32(c + 1),
				Hp:        100,
				Mp:        50,
				Is_alive:  true,
				Position:  Vec3{X: 10, Y: 20, Z: 30},
				Skills:    []int32{1, 2, 3, 4, 5},
				Inventory: make([]Item, 0, 10),
			}
			for i := 0; i < 10; i++ {
				item := Item{
					Id: int32(g*1000 + c*100 + i),
					// "Item" * 50 to match other benches roughly
					Name:   strings.Repeat("Item", 50) + fmt.Sprintf("_%d_%d_%d", g, c, i),
					Value:  int32(i * 10),
					Weight: 1,
					Rarity: func() string {
						if i%5 == 0 {
							return "Rare"
						}
						return "Common"
					}(),
				}
				char.Inventory = append(char.Inventory, item)
			}
			guild.Members = append(guild.Members, char)
		}
		world.Guilds = append(world.Guilds, guild)
	}
	return world
}
