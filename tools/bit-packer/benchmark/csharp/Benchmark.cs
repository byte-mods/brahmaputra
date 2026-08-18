using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using MessagePack;
using Google.Protobuf;
using Google.FlatBuffers;
using Generated; // BitPacker

namespace Benchmark {
    class Program {
        static WorldState CreateComplexWorld() {
            var world = new WorldState {
                world_id = 1,
                seed = "benchmark_seed",
                guilds = new Guild[1000],
                loot_table = new Item[0]
            };

            for (int g = 0; g < 1000; g++) {
                var guild = new Guild {
                    name = "Guild_" + g,
                    description = "A very powerful guild",
                    members = new Character[20]
                };
                for (int c = 0; c < 20; c++) {
                    var ch = new Character {
                        name = "Char_" + g + "_" + c,
                        level = (c + 1),
                        hp = 100,
                        mp = 50,
                        is_alive = true,
                        position = new Vec3 { x = 10, y = 20, z = 30 },
                        skills = new int[] { 1, 2, 3, 4, 5 },
                        inventory = new Item[10]
                    };
                    for (int i = 0; i < 10; i++) {
                        ch.inventory[i] = new Item {
                            id = (g * 1000 + c * 100 + i),
                            name = string.Concat(Enumerable.Repeat("Item", 50)) + "_" + g + "_" + c + "_" + i,
                            value = (i * 10),
                            weight = 1,
                            rarity = (i % 5 == 0) ? "Rare" : "Common"
                        };
                    }
                    guild.members[c] = ch;
                }
                world.guilds[g] = guild;
            }
            return world;
        }

        static BenchProto.WorldState CreateProtoData(WorldState src) {
            var pw = new BenchProto.WorldState { WorldId = src.world_id, Seed = src.seed };
            foreach (var g in src.guilds) {
                var pg = new BenchProto.Guild { Name = g.name, Description = g.description };
                foreach (var c in g.members) {
                    var pc = new BenchProto.Character {
                        Name = c.name, Level = c.level, Hp = c.hp, Mp = c.mp,
                        IsAlive = c.is_alive,
                        Position = new BenchProto.Vec3 { X = c.position.x, Y = c.position.y, Z = c.position.z }
                    };
                    pc.Skills.AddRange(c.skills);
                    foreach (var item in c.inventory) {
                        pc.Inventory.Add(new BenchProto.Item {
                            Id = item.id, Name = item.name, Value = item.value,
                            Weight = item.weight, Rarity = item.rarity
                        });
                    }
                    pg.Members.Add(pc);
                }
                pw.Guilds.Add(pg);
            }
            return pw;
        }

        static byte[] EncodeFlatBuffers(FlatBufferBuilder builder, WorldState src) {
            builder.Clear();
            
            var guildOffsets = new Offset<bench_fb.Guild>[src.guilds.Length];
            for (int g = 0; g < src.guilds.Length; g++) {
                var guild = src.guilds[g];
                
                var memberOffsets = new Offset<bench_fb.Character>[guild.members.Length];
                for (int m = 0; m < guild.members.Length; m++) {
                    var ch = guild.members[m];
                    
                    var invOffsets = new Offset<bench_fb.Item>[ch.inventory.Length];
                    for (int i = 0; i < ch.inventory.Length; i++) {
                        var item = ch.inventory[i];
                        var nameOff = builder.CreateString(item.name);
                        var rarityOff = builder.CreateString(item.rarity);
                        bench_fb.Item.StartItem(builder);
                        bench_fb.Item.AddId(builder, item.id);
                        bench_fb.Item.AddName(builder, nameOff);
                        bench_fb.Item.AddValue(builder, item.value);
                        bench_fb.Item.AddWeight(builder, item.weight);
                        bench_fb.Item.AddRarity(builder, rarityOff);
                        invOffsets[i] = bench_fb.Item.EndItem(builder);
                    }
                    var invVector = bench_fb.Character.CreateInventoryVector(builder, invOffsets);
                    var skillsVector = bench_fb.Character.CreateSkillsVector(builder, ch.skills);
                    var cNameOff = builder.CreateString(ch.name);
                    
                    bench_fb.Character.StartCharacter(builder);
                    bench_fb.Character.AddName(builder, cNameOff);
                    bench_fb.Character.AddLevel(builder, ch.level);
                    bench_fb.Character.AddHp(builder, ch.hp);
                    bench_fb.Character.AddMp(builder, ch.mp);
                    bench_fb.Character.AddIsAlive(builder, ch.is_alive);
                    bench_fb.Character.AddPosition(builder, bench_fb.Vec3.CreateVec3(builder, ch.position.x, ch.position.y, ch.position.z));
                    bench_fb.Character.AddSkills(builder, skillsVector);
                    bench_fb.Character.AddInventory(builder, invVector);
                    memberOffsets[m] = bench_fb.Character.EndCharacter(builder);
                }
                var membersVector = bench_fb.Guild.CreateMembersVector(builder, memberOffsets);
                var gNameOff = builder.CreateString(guild.name);
                var descOff = builder.CreateString(guild.description);
                
                bench_fb.Guild.StartGuild(builder);
                bench_fb.Guild.AddName(builder, gNameOff);
                bench_fb.Guild.AddDescription(builder, descOff);
                bench_fb.Guild.AddMembers(builder, membersVector);
                guildOffsets[g] = bench_fb.Guild.EndGuild(builder);
            }
            var guildsVector = bench_fb.WorldState.CreateGuildsVector(builder, guildOffsets);
            var seedOff = builder.CreateString(src.seed);
            
            var root = bench_fb.WorldState.CreateWorldState(builder, src.world_id, seedOff, guildsVector);
            builder.Finish(root.Value);
            return builder.SizedByteArray();
        }

        static void Main(string[] args) {
            Console.WriteLine("Generating Data...");
            var world = CreateComplexWorld();
            var protoWorld = CreateProtoData(world);
            Console.WriteLine("Data Generation Complete. Starting Benchmark...");

            int ITERATIONS = 10;

            // JSON Prep
            string jsonStr = JsonConvert.SerializeObject(world);

            // MsgPack Prep
            var options = MessagePackSerializerOptions.Standard
                .WithResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);
            byte[] msgpackData = MessagePackSerializer.Serialize(world, options);

            // BitPacker Prep
            byte[] bpData = world.Encode();

            // Protobuf Prep
            byte[] protoData = protoWorld.ToByteArray();

            // FlatBuffers Prep
            var fbBuilder = new FlatBufferBuilder(65536);
            byte[] fbData = EncodeFlatBuffers(fbBuilder, world);

            Console.WriteLine($"\nPayload Sizes:");
            Console.WriteLine($"JSON: {jsonStr.Length}");
            Console.WriteLine($"MsgPack: {msgpackData.Length}");
            Console.WriteLine($"BitPacker: {bpData.Length}");
            Console.WriteLine($"Protobuf: {protoData.Length}");
            Console.WriteLine($"FlatBuffers: {fbData.Length}");

            // --- JSON ---
            {
                var sw = Stopwatch.StartNew();
                for (int i = 0; i < ITERATIONS; i++) {
                    string s = JsonConvert.SerializeObject(world);
                    var w = JsonConvert.DeserializeObject<WorldState>(s);
                }
                sw.Stop();
                Console.WriteLine($"\nJSON:        {sw.Elapsed.TotalSeconds:F3}s (Size: {jsonStr.Length} bytes)");
            }

            // --- MsgPack ---
            {
                var sw = Stopwatch.StartNew();
                for (int i = 0; i < ITERATIONS; i++) {
                    byte[] b = MessagePackSerializer.Serialize(world, options);
                    var w = MessagePackSerializer.Deserialize<WorldState>(b, options);
                }
                sw.Stop();
                Console.WriteLine($"MsgPack:     {sw.Elapsed.TotalSeconds:F3}s (Size: {msgpackData.Length} bytes)");
            }

            // --- BitPacker ---
            {
                var sw = Stopwatch.StartNew();
                for (int i = 0; i < ITERATIONS; i++) {
                    byte[] b = world.Encode();
                    var w = WorldState.Decode(b);
                }
                sw.Stop();
                Console.WriteLine($"BitPacker:   {sw.Elapsed.TotalSeconds:F3}s (Size: {bpData.Length} bytes)");
            }

            // --- Protobuf ---
            {
                var sw = Stopwatch.StartNew();
                for (int i = 0; i < ITERATIONS; i++) {
                    byte[] b = protoWorld.ToByteArray();
                    var w = BenchProto.WorldState.Parser.ParseFrom(b);
                }
                sw.Stop();
                Console.WriteLine($"Protobuf:    {sw.Elapsed.TotalSeconds:F3}s (Size: {protoData.Length} bytes)");
            }

            // --- FlatBuffers ---
            {
                // Encode
                var sw = Stopwatch.StartNew();
                for (int i = 0; i < ITERATIONS; i++) {
                    EncodeFlatBuffers(fbBuilder, world);
                }
                sw.Stop();
                Console.WriteLine($"FlatBuffers Encode: {sw.Elapsed.TotalSeconds:F3}s (Size: {fbData.Length} bytes)");

                // Decode
                var buf = new ByteBuffer(fbData);
                sw = Stopwatch.StartNew();
                for (int i = 0; i < ITERATIONS; i++) {
                    var w = bench_fb.WorldState.GetRootAsWorldState(buf);
                    var _ = w.WorldId;
                    var __ = w.GuildsLength;
                }
                sw.Stop();
                Console.WriteLine($"FlatBuffers Decode: {sw.Elapsed.TotalSeconds:F3}s");
            }
        }
    }
}
