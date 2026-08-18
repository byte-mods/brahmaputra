package org.bitpacker.benchmark;

import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.bitpacker.benchmark.Vec3Gen.WorldState;
import org.bitpacker.benchmark.Vec3Gen.Guild;
import org.bitpacker.benchmark.Vec3Gen.Item;
import org.bitpacker.benchmark.Vec3Gen.Vec3;

public class Benchmark {
    public static void main(String[] args) throws Exception {
        System.out.println("Generating Data...");
        WorldState world = createBenchmarkData();
        System.out.println("Data Generation Complete. Starting Benchmark...");

        int ITERATIONS = 100;

        // --- JSON (Jackson) ---
        {
            ObjectMapper mapper = new ObjectMapper();
            long start = System.nanoTime();
            int size = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                byte[] b = mapper.writeValueAsBytes(world);
                if (i == 0) size = b.length;
                WorldState w = mapper.readValue(b, WorldState.class);
            }
            double duration = (System.nanoTime() - start) / 1e9;
            System.out.printf("JSON:      %.3fs (Size: %d bytes)\n", duration, size);
        }

        // --- MsgPack (Jackson + MsgPack) ---
        {
            ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());
            long start = System.nanoTime();
            int size = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                byte[] b = mapper.writeValueAsBytes(world);
                if (i == 0) size = b.length;
                WorldState w = mapper.readValue(b, WorldState.class);
            }
            double duration = (System.nanoTime() - start) / 1e9;
            System.out.printf("MsgPack:   %.3fs (Size: %d bytes)\n", duration, size);
        }

        // --- BitPacker ---
        {
            long start = System.nanoTime();
            int size = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                byte[] b = world.encode();
                if (i == 0) size = b.length;
                WorldState w = WorldState.decode(b);
            }
            double duration = (System.nanoTime() - start) / 1e9;
            System.out.printf("BitPacker: %.3fs (Size: %d bytes)\n", duration, size);
        }

        // --- Protobuf ---
        {
            bench_proto.BenchComplex.WorldState protoWorld = createProtoData(world);
            long start = System.nanoTime();
            int size = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                byte[] b = protoWorld.toByteArray();
                if (i == 0) size = b.length;
                bench_proto.BenchComplex.WorldState w = bench_proto.BenchComplex.WorldState.parseFrom(b);
            }
            double duration = (System.nanoTime() - start) / 1e9;
            System.out.printf("Protobuf:  %.3fs (Size: %d bytes)\n", duration, size);
        }

        // --- FlatBuffers ---
        {
            long start = System.nanoTime();
            int size = 0;

            // Pre-warm / Encode Test
            com.google.flatbuffers.FlatBufferBuilder builder = new com.google.flatbuffers.FlatBufferBuilder(1024);
            int root = createFlatBufferData(builder, world);
            builder.finish(root);
            byte[] fbBytes = builder.sizedByteArray();
            size = fbBytes.length;

            start = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                builder.clear();
                int r = createFlatBufferData(builder, world);
                builder.finish(r);
                if (i==0) {
                     byte[] b = builder.sizedByteArray();
                     size = b.length;
                }
            }
            double duration = (System.nanoTime() - start) / 1e9;
            System.out.printf("FlatBuffers Encode: %.3fs (Size: %d bytes)\n", duration, size);
            
            // Decode
            start = System.nanoTime();
            java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(fbBytes);
            for (int i = 0; i < ITERATIONS; i++) {
                bench_fb.WorldState w = bench_fb.WorldState.getRootAsWorldState(bb);
                // Access data to verify
                w.worldId();
                w.guildsLength();
            }
            duration = (System.nanoTime() - start) / 1e9;
            System.out.printf("FlatBuffers Decode: %.3fs\n", duration);
        }
    }

    private static bench_proto.BenchComplex.WorldState createProtoData(WorldState src) {
        bench_proto.BenchComplex.WorldState.Builder wb = bench_proto.BenchComplex.WorldState.newBuilder();
        wb.setWorldId(src.world_id);
        wb.setSeed(src.seed);
        
        for (Guild g : src.guilds) {
            bench_proto.BenchComplex.Guild.Builder gb = bench_proto.BenchComplex.Guild.newBuilder();
            gb.setName(g.name);
            gb.setDescription(g.description);
            
            for (Vec3Gen.Character c : g.members) {
                bench_proto.BenchComplex.Character.Builder cb = bench_proto.BenchComplex.Character.newBuilder();
                cb.setName(c.name);
                cb.setLevel(c.level);
                cb.setHp(c.hp);
                cb.setMp(c.mp);
                cb.setIsAlive(c.is_alive);
                
                bench_proto.BenchComplex.Vec3.Builder vb = bench_proto.BenchComplex.Vec3.newBuilder();
                vb.setX(c.position.x);
                vb.setY(c.position.y);
                vb.setZ(c.position.z);
                cb.setPosition(vb);
                
                for(int s : c.skills) cb.addSkills(s);
                
                for(Item item : c.inventory) {
                     bench_proto.BenchComplex.Item.Builder ib = bench_proto.BenchComplex.Item.newBuilder();
                     ib.setId(item.id);
                     ib.setName(item.name);
                     ib.setValue(item.value);
                     ib.setWeight(item.weight);
                     ib.setRarity(item.rarity);
                     cb.addInventory(ib);
                }
                gb.addMembers(cb);
            }
            wb.addGuilds(gb);
        }
        return wb.build();
    }

    private static int createFlatBufferData(com.google.flatbuffers.FlatBufferBuilder builder, WorldState src) {
        // FlatBuffers builds from leaves to root
        
        // Create Guilds (which contain Members -> Inventory/Skills/Position)
        int[] guildOffsets = new int[src.guilds.length];
        
        for(int i=0; i<src.guilds.length; i++) {
            Guild g = src.guilds[i];
            
            int[] memberOffsets = new int[g.members.length];
            for(int j=0; j<g.members.length; j++) {
                Vec3Gen.Character c = g.members[j];
                
                // Inventory
                int[] invOffsets = new int[c.inventory.length];
                for (int k=0; k<c.inventory.length; k++) {
                    Item item = c.inventory[k];
                    int nameOff = builder.createString(item.name);
                    int rarityOff = builder.createString(item.rarity);
                    
                    bench_fb.Item.startItem(builder);
                    bench_fb.Item.addId(builder, item.id);
                    bench_fb.Item.addName(builder, nameOff);
                    bench_fb.Item.addValue(builder, item.value);
                    bench_fb.Item.addWeight(builder, item.weight);
                    bench_fb.Item.addRarity(builder, rarityOff);
                    invOffsets[k] = bench_fb.Item.endItem(builder);
                }
                int invVector = bench_fb.Character.createInventoryVector(builder, invOffsets);
                
                // Skills
                int skillsVector = bench_fb.Character.createSkillsVector(builder, c.skills);
                
                int nameOff = builder.createString(c.name);
                
                bench_fb.Character.startCharacter(builder);
                bench_fb.Character.addName(builder, nameOff);
                bench_fb.Character.addLevel(builder, c.level);
                bench_fb.Character.addHp(builder, c.hp);
                bench_fb.Character.addMp(builder, c.mp);
                bench_fb.Character.addIsAlive(builder, c.is_alive);
                // Position (Struct) — must be created inline during table construction
                bench_fb.Character.addPosition(builder, bench_fb.Vec3.createVec3(builder, c.position.x, c.position.y, c.position.z));
                bench_fb.Character.addSkills(builder, skillsVector);
                bench_fb.Character.addInventory(builder, invVector);
                memberOffsets[j] = bench_fb.Character.endCharacter(builder);
            }
            int membersVector = bench_fb.Guild.createMembersVector(builder, memberOffsets);
            
            int nameOff = builder.createString(g.name);
            int descOff = builder.createString(g.description);
            
            bench_fb.Guild.startGuild(builder);
            bench_fb.Guild.addName(builder, nameOff);
            bench_fb.Guild.addDescription(builder, descOff);
            bench_fb.Guild.addMembers(builder, membersVector);
            guildOffsets[i] = bench_fb.Guild.endGuild(builder);
        }
        
        int guildsVector = bench_fb.WorldState.createGuildsVector(builder, guildOffsets);
        int seedOff = builder.createString(src.seed);
        
        bench_fb.WorldState.startWorldState(builder);
        bench_fb.WorldState.addWorldId(builder, src.world_id);
        bench_fb.WorldState.addSeed(builder, seedOff);
        bench_fb.WorldState.addGuilds(builder, guildsVector);
        return bench_fb.WorldState.endWorldState(builder);
    }
    
    private static WorldState createBenchmarkData() {
        WorldState world = new WorldState();
        world.world_id = 1;
        world.seed = "benchmark_seed";
        world.guilds = new Guild[1000];
        world.loot_table = new Item[0];

        for (int g = 0; g < 1000; g++) {
            Guild guild = new Guild();
            guild.name = "Guild_" + g;
            guild.description = "A very powerful guild";
            guild.members = new Vec3Gen.Character[20];
            
            for (int c = 0; c < 20; c++) {
                Vec3Gen.Character charObj = new Vec3Gen.Character();
                charObj.name = "Char_" + g + "_" + c;
                charObj.level = (c + 1);
                charObj.hp = 100;
                charObj.mp = 50;
                charObj.is_alive = true;
                
                Vec3 pos = new Vec3();
                pos.x = 10; pos.y = 20; pos.z = 30;
                charObj.position = pos;
                
                charObj.skills = new int[]{1, 2, 3, 4, 5};
                charObj.inventory = new Item[10];
                
                for (int i = 0; i < 10; i++) {
                    Item item = new Item();
                    item.id = (g * 1000 + c * 100 + i);
                    item.name = "Item".repeat(50) + "_" + g + "_" + c + "_" + i;
                    item.value = (i * 10);
                    item.weight = 1;
                    item.rarity = (i % 5 == 0) ? "Rare" : "Common";
                    charObj.inventory[i] = item;
                }
                guild.members[c] = charObj;
            }
            world.guilds[g] = guild;
        }
        return world;
    }
}
