const { WorldState, Guild, Character, Item, Vec3 } = require('../../generated/js/vec3');
const msgpack = require('msgpack-lite');
const protobuf = require('protobufjs');
const flatbuffers = require('flatbuffers');
const path = require('path');

function createBenchmarkData() {
    const world = new WorldState();
    world.world_id = 1;
    world.seed = "benchmark_seed";

    for (let g = 0; g < 1000; g++) {
        const guild = new Guild();
        guild.name = `Guild_${g}`;
        guild.description = "A very powerful guild";

        for (let c = 0; c < 20; c++) {
            const char = new Character();
            char.name = `Char_${g}_${c}`;
            char.level = (c + 1);
            char.hp = 100;
            char.mp = 50;
            char.is_alive = true;

            const pos = new Vec3();
            pos.x = 10;
            pos.y = 20;
            pos.z = 30;
            char.position = pos;

            char.skills = [1, 2, 3, 4, 5];

            for (let i = 0; i < 10; i++) {
                const item = new Item();
                item.id = (g * 1000 + c * 100 + i);
                item.name = "Item".repeat(50) + `_${g}_${c}_${i}`;
                item.value = i * 10;
                item.weight = 1;
                item.rarity = (i % 5 === 0) ? "Rare" : "Common";

                char.inventory.push(item);
            }

            guild.members.push(char);
        }

        world.guilds.push(guild);
    }

    world.loot_table = [];
    return world;
}

function toPlainObj(world) {
    return {
        world_id: world.world_id,
        seed: world.seed,
        guilds: world.guilds.map(g => ({
            name: g.name,
            description: g.description,
            members: g.members.map(c => ({
                name: c.name,
                level: c.level,
                hp: c.hp,
                mp: c.mp,
                is_alive: c.is_alive,
                position: { x: c.position.x, y: c.position.y, z: c.position.z },
                skills: c.skills,
                inventory: c.inventory.map(i => ({
                    id: i.id,
                    name: i.name,
                    value: i.value,
                    weight: i.weight,
                    rarity: i.rarity
                }))
            }))
        })),
        loot_table: []
    };
}

// FlatBuffers manual encode using the Builder API
function encodeFlatBuffers(builder, world) {
    builder.clear();

    const guildOffsets = [];
    for (const g of world.guilds) {
        const memberOffsets = [];
        for (const c of g.members) {
            // Inventory
            const invOffsets = [];
            for (const item of c.inventory) {
                const nameOff = builder.createString(item.name);
                const rarityOff = builder.createString(item.rarity);
                // Item table: id, name, value, weight, rarity
                builder.startObject(5);
                builder.addFieldInt32(0, item.id, 0);
                builder.addFieldOffset(1, nameOff, 0);
                builder.addFieldInt32(2, item.value, 0);
                builder.addFieldInt32(3, item.weight, 0);
                builder.addFieldOffset(4, rarityOff, 0);
                invOffsets.push(builder.endObject());
            }

            // Inventory vector
            builder.startVector(4, invOffsets.length, 4);
            for (let i = invOffsets.length - 1; i >= 0; i--) builder.addOffset(invOffsets[i]);
            const invVector = builder.endVector();

            // Skills vector
            builder.startVector(4, c.skills.length, 4);
            for (let i = c.skills.length - 1; i >= 0; i--) builder.addInt32(c.skills[i]);
            const skillsVector = builder.endVector();

            const nameOff = builder.createString(c.name);

            // Character table: name, level, hp, mp, is_alive, position, skills, inventory
            builder.startObject(8);
            builder.addFieldOffset(0, nameOff, 0);
            builder.addFieldInt32(1, c.level, 0);
            builder.addFieldInt32(2, c.hp, 0);
            builder.addFieldInt32(3, c.mp, 0);
            builder.addFieldInt8(4, c.is_alive ? 1 : 0, 0);
            // Position struct (Vec3: 3 x int32 = 12 bytes) — must be inline
            builder.prep(4, 12);
            builder.writeInt32(c.position.z);
            builder.writeInt32(c.position.y);
            builder.writeInt32(c.position.x);
            builder.addFieldStruct(5, builder.offset(), 0);
            builder.addFieldOffset(6, skillsVector, 0);
            builder.addFieldOffset(7, invVector, 0);
            memberOffsets.push(builder.endObject());
        }

        // Members vector
        builder.startVector(4, memberOffsets.length, 4);
        for (let i = memberOffsets.length - 1; i >= 0; i--) builder.addOffset(memberOffsets[i]);
        const membersVector = builder.endVector();

        const nameOff = builder.createString(g.name);
        const descOff = builder.createString(g.description);

        // Guild table: name, description, members
        builder.startObject(3);
        builder.addFieldOffset(0, nameOff, 0);
        builder.addFieldOffset(1, descOff, 0);
        builder.addFieldOffset(2, membersVector, 0);
        guildOffsets.push(builder.endObject());
    }

    // Guilds vector
    builder.startVector(4, guildOffsets.length, 4);
    for (let i = guildOffsets.length - 1; i >= 0; i--) builder.addOffset(guildOffsets[i]);
    const guildsVector = builder.endVector();

    const seedOff = builder.createString(world.seed);

    // WorldState table: world_id, seed, guilds, loot_table
    builder.startObject(4);
    builder.addFieldInt32(0, world.world_id, 0);
    builder.addFieldOffset(1, seedOff, 0);
    builder.addFieldOffset(2, guildsVector, 0);
    // empty loot_table - skip
    const root = builder.endObject();
    builder.finish(root);
    return builder.asUint8Array();
}

async function main() {
    console.log("Generating Data...");
    const data = createBenchmarkData();
    const plainData = toPlainObj(data);
    console.log("Data Generation Complete. Starting Benchmark...");

    const ITERATIONS = 10;

    // --- JSON ---
    {
        const start = process.hrtime.bigint();
        let size = 0;
        for (let i = 0; i < ITERATIONS; i++) {
            const encoded = JSON.stringify(plainData);
            if (i === 0) size = encoded.length;
            const decoded = JSON.parse(encoded);
        }
        const end = process.hrtime.bigint();
        const duration = Number(end - start) / 1e9;
        console.log(`JSON:        ${duration.toFixed(3)}s (Size: ${size} bytes)`);
    }

    // --- MsgPack ---
    {
        const start = process.hrtime.bigint();
        let size = 0;
        for (let i = 0; i < ITERATIONS; i++) {
            const encoded = msgpack.encode(plainData);
            if (i === 0) size = encoded.length;
            const decoded = msgpack.decode(encoded);
        }
        const end = process.hrtime.bigint();
        const duration = Number(end - start) / 1e9;
        console.log(`MsgPack:     ${duration.toFixed(3)}s (Size: ${size} bytes)`);
    }

    // --- BitPacker ---
    {
        try {
            const start = process.hrtime.bigint();
            let size = 0;
            for (let i = 0; i < ITERATIONS; i++) {
                const encoded = data.encode();
                if (i === 0) size = encoded.length;
                const decoded = WorldState.decode(encoded);
            }
            const end = process.hrtime.bigint();
            const duration = Number(end - start) / 1e9;
            console.log(`BitPacker:   ${duration.toFixed(3)}s (Size: ${size} bytes)`);
        } catch (e) {
            console.error("BitPacker Error:", e.message);
        }
    }

    // --- Protobuf ---
    {
        try {
            const root = await protobuf.load(path.join(__dirname, '../schemas/bench_complex.proto'));
            const ProtoWorldState = root.lookupType('bench_proto.WorldState');

            // Convert to proto-compatible object
            const protoObj = {
                worldId: plainData.world_id,
                seed: plainData.seed,
                guilds: plainData.guilds.map(g => ({
                    name: g.name,
                    description: g.description,
                    members: g.members.map(c => ({
                        name: c.name,
                        level: c.level,
                        hp: c.hp,
                        mp: c.mp,
                        isAlive: c.is_alive,
                        position: c.position,
                        skills: c.skills,
                        inventory: c.inventory.map(i => ({
                            id: i.id,
                            name: i.name,
                            value: i.value,
                            weight: i.weight,
                            rarity: i.rarity
                        }))
                    }))
                })),
                lootTable: []
            };

            const msg = ProtoWorldState.create(protoObj);

            const start = process.hrtime.bigint();
            let size = 0;
            for (let i = 0; i < ITERATIONS; i++) {
                const encoded = ProtoWorldState.encode(msg).finish();
                if (i === 0) size = encoded.length;
                const decoded = ProtoWorldState.decode(encoded);
            }
            const end = process.hrtime.bigint();
            const duration = Number(end - start) / 1e9;
            console.log(`Protobuf:    ${duration.toFixed(3)}s (Size: ${size} bytes)`);
        } catch (e) {
            console.error("Protobuf Error:", e.message);
        }
    }

    // --- FlatBuffers ---
    {
        try {
            const builder = new flatbuffers.Builder(65536);

            // Encode
            let fbBytes = encodeFlatBuffers(builder, data);
            const size = fbBytes.length;

            const start = process.hrtime.bigint();
            for (let i = 0; i < ITERATIONS; i++) {
                encodeFlatBuffers(builder, data);
            }
            const end = process.hrtime.bigint();
            const duration = Number(end - start) / 1e9;
            console.log(`FlatBuffers Encode: ${duration.toFixed(3)}s (Size: ${size} bytes)`);

            // Decode (access root fields)
            const buf = new flatbuffers.ByteBuffer(fbBytes);
            const start2 = process.hrtime.bigint();
            for (let i = 0; i < ITERATIONS; i++) {
                // FlatBuffers zero-copy: just reading offset
                const off = buf.readInt32(buf.position()) + buf.position();
                // Access world_id (field 0, vtable offset 4)
                const vtable = off - buf.readInt32(off);
                const vlen = buf.readInt16(vtable);
                if (vlen > 4) {
                    const _ = buf.readInt32(off + buf.readInt16(vtable + 4));
                }
            }
            const end2 = process.hrtime.bigint();
            const duration2 = Number(end2 - start2) / 1e9;
            console.log(`FlatBuffers Decode: ${duration2.toFixed(3)}s`);
        } catch (e) {
            console.error("FlatBuffers Error:", e.message);
        }
    }
}

main().catch(console.error);
