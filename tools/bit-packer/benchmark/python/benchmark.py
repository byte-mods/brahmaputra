import timeit
import json
import msgpack
import sys
import os
import time
import struct

# Add local generated path for Proto/FB
sys.path.append(os.path.abspath("generated"))

from bench_complex import WorldState, Guild, Character, Item, Vec3

# Protobuf
from bench_complex_pb2 import (
    WorldState as ProtoWorldState,
    Guild as ProtoGuild,
    Character as ProtoCharacter,
    Item as ProtoItem,
    Vec3 as ProtoVec3,
)

# FlatBuffers
import flatbuffers
from bench_fb import WorldState as FBWorldState, Guild as FBGuild, Character as FBCharacter, Item as FBItem, Vec3 as FBVec3


def create_benchmark_data():
    world = WorldState()
    world.world_id = 1
    world.seed = "benchmark_seed"
    
    for g in range(1000):
        guild = Guild()
        guild.name = f"Guild_{g}"
        guild.description = "A very powerful guild"
        
        for c in range(20):
            char = Character()
            char.name = f"Char_{g}_{c}"
            char.level = c + 1
            char.hp = 100
            char.mp = 50
            char.is_alive = True
            
            pos = Vec3()
            pos.x = 10
            pos.y = 20
            pos.z = 30
            char.position = pos
            
            char.skills = [1, 2, 3, 4, 5]
            
            for i in range(10):
                item = Item()
                item.id = g * 1000 + c * 100 + i
                item.name = "Item" * 50 + f"_{g}_{c}_{i}"
                item.value = i * 10
                item.weight = 1
                item.rarity = "Rare" if i % 5 == 0 else "Common"
                char.inventory.append(item)
            
            guild.members.append(char)
        
        world.guilds.append(guild)
    
    world.loot_table = []
    return world


def to_dict(world):
    return {
        "world_id": world.world_id,
        "seed": world.seed,
        "guilds": [{
            "name": g.name,
            "description": g.description,
            "members": [{
                "name": c.name,
                "level": c.level,
                "hp": c.hp,
                "mp": c.mp,
                "is_alive": c.is_alive,
                "position": {"x": c.position.x, "y": c.position.y, "z": c.position.z},
                "skills": c.skills,
                "inventory": [{
                    "id": item.id,
                    "name": item.name,
                    "value": item.value,
                    "weight": item.weight,
                    "rarity": item.rarity,
                } for item in c.inventory]
            } for c in g.members]
        } for g in world.guilds],
        "loot_table": [],
    }


def create_proto_data(world):
    pw = ProtoWorldState()
    pw.world_id = world.world_id
    pw.seed = world.seed
    
    for g in world.guilds:
        pg = ProtoGuild()
        pg.name = g.name
        pg.description = g.description
        
        for c in g.members:
            pc = ProtoCharacter()
            pc.name = c.name
            pc.level = c.level
            pc.hp = c.hp
            pc.mp = c.mp
            pc.is_alive = c.is_alive
            
            pv = ProtoVec3()
            pv.x = c.position.x
            pv.y = c.position.y
            pv.z = c.position.z
            pc.position.CopyFrom(pv)
            
            pc.skills.extend(c.skills)
            
            for item in c.inventory:
                pi = ProtoItem()
                pi.id = item.id
                pi.name = item.name
                pi.value = item.value
                pi.weight = item.weight
                pi.rarity = item.rarity
                pc.inventory.append(pi)
            
            pg.members.append(pc)
        
        pw.guilds.append(pg)
    
    return pw


def encode_flatbuffers(builder, world):
    builder.Clear()
    
    guild_offsets = []
    for g in world.guilds:
        member_offsets = []
        for c in g.members:
            inv_offsets = []
            for item in c.inventory:
                name_off = builder.CreateString(item.name)
                rarity_off = builder.CreateString(item.rarity)
                FBItem.ItemStart(builder)
                FBItem.ItemAddId(builder, item.id)
                FBItem.ItemAddName(builder, name_off)
                FBItem.ItemAddValue(builder, item.value)
                FBItem.ItemAddWeight(builder, item.weight)
                FBItem.ItemAddRarity(builder, rarity_off)
                inv_offsets.append(FBItem.ItemEnd(builder))
            
            FBCharacter.CharacterStartInventoryVector(builder, len(inv_offsets))
            for off in reversed(inv_offsets):
                builder.PrependUOffsetTRelative(off)
            inv_vector = builder.EndVector()
            
            FBCharacter.CharacterStartSkillsVector(builder, len(c.skills))
            for s in reversed(c.skills):
                builder.PrependInt32(s)
            skills_vector = builder.EndVector()
            
            name_off = builder.CreateString(c.name)
            
            FBCharacter.CharacterStart(builder)
            FBCharacter.CharacterAddName(builder, name_off)
            FBCharacter.CharacterAddLevel(builder, c.level)
            FBCharacter.CharacterAddHp(builder, c.hp)
            FBCharacter.CharacterAddMp(builder, c.mp)
            FBCharacter.CharacterAddIsAlive(builder, c.is_alive)
            # Position struct — must be created inline during table construction
            FBCharacter.CharacterAddPosition(builder, FBVec3.CreateVec3(builder, c.position.x, c.position.y, c.position.z))
            FBCharacter.CharacterAddSkills(builder, skills_vector)
            FBCharacter.CharacterAddInventory(builder, inv_vector)
            member_offsets.append(FBCharacter.CharacterEnd(builder))
        
        FBGuild.GuildStartMembersVector(builder, len(member_offsets))
        for off in reversed(member_offsets):
            builder.PrependUOffsetTRelative(off)
        members_vector = builder.EndVector()
        
        name_off = builder.CreateString(g.name)
        desc_off = builder.CreateString(g.description)
        
        FBGuild.GuildStart(builder)
        FBGuild.GuildAddName(builder, name_off)
        FBGuild.GuildAddDescription(builder, desc_off)
        FBGuild.GuildAddMembers(builder, members_vector)
        guild_offsets.append(FBGuild.GuildEnd(builder))
    
    FBWorldState.WorldStateStartGuildsVector(builder, len(guild_offsets))
    for off in reversed(guild_offsets):
        builder.PrependUOffsetTRelative(off)
    guilds_vector = builder.EndVector()
    
    seed_off = builder.CreateString(world.seed)
    
    FBWorldState.WorldStateStart(builder)
    FBWorldState.WorldStateAddWorldId(builder, world.world_id)
    FBWorldState.WorldStateAddSeed(builder, seed_off)
    FBWorldState.WorldStateAddGuilds(builder, guilds_vector)
    root = FBWorldState.WorldStateEnd(builder)
    builder.Finish(root)
    return bytes(builder.Output())


def main():
    print("Generating Data...")
    world = create_benchmark_data()
    plain = to_dict(world)
    proto_world = create_proto_data(world)
    print("Data Generation Complete. Starting Benchmark...")
    
    ITERATIONS = 10

    # Prep sizes
    json_data = json.dumps(plain)
    msgpack_data = msgpack.packb(plain)
    bp_data = world.encode()
    proto_data = proto_world.SerializeToString()
    fb_builder = flatbuffers.Builder(65536)
    fb_data = encode_flatbuffers(fb_builder, world)
    
    print(f"\nPayload Sizes:")
    print(f"JSON: {len(json_data)}")
    print(f"MsgPack: {len(msgpack_data)}")
    print(f"BitPacker: {len(bp_data)}")
    print(f"Protobuf: {len(proto_data)}")
    print(f"FlatBuffers: {len(fb_data)}")

    # --- JSON ---
    start = time.time()
    for _ in range(ITERATIONS):
        s = json.dumps(plain)
        d = json.loads(s)
    elapsed = time.time() - start
    print(f"\nJSON:        {elapsed:.3f}s (Size: {len(json_data)} bytes)")

    # --- MsgPack ---
    start = time.time()
    for _ in range(ITERATIONS):
        b = msgpack.packb(plain)
        d = msgpack.unpackb(b)
    elapsed = time.time() - start
    print(f"MsgPack:     {elapsed:.3f}s (Size: {len(msgpack_data)} bytes)")

    # --- BitPacker ---
    start = time.time()
    for _ in range(ITERATIONS):
        b = world.encode()
        d = WorldState.decode(b)
    elapsed = time.time() - start
    print(f"BitPacker:   {elapsed:.3f}s (Size: {len(bp_data)} bytes)")

    # --- Protobuf ---
    start = time.time()
    for _ in range(ITERATIONS):
        b = proto_world.SerializeToString()
        d = ProtoWorldState()
        d.ParseFromString(b)
    elapsed = time.time() - start
    print(f"Protobuf:    {elapsed:.3f}s (Size: {len(proto_data)} bytes)")

    # --- FlatBuffers ---
    start = time.time()
    for _ in range(ITERATIONS):
        encode_flatbuffers(fb_builder, world)
    elapsed = time.time() - start
    print(f"FlatBuffers Encode: {elapsed:.3f}s (Size: {len(fb_data)} bytes)")

    start = time.time()
    for _ in range(ITERATIONS):
        w = FBWorldState.WorldState.GetRootAs(fb_data, 0)
        _ = w.WorldId()
        _ = w.GuildsLength()
    elapsed = time.time() - start
    print(f"FlatBuffers Decode: {elapsed:.3f}s")


if __name__ == "__main__":
    main()
