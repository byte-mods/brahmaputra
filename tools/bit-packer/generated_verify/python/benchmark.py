import time
import json
import msgpack
import sys
from vec3 import WorldState, Guild, Character, Item, Vec3

def create_benchmark_data():
    world = WorldState()
    world.world_id = 1
    world.seed = "benchmark_seed"
    world.guilds = []
    
    # Matching the same data structure as Go/JS/Java benchmarks
    # 1000 Guilds, each with 20 members, each with 10 items
    for g in range(1000):
        guild = Guild()
        guild.name = f"Guild_{g}"
        guild.description = "A very powerful guild"
        guild.members = []
        
        for c in range(20):
            char_obj = Character()
            char_obj.name = f"Char_{g}_{c}"
            char_obj.level = c + 1
            char_obj.hp = 100
            char_obj.mp = 50
            char_obj.is_alive = True
            
            pos = Vec3()
            pos.x = 10
            pos.y = 20
            pos.z = 30
            char_obj.position = pos
            
            char_obj.skills = [1, 2, 3, 4, 5]
            char_obj.inventory = []
            
            for i in range(10):
                item = Item()
                item.id = (g * 1000 + c * 100 + i)
                item.name = "Item" * 50 + f"_{g}_{c}_{i}"
                item.value = i * 10
                item.weight = 1
                item.rarity = "Rare" if i % 5 == 0 else "Common"
                char_obj.inventory.append(item)
                
            guild.members.append(char_obj)
        world.guilds.append(guild)
    
    # Empty loot table for simplicity as per other benchmarks
    world.loot_table = []
    
    return world

def to_dict(obj):
    # Quick helper to convert our objects to dicts for JSON/MsgPack
    if hasattr(obj, '__slots__'):
        d = {}
        for k in obj.__slots__:
            v = getattr(obj, k)
            if isinstance(v, list):
                d[k] = [to_dict(x) for x in v]
            elif hasattr(v, '__slots__'):
                d[k] = to_dict(v)
            else:
                d[k] = v
        return d
    return obj

def main():
    print("Generating Data...", file=sys.stderr)
    world = create_benchmark_data()
    print("Data Generation Complete. Starting Benchmark...", file=sys.stderr)

    ITERATIONS = 10

    # --- JSON ---
    world_dict = to_dict(world)
    start = time.time()
    size = 0
    for i in range(ITERATIONS):
        b = json.dumps(world_dict).encode('utf-8')
        if i == 0: size = len(b)
        # simplistic decode
        _ = json.loads(b)
    duration = time.time() - start
    print(f"JSON:      {duration:.3f}s (Size: {size} bytes)")

    # --- MsgPack ---
    start = time.time()
    size = 0
    for i in range(ITERATIONS):
        b = msgpack.packb(world_dict)
        if i == 0: size = len(b)
        _ = msgpack.unpackb(b)
    duration = time.time() - start
    print(f"MsgPack:   {duration:.3f}s (Size: {size} bytes)")

    # --- BitPacker ---
    start = time.time()
    size = 0
    for i in range(ITERATIONS):
        b = world.encode()
        if i == 0: size = len(b)
        _ = WorldState.decode(b)
    duration = time.time() - start
    print(f"BitPacker: {duration:.3f}s (Size: {size} bytes)")

if __name__ == "__main__":
    main()
