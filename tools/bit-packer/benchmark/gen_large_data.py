import sys
import os
import random
import time

# Ensure we can import the generated code
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "generated/python_bench/python")))

from bench_complex import WorldState, Guild, Character, Item, Vec3

def random_string(length=10):
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(random.choice(chars) for _ in range(length))

def create_item(i):
    item = Item()
    item.id = i
    item.name = f"Item_{i}_{random_string(5)}"
    item.value = random.randint(1, 1000)
    item.weight = random.randint(1, 100)
    item.rarity = random.choice(["Common", "Uncommon", "Rare", "Epic", "Legendary"])
    return item

def create_character(i):
    char = Character()
    char.name = f"Hero_{i}_{random_string(5)}"
    char.level = random.randint(1, 100)
    char.hp = random.randint(100, 10000)
    char.mp = random.randint(0, 5000)
    char.is_alive = True
    char.position = Vec3()
    char.position.x = random.randint(-1000, 1000)
    char.position.y = random.randint(-1000, 1000)
    char.position.z = random.randint(-1000, 1000)
    char.skills = [random.randint(1, 100) for _ in range(5)]
    char.inventory = [create_item(j) for j in range(5)]
    return char

def create_guild(i):
    guild = Guild()
    guild.name = f"Guild_{i}_{random_string(10)}"
    guild.description = f"Description for Guild {i} " + random_string(20)
    guild.members = [create_character(j) for j in range(20)] # 20 members per guild
    return guild

def main():
    target_size = 100 * 1024 * 1024 # 100 MB
    current_size = 0
    
    world = WorldState()
    world.world_id = 12345
    world.seed = "bench_seed_100mb"
    world.guilds = []
    world.loot_table = []

    print(f"Generating data aiming for ~{target_size/1024/1024:.2f} MB...")

    # Estimate size of one guild
    temp_guild = create_guild(0)
    encoded_guild = temp_guild.encode()
    avg_guild_size = len(encoded_guild)
    
    # Calculate how many guilds needed
    # We ignore the overhead of strict array length prefix vs repeated fields for estimation
    # but WorldState contains array of guilds, so it should be fine.
    
    # Let's generate in chunks to avoid huge memory usage during construction if possible,
    # but BitPacker requires full object in memory to encode (unless stream encoding is supported which is not)
    # So we build the full object.
    
    num_guilds = int(target_size / avg_guild_size)
    print(f"Estimated guild size: {avg_guild_size} bytes")
    print(f"Generating ~{num_guilds} guilds...")

    for i in range(num_guilds):
        world.guilds.append(create_guild(i))
        if i % 100 == 0:
            sys.stdout.write(f"\rGenerated {i}/{num_guilds} guilds")
            sys.stdout.flush()
    print()

    print("Encoding...")
    start_time = time.time()
    data = world.encode()
    end_time = time.time()
    
    size = len(data)
    print(f"Encoded size: {size} bytes ({size/1024/1024:.2f} MB)")
    print(f"Encoding time: {end_time - start_time:.4f}s")

    with open("large_payload.bin", "wb") as f:
        f.write(data)
    print("Saved to large_payload.bin")

if __name__ == "__main__":
    main()
