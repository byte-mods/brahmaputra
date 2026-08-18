import struct
import zlib
import sys

VERSION = "1.0.0"

# --- ZeroCopyByteBuff ---
# --- ZeroCopyByteBuff ---
try:
    from _bitpacker import ZeroCopyByteBuff
    print("Using C-Extension for ZeroCopyByteBuff", file=sys.stderr)
except ImportError as e:
    print(f"Failed to import C-Extension: {e}. using Python fallback", file=sys.stderr)
    # Fallback to Python implementation if needed, but for this task we want C.
    # We will keep the class definition here but renamed to avoid conflict if we wanted fallback logic.
    # But to keep file clean and ensure we use C, I will comment it out or just rely on import.
    
    # ... (Rest of Python ZeroCopyByteBuff implementation would go here if we kept it)
    raise e # Crash if C extension not found, as that is the goal.



class Vec3:
    __slots__ = ('x', 'y', 'z', )

    def __init__(self):
        self.x = 0
        self.y = 0
        self.z = 0
        

    def encode(self):
        buf = ZeroCopyByteBuff()
        buf.put_string(VERSION)
        self.encode_to(buf)
        return buf.get_bytes()

    def encode_to(self, buf):
        
        
        buf.put_int32(self.x)
        
        
        
        buf.put_int32(self.y)
        
        
        
        buf.put_int32(self.z)
        
        
        
    @staticmethod
    def decode(data):
        buf = ZeroCopyByteBuff(data)
        version = buf.get_string()
        if version != VERSION:
            raise Exception(f"Version Mismatch: Expected {VERSION}, got {version}")
        return Vec3.decode_from(buf)
    
    @staticmethod
    def decode_from(buf):
        obj = Vec3.__new__(Vec3)
        
        
        obj.x = buf.get_int32()
        
        
        
        obj.y = buf.get_int32()
        
        
        
        obj.z = buf.get_int32()
        
        
        return obj

class Item:
    __slots__ = ('id', 'name', 'value', 'weight', 'rarity', )

    def __init__(self):
        self.id = 0
        self.name = ''
        self.value = 0
        self.weight = 0
        self.rarity = ''
        

    def encode(self):
        buf = ZeroCopyByteBuff()
        buf.put_string(VERSION)
        self.encode_to(buf)
        return buf.get_bytes()

    def encode_to(self, buf):
        
        
        buf.put_int32(self.id)
        
        
        
        buf.put_string(self.name)
        
        
        
        buf.put_int32(self.value)
        
        
        
        buf.put_int32(self.weight)
        
        
        
        buf.put_string(self.rarity)
        
        
        
    @staticmethod
    def decode(data):
        buf = ZeroCopyByteBuff(data)
        version = buf.get_string()
        if version != VERSION:
            raise Exception(f"Version Mismatch: Expected {VERSION}, got {version}")
        return Item.decode_from(buf)
    
    @staticmethod
    def decode_from(buf):
        obj = Item.__new__(Item)
        
        
        obj.id = buf.get_int32()
        
        
        
        obj.name = buf.get_string()
        
        
        
        obj.value = buf.get_int32()
        
        
        
        obj.weight = buf.get_int32()
        
        
        
        obj.rarity = buf.get_string()
        
        
        return obj

class Character:
    __slots__ = ('name', 'level', 'hp', 'mp', 'is_alive', 'position', 'skills', 'inventory', )

    def __init__(self):
        self.name = ''
        self.level = 0
        self.hp = 0
        self.mp = 0
        self.is_alive = False
        self.position = 0
        self.skills = []
        self.inventory = []
        

    def encode(self):
        buf = ZeroCopyByteBuff()
        buf.put_string(VERSION)
        self.encode_to(buf)
        return buf.get_bytes()

    def encode_to(self, buf):
        
        
        buf.put_string(self.name)
        
        
        
        buf.put_int32(self.level)
        
        
        
        buf.put_int32(self.hp)
        
        
        
        buf.put_int32(self.mp)
        
        
        
        buf.put_bool(self.is_alive)
        
        
        
        self.position.encode_to(buf)
        
        
        
        buf.put_int32(len(self.skills))
        for item in self.skills:
            buf.put_int32(item)
        
        
        
        buf.put_int32(len(self.inventory))
        for item in self.inventory:
            item.encode_to(buf)
        
        
        
    @staticmethod
    def decode(data):
        buf = ZeroCopyByteBuff(data)
        version = buf.get_string()
        if version != VERSION:
            raise Exception(f"Version Mismatch: Expected {VERSION}, got {version}")
        return Character.decode_from(buf)
    
    @staticmethod
    def decode_from(buf):
        obj = Character.__new__(Character)
        
        
        obj.name = buf.get_string()
        
        
        
        obj.level = buf.get_int32()
        
        
        
        obj.hp = buf.get_int32()
        
        
        
        obj.mp = buf.get_int32()
        
        
        
        obj.is_alive = buf.get_bool()
        
        
        
        obj.position = Vec3.decode_from(buf)
        
        
        
        length_skills = buf.get_int32()
        obj.skills = [None] * length_skills
        for i in range(length_skills):
            obj.skills[i] = buf.get_int32()
        
        
        
        length_inventory = buf.get_int32()
        obj.inventory = [None] * length_inventory
        for i in range(length_inventory):
            obj.inventory[i] = Item.decode_from(buf)
        
        
        return obj

class Guild:
    __slots__ = ('name', 'description', 'members', )

    def __init__(self):
        self.name = ''
        self.description = ''
        self.members = []
        

    def encode(self):
        buf = ZeroCopyByteBuff()
        buf.put_string(VERSION)
        self.encode_to(buf)
        return buf.get_bytes()

    def encode_to(self, buf):
        
        
        buf.put_string(self.name)
        
        
        
        buf.put_string(self.description)
        
        
        
        buf.put_int32(len(self.members))
        for item in self.members:
            item.encode_to(buf)
        
        
        
    @staticmethod
    def decode(data):
        buf = ZeroCopyByteBuff(data)
        version = buf.get_string()
        if version != VERSION:
            raise Exception(f"Version Mismatch: Expected {VERSION}, got {version}")
        return Guild.decode_from(buf)
    
    @staticmethod
    def decode_from(buf):
        obj = Guild.__new__(Guild)
        
        
        obj.name = buf.get_string()
        
        
        
        obj.description = buf.get_string()
        
        
        
        length_members = buf.get_int32()
        obj.members = [None] * length_members
        for i in range(length_members):
            obj.members[i] = Character.decode_from(buf)
        
        
        return obj

class WorldState:
    __slots__ = ('world_id', 'seed', 'guilds', 'loot_table', )

    def __init__(self):
        self.world_id = 0
        self.seed = ''
        self.guilds = []
        self.loot_table = []
        

    def encode(self):
        buf = ZeroCopyByteBuff()
        buf.put_string(VERSION)
        self.encode_to(buf)
        return buf.get_bytes()

    def encode_to(self, buf):
        
        
        buf.put_int32(self.world_id)
        
        
        
        buf.put_string(self.seed)
        
        
        
        buf.put_int32(len(self.guilds))
        for item in self.guilds:
            item.encode_to(buf)
        
        
        
        buf.put_int32(len(self.loot_table))
        for item in self.loot_table:
            item.encode_to(buf)
        
        
        
    @staticmethod
    def decode(data):
        buf = ZeroCopyByteBuff(data)
        version = buf.get_string()
        if version != VERSION:
            raise Exception(f"Version Mismatch: Expected {VERSION}, got {version}")
        return WorldState.decode_from(buf)
    
    @staticmethod
    def decode_from(buf):
        obj = WorldState.__new__(WorldState)
        
        
        obj.world_id = buf.get_int32()
        
        
        
        obj.seed = buf.get_string()
        
        
        
        length_guilds = buf.get_int32()
        obj.guilds = [None] * length_guilds
        for i in range(length_guilds):
            obj.guilds[i] = Guild.decode_from(buf)
        
        
        
        length_loot_table = buf.get_int32()
        obj.loot_table = [None] * length_loot_table
        for i in range(length_loot_table):
            obj.loot_table[i] = Item.decode_from(buf)
        
        
        return obj

