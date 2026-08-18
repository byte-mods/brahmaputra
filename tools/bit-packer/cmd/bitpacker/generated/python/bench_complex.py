import struct
import zlib
import sys

# Try to import C extension for best performance
_USING_C_EXT = False
try:
    from _bitpacker import ZeroCopyByteBuff
    _USING_C_EXT = True
except ImportError:
    # Pure-Python fallback — works out of the box, but C extension is ~10x faster
    # To build C extension: python3 setup.py build_ext --inplace
    class ZeroCopyByteBuff:
        def __init__(self, data=None):
            if data is not None:
                self._buf = bytearray(data)
                self._offset = 0
            else:
                self._buf = bytearray(65536)
                self._offset = 0
                self._write_pos = 0

        def _ensure(self, n):
            while self._write_pos + n > len(self._buf):
                self._buf.extend(bytearray(len(self._buf)))

        def _put_varint(self, v):
            zz = (v << 1) ^ (v >> 63)
            zz &= 0xFFFFFFFFFFFFFFFF
            if zz < 0x80:
                self._ensure(1)
                self._buf[self._write_pos] = zz
                self._write_pos += 1
                return
            if zz < 0x4000:
                self._ensure(2)
                self._buf[self._write_pos] = (zz & 0x7F) | 0x80
                self._buf[self._write_pos + 1] = zz >> 7
                self._write_pos += 2
                return
            self._ensure(10)
            while zz > 0x7F:
                self._buf[self._write_pos] = (zz & 0x7F) | 0x80
                self._write_pos += 1
                zz >>= 7
            self._buf[self._write_pos] = zz
            self._write_pos += 1

        def _get_varint(self):
            result = 0
            shift = 0
            while True:
                b = self._buf[self._offset]
                self._offset += 1
                result |= (b & 0x7F) << shift
                if not (b & 0x80):
                    break
                shift += 7
            return (result >> 1) ^ -(result & 1)

        def put_int32(self, v): self._put_varint(v)
        def put_int64(self, v): self._put_varint(v)
        def put_varint64(self, v): self._put_varint(v)
        def put_float(self, v): self._put_varint(int(v * 10000.0))
        def put_double(self, v): self._put_varint(int(v * 10000.0))
        def put_bool(self, v):
            self._ensure(1)
            self._buf[self._write_pos] = 1 if v else 0
            self._write_pos += 1
        def put_string(self, v):
            b = v.encode('utf-8')
            self._put_varint(len(b))
            self._ensure(len(b))
            self._buf[self._write_pos:self._write_pos + len(b)] = b
            self._write_pos += len(b)
        def ensure_capacity(self, n): self._ensure(n)

        def get_int32(self): return self._get_varint()
        def get_int64(self): return self._get_varint()
        def get_varint64(self): return self._get_varint()
        def get_float(self): return self._get_varint() / 10000.0
        def get_double(self): return self._get_varint() / 10000.0
        def get_bool(self):
            v = self._buf[self._offset] != 0
            self._offset += 1
            return v
        def get_string(self):
            length = self._get_varint()
            s = self._buf[self._offset:self._offset + length].decode('utf-8')
            self._offset += length
            return s
        def get_bytes(self):
            return bytes(self._buf[:self._write_pos])

VERSION = "1.0.0"


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
        obj = Vec3.__new__(Vec3) # Optimization: Skip __init__
        
        
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
        obj = Item.__new__(Item) # Optimization: Skip __init__
        
        
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
        obj = Character.__new__(Character) # Optimization: Skip __init__
        
        
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
        obj = Guild.__new__(Guild) # Optimization: Skip __init__
        
        
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
        obj = WorldState.__new__(WorldState) # Optimization: Skip __init__
        
        
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

