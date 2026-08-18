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

VERSION = "1.0.2"


class Player:
    __slots__ = ('username', 'level', 'score', 'inventory', )

    def __init__(self):
        self.username = ''
        self.level = 0
        self.score = 0
        self.inventory = []
        

    def encode(self):
        buf = ZeroCopyByteBuff()
        buf.put_string(VERSION)
        self.encode_to(buf)
        return buf.get_bytes()

    def encode_to(self, buf):
        
        
        buf.put_string(self.username)
        
        
        
        buf.put_int32(self.level)
        
        
        
        buf.put_int32(self.score)
        
        
        
        buf.put_int32(len(self.inventory))
        for item in self.inventory:
            buf.put_string(item)
        
        
        
    @staticmethod
    def decode(data):
        buf = ZeroCopyByteBuff(data)
        version = buf.get_string()
        if version != VERSION:
            raise Exception(f"Version Mismatch: Expected {VERSION}, got {version}")
        return Player.decode_from(buf)
    
    @staticmethod
    def decode_from(buf):
        obj = Player.__new__(Player) # Optimization: Skip __init__
        
        
        obj.username = buf.get_string()
        
        
        
        obj.level = buf.get_int32()
        
        
        
        obj.score = buf.get_int32()
        
        
        
        length_inventory = buf.get_int32()
        obj.inventory = [None] * length_inventory
        for i in range(length_inventory):
            obj.inventory[i] = buf.get_string()
        
        
        return obj

class GameState:
    __slots__ = ('id', 'isActive', 'players', )

    def __init__(self):
        self.id = 0
        self.isActive = False
        self.players = []
        

    def encode(self):
        buf = ZeroCopyByteBuff()
        buf.put_string(VERSION)
        self.encode_to(buf)
        return buf.get_bytes()

    def encode_to(self, buf):
        
        
        buf.put_int32(self.id)
        
        
        
        buf.put_bool(self.isActive)
        
        
        
        buf.put_int32(len(self.players))
        for item in self.players:
            item.encode_to(buf)
        
        
        
    @staticmethod
    def decode(data):
        buf = ZeroCopyByteBuff(data)
        version = buf.get_string()
        if version != VERSION:
            raise Exception(f"Version Mismatch: Expected {VERSION}, got {version}")
        return GameState.decode_from(buf)
    
    @staticmethod
    def decode_from(buf):
        obj = GameState.__new__(GameState) # Optimization: Skip __init__
        
        
        obj.id = buf.get_int32()
        
        
        
        obj.isActive = buf.get_bool()
        
        
        
        length_players = buf.get_int32()
        obj.players = [None] * length_players
        for i in range(length_players):
            obj.players[i] = Player.decode_from(buf)
        
        
        return obj

