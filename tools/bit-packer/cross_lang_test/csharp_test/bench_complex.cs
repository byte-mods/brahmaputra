using System;
using System.IO;
using System.Text;
using System.Collections.Generic;

namespace Generated {

    // --- ZeroCopyByteBuff Implementation ---
    public class ZeroCopyByteBuff {
        private byte[] _buf;
        private int _offset;

        public ZeroCopyByteBuff(int capacity = 65536) {
            _buf = new byte[capacity];
            _offset = 0;
        }

        public ZeroCopyByteBuff(byte[] data) {
            _buf = data;
            _offset = 0;
        }

        public byte[] ToArray() {
            var res = new byte[_offset];
            Array.Copy(_buf, 0, res, 0, _offset);
            return res;
        }

        private void EnsureCapacity(int needed) {
            if (_offset + needed > _buf.Length) {
                int newSize = Math.Max(_buf.Length * 2, _offset + needed);
                Array.Resize(ref _buf, newSize);
            }
        }

        // ZigZag
        private static uint ZigZagEncode32(int n) { return (uint)((n << 1) ^ (n >> 31)); }
        private static int ZigZagDecode32(uint n) { return (int)(n >> 1) ^ -(int)(n & 1); }
        private static ulong ZigZagEncode64(long n) { return (ulong)((n << 1) ^ (n >> 63)); }
        private static long ZigZagDecode64(ulong n) { return (long)(n >> 1) ^ -(long)(n & 1); }

        public void PutVarInt64(long v) {
            ulong uv = (ulong)v;
            // FAST PATH: 1 byte (0-127, most common for game data)
            if ((uv & ~0x7FUL) == 0) {
                if (_offset >= _buf.Length) EnsureCapacity(1);
                _buf[_offset++] = (byte)uv;
                return;
            }
            // FAST PATH: 2 bytes (128-16383)
            if ((uv & ~0x3FFFUL) == 0) {
                if (_offset + 2 > _buf.Length) EnsureCapacity(2);
                _buf[_offset++] = (byte)((uv & 0x7F) | 0x80);
                _buf[_offset++] = (byte)(uv >> 7);
                return;
            }
            // General path
            EnsureCapacity(10);
            while (uv >= 0x80) {
                _buf[_offset++] = (byte)((uv & 0x7F) | 0x80);
                uv >>= 7;
            }
            _buf[_offset++] = (byte)uv;
        }

        public long GetVarInt64() {
            ulong result = 0;
            int shift = 0;
            // FAST PATH: 1 byte
            byte b = _buf[_offset++];
            if ((b & 0x80) == 0) {
                result = (ulong)(b & 0x7F);
            } else {
                result = (ulong)(b & 0x7F);
                shift = 7;
                while (true) {
                    b = _buf[_offset++];
                    result |= (ulong)(b & 0x7F) << shift;
                    if ((b & 0x80) == 0) break;
                    shift += 7;
                }
            }
            return (long)result;
        }

        public void PutInt32(int v) { PutVarInt64(ZigZagEncode32(v)); }
        public void PutInt64(long v) { PutVarInt64((long)ZigZagEncode64(v)); }
        public void PutFloat(float v) { PutVarInt64((long)ZigZagEncode64((long)(v * 10000.0f))); }
        public void PutDouble(double v) { PutVarInt64((long)ZigZagEncode64((long)(v * 10000.0))); }
        public void PutBool(bool v) { 
            EnsureCapacity(1);
            _buf[_offset++] = v ? (byte)1 : (byte)0; 
        }
        public void PutString(string v) {
            byte[] bytes = Encoding.UTF8.GetBytes(v);
            PutInt32(bytes.Length);
            EnsureCapacity(bytes.Length);
            Array.Copy(bytes, 0, _buf, _offset, bytes.Length);
            _offset += bytes.Length;
        }

        public int GetInt32() { return ZigZagDecode32((uint)GetVarInt64()); }
        public long GetInt64() { return ZigZagDecode64((ulong)GetVarInt64()); }
        public float GetFloat() { return (float)ZigZagDecode64((ulong)GetVarInt64()) / 10000.0f; }
        public double GetDouble() { return (double)ZigZagDecode64((ulong)GetVarInt64()) / 10000.0; }
        public bool GetBool() { 
             if (_offset >= _buf.Length) throw new EndOfStreamException();
             return _buf[_offset++] != 0;
        }
        public string GetString() {
            int len = GetInt32();
            if (_offset + len > _buf.Length) throw new EndOfStreamException();
            string s = Encoding.UTF8.GetString(_buf, _offset, len);
            _offset += len;
            return s;
        }
    }

    // --- Generated Classes ---
    
    public class Vec3 {
        public const string VERSION = "1.0.0";
        public int x { get; set; }
        public int y { get; set; }
        public int z { get; set; }
        

        public byte[] Encode() {
            var buf = new ZeroCopyByteBuff();
            buf.PutString(VERSION);
            EncodeTo(buf);
            return buf.ToArray();
        }

        public void EncodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.PutInt32(this.x);
            
            
            
            buf.PutInt32(this.y);
            
            
            
            buf.PutInt32(this.z);
            
            
        }

        public static Vec3 Decode(byte[] data) {
            var buf = new ZeroCopyByteBuff(data);
            string ver = buf.GetString();
            if (ver != VERSION) throw new Exception($"Version Mismatch: Expected {VERSION}, got {ver}");
            return DecodeFrom(buf);
        }

        public static Vec3 DecodeFrom(ZeroCopyByteBuff buf) {
            var obj = new Vec3();
            
            
            obj.x = buf.GetInt32();
            
            
            
            obj.y = buf.GetInt32();
            
            
            
            obj.z = buf.GetInt32();
            
            
            return obj;
        }
    }
    
    public class Item {
        public const string VERSION = "1.0.0";
        public int id { get; set; }
        public string name { get; set; }
        public int value { get; set; }
        public int weight { get; set; }
        public string rarity { get; set; }
        

        public byte[] Encode() {
            var buf = new ZeroCopyByteBuff();
            buf.PutString(VERSION);
            EncodeTo(buf);
            return buf.ToArray();
        }

        public void EncodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.PutInt32(this.id);
            
            
            
            buf.PutString(this.name);
            
            
            
            buf.PutInt32(this.value);
            
            
            
            buf.PutInt32(this.weight);
            
            
            
            buf.PutString(this.rarity);
            
            
        }

        public static Item Decode(byte[] data) {
            var buf = new ZeroCopyByteBuff(data);
            string ver = buf.GetString();
            if (ver != VERSION) throw new Exception($"Version Mismatch: Expected {VERSION}, got {ver}");
            return DecodeFrom(buf);
        }

        public static Item DecodeFrom(ZeroCopyByteBuff buf) {
            var obj = new Item();
            
            
            obj.id = buf.GetInt32();
            
            
            
            obj.name = buf.GetString();
            
            
            
            obj.value = buf.GetInt32();
            
            
            
            obj.weight = buf.GetInt32();
            
            
            
            obj.rarity = buf.GetString();
            
            
            return obj;
        }
    }
    
    public class Character {
        public const string VERSION = "1.0.0";
        public string name { get; set; }
        public int level { get; set; }
        public int hp { get; set; }
        public int mp { get; set; }
        public bool is_alive { get; set; }
        public Vec3 position { get; set; }
        public int[] skills { get; set; }
        public Item[] inventory { get; set; }
        

        public byte[] Encode() {
            var buf = new ZeroCopyByteBuff();
            buf.PutString(VERSION);
            EncodeTo(buf);
            return buf.ToArray();
        }

        public void EncodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.PutString(this.name);
            
            
            
            buf.PutInt32(this.level);
            
            
            
            buf.PutInt32(this.hp);
            
            
            
            buf.PutInt32(this.mp);
            
            
            
            buf.PutBool(this.is_alive);
            
            
            
            this.position.EncodeTo(buf);
            
            
            
            buf.PutInt32(this.skills.Length);
            foreach (var item in this.skills) {
                buf.PutInt32(item);
            }
            
            
            
            buf.PutInt32(this.inventory.Length);
            foreach (var item in this.inventory) {
                item.EncodeTo(buf);
            }
            
            
        }

        public static Character Decode(byte[] data) {
            var buf = new ZeroCopyByteBuff(data);
            string ver = buf.GetString();
            if (ver != VERSION) throw new Exception($"Version Mismatch: Expected {VERSION}, got {ver}");
            return DecodeFrom(buf);
        }

        public static Character DecodeFrom(ZeroCopyByteBuff buf) {
            var obj = new Character();
            
            
            obj.name = buf.GetString();
            
            
            
            obj.level = buf.GetInt32();
            
            
            
            obj.hp = buf.GetInt32();
            
            
            
            obj.mp = buf.GetInt32();
            
            
            
            obj.is_alive = buf.GetBool();
            
            
            
            obj.position = Vec3.DecodeFrom(buf);
            
            
            
            int len_skills = buf.GetInt32();
            obj.skills = new int[len_skills];
            for (int i=0; i<len_skills; i++) {
                obj.skills[i] = buf.GetInt32();
            }
            
            
            
            int len_inventory = buf.GetInt32();
            obj.inventory = new Item[len_inventory];
            for (int i=0; i<len_inventory; i++) {
                obj.inventory[i] = Item.DecodeFrom(buf);
            }
            
            
            return obj;
        }
    }
    
    public class Guild {
        public const string VERSION = "1.0.0";
        public string name { get; set; }
        public string description { get; set; }
        public Character[] members { get; set; }
        

        public byte[] Encode() {
            var buf = new ZeroCopyByteBuff();
            buf.PutString(VERSION);
            EncodeTo(buf);
            return buf.ToArray();
        }

        public void EncodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.PutString(this.name);
            
            
            
            buf.PutString(this.description);
            
            
            
            buf.PutInt32(this.members.Length);
            foreach (var item in this.members) {
                item.EncodeTo(buf);
            }
            
            
        }

        public static Guild Decode(byte[] data) {
            var buf = new ZeroCopyByteBuff(data);
            string ver = buf.GetString();
            if (ver != VERSION) throw new Exception($"Version Mismatch: Expected {VERSION}, got {ver}");
            return DecodeFrom(buf);
        }

        public static Guild DecodeFrom(ZeroCopyByteBuff buf) {
            var obj = new Guild();
            
            
            obj.name = buf.GetString();
            
            
            
            obj.description = buf.GetString();
            
            
            
            int len_members = buf.GetInt32();
            obj.members = new Character[len_members];
            for (int i=0; i<len_members; i++) {
                obj.members[i] = Character.DecodeFrom(buf);
            }
            
            
            return obj;
        }
    }
    
    public class WorldState {
        public const string VERSION = "1.0.0";
        public int world_id { get; set; }
        public string seed { get; set; }
        public Guild[] guilds { get; set; }
        public Item[] loot_table { get; set; }
        

        public byte[] Encode() {
            var buf = new ZeroCopyByteBuff();
            buf.PutString(VERSION);
            EncodeTo(buf);
            return buf.ToArray();
        }

        public void EncodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.PutInt32(this.world_id);
            
            
            
            buf.PutString(this.seed);
            
            
            
            buf.PutInt32(this.guilds.Length);
            foreach (var item in this.guilds) {
                item.EncodeTo(buf);
            }
            
            
            
            buf.PutInt32(this.loot_table.Length);
            foreach (var item in this.loot_table) {
                item.EncodeTo(buf);
            }
            
            
        }

        public static WorldState Decode(byte[] data) {
            var buf = new ZeroCopyByteBuff(data);
            string ver = buf.GetString();
            if (ver != VERSION) throw new Exception($"Version Mismatch: Expected {VERSION}, got {ver}");
            return DecodeFrom(buf);
        }

        public static WorldState DecodeFrom(ZeroCopyByteBuff buf) {
            var obj = new WorldState();
            
            
            obj.world_id = buf.GetInt32();
            
            
            
            obj.seed = buf.GetString();
            
            
            
            int len_guilds = buf.GetInt32();
            obj.guilds = new Guild[len_guilds];
            for (int i=0; i<len_guilds; i++) {
                obj.guilds[i] = Guild.DecodeFrom(buf);
            }
            
            
            
            int len_loot_table = buf.GetInt32();
            obj.loot_table = new Item[len_loot_table];
            for (int i=0; i<len_loot_table; i++) {
                obj.loot_table[i] = Item.DecodeFrom(buf);
            }
            
            
            return obj;
        }
    }
    
}
