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
    
    public class Player {
        public const string VERSION = "1.0.2";
        public string username { get; set; }
        public int level { get; set; }
        public int score { get; set; }
        public string[] inventory { get; set; }
        

        public byte[] Encode() {
            var buf = new ZeroCopyByteBuff();
            buf.PutString(VERSION);
            EncodeTo(buf);
            return buf.ToArray();
        }

        public void EncodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.PutString(this.username);
            
            
            
            buf.PutInt32(this.level);
            
            
            
            buf.PutInt32(this.score);
            
            
            
            buf.PutInt32(this.inventory.Length);
            foreach (var item in this.inventory) {
                buf.PutString(item);
            }
            
            
        }

        public static Player Decode(byte[] data) {
            var buf = new ZeroCopyByteBuff(data);
            string ver = buf.GetString();
            if (ver != VERSION) throw new Exception($"Version Mismatch: Expected {VERSION}, got {ver}");
            return DecodeFrom(buf);
        }

        public static Player DecodeFrom(ZeroCopyByteBuff buf) {
            var obj = new Player();
            
            
            obj.username = buf.GetString();
            
            
            
            obj.level = buf.GetInt32();
            
            
            
            obj.score = buf.GetInt32();
            
            
            
            int len_inventory = buf.GetInt32();
            obj.inventory = new string[len_inventory];
            for (int i=0; i<len_inventory; i++) {
                obj.inventory[i] = buf.GetString();
            }
            
            
            return obj;
        }
    }
    
    public class GameState {
        public const string VERSION = "1.0.2";
        public int id { get; set; }
        public bool isActive { get; set; }
        public Player[] players { get; set; }
        

        public byte[] Encode() {
            var buf = new ZeroCopyByteBuff();
            buf.PutString(VERSION);
            EncodeTo(buf);
            return buf.ToArray();
        }

        public void EncodeTo(ZeroCopyByteBuff buf) {
            
            
            buf.PutInt32(this.id);
            
            
            
            buf.PutBool(this.isActive);
            
            
            
            buf.PutInt32(this.players.Length);
            foreach (var item in this.players) {
                item.EncodeTo(buf);
            }
            
            
        }

        public static GameState Decode(byte[] data) {
            var buf = new ZeroCopyByteBuff(data);
            string ver = buf.GetString();
            if (ver != VERSION) throw new Exception($"Version Mismatch: Expected {VERSION}, got {ver}");
            return DecodeFrom(buf);
        }

        public static GameState DecodeFrom(ZeroCopyByteBuff buf) {
            var obj = new GameState();
            
            
            obj.id = buf.GetInt32();
            
            
            
            obj.isActive = buf.GetBool();
            
            
            
            int len_players = buf.GetInt32();
            obj.players = new Player[len_players];
            for (int i=0; i<len_players; i++) {
                obj.players[i] = Player.DecodeFrom(buf);
            }
            
            
            return obj;
        }
    }
    
}
