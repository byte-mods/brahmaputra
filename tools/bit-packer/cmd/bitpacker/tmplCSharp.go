package main

import "fmt"

const tmplCSharp = `using System;
using System.IO;
using System.Text;
using System.Collections.Generic;

namespace {{.Config.PackageName}} {

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
    {{range .Classes}}
    public class {{.Name}} {
        public const string VERSION = "{{$.Config.Version}}";
        {{range .Fields}}public {{mapTypeCS .Type}}{{if .IsArray}}[]{{end}} {{.Name}} { get; set; }
        {{end}}

        public byte[] Encode() {
            var buf = new ZeroCopyByteBuff();
            buf.PutString(VERSION);
            EncodeTo(buf);
            return buf.ToArray();
        }

        public void EncodeTo(ZeroCopyByteBuff buf) {
            {{range .Fields}}
            {{if .IsArray}}
            buf.PutInt32(this.{{.Name}}.Length);
            foreach (var item in this.{{.Name}}) {
                {{encodeFieldCS "item" .Type}}
            }
            {{else}}
            {{encodeFieldCS (printf "this.%s" .Name) .Type}}
            {{end}}
            {{end}}
        }

        public static {{.Name}} Decode(byte[] data) {
            var buf = new ZeroCopyByteBuff(data);
            string ver = buf.GetString();
            if (ver != VERSION) throw new Exception($"Version Mismatch: Expected {VERSION}, got {ver}");
            return DecodeFrom(buf);
        }

        public static {{.Name}} DecodeFrom(ZeroCopyByteBuff buf) {
            var obj = new {{.Name}}();
            {{range .Fields}}
            {{if .IsArray}}
            int len_{{.Name}} = buf.GetInt32();
            obj.{{.Name}} = new {{mapTypeCS .Type}}[len_{{.Name}}];
            for (int i=0; i<len_{{.Name}}; i++) {
                {{decodeFieldCS (printf "obj.%s[i]" .Name) .Type}}
            }
            {{else}}
            {{decodeFieldCS (printf "obj.%s" .Name) .Type}}
            {{end}}
            {{end}}
            return obj;
        }
    }
    {{end}}
}
`

// --- C# Helpers ---

func mapTypeCS(t string) string {
	switch t {
	case "int":
		return "int"
	case "long":
		return "long"
	case "float":
		return "float"
	case "double":
		return "double"
	case "bool":
		return "bool"
	case "string":
		return "string"
	default:
		return t
	}
}

func encodeFieldCS(varName, fieldType string) string {
	switch fieldType {
	case "int":
		return fmt.Sprintf("buf.PutInt32(%s);", varName)
	case "long":
		return fmt.Sprintf("buf.PutInt64(%s);", varName)
	case "float":
		return fmt.Sprintf("buf.PutFloat(%s);", varName)
	case "double":
		return fmt.Sprintf("buf.PutDouble(%s);", varName)
	case "bool":
		return fmt.Sprintf("buf.PutBool(%s);", varName)
	case "string":
		return fmt.Sprintf("buf.PutString(%s);", varName)
	default:
		return fmt.Sprintf("%s.EncodeTo(buf);", varName)
	}
}

func decodeFieldCS(target, fieldType string) string {
	switch fieldType {
	case "int":
		return fmt.Sprintf("%s = buf.GetInt32();", target)
	case "long":
		return fmt.Sprintf("%s = buf.GetInt64();", target)
	case "float":
		return fmt.Sprintf("%s = buf.GetFloat();", target)
	case "double":
		return fmt.Sprintf("%s = buf.GetDouble();", target)
	case "bool":
		return fmt.Sprintf("%s = buf.GetBool();", target)
	case "string":
		return fmt.Sprintf("%s = buf.GetString();", target)
	default:
		return fmt.Sprintf("%s = %s.DecodeFrom(buf);", target, fieldType)
	}
}
