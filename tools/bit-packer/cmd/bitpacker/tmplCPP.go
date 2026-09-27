package main

import "fmt"

const tmplCPPHeader = `
#ifndef {{.Config.InputFileName | Title}}_HPP
#define {{.Config.InputFileName | Title}}_HPP

#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#define VERSION "{{.Config.Version}}"

// --- ZeroCopyByteBuff Class ---
class ZeroCopyByteBuff {
private:
    std::vector<uint8_t> buffer;
    size_t offset;

public:
    ZeroCopyByteBuff();
    ZeroCopyByteBuff(const std::vector<uint8_t>& data);
    
    const std::vector<uint8_t>& getBuffer() const;
    
    // Write
    void putInt32(int32_t v);
    void putInt64(int64_t v);
    void putFloat(float v);
    void putDouble(double v);
    void putBool(bool v);
    void putString(const std::string& v);
    void putVarInt64(int64_t v);

    // Read
    int32_t getInt32();
    int64_t getInt64();
    float getFloat();
    double getDouble();
    bool getBool();
    std::string getString();
    int64_t getVarInt64();
    // Array element count, validated against the bytes left.
    int32_t getLength();
    size_t remaining() const;
};

// --- Generated Classes ---
{{range .Classes}}
struct {{.Name}} {
    {{range .Fields}}{{if .IsArray}}std::vector<{{mapTypeCPP .Type}}>{{else}}{{mapTypeCPP .Type}}{{end}} {{.Name}};
    {{end}}

    void encode(ZeroCopyByteBuff& buf) const;
    void decode(ZeroCopyByteBuff& buf);
    
    // Helper to encode directly to bytes
    std::vector<uint8_t> encode() const;
    
    // Helper to decode directly from bytes
    static {{.Name}} decode(const std::vector<uint8_t>& data);
};
{{end}}

#endif // {{.Config.InputFileName | Title}}_HPP
`

const tmplCPPImpl = `
#include "{{.Config.InputFileName}}.hpp"
#include <cmath>

// --- ZeroCopyByteBuff Implementation ---

ZeroCopyByteBuff::ZeroCopyByteBuff() : offset(0) {
    buffer.reserve(65536);
}

ZeroCopyByteBuff::ZeroCopyByteBuff(const std::vector<uint8_t>& data) : buffer(data), offset(0) {}

const std::vector<uint8_t>& ZeroCopyByteBuff::getBuffer() const {
    return buffer;
}

// ZigZag Helpers (shifts done on unsigned values: no signed-overflow UB)
static uint32_t zigzag_encode32(int32_t n) { return ((uint32_t)n << 1) ^ (uint32_t)(n >> 31); }
static int32_t zigzag_decode32(uint32_t n) { return (int32_t)((n >> 1) ^ (~(n & 1) + 1)); }
static uint64_t zigzag_encode64(int64_t n) { return ((uint64_t)n << 1) ^ (uint64_t)(n >> 63); }
static int64_t zigzag_decode64(uint64_t n) { return (int64_t)((n >> 1) ^ (~(n & 1) + 1)); }

inline void ZeroCopyByteBuff::putVarInt64(int64_t v) {
    uint64_t uv = (uint64_t)v;
    // FAST PATH: 1 byte
    if (uv < 0x80) {
        buffer.push_back((uint8_t)uv);
        return;
    }
    // FAST PATH: 2 bytes
    if (uv < 0x4000) {
        buffer.push_back((uint8_t)((uv & 0x7F) | 0x80));
        buffer.push_back((uint8_t)(uv >> 7));
        return;
    }
    // General path
    while (uv >= 0x80) {
        buffer.push_back((uint8_t)((uv & 0x7F) | 0x80));
        uv >>= 7;
    }
    buffer.push_back((uint8_t)uv);
}

// Reads one unsigned LEB128 varint (at most 10 bytes) as raw bits.
inline int64_t ZeroCopyByteBuff::getVarInt64() {
    uint64_t result = 0;
    for (int i = 0; i < 10; i++) {
        if (offset >= buffer.size()) throw std::runtime_error("Buffer underflow");
        uint8_t byte = buffer[offset++];
        if (i == 9 && byte > 1) throw std::runtime_error("Varint overflows 64 bits");
        result |= ((uint64_t)(byte & 0x7F)) << (7 * i);
        if (!(byte & 0x80)) return (int64_t)result;
    }
    throw std::runtime_error("Varint too long");
}

size_t ZeroCopyByteBuff::remaining() const { return buffer.size() - offset; }

inline void ZeroCopyByteBuff::putInt32(int32_t v) { putVarInt64(zigzag_encode32(v)); }
inline void ZeroCopyByteBuff::putInt64(int64_t v) { putVarInt64(zigzag_encode64(v)); }
// float and double are fixed point: trunc(v * 10000) as an int64, with the
// multiplication done in the field's own precision.
// NaN, infinities and values whose scaled form does not fit an int64 are
// rejected (std::range_error) rather than converted (which is UB).
static int64_t checked_scaled(double s) {
    if (!(s >= -9223372036854775808.0 && s < 9223372036854775808.0))
        throw std::range_error("float is NaN, infinite or out of range");
    return (int64_t)s;
}
inline void ZeroCopyByteBuff::putFloat(float v) { volatile float s = v * 10000.0f; putVarInt64(zigzag_encode64(checked_scaled(s))); }
inline void ZeroCopyByteBuff::putDouble(double v) { volatile double s = v * 10000.0; putVarInt64(zigzag_encode64(checked_scaled(s))); }
inline void ZeroCopyByteBuff::putBool(bool v) { buffer.push_back(v ? 1 : 0); }
inline void ZeroCopyByteBuff::putString(const std::string& v) {
    putVarInt64(zigzag_encode64(v.length()));
    buffer.insert(buffer.end(), v.begin(), v.end());
}

inline int32_t ZeroCopyByteBuff::getInt32() { return zigzag_decode32((uint32_t)getVarInt64()); }
inline int64_t ZeroCopyByteBuff::getInt64() { return zigzag_decode64((uint64_t)getVarInt64()); }
inline float ZeroCopyByteBuff::getFloat() { return (float)zigzag_decode64((uint64_t)getVarInt64()) / 10000.0f; }
inline double ZeroCopyByteBuff::getDouble() { return (double)zigzag_decode64((uint64_t)getVarInt64()) / 10000.0; }

inline bool ZeroCopyByteBuff::getBool() {
    if (offset >= buffer.size()) throw std::runtime_error("Buffer underflow");
    return buffer[offset++] != 0;
}

int32_t ZeroCopyByteBuff::getLength() {
    int32_t n = getInt32();
    if (n < 0 || (size_t)n > remaining()) throw std::runtime_error("Invalid array length");
    return n;
}

// Strict UTF-8 check: no overlongs, surrogates or code points > U+10FFFF.
static bool valid_utf8(const uint8_t* p, size_t n) {
    size_t i = 0;
    while (i < n) {
        uint8_t c = p[i];
        size_t len;
        uint32_t cp;
        if (c < 0x80) { i++; continue; }
        else if ((c & 0xE0) == 0xC0) { len = 2; cp = c & 0x1F; }
        else if ((c & 0xF0) == 0xE0) { len = 3; cp = c & 0x0F; }
        else if ((c & 0xF8) == 0xF0) { len = 4; cp = c & 0x07; }
        else return false;
        if (i + len > n) return false;
        for (size_t k = 1; k < len; k++) {
            if ((p[i + k] & 0xC0) != 0x80) return false;
            cp = (cp << 6) | (p[i + k] & 0x3F);
        }
        if ((len == 2 && cp < 0x80) || (len == 3 && cp < 0x800) || (len == 4 && cp < 0x10000)) return false;
        if (cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) return false;
        i += len;
    }
    return true;
}

inline std::string ZeroCopyByteBuff::getString() {
    int64_t l = zigzag_decode64((uint64_t)getVarInt64());
    if (l < 0 || (uint64_t)l > (uint64_t)remaining()) throw std::runtime_error("Invalid string length");
    size_t len = (size_t)l;
    if (!valid_utf8(buffer.data() + offset, len)) throw std::runtime_error("Invalid UTF-8 string");
    std::string s(buffer.begin() + offset, buffer.begin() + offset + len);
    offset += len;
    return s;
}

// --- Generated Implementation ---
{{range .Classes}}

// {{.Name}} Implementation
void {{.Name}}::encode(ZeroCopyByteBuff& buf) const {
    {{range .Fields}}
    {{if .IsArray}}
    buf.putInt32((int32_t){{.Name}}.size());
    for(const auto& item : {{.Name}}) {
        {{encodeFieldCPP "item" .Type}}
    }
    {{else}}
    {{encodeFieldCPP (printf "this->%s" .Name) .Type}}
    {{end}}
    {{end}}
}

void {{.Name}}::decode(ZeroCopyByteBuff& buf) {
    {{range .Fields}}
    {{if .IsArray}}
    int32_t len_{{.Name}} = buf.getLength();
    {{.Name}}.resize(len_{{.Name}});
    for(int i=0; i<len_{{.Name}}; i++) {
        {{decodeFieldCPP (printf "%s[i]" .Name) .Type}}
    }
    {{else}}
    {{decodeFieldCPP (printf "this->%s" .Name) .Type}}
    {{end}}
    {{end}}
}

std::vector<uint8_t> {{.Name}}::encode() const {
    ZeroCopyByteBuff buf;
    buf.putString(VERSION);
    encode(buf);
    return buf.getBuffer();
}

{{.Name}} {{.Name}}::decode(const std::vector<uint8_t>& data) {
    ZeroCopyByteBuff buf(data);
    std::string ver = buf.getString();
    if (ver != VERSION) {
        throw std::runtime_error("Version mismatch");
    }
    {{.Name}} obj;
    obj.decode(buf);
    return obj;
}
{{end}}
`

// --- CPP Helpers ---

func mapTypeCPP(t string) string {
	switch t {
	case "int":
		return "int32_t"
	case "long":
		return "int64_t"
	case "float":
		return "float"
	case "double":
		return "double"
	case "bool":
		return "bool"
	case "string":
		return "std::string"
	default:
		return t // Class Name
	}
}

func encodeFieldCPP(varName, fieldType string) string {
	switch fieldType {
	case "int":
		return fmt.Sprintf("buf.putInt32(%s);", varName)
	case "long":
		return fmt.Sprintf("buf.putInt64(%s);", varName)
	case "float":
		return fmt.Sprintf("buf.putFloat(%s);", varName)
	case "double":
		return fmt.Sprintf("buf.putDouble(%s);", varName)
	case "bool":
		return fmt.Sprintf("buf.putBool(%s);", varName)
	case "string":
		return fmt.Sprintf("buf.putString(%s);", varName)
	default: // Nested Class
		return fmt.Sprintf("%s.encode(buf);", varName)
	}
}

func decodeFieldCPP(varName, fieldType string) string {
	switch fieldType {
	case "int":
		return fmt.Sprintf("%s = buf.getInt32();", varName)
	case "long":
		return fmt.Sprintf("%s = buf.getInt64();", varName)
	case "float":
		return fmt.Sprintf("%s = buf.getFloat();", varName)
	case "double":
		return fmt.Sprintf("%s = buf.getDouble();", varName)
	case "bool":
		return fmt.Sprintf("%s = buf.getBool();", varName)
	case "string":
		return fmt.Sprintf("%s = buf.getString();", varName)
	default: // Nested Class
		return fmt.Sprintf("%s.decode(buf);", varName)
	}
}
