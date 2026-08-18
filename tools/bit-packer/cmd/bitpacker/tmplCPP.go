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

// ZigZag Helpers
static uint32_t zigzag_encode32(int32_t n) { return (n << 1) ^ (n >> 31); }
static int32_t zigzag_decode32(uint32_t n) { return (n >> 1) ^ -(n & 1); }
static uint64_t zigzag_encode64(int64_t n) { return (n << 1) ^ (n >> 63); }
static int64_t zigzag_decode64(uint64_t n) { return (n >> 1) ^ -(n & 1); }

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

inline int64_t ZeroCopyByteBuff::getVarInt64() {
    uint64_t result = 0;
    int shift = 0;
    // FAST PATH: 1 byte
    uint8_t byte = buffer[offset++];
    if (!(byte & 0x80)) {
        return (int64_t)byte;
    }
    result = byte & 0x7F;
    shift = 7;
    while (true) {
        byte = buffer[offset++];
        result |= ((uint64_t)(byte & 0x7F)) << shift;
        if (!(byte & 0x80)) break;
        shift += 7;
    }
    return (int64_t)result;
}

inline void ZeroCopyByteBuff::putInt32(int32_t v) { putVarInt64(zigzag_encode32(v)); }
inline void ZeroCopyByteBuff::putInt64(int64_t v) { putVarInt64(zigzag_encode64(v)); }
inline void ZeroCopyByteBuff::putFloat(float v) { putVarInt64(zigzag_encode64((int64_t)(v * 10000.0f))); }
inline void ZeroCopyByteBuff::putDouble(double v) { putVarInt64(zigzag_encode64((int64_t)(v * 10000.0))); }
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
    return buffer[offset++] != 0;
}

inline std::string ZeroCopyByteBuff::getString() {
    size_t len = (size_t)zigzag_decode64((uint64_t)getVarInt64());
    if (offset + len > buffer.size()) throw std::runtime_error("Buffer underflow");
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
    int32_t len_{{.Name}} = buf.getInt32();
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
