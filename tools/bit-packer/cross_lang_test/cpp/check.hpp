// Tiny check helpers shared by bench_test.cpp and edge_test.cpp.
#pragma once
#include <cstdio>
#include <fstream>
#include <functional>
#include <iterator>
#include <string>
#include <vector>

static int g_passed = 0, g_failed = 0;

static void check(const std::string& name, bool ok, const std::string& detail = "") {
    if (ok) { g_passed++; std::printf("  ok   %s\n", name.c_str()); }
    else { g_failed++; std::printf("  FAIL %s (%s)\n", name.c_str(), detail.c_str()); }
}

static std::vector<uint8_t> readFile(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}

static void writeFile(const std::string& path, const std::vector<uint8_t>& data) {
    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(data.data()), (std::streamsize)data.size());
}

static bool rejects(const std::function<void(const std::vector<uint8_t>&)>& decode, const std::vector<uint8_t>& b) {
    try { decode(b); return false; } catch (const std::exception&) { return true; }
}

static void truncations(const std::string& name, const std::vector<uint8_t>& data,
                        const std::function<void(const std::vector<uint8_t>&)>& decode) {
    for (size_t cut = 0; cut < data.size(); cut++) {
        std::vector<uint8_t> prefix(data.begin(), data.begin() + (long)cut);
        if (!rejects(decode, prefix)) {
            check(name, false, "prefix of " + std::to_string(cut) + "/" + std::to_string(data.size()) + " bytes decoded");
            return;
        }
    }
    check(name, true);
}

static std::vector<uint8_t> unhex(const std::string& s) {
    std::vector<uint8_t> b;
    for (size_t i = 0; i + 1 < s.size(); i += 2) b.push_back((uint8_t)std::stoi(s.substr(i, 2), nullptr, 16));
    return b;
}

static std::vector<uint8_t> badVersion(std::vector<uint8_t> b) { b[1] ^= 1; return b; }
