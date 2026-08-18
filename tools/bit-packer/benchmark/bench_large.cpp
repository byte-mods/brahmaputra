#include <iostream>
#include <fstream>
#include <vector>
#include <chrono>
#include "generated/cpp_bench/cpp/bench_complex.hpp"

int main() {
    std::string fileName = "large_payload.bin";
    std::ifstream file(fileName, std::ios::binary | std::ios::ate);
    if (!file) {
        std::cerr << "Failed to open " << fileName << std::endl;
        return 1;
    }
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<uint8_t> buffer(size);
    if (!file.read((char*)buffer.data(), size)) {
        std::cerr << "Failed to read " << fileName << std::endl;
        return 1;
    }
    std::cout << "Read " << size << " bytes from " << fileName << std::endl;

    // Decode
    auto start = std::chrono::high_resolution_clock::now();
    WorldState world = WorldState::decode(buffer);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsedDecode = end - start;
    std::cout << "Decode time: " << elapsedDecode.count() << "s" << std::endl;

    std::cout << "Decoded " << world.guilds.size() << " guilds" << std::endl;

    // Encode
    start = std::chrono::high_resolution_clock::now();
    std::vector<uint8_t> encodedData = world.encode();
    end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsedEncode = end - start;
    std::cout << "Encode time: " << elapsedEncode.count() << "s" << std::endl;
    std::cout << "Encoded size: " << encodedData.size() << " bytes" << std::endl;

    if (encodedData.size() != buffer.size()) {
        std::cout << "WARNING: Encoded size mismatch! Original: " << buffer.size() << ", New: " << encodedData.size() << std::endl;
    }

    return 0;
}
