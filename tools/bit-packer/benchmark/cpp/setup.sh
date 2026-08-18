#!/bin/bash
set -e

# 1. Install Dependencies
echo "📦 Checking dependencies..."

# Function to check if a command exists
command_exists () {
    type "$1" &> /dev/null ;
}

if command_exists brew; then
    if brew list nlohmann-json &>/dev/null; then
        echo "✅ nlohmann-json found"
    else
        echo "🔽 Installing nlohmann-json..."
        brew install nlohmann-json || echo "⚠️ Brew install failed, will try header download"
    fi

    if brew list msgpack-cxx &>/dev/null; then
        echo "✅ msgpack-cxx found"
    else
        echo "🔽 Installing msgpack-cxx..."
        # msgpack-cxx might be keg-only or need linking
        brew install msgpack-cxx || echo "⚠️ Brew install failed for msgpack"
    fi
else
    echo "⚠️ Brew not found, skipping package installation"
fi

# 2. Generate Code
echo "🛠 Generating Code..."
protoc --cpp_out=. --proto_path=../schemas ../schemas/bench_complex.proto
flatc --cpp -o . ../schemas/bench_complex.fbs

# 3. Compile
echo "🚀 Compiling Benchmark..."
# Default include paths for Homebrew
INCLUDES="-I. -I/opt/homebrew/include -I/usr/local/include -I../../generated/cpp"

# Check if we can use MsgPack
FLAGS=""
if [ -f "/opt/homebrew/include/msgpack.hpp" ] || [ -f "/usr/local/include/msgpack.hpp" ]; then
    echo "✅ MsgPack headers found, enabling..."
    FLAGS="-DUSE_MSGPACK"
else
    echo "⚠️ MsgPack headers not found, skipping MsgPack benchmark..."
fi

# Manual JSON download fallback if not found
if [ ! -f "/opt/homebrew/include/nlohmann/json.hpp" ] && [ ! -f "/usr/local/include/nlohmann/json.hpp" ]; then
    mkdir -p include/nlohmann
    if [ ! -f "include/nlohmann/json.hpp" ]; then
        echo "🔽 Downloading json.hpp..."
        curl -L -o include/nlohmann/json.hpp https://github.com/nlohmann/json/releases/download/v3.11.2/json.hpp
    fi
    INCLUDES="$INCLUDES -Iinclude"
fi

echo "Compiling with: g++ -std=c++17 -O3 $FLAGS $INCLUDES main.cpp ../../generated/cpp/bench_complex.cpp bench_complex.pb.cc -o bench_cpp $(pkg-config --cflags --libs protobuf)"
g++ -std=c++17 -O3 $FLAGS $INCLUDES main.cpp ../../generated/cpp/bench_complex.cpp bench_complex.pb.cc -o bench_cpp $(pkg-config --cflags --libs protobuf)

# 4. Run
echo "🏃 Running Benchmark (with increased stack)..."
ulimit -s 65532 || echo "⚠️ Could not increase stack size"
./bench_cpp
