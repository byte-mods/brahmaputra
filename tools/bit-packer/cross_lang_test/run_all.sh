#!/bin/bash
# Cross-Language Compatibility Test Runner
# Runs all 7 languages: Python → Go → JS → Java → Rust → C++ → C#
# Each test: roundtrip encode/decode + decode Python's binary data
set -e
cd "$(dirname "$0")"

echo "═══════════════════════════════════════════════════"
echo "  BitPacker Cross-Language Compatibility Tests"
echo "═══════════════════════════════════════════════════"
echo ""

PASS=0
FAIL=0

run_test() {
    local name=$1
    shift
    echo "─── $name ───"
    if "$@" 2>&1; then
        PASS=$((PASS + 1))
    else
        echo "   ❌ $name FAILED"
        FAIL=$((FAIL + 1))
    fi
    echo ""
}

# 1. Python (generates test_data.bin for all others)
run_test "Python" python3 test_python.py

# 2. Go
echo "─── Go (building) ───"
(cd . && go build -o test_go_bin test_go.go) 2>&1
run_test "Go" ./test_go_bin test_data_go.bin test_data.bin

# 3. JavaScript
run_test "JavaScript" node test_js.js

# 4. Java
echo "─── Java (compiling) ───"
javac -cp generated/java TestJava.java 2>&1
run_test "Java" java -ea -cp .:generated/java TestJava

# 5. Rust
echo "─── Rust (building) ───"
(cd rust_test && cp ../test_data.bin . 2>/dev/null; cargo build --release --quiet 2>&1)
run_test "Rust" bash -c "cd rust_test && ./target/release/test_rust"

# 6. C++
echo "─── C++ (compiling) ───"
g++ -std=c++17 -O2 -o test_cpp_bin test_cpp.cpp 2>&1
run_test "C++" ./test_cpp_bin

# 7. C# (skip if dotnet/csc not available)
# 7. C#
if command -v dotnet &> /dev/null; then
    echo "─── C# (dotnet) ───"
    cp test_data.bin csharp_test/ 2>/dev/null
    cp generated/csharp/csharp/bench_complex.cs csharp_test/
    if dotnet run --project csharp_test/CrossTest.csproj; then
        PASS=$((PASS + 1))
        # Move generated bin file to root for comparison
        mv csharp_test/test_data_csharp.bin . 2>/dev/null || true
    else
        echo "   ❌ C# FAILED"
        FAIL=$((FAIL + 1))
    fi
    echo ""
else
    echo "─── C# ───"
    echo "   ⚠️  dotnet not installed, skipping C# test"
fi

echo "═══════════════════════════════════════════════════"
echo "  Results: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════════════════════"
echo ""

# Verify all binary files are identical
echo "─── Binary Compatibility Check ───"
echo "   Comparing all test_data_*.bin files..."
REF_MD5=$(md5 -q test_data.bin 2>/dev/null || md5sum test_data.bin | awk '{print $1}')
ALL_MATCH=true
for f in test_data_go.bin test_data_js.bin test_data_java.bin test_data_rust.bin test_data_cpp.bin test_data_csharp.bin; do
    if [ -f "$f" ]; then
        F_MD5=$(md5 -q "$f" 2>/dev/null || md5sum "$f" | awk '{print $1}')
        if [ "$REF_MD5" = "$F_MD5" ]; then
            echo "   ✅ $f matches test_data.bin"
        else
            echo "   ❌ $f DIFFERS from test_data.bin"
            ALL_MATCH=false
        fi
    else
        echo "   ⚠️  $f not found"
    fi
done

if [ "$ALL_MATCH" = true ]; then
    echo ""
    echo "   🎉 All binary outputs are byte-identical!"
fi

echo ""
if [ $FAIL -gt 0 ]; then
    exit 1
fi
