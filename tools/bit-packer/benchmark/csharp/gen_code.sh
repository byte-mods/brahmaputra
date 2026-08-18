#!/bin/bash
set -e

# Output directory for generated code
OUT_DIR="generated"
mkdir -p $OUT_DIR

# Protobuf
echo "Generating Protobuf C#..."
protoc -I../schemas \
    --csharp_out=$OUT_DIR \
    ../schemas/bench_complex.proto

# FlatBuffers
echo "Generating FlatBuffers C#..."
flatc --csharp -o $OUT_DIR ../schemas/bench_complex.fbs

echo "✅ Code generation complete!"
ls -la $OUT_DIR/
