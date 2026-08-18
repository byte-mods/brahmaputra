#!/bin/bash
set -e

OUT_DIR="generated"
mkdir -p $OUT_DIR

# Protobuf
echo "Generating Protobuf Python..."
protoc -I../schemas \
    --python_out=$OUT_DIR \
    ../schemas/bench_complex.proto

# FlatBuffers
echo "Generating FlatBuffers Python..."
flatc --python -o $OUT_DIR ../schemas/bench_complex.fbs

echo "✅ Code generation complete!"
ls -la $OUT_DIR/
