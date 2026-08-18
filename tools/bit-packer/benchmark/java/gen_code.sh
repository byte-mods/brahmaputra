#!/bin/bash
set -e

# Output directory
OUT_DIR="src/main/java"
mkdir -p $OUT_DIR

# Protobuf
# We use the default package 'bench_proto' from valid syntax, 
# but we can override it via command line or just accept it generates in $OUT_DIR/bench_proto
echo "Generating Protobuf..."
protoc -I../schemas \
    --java_out=$OUT_DIR \
    ../schemas/bench_complex.proto

# FlatBuffers
# Namespace 'bench_fb' becomes package 'bench_fb'
echo "Generating FlatBuffers..."
flatc --java -o $OUT_DIR ../schemas/bench_complex.fbs
