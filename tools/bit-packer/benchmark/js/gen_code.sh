#!/bin/bash
set -e

# Generate Protobuf JS code using pbjs (from protobufjs)
echo "Generating Protobuf JS (static)..."
npx pbjs -t static-module -w commonjs -o bench_proto.js ../schemas/bench_complex.proto

# Generate FlatBuffers JS code
echo "Generating FlatBuffers JS..."
flatc --js -o . ../schemas/bench_complex.fbs

echo "✅ Code generation complete!"
