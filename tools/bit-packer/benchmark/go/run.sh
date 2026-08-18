#!/bin/bash
set -e
echo "Generating Go code..."
pushd ../../
go run ./cmd/bitpacker --file examples/bench_complex.buff --lang go --out benchmark/go --sep
popd
# Rename files to match main.go expectations (generator uses first class name 'Vec3')
mv vec3_structs.go bench_complex_structs.go 2>/dev/null || true
mv vec3_impl.go bench_complex_impl.go 2>/dev/null || true

echo "Running Go benchmark..."
go run main.go bench_complex_structs.go bench_complex_impl.go
