#!/bin/bash
set -e

# Ensure protoc-gen-go is installed and in PATH
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
export PATH=$PATH:$(go env GOPATH)/bin

mkdir -p bench_proto
protoc -I../schemas --go_out=bench_proto --go_opt=paths=source_relative --go_opt=Mbench_complex.proto=benchmark/bench_proto ../schemas/bench_complex.proto

# FlatBuffers
# Generates into benchmark/go/bench_fb (or similar based on namespace)
mkdir -p bench_fb
flatc --go -o . ../schemas/bench_complex.fbs
