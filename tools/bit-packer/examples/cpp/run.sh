#!/bin/bash
set -e

# Generate code
echo "Generating C++ code..."
go run ../../cmd/bitpacker --file game.buff --lang cpp --out .

# Compile
echo "Compiling..."
g++ -std=c++17 main.cpp -o example

# Run
echo "Running example..."
./example
