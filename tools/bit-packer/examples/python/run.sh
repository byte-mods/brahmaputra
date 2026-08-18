#!/bin/bash
set -e

# Generate code
echo "Generating Python code..."
go run ../../cmd/bitpacker --file game.buff --lang python --out .

# Build C extension
echo "Building C extension..."
python3 setup.py build_ext --inplace

# Run
echo "Running example..."
python3 main.py
