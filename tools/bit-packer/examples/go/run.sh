#!/bin/bash
set -e

# Generate code
echo "Generating Go code..."
go run ../../cmd/bitpacker --file game.buff --lang go --out . --sep

# Run example
echo "Running example..."
go run main.go game_structs.go game_impl.go
