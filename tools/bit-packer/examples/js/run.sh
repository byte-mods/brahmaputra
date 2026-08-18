#!/bin/bash
set -e
echo "Generating JS code..."
go run ../../cmd/bitpacker --file game.buff --lang js --out .
echo "Running example..."
node example.js
