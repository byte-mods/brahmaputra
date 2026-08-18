#!/bin/bash
set -e
echo "Generating Java code..."
# Generate into current directory with package 'example'
go run ../../cmd/bitpacker --file game.buff --lang java --out . --package example

# Compile
echo "Compiling..."
javac -d . *.java

# Run
echo "Running example..."
java example.Example
