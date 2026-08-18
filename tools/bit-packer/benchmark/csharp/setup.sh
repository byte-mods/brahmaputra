#!/bin/bash
set -e

echo "🚀 Restore and Build..."
dotnet restore
dotnet build -c Release

echo "🏃 Running Benchmark..."
dotnet run -c Release
