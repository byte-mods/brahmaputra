#!/bin/bash
set -e
echo "Generating C# code..."
# Output filename will be game.cs
go run ../../cmd/bitpacker --file game.buff --lang csharp --out .

# Setup project
if [ ! -f "Example.csproj" ]; then
    # Create console app targeting net8.0
    dotnet new console --force -o . -n Example -f net8.0
    # Remove default Program.cs
    rm Program.cs
fi

# Run
echo "Running example..."
dotnet run
