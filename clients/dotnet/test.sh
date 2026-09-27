#!/usr/bin/env bash
# Builds the .NET driver and runs the end-to-end suite against a live broker.
#   ./test.sh HOST PORT
# Exits non-zero if the build fails or any check fails.
set -euo pipefail

if [ $# -ne 2 ]; then
    echo "usage: $0 HOST PORT" >&2
    exit 64
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DOTNET_CLI_TELEMETRY_OPTOUT=1
export DOTNET_NOLOGO=1

dotnet build "$DIR/Brahmaputra.sln" -c Release --nologo -v quiet
dotnet "$DIR/Brahmaputra.ManualTest/bin/Release/net8.0/Brahmaputra.ManualTest.dll" "$1" "$2"
