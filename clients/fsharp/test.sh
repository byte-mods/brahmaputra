#!/usr/bin/env bash
# Builds the F# API (and the .NET driver it wraps) and runs the end-to-end
# suite against a live broker.
#   ./test.sh HOST PORT
# Exits non-zero if the build fails or any check fails (1 = a check failed,
# 2 = setup failed, e.g. no broker reachable).
set -euo pipefail

if [ $# -ne 2 ]; then
    echo "usage: $0 HOST PORT" >&2
    exit 64
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DOTNET_CLI_TELEMETRY_OPTOUT=1
export DOTNET_NOLOGO=1

# All build output (including the referenced C# driver's) goes under
# $DIR/artifacts, so this never writes into clients/dotnet.
ARTIFACTS="$DIR/artifacts"
dotnet build "$DIR/Brahmaputra.FSharp.ManualTest/Brahmaputra.FSharp.ManualTest.fsproj" \
    -c Release --nologo -v quiet \
    -p:UseArtifactsOutput=true -p:ArtifactsPath="$ARTIFACTS"
dotnet "$ARTIFACTS/bin/Brahmaputra.FSharp.ManualTest/release/Brahmaputra.FSharp.ManualTest.dll" "$1" "$2"
