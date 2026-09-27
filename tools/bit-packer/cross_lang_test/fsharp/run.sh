#!/usr/bin/env bash
# F# conformance test for the BitPacker `fsharp` target.
# Usage: [BITPACKER=/path/to/bitpacker] ./run.sh   (works from any cwd)
# Needs the .NET 8 SDK (FSharp.Core comes from the SDK's offline library pack).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1 DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1

for c in dotnet go; do command -v "$c" >/dev/null 2>&1 || { echo "SKIP fsharp: $c not installed"; exit 77; }; done

if [ -z "${BITPACKER:-}" ]; then
  TMPD="$(mktemp -d)"
  trap 'rm -rf "$TMPD"' EXIT
  (cd "$ROOT" && go build -o "$TMPD/bitpacker" ./cmd/bitpacker)
  BITPACKER="$TMPD/bitpacker"
fi

rm -rf "$HERE/gen" "$HERE/build"
mkdir -p "$HERE/gen"
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang fsharp --package bench --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$ROOT/cross_lang_test/edge/edge.buff" --lang fsharp --package edge --out "$HERE/gen" >/dev/null
for f in bench_complex.fs edge.fs; do
  [ -f "$HERE/gen/fsharp/$f" ] || { echo "fsharp: generator did not produce gen/fsharp/$f"; exit 1; }
done

dotnet build "$HERE/FSharpTest.fsproj" -c Release -o "$HERE/build" --nologo -v quiet -clp:ErrorsOnly
dotnet "$HERE/build/FSharpTest.dll" "$ROOT/cross_lang_test"
