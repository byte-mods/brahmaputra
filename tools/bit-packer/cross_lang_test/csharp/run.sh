#!/usr/bin/env bash
# C# target: generate fresh code, build with the dotnet SDK, run checks.
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bp_init csharp
bp_require dotnet
dotnet --list-sdks 2>/dev/null | grep -q . || { echo "SKIP: no .NET SDK installed"; exit 77; }

bp_gen csharp "$GEN/bench" "$BENCH_BUFF" --package BenchGen || bp_finish
bp_gen csharp "$GEN/edge" "$EDGE_BUFF" --package EdgeGen || bp_finish

# Target whatever SDK is installed (e.g. net8.0); everything lands in gen/.
TFM="net$(dotnet --version | cut -d. -f1).0"
PROJ="$GEN/proj"
mkdir -p "$PROJ"
cp "$HERE/CrossTest.cs" "$GEN"/bench/csharp/*.cs "$GEN"/edge/csharp/*.cs "$PROJ/"
cat >"$PROJ/CrossTest.csproj" <<XML
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>$TFM</TargetFramework>
    <Nullable>disable</Nullable>
    <ImplicitUsings>disable</ImplicitUsings>
    <TreatWarningsAsErrors>false</TreatWarningsAsErrors>
  </PropertyGroup>
</Project>
XML
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1 DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
bp_step "dotnet build" dotnet build "$PROJ/CrossTest.csproj" -c Release -o "$PROJ/out" --nologo -v q || bp_finish
bp_run dotnet "$PROJ/out/CrossTest.dll" "$ROOT"
bp_finish
