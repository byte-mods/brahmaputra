#!/usr/bin/env bash
# Build the Java driver with plain javac and run the end-to-end suite.
#
#   ./test.sh HOST PORT
#
# Needs only a JDK (17+). Exits non-zero if the build fails or any check fails.
set -euo pipefail

HOST="${1:-127.0.0.1}"
PORT="${2:-9092}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="$DIR/out"

rm -rf "$OUT"
mkdir -p "$OUT"
javac -Xlint:all -Werror --release 17 -d "$OUT" \
    $(find "$DIR/src/main/java" "$DIR/src/test/java" -name '*.java')
exec java -cp "$OUT" io.brahmaputra.ManualTest "$HOST" "$PORT"
