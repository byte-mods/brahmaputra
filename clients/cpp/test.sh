#!/usr/bin/env bash
# Builds the C++ driver and runs the end-to-end suite against a live broker.
#   usage: ./test.sh HOST PORT
set -euo pipefail
if [ "$#" -ne 2 ]; then
  echo "usage: $0 HOST PORT" >&2
  exit 2
fi
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD="$DIR/build"
cmake -S "$DIR" -B "$BUILD" -DCMAKE_BUILD_TYPE=RelWithDebInfo >/dev/null
cmake --build "$BUILD" -j"$(nproc 2>/dev/null || echo 4)" >/dev/null
exec "$BUILD/manual_test" "$1" "$2"
