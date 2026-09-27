#!/usr/bin/env bash
# Build the Crystal driver's end-to-end suite and run it against a live broker.
#   ./test.sh HOST PORT
# Checks formatting, compiles test/manual_test.cr with --release into bin/,
# and exits non-zero if any check fails.
set -euo pipefail
if [ $# -ne 2 ]; then
  echo "usage: $0 HOST PORT" >&2
  exit 64
fi
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"
crystal tool format --check src test
mkdir -p bin
crystal build --release --no-debug test/manual_test.cr -o bin/manual_test
exec ./bin/manual_test "$1" "$2"
