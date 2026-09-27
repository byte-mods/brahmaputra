#!/usr/bin/env bash
# Build and run the Nim driver's end-to-end suite against a live broker.
#
#   ./test.sh HOST PORT
#
# Compiles with --threads:on --mm:orc (the driver runs a linger thread per
# producer and a heartbeat thread per group consumer) and fails the build
# on any warning from this package's code.
set -euo pipefail
if [ $# -ne 2 ]; then
  echo "usage: $0 HOST PORT" >&2
  exit 2
fi
cd "$(dirname "$0")"
mkdir -p build
log=build/compile.log
if ! nim c -d:release --threads:on --mm:orc --hints:off \
    --nimcache:build/nimcache --outdir:build tests/manual_test.nim >"$log" 2>&1; then
  cat "$log" >&2
  exit 1
fi
if grep -E "(src|tests)/.*Warning" "$log" >&2; then
  echo "compiler warnings in the driver or suite" >&2
  exit 1
fi
exec ./build/manual_test "$1" "$2"
