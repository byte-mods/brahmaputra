#!/usr/bin/env bash
# Build the Haskell driver and run its end-to-end suite against a live broker.
#   ./test.sh HOST PORT
# Exits non-zero if the driver fails to compile (warnings are errors) or any
# check fails. Works from any directory.
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 HOST PORT" >&2
    exit 2
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GHC="${GHC:-ghc}"
GHC_FLAGS="${GHC_FLAGS:--Wall -Werror}"

mkdir -p "$DIR/build"
# shellcheck disable=SC2086
"$GHC" -threaded -O1 $GHC_FLAGS -v0 \
    -i"$DIR/src" -outputdir "$DIR/build" \
    -o "$DIR/build/manualtest" "$DIR/test/ManualTest.hs"
exec "$DIR/build/manualtest" "$1" "$2"
