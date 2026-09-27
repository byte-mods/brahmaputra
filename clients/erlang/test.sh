#!/usr/bin/env bash
# Usage: ./test.sh HOST PORT
# Builds the driver and runs the end-to-end suite against a live broker.
set -euo pipefail
HOST="${1:-127.0.0.1}"
PORT="${2:-9092}"
cd "$(dirname "$0")"
make -s all test
exec erl -noshell -pa ebin -pa test \
    -eval "brahmaputra_manual_test:main([\"$HOST\", \"$PORT\"])"
