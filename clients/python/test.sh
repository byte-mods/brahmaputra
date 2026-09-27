#!/usr/bin/env bash
# Run the Python driver's end-to-end suite against a live broker.
#   ./test.sh HOST PORT
# Exits non-zero if the driver fails to compile or any check fails.
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 HOST PORT" >&2
    exit 2
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"

# "Build": byte-compile the package so a syntax error fails fast and loudly.
"$PYTHON" -m compileall -q "$DIR/brahmaputra" "$DIR/test_manual.py"
exec "$PYTHON" "$DIR/test_manual.py" "$1" "$2"
