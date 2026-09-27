#!/usr/bin/env bash
# Run the Node.js driver's end-to-end suite against a live broker.
#
#   ./test.sh HOST PORT
#
# No build step and no dependencies: the driver uses only Node's standard
# library. Exits non-zero if any check fails.
set -euo pipefail
if [ $# -ne 2 ]; then
  echo "usage: $0 HOST PORT" >&2
  exit 2
fi
cd "$(dirname "$0")"
node --check src/index.js
for file in src/*.js test_manual.js; do node --check "$file"; done
node test_manual.js "$1" "$2"
