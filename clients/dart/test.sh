#!/usr/bin/env bash
# Analyze and run the Dart driver's end-to-end suite against a live broker.
#   ./test.sh HOST PORT
# Exits non-zero if analysis reports anything or any check fails.
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 HOST PORT" >&2
    exit 2
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DART="${DART:-dart}"
cd "$DIR"

# No dependencies, so resolution never needs the network.
"$DART" pub get --offline >/dev/null
"$DART" analyze --fatal-infos
exec "$DART" run bin/manual_test.dart "$1" "$2"
