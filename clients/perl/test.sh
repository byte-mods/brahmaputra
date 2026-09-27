#!/usr/bin/env bash
# Run the Perl driver's end-to-end suite against a live broker.
#   ./test.sh HOST PORT
set -euo pipefail
if [ $# -ne 2 ]; then
    echo "usage: $0 HOST PORT" >&2
    exit 64
fi
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PERL="${PERL:-perl}"
# Compile-check every module and the suite with warnings on; nothing else
# needs building.
while IFS= read -r -d '' file; do
    out="$("$PERL" -I"$DIR/lib" -wc "$file" 2>&1)" || { echo "$out" >&2; exit 1; }
    if [ "$out" != "$file syntax OK" ]; then echo "$out" >&2; exit 1; fi
done < <(find "$DIR/lib" "$DIR/t" \( -name '*.pm' -o -name '*.pl' -o -name '*.t' \) -print0)
# Offline encoding checks, then the live suite.
"$PERL" -I"$DIR/lib" "$DIR/t/01-unit.t" >/dev/null || { echo "unit tests failed" >&2; exit 1; }
exec "$PERL" -I"$DIR/lib" "$DIR/t/manual_test.pl" "$1" "$2"
