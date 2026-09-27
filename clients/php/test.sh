#!/usr/bin/env bash
# Run the PHP driver's end-to-end suite against a live broker.
#   ./test.sh HOST PORT
set -euo pipefail
if [ $# -ne 2 ]; then
    echo "usage: $0 HOST PORT" >&2
    exit 64
fi
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PHP="${PHP:-php}"
# Syntax-check every source file first; there is nothing else to build.
find "$DIR/src" "$DIR/autoload.php" "$DIR/test_manual.php" -name '*.php' -print0 |
    while IFS= read -r -d '' file; do
        "$PHP" -l "$file" >/dev/null || { echo "syntax error in $file" >&2; exit 1; }
    done
exec "$PHP" -d zend.assertions=1 "$DIR/test_manual.php" "$1" "$2"
