#!/usr/bin/env bash
# Run the Ruby driver's end-to-end suite against a live broker.
#   ./test.sh HOST PORT
set -euo pipefail
if [ $# -ne 2 ]; then
  echo "usage: $0 HOST PORT" >&2
  exit 64
fi
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Nothing to build: the gem is pure Ruby with no dependencies. Syntax-check
# every file first so a typo fails fast rather than mid-suite.
for file in "$DIR"/lib/brahmaputra.rb "$DIR"/lib/brahmaputra/*.rb "$DIR"/test/manual_test.rb; do
  ruby -c "$file" > /dev/null
done
exec ruby -I"$DIR/lib" "$DIR/test/manual_test.rb" "$1" "$2"
