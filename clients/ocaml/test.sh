#!/usr/bin/env bash
# Build the OCaml driver and run its end-to-end suite against a live broker.
#
#   ./test.sh HOST PORT
#
# Builds with ocamlfind (unix, threads.posix, zip/camlzip), warnings as
# errors, so it needs neither dune nor opam.
set -euo pipefail
if [ $# -ne 2 ]; then
  echo "usage: $0 HOST PORT" >&2
  exit 2
fi
cd "$(dirname "$0")"
make --no-print-directory -s all >/dev/null
exec ./build/manual_test.exe "$1" "$2"
