#!/usr/bin/env bash
# Type-check and run the TypeScript end-to-end suite for the Node.js driver
# (../nodejs) against a live broker.
#
#   ./test.sh HOST PORT
#
#   1. npm ci             TypeScript + @types/node (dev only; the driver has no deps)
#   2. tsc --noEmit       strict check of the suite AND tests/types.test-d.ts,
#                         whose @ts-expect-error lines must fail to compile
#   3. tsc -> out/        compile the suite, then run it with node
#
# Exits non-zero if the typings or any check fail.
set -euo pipefail
if [ $# -ne 2 ]; then
  echo "usage: $0 HOST PORT" >&2
  exit 2
fi
cd "$(dirname "$0")"
if [ -f package-lock.json ]; then
  npm ci --no-audit --no-fund --loglevel=error
else
  npm install --no-audit --no-fund --loglevel=error
fi
echo "== tsc --noEmit (suite + typings tests)"
npx --no-install tsc -p tsconfig.json
echo "== tsc -> out/"
rm -rf out
npx --no-install tsc -p tsconfig.build.json
node out/test_manual.js "$1" "$2"
