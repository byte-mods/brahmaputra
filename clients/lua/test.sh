#!/usr/bin/env bash
# Run the Lua driver's end-to-end suite against a live broker.
#   ./test.sh HOST PORT
# Works from any cwd: LUA_PATH is pointed at this directory.
set -euo pipefail
if [ $# -ne 2 ]; then
    echo "usage: $0 HOST PORT" >&2
    exit 64
fi
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LUA="${LUA:-lua5.4}"
command -v "$LUA" >/dev/null || { echo "$LUA not found (apt install lua5.4)" >&2; exit 1; }
"$LUA" -e 'require "socket"' 2>/dev/null || { echo "LuaSocket missing (apt install lua-socket)" >&2; exit 1; }
"$LUA" -e 'require "zlib"' 2>/dev/null || { echo "lua-zlib missing (apt install lua-zlib): the suite checks gzip" >&2; exit 1; }
export LUA_PATH="$DIR/?.lua;$DIR/?/init.lua;${LUA_PATH:-;}"
# Nothing to build: compile-check every source file first.
for file in "$DIR"/brahmaputra.lua "$DIR"/brahmaputra/*.lua "$DIR"/test/*.lua; do
    "$LUA" -e "assert(loadfile('$file'))" || { echo "syntax error in $file" >&2; exit 1; }
done
exec "$LUA" "$DIR/test/manual_test.lua" "$1" "$2"
