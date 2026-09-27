#!/usr/bin/env bash
# C++ target: generate fresh code, build (with ASan/UBSan when available, so
# an out-of-bounds read on hostile input fails loudly), run checks.
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bp_init cpp
CXX=${CXX:-}
if [ -z "$CXX" ]; then
    if command -v g++ >/dev/null 2>&1; then CXX=g++; elif command -v clang++ >/dev/null 2>&1; then CXX=clang++; fi
fi
[ -n "$CXX" ] || { echo "SKIP: no C++ compiler (g++/clang++)"; exit 77; }

bp_gen cpp "$GEN/bench" "$BENCH_BUFF" || bp_finish
bp_gen cpp "$GEN/edge" "$EDGE_BUFF" || bp_finish

FLAGS=(-std=c++17 -O1 -g -Wall -Wextra -Wno-unused-parameter -I"$HERE")
SAN=(-fsanitize=address,undefined -fno-sanitize-recover=undefined)
echo 'int main(){}' >"$GEN/probe.cpp"
if ! "$CXX" "${SAN[@]}" "$GEN/probe.cpp" -o "$GEN/probe" >/dev/null 2>&1; then
    echo "  (sanitizers unavailable with $CXX; building without them)"
    SAN=()
fi
for t in bench edge; do
    bp_step "$CXX $t" "$CXX" "${FLAGS[@]}" "${SAN[@]}" -I"$GEN/$t/cpp" \
        "$HERE/${t}_test.cpp" "$GEN/$t/cpp/"*.cpp -o "$GEN/${t}_test" || continue
    bp_run "$GEN/${t}_test" "$ROOT"
done
bp_finish
