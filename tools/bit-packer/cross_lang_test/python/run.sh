#!/usr/bin/env bash
# Python target: generate fresh code and run the checks twice, once with the
# pure-Python runtime and once with the generated C extension (_bitpacker).
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bp_init python
bp_require python3

bp_gen python "$GEN/bench" "$BENCH_BUFF" || bp_finish
bp_gen python "$GEN/edge" "$EDGE_BUFF" || bp_finish
export PYTHONDONTWRITEBYTECODE=1

bp_run python3 "$HERE/crosstest.py" "$ROOT" "$GEN" pure

# The C extension needs the CPython headers (apt-get install python3-dev).
PYINC=$(python3 -c 'import sysconfig; print(sysconfig.get_paths()["include"])')
if [ ! -f "$PYINC/Python.h" ]; then
    echo "  (C extension not tested: $PYINC/Python.h missing; install python3-dev)"
elif (cd "$GEN/bench/python" && bp_step "build C extension" \
        python3 setup.py build_ext --build-lib "$GEN/cext" --build-temp "$GEN/cext-build"); then
    bp_run python3 "$HERE/crosstest.py" "$ROOT" "$GEN" cext
else
    BP_FAIL=$((BP_FAIL + 1))
fi
bp_finish
