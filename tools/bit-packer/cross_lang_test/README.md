# BitPacker cross-language conformance tests

Every target is tested against **committed reference bytes**, never against
another generator, and always with code generated fresh by the current
generator (nothing generated is committed).

```bash
./run_all.sh              # every <lang>/run.sh in this directory
./run_all.sh go rust php  # a subset
BITPACKER=/path/to/bitpacker ./run_all.sh   # reuse a built compiler
```

`run_all.sh` builds `bitpacker` once from `../cmd/bitpacker`, exports it as
`$BITPACKER`, runs each `<lang>/run.sh` it finds (new languages are picked up
automatically), prints a summary table, then checks that every
`test_data_<lang>.bin` written during the run is byte-identical to
`test_data.bin`. It exits non-zero if any language fails or any binary
differs. `BP_TIMEOUT` (seconds, default 1200) bounds each language.

## Fixtures

| File | What it is | Written by |
|---|---|---|
| `test_data.bin` | the canonical `bench_complex.buff` `WorldState` (values in `test_python.py`) | `make_test_data.py` (hand-written encoder) |
| `edge/edge_ref.bin` | the canonical `edge.buff` `Edge`: every type at its extremes | `edge/make_ref.py` |
| `edge/edge_float32_ref.bin` | the same `Edge` with float values whose ×10000 is inexact in float32 | `edge/make_ref.py` |

The wire rules are in [`edge/README.md`](edge/README.md). The reference
encoders share no code with any generator. `python3 make_test_data.py --check`
verifies the committed `test_data.bin` (run_all does this too).

## The per-language contract

`<lang>/run.sh` (executable, works from any cwd):

1. uses `$BITPACKER`, or builds one from `../../cmd/bitpacker` into `gen/` if unset;
2. generates code for `../../examples/bench_complex.buff` and `../edge/edge.buff` into `<lang>/gen/` (gitignored);
3. builds and runs a test program that checks:
   - **bench**: encode the canonical `WorldState`, write `../test_data_<lang>.bin`, and compare it with `../test_data.bin`
     byte for byte; decode `test_data.bin` and check every field `test_python.py verify()` checks; round-trip;
   - **edge**: encode the canonical `Edge` == `edge/edge_ref.bin`; decode it and check every field exactly (floats
     compare equal); re-encode == ref; wrong version prefix → error; every truncated prefix → error;
   - the rest of `edge/README.md` (float32 fixture, trailing bytes ignored, NaN/out-of-range floats rejected on
     encode, invalid UTF-8 and bogus lengths rejected on decode) where the target implements it;
4. prints one line per check, `  ok   name` or `  FAIL name (detail)`, and finally
   `<lang>: N passed, M failed`; exits 0 only if nothing failed.

**Exit code 77 means SKIP**: the language's toolchain is missing (e.g. no `dotnet`, no `php`). `run_all.sh` reports it as
`SKIP` with the reason (the first line starting with `SKIP`), never as a pass, and it does not fail the run. Any other
non-zero exit, a missing summary line, or a summary with failures is a `FAIL`.

Add a `.gitignore` for `gen/` and build output if the language writes anything else (`*/gen/` is already ignored here).

## Helpers for run.sh

`lib/common.sh` implements the boilerplate; the original eight targets use it:

```bash
#!/usr/bin/env bash
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bp_init go                 # HERE, ROOT, GEN, BITPACKER, BENCH_BUFF, EDGE_BUFF; recreates gen/
bp_require go              # exit 77 if a command is missing
bp_gen go "$GEN/bench" "$BENCH_BUFF" --package bench || bp_finish
bp_step "go build" go build -o "$GEN/t" . || bp_finish   # a build step: failure counts, output shown
bp_run "$GEN/t" "$ROOT"    # run a test program; its ok/FAIL lines are tallied, a crash counts as a failure
bp_finish                  # prints "<lang>: N passed, M failed" and exits
```

## The original eight

| Dir | Notes |
|---|---|
| `go/` | own module (`go/go.mod`); also runs `go vet` on the generated code |
| `rust/` | standalone crate with its own `[workspace]`; tests single-file **and** `--sep` output (what the broker compiles) |
| `java/` | `javac` + `java`, packages `bench` / `edge` |
| `csharp/` | builds a throwaway project in `gen/` for the installed SDK's `netN.0` |
| `python/` | runs everything twice: the pure-Python runtime and the generated `_bitpacker` C extension (needs `Python.h`, e.g. `apt-get install python3-dev`; without it the C run is reported as not tested) |
| `cpp/` | `g++`/`clang++`, with ASan+UBSan when available so an out-of-bounds read or UB fails the run |
| `js/` | Node.js; `long` fields are `BigInt` |
| `php/` | 64-bit PHP CLI |
