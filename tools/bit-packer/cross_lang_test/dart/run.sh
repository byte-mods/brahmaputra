#!/usr/bin/env bash
# Dart conformance test for the BitPacker `dart` target (Dart VM).
# Usage: [BITPACKER=/path/to/bitpacker] ./run.sh   (works from any cwd)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
export DART_DISABLE_ANALYTICS=1

for c in dart go; do command -v "$c" >/dev/null 2>&1 || { echo "SKIP dart: $c not installed"; exit 77; }; done

if [ -z "${BITPACKER:-}" ]; then
  TMPD="$(mktemp -d)"
  trap 'rm -rf "$TMPD"' EXIT
  (cd "$ROOT" && go build -o "$TMPD/bitpacker" ./cmd/bitpacker)
  BITPACKER="$TMPD/bitpacker"
fi

rm -rf "$HERE/gen" "$HERE/build"
mkdir -p "$HERE/gen"
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang dart --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$ROOT/cross_lang_test/edge/edge.buff" --lang dart --out "$HERE/gen" >/dev/null
for f in bench_complex.dart edge.dart; do
  [ -f "$HERE/gen/dart/$f" ] || { echo "dart: generator did not produce gen/dart/$f"; exit 1; }
done

# Static analysis must be clean, infos included.
(cd "$HERE" && dart analyze --fatal-infos gen/dart test.dart >/dev/null) || {
  (cd "$HERE" && dart analyze --fatal-infos gen/dart test.dart); exit 1; }
dart run "$HERE/test.dart" "$ROOT/cross_lang_test"
