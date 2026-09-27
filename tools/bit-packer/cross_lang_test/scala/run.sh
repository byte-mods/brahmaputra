#!/usr/bin/env bash
# Scala conformance test for the BitPacker `scala` target.
# Usage: [BITPACKER=/path/to/bitpacker] ./run.sh   (works from any cwd)
# Needs a JDK (17+). The Scala 2.13 compiler is fetched once from Maven
# Central into .cache/ (gitignored); the apt scala (2.11) is too old.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
SCALA_VERSION=2.13.18
# Maven Central, then its Apache mirror (Central answers 429 under load).
MAVEN_MIRRORS="${MAVEN_REPO:-https://repo1.maven.org/maven2 https://repo.maven.apache.org/maven2}"

# fetch <repo-relative path> <output file>
fetch() {
  local m
  for m in $MAVEN_MIRRORS; do
    curl -fsSL --retry 4 --retry-delay 5 --retry-all-errors -o "$2" "$m/$1" 2>/dev/null && return 0
  done
  echo "scala: could not download $1" >&2
  return 1
}

for c in java curl go; do command -v "$c" >/dev/null 2>&1 || { echo "SKIP scala: $c not installed"; exit 77; }; done

if [ -z "${BITPACKER:-}" ]; then
  TMPD="$(mktemp -d)"
  trap 'rm -rf "$TMPD"' EXIT
  (cd "$ROOT" && go build -o "$TMPD/bitpacker" ./cmd/bitpacker)
  BITPACKER="$TMPD/bitpacker"
fi

rm -rf "$HERE/gen" "$HERE/build"
mkdir -p "$HERE/gen" "$HERE/build"
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang scala --package bench --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$ROOT/cross_lang_test/edge/edge.buff" --lang scala --package edge --out "$HERE/gen" >/dev/null
for f in bench_complex.scala edge.scala; do
  [ -f "$HERE/gen/scala/$f" ] || { echo "scala: generator did not produce gen/scala/$f"; exit 1; }
done

CACHE="$HERE/.cache/scala-$SCALA_VERSION"
mkdir -p "$CACHE"
CP=""
for a in scala-compiler scala-library scala-reflect; do
  jar="$CACHE/$a-$SCALA_VERSION.jar"
  if [ ! -s "$jar" ]; then
    path="org/scala-lang/$a/$SCALA_VERSION/$a-$SCALA_VERSION.jar"
    fetch "$path" "$jar.part"
    fetch "$path.sha1" "$jar.sha1"
    want="$(cut -c1-40 "$jar.sha1")"
    got="$(sha1sum "$jar.part" | cut -c1-40)"
    [ "$want" = "$got" ] || { echo "scala: checksum mismatch for $path"; rm -f "$jar.part"; exit 1; }
    mv "$jar.part" "$jar"
  fi
  CP="$CP${CP:+:}$jar"
done
LIB="$CACHE/scala-library-$SCALA_VERSION.jar"

java -cp "$CP" scala.tools.nsc.Main -usejavacp:false -classpath "$LIB" \
  -deprecation -feature -unchecked -Xlint:_ -Werror \
  -d "$HERE/build" "$HERE/gen/scala/bench_complex.scala" "$HERE/gen/scala/edge.scala" "$HERE/Test.scala" 2>&1 \
  | grep -v '^Picked up JAVA_TOOL_OPTIONS' || true
[ -f "$HERE/build/Test.class" ] || { echo "scala: compilation failed"; exit 1; }
java -cp "$HERE/build:$LIB" Test "$ROOT/cross_lang_test" 2> >(grep -v '^Picked up JAVA_TOOL_OPTIONS' >&2)
