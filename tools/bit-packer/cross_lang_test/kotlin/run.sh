#!/usr/bin/env bash
# Kotlin conformance test for the BitPacker `kotlin` target.
# Usage: [BITPACKER=/path/to/bitpacker] ./run.sh   (works from any cwd)
# Needs a JDK (17+). The Kotlin 2.x compiler is fetched once from Maven
# Central into .cache/ (gitignored); the apt kotlinc (1.3) is too old.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
KOTLIN_VERSION=2.2.20
# Maven Central, then its Apache mirror (Central answers 429 under load).
MAVEN_MIRRORS="${MAVEN_REPO:-https://repo1.maven.org/maven2 https://repo.maven.apache.org/maven2}"

# fetch <repo-relative path> <output file>
fetch() {
  local m
  for m in $MAVEN_MIRRORS; do
    curl -fsSL --retry 4 --retry-delay 5 --retry-all-errors -o "$2" "$m/$1" 2>/dev/null && return 0
  done
  echo "kotlin: could not download $1" >&2
  return 1
}

for c in java curl go; do command -v "$c" >/dev/null 2>&1 || { echo "SKIP kotlin: $c not installed"; exit 77; }; done

if [ -z "${BITPACKER:-}" ]; then
  TMPD="$(mktemp -d)"
  trap 'rm -rf "$TMPD"' EXIT
  (cd "$ROOT" && go build -o "$TMPD/bitpacker" ./cmd/bitpacker)
  BITPACKER="$TMPD/bitpacker"
fi

rm -rf "$HERE/gen" "$HERE/build"
mkdir -p "$HERE/gen" "$HERE/build"
"$BITPACKER" --file "$ROOT/examples/bench_complex.buff" --lang kotlin --package bench --out "$HERE/gen" >/dev/null
"$BITPACKER" --file "$ROOT/cross_lang_test/edge/edge.buff" --lang kotlin --package edge --out "$HERE/gen" >/dev/null
for f in bench_complex.kt edge.kt; do
  [ -f "$HERE/gen/kotlin/$f" ] || { echo "kotlin: generator did not produce gen/kotlin/$f"; exit 1; }
done

# group:artifact:version of the compiler and its runtime dependencies.
DEPS="
org.jetbrains.kotlin:kotlin-compiler-embeddable:$KOTLIN_VERSION
org.jetbrains.kotlin:kotlin-stdlib:$KOTLIN_VERSION
org.jetbrains.kotlin:kotlin-script-runtime:$KOTLIN_VERSION
org.jetbrains.kotlin:kotlin-daemon-embeddable:$KOTLIN_VERSION
org.jetbrains.kotlin:kotlin-reflect:1.6.10
org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.8.0
org.jetbrains:annotations:13.0
"
CACHE="$HERE/.cache/kotlin-$KOTLIN_VERSION"
mkdir -p "$CACHE"
CP=""
for dep in $DEPS; do
  IFS=: read -r g a v <<<"$dep"
  jar="$CACHE/$a-$v.jar"
  if [ ! -s "$jar" ]; then
    path="${g//.//}/$a/$v/$a-$v.jar"
    fetch "$path" "$jar.part"
    fetch "$path.sha1" "$jar.sha1"
    want="$(cut -c1-40 "$jar.sha1")"
    got="$(sha1sum "$jar.part" | cut -c1-40)"
    [ "$want" = "$got" ] || { echo "kotlin: checksum mismatch for $path"; rm -f "$jar.part"; exit 1; }
    mv "$jar.part" "$jar"
  fi
  CP="$CP${CP:+:}$jar"
done
STDLIB="$CACHE/kotlin-stdlib-$KOTLIN_VERSION.jar"

java -cp "$CP" org.jetbrains.kotlin.cli.jvm.K2JVMCompiler \
  -no-stdlib -no-reflect -classpath "$STDLIB" -Werror -jvm-target 17 \
  -d "$HERE/build" "$HERE/gen/kotlin/bench_complex.kt" "$HERE/gen/kotlin/edge.kt" "$HERE/Test.kt" 2>&1 \
  | grep -v '^Picked up JAVA_TOOL_OPTIONS' || true
[ -f "$HERE/build/TestKt.class" ] || { echo "kotlin: compilation failed"; exit 1; }
java -cp "$HERE/build:$STDLIB" TestKt "$ROOT/cross_lang_test" 2> >(grep -v '^Picked up JAVA_TOOL_OPTIONS' >&2)
