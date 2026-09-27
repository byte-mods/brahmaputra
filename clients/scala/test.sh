#!/usr/bin/env bash
# Build the Scala wrapper (and the Java driver it wraps) and run the end-to-end suite.
#
#   ./test.sh HOST PORT
#
# Needs a JDK (17+) and curl — no sbt, no scala-cli. The Scala 3 compiler and library are
# fetched once from Maven Central into ~/.cache/brahmaputra-jvm (override with
# BRAHMAPUTRA_JVM_CACHE), checked against Central's SHA-1s, and reused after that.
# Exits non-zero if the build fails or any check fails. Works from any cwd.
set -euo pipefail

HOST="${1:-127.0.0.1}"
PORT="${2:-9092}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAVA_SRC="$DIR/../java/src/main/java"
OUT="$DIR/build/test-sh"
CACHE="${BRAHMAPUTRA_JVM_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/brahmaputra-jvm}/maven"
CENTRAL="${MAVEN_CENTRAL:-https://repo1.maven.org/maven2}"

SCALA=3.3.8          # the Scala 3 LTS line
SCALA2_LIBRARY=2.13.18

# fetch group:artifact:version -> prints the cached jar path
fetch() {
    local group="${1%%:*}" rest="${1#*:}"
    local artifact="${rest%%:*}" version="${rest#*:}"
    local path="${group//.//}/$artifact/$version/$artifact-$version.jar"
    local jar="$CACHE/$path"
    if [[ ! -s "$jar" ]]; then
        mkdir -p "$(dirname "$jar")"
        echo "fetching $1" >&2
        curl -fsSL --retry 3 -o "$jar.part" "$CENTRAL/$path"
        local want got
        want="$(curl -fsSL --retry 3 "$CENTRAL/$path.sha1" | cut -c1-40)"
        got="$(sha1sum "$jar.part" | cut -c1-40)"
        if [[ "$want" != "$got" ]]; then
            rm -f "$jar.part"
            echo "checksum mismatch for $1 ($got != $want)" >&2
            return 1
        fi
        mv "$jar.part" "$jar"
    fi
    printf '%s' "$jar"
}

classpath() {
    local cp="" coordinate
    for coordinate in "$@"; do
        cp="$cp${cp:+:}$(fetch "$coordinate")"
    done
    printf '%s' "$cp"
}

# What the wrapper compiles against and runs with.
LIB_CP="$(classpath \
    org.scala-lang:scala3-library_3:$SCALA \
    org.scala-lang:scala-library:$SCALA2_LIBRARY)"
# What the compiler itself runs on (scala3-compiler_3's compile-time dependencies; the
# jline jars it also lists are only for the REPL).
COMPILER_CP="$(classpath \
    org.scala-lang:scala3-compiler_3:$SCALA \
    org.scala-lang:scala3-interfaces:$SCALA \
    org.scala-lang:tasty-core_3:$SCALA \
    org.scala-lang.modules:scala-asm:9.9.0-scala-1 \
    org.scala-sbt:compiler-interface:1.10.7 \
    org.scala-sbt:util-interface:1.10.7):$LIB_CP"

rm -rf "$OUT"
mkdir -p "$OUT/classes"

# 1. The Java driver — compiled from clients/java, never copied.
javac -Xlint:all -Werror --release 17 -d "$OUT/classes" $(find "$JAVA_SRC" -name '*.java')

# 2. The Scala wrapper and the suite, against it.
java -Xss8m -cp "$COMPILER_CP" dotty.tools.dotc.Main \
    -deprecation -feature -Werror -release 17 \
    -classpath "$OUT/classes:$LIB_CP" -d "$OUT/classes" \
    $(find "$DIR/src/main/scala" "$DIR/src/test/scala" -name '*.scala')

exec java -cp "$OUT/classes:$LIB_CP" io.brahmaputra.scaladsl.ManualTest "$HOST" "$PORT"
