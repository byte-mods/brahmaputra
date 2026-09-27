#!/usr/bin/env bash
# Build the Kotlin wrapper (and the Java driver it wraps) and run the end-to-end suite.
#
#   ./test.sh HOST PORT
#
# Needs a JDK (17+) and curl. The Kotlin compiler, kotlin-stdlib and kotlinx-coroutines
# are fetched once from Maven Central into ~/.cache/brahmaputra-jvm (override with
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

KOTLIN=2.4.20
COROUTINES=1.11.0

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

# What the compiler itself runs on (kotlin-compiler-embeddable's runtime dependencies).
COMPILER_CP="$(classpath \
    org.jetbrains.kotlin:kotlin-compiler-embeddable:$KOTLIN \
    org.jetbrains.kotlin:kotlin-stdlib:$KOTLIN \
    org.jetbrains.kotlin:kotlin-script-runtime:$KOTLIN \
    org.jetbrains.kotlin:kotlin-reflect:1.6.10 \
    org.jetbrains.kotlin:kotlin-daemon-embeddable:$KOTLIN \
    org.jetbrains.kotlin:kotlin-build-tools-api:$KOTLIN \
    org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.8.0 \
    org.jetbrains:annotations:13.0)"
# What the wrapper compiles against and runs with.
LIB_CP="$(classpath \
    org.jetbrains.kotlin:kotlin-stdlib:$KOTLIN \
    org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:$COROUTINES \
    org.jetbrains:annotations:23.0.0)"

rm -rf "$OUT"
mkdir -p "$OUT/classes"

# 1. The Java driver — compiled from clients/java, never copied.
javac -Xlint:all -Werror --release 17 -d "$OUT/classes" $(find "$JAVA_SRC" -name '*.java')

# 2. The Kotlin wrapper and the suite, against it.
java -Xss8m -cp "$COMPILER_CP" org.jetbrains.kotlin.cli.jvm.K2JVMCompiler \
    -no-stdlib -no-reflect -jvm-target 17 -Werror \
    -cp "$OUT/classes:$LIB_CP" -d "$OUT/classes" \
    $(find "$DIR/src/main/kotlin" "$DIR/src/test/kotlin" -name '*.kt')

exec java -cp "$OUT/classes:$LIB_CP" io.brahmaputra.kt.ManualTestKt "$HOST" "$PORT"
