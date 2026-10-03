#!/usr/bin/env bash
# Regenerates paradex-vectors.tsv from the Java signer (FBC-6). Needs a FueledByChaiTrading
# checkout with commons/paradex-common-api compiled (mvn -pl commons/paradex-common-api -am
# compile) and its dependencies in the local Maven repository; reads nothing else and touches
# nothing outside this directory and a temporary build directory.
#
#   FBC_JAVA_ROOT=/path/to/FueledByChaiTrading fixtures/paradex/signing/run-oracle.sh [--bench]
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
: "${FBC_JAVA_ROOT:?set FBC_JAVA_ROOT to a FueledByChaiTrading checkout}"
API="$FBC_JAVA_ROOT/commons/paradex-common-api"
[ -d "$API/target/classes" ] || { echo "compile $API first" >&2; exit 1; }
BUILD="$(mktemp -d)"
trap 'rm -rf "$BUILD"' EXIT
(cd "$API" && mvn -o -q dependency:build-classpath -Dmdep.outputFile="$BUILD/cp.txt" >/dev/null)
CP="$API/target/classes:$(cat "$BUILD/cp.txt")"
javac -nowarn -d "$BUILD" -cp "$CP" "$HERE/ParadexHashOracle.java"
java --enable-native-access=ALL-UNNAMED -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp "$BUILD:$CP" ParadexHashOracle "$HERE/paradex-vectors.tsv" "$@"
echo "java signer at $(git -C "$FBC_JAVA_ROOT" rev-parse --short HEAD) ($(git -C "$FBC_JAVA_ROOT" status --porcelain -- "$API/src/main/java/com/fueledbychai/paradex/common/api/ParadexTypedDataSigner.java" "$API/src/main/java/com/fueledbychai/paradex/common/api/BcStarknetCurveSigner.java" "$API/src/main/java/com/fueledbychai/paradex/common/api/order" | wc -l | tr -d ' ') local changes to the signer files)"
