#!/usr/bin/env bash
# Regenerates hibachi-vectors.tsv from the Java packer and signer (FBC-8mzv). Needs a
# FueledByChaiTrading checkout with commons/hibachi-common-api compiled (mvn -pl
# commons/hibachi-common-api -am compile) and its dependencies in the local Maven repository;
# reads nothing else and touches nothing outside this directory and a temporary build directory.
#
#   FBC_JAVA_ROOT=/path/to/FueledByChaiTrading fixtures/hibachi/signing/run-oracle.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
: "${FBC_JAVA_ROOT:?set FBC_JAVA_ROOT to a FueledByChaiTrading checkout}"
API="$FBC_JAVA_ROOT/commons/hibachi-common-api"
[ -d "$API/target/classes" ] || { echo "compile $API first" >&2; exit 1; }
SRC="$API/src/main/java/com/fueledbychai/hibachi/common/api"
SIGNER_FILES=(
  "$SRC/signer/HibachiPayloadPacker.java"
  "$SRC/signer/EcdsaHibachiSigner.java"
  "$SRC/HibachiContract.java"
  "$SRC/order/HibachiSide.java"
)
COMMIT="$(git -C "$FBC_JAVA_ROOT" rev-parse HEAD)"
CHANGES="$(git -C "$FBC_JAVA_ROOT" status --porcelain -- "${SIGNER_FILES[@]}" | wc -l | tr -d ' ')"
BUILD="$(mktemp -d)"
trap 'rm -rf "$BUILD"' EXIT
(cd "$API" && mvn -o -q dependency:build-classpath -Dmdep.outputFile="$BUILD/cp.txt" >/dev/null)
CP="$API/target/classes:$(cat "$BUILD/cp.txt")"
javac -nowarn -d "$BUILD" -cp "$CP" "$HERE/HibachiSignOracle.java"
java -cp "$BUILD:$CP" HibachiSignOracle "$HERE/hibachi-vectors.tsv" "$COMMIT" "$CHANGES"
echo "java signer at ${COMMIT:0:8} ($CHANGES local changes to the signer files)"
