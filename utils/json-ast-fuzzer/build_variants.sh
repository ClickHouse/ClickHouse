#!/bin/bash
# Configure + build the three fuzzer binaries for a sanitizer/build variant.
# Usage: build_variants.sh <variant>   where variant = asan_ubsan | debug | tsan | msan | release
# Builds into $REPO/build_fuzz_<variant> (asan_ubsan uses $REPO/build_fuzz). Pause the chain first on a shared box:
#   touch $WORK/STOP_ROUNDS; and wait_for_quiet.sh before building (heavy: ~2-3h, 50-100 GB, 32 compilers).
set -e
. "$(dirname "$(readlink -f "$0")")/env.sh"
V="${1:?variant: asan_ubsan|debug|tsan|msan|release}"
case "$V" in
  asan_ubsan) B="$BUILD_FUZZ"; SAN="-DSANITIZE=address,undefined" ;;
  tsan)       B="$REPO/build_fuzz_tsan";  SAN="-DSANITIZE=thread" ;;
  msan)       B="$REPO/build_fuzz_msan";  SAN="-DSANITIZE=memory" ;;
  debug)      B="$REPO/build_fuzz_debug"; SAN="-DSANITIZE= -DCMAKE_BUILD_TYPE=Debug" ;;   # chassert/LOGICAL_ERROR aborts, fuzzes at -O0 (slow)
  release)    B="$REPO/build_fuzz_release"; SAN="-DSANITIZE=" ;;
  *) echo "unknown variant $V"; exit 1 ;;
esac
echo "configuring $B ($V) ..."
cmake -S "$REPO" -B "$B" -G Ninja \
  -DCMAKE_C_COMPILER="$(command -v clang-21 || command -v clang)" \
  -DCMAKE_CXX_COMPILER="$(command -v clang++-21 || command -v clang++)" \
  -DENABLE_FUZZING=1 -DENABLE_TESTS=0 -DENABLE_LIBRARIES=ON -DENABLE_EMBEDDED_COMPILER=ON \
  $SAN > "$B.configure.log" 2>&1
echo "building (no -j; ninja auto-parallelises) ..."
ninja -C "$B" json_ast_sql_execution_fuzzer json_ast_sql_parser_fuzzer json_ast_seed_converter > "$B.build.log" 2>&1
echo "exit $?; binary: $B/programs/local/fuzzers/json_ast_sql_execution_fuzzer"
echo "stage it for the chain:  cp <that> $WORK/json_ast_sql_execution_fuzzer_${V}.bin"
