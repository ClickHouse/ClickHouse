#!/usr/bin/env bash
set -euo pipefail

CXX=${CXX:-clang++-21}
LLVM_CONFIG=${LLVM_CONFIG:-llvm-config-$("$CXX" --version | sed -n 's/.*clang version \([0-9]*\).*/\1/p')}
SOURCE_DIR=$(cd "$(dirname "$0")" && pwd)
OUTPUT=${1:-$SOURCE_DIR/SilkThreadLocalStorageSanitizerPass.so}

# shellcheck disable=SC2046
"$CXX" -O2 -fPIC -shared $("$LLVM_CONFIG" --cxxflags) "$SOURCE_DIR/SilkThreadLocalStorageSanitizerPass.cpp" -o "$OUTPUT"

"$CXX" -fpass-plugin="$OUTPUT" -fsyntax-only -x c++ /dev/null

echo "Built $OUTPUT"
