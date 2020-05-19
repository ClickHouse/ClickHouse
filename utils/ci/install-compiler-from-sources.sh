#!/usr/bin/env bash
set -e -x

source default-config

if [[ "$COMPILER" == "gcc" ]]; then
    . build-gcc-from-sources.sh
elif [[ "$COMPILER" == "clang" ]]; then
    . build-clang-from-sources.sh
else
    die "Unknown COMPILER"
fi
