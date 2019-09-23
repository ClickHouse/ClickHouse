#!/usr/bin/env bash
set -e -x

source default-config

# TODO Install from PPA on older Ubuntu

./install-os-packages.sh ${COMPILER}-${COMPILER_PACKAGE_VERSION}

if [[ "$COMPILER" == "gcc" ]]; then
    if command -v gcc-${COMPILER_PACKAGE_VERSION}; then export CC=gcc-${COMPILER_PACKAGE_VERSION} CXX=g++-${COMPILER_PACKAGE_VERSION};
    elif command -v gcc${COMPILER_PACKAGE_VERSION}; then export CC=gcc${COMPILER_PACKAGE_VERSION} CXX=g++${COMPILER_PACKAGE_VERSION};
    elif command -v gcc; then export CC=gcc CXX=g++;
    fi
elif [[ "$COMPILER" == "clang" ]]; then
    if command -v clang-${COMPILER_PACKAGE_VERSION}; then export CC=clang-${COMPILER_PACKAGE_VERSION} CXX=clang++-${COMPILER_PACKAGE_VERSION};
    elif command -v clang${COMPILER_PACKAGE_VERSION}; then export CC=clang${COMPILER_PACKAGE_VERSION} CXX=clang++${COMPILER_PACKAGE_VERSION};
    elif command -v clang; then export CC=clang CXX=clang++;
    fi
else
    die "Unknown compiler specified"
fi
