#!/usr/bin/env bash
set -e -x

source default-config

./install-os-packages.sh cmake
./install-os-packages.sh ninja

if [[ "$COMPILER_INSTALL_METHOD" == "packages" ]]; then
    . install-compiler-from-packages.sh
elif [[ "$COMPILER_INSTALL_METHOD" == "sources" ]]; then
    . install-compiler-from-sources.sh
else
    die "Unknown COMPILER_INSTALL_METHOD"
fi
