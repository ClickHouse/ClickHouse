#!/bin/bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd ../.. && pwd)

CMAKE_FLAGS="-DUSE_STATIC_LIBRARIES=0 -DCLICKHOUSE_SPLIT_BINARY=1 $CMAKE_FLAGS"
. $ROOT_DIR/release
