#!/bin/bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CMAKE_FLAGS+=" -DCLICKHOUSE_SPLIT_BINARY=1 "
. $CUR_DIR/build_debian_unbundled.sh
