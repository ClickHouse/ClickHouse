#!/bin/bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CMAKE_FLAGS+=" -DENABLE_LIBRARIES=0 "
. $CUR_DIR/build_no_submodules.sh
