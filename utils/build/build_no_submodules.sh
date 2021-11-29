#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

cd ${CUR_DIR}/../..
BRANCH=`git rev-parse --abbrev-ref HEAD`
BRANCH=${BRANCH:=master}
ROOT_DIR=${CUR_DIR}/../../build_no_submodules
mkdir -p $ROOT_DIR
cd $ROOT_DIR
URL=`git remote get-url origin | sed 's/.git$//'`
wget -nv -O ch.zip $URL/archive/${BRANCH}.zip
unzip -ou ch.zip

# TODO: make disableable lz4 zstd
# TODO: USE_INTERNAL_DOUBLE_CONVERSION_LIBRARY : cmake test
# Shared because /usr/bin/ld.gold: error: /usr/lib/x86_64-linux-gnu/libcrypto.a(err.o): multiple definition of 'ERR_remove_thread_state'
CMAKE_FLAGS+="-DUSE_STATIC_LIBRARIES=0 -DUSE_INTERNAL_DOUBLE_CONVERSION_LIBRARY=0 $CMAKE_FLAGS"
EXTRAPACKAGES+="libboost-program-options-dev libboost-system-dev libboost-filesystem-dev libboost-thread-dev libboost-iostreams-dev libboost-regex-dev liblz4-dev libzstd-dev libpoco-dev libdouble-conversion-dev libcctz-dev libre2-dev libsparsehash-dev $EXTRAPACKAGES"
. $ROOT_DIR/ClickHouse-${BRANCH}/release
