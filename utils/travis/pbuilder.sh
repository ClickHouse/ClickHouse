#!/bin/bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# env CXX=clang++-5.0 CC=clang-5.0 DH_VERBOSE=1 utils/travis/pbuilder.sh

set -e
set -x

df -h

date

env TEST_RUN=${TEST_RUN=1} \
    `# Skip tests:` \
    `# 00281 requires internal compiler` \
    `# 00416 requires patched poco from contrib/` \
    TEST_OPT="--skip long pocopatch $TEST_OPT" \
    TEST_SSL="" `# <Error> Application: SSL context exception: Error loading certificate from file /etc/clickhouse-server/server.crt: No error -- when using system poco on artful` \
    TEST_TRUE=${TEST_TRUE=false} \
    `# travisci will not upload ccache cache after timeout (48min), use our less timeout` \
    PBUILDER_OPT="--timeout ${PBUILDER_TIMEOUT:=35m} $PBUILDER_OPT" \
    `# clang is faster than gcc` \
    DEB_CC=${DEB_CC=$CC} DEB_CXX=${DEB_CXX=$CXX} \
    CCACHE_SIZE=${CCACHE_SIZE:=4G} \
    `# Disable all features` \
    CMAKE_FLAGS="-D CMAKE_BUILD_TYPE=Debug -D UNBUNDLED=1 -D ENABLE_UNWIND=0 -D ENABLE_MYSQL=0 -D ENABLE_CAPNP=0 -D ENABLE_RDKAFKA=0 -D USE_INTERNAL_LLVM_LIBRARY=0 -D CMAKE_C_FLAGS_ADD='-O0 -g0' -D CMAKE_CXX_FLAGS_ADD='-O0 -g0' $CMAKE_FLAGS" \
    `# Use all possible contrib libs from system` \
    `# psmisc - killall` \
    EXTRAPACKAGES="psmisc clang-5.0 lld-5.0 liblld-5.0-dev libclang-5.0-dev liblld-5.0 libc++abi-dev libc++-dev libboost-program-options-dev libboost-system-dev libboost-filesystem-dev libboost-thread-dev zlib1g-dev liblz4-dev libdouble-conversion-dev libsparsehash-dev librdkafka-dev libpoco-dev libsparsehash-dev libgoogle-perftools-dev libzstd-dev libre2-dev $EXTRAPACKAGES" \
    `# Travis trusty cant unpack bionic: E: debootstrap failed, TODO: check again, can be fixed` \
    DIST=${DIST=artful} \
    $CUR_DIR/../../release $RELEASE_OPT

date
