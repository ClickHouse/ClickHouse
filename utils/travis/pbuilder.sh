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
    `# clang faster than gcc` \
    DEB_CC=${DEB_CC=$CC} DEB_CXX=${DEB_CXX=$CXX} \
    CCACHE_SIZE=${CCACHE_SIZE:=4G} \
    `# Disable all features` \
    CMAKE_FLAGS="-DCMAKE_BUILD_TYPE=Debug -DUNBUNDLED=1 -DENABLE_UNWIND=0 -DENABLE_MYSQL=0 -DENABLE_CAPNP=0 -DENABLE_RDKAFKA=0 -DENABLE_EMBEDDED_COMPILER=1 -DCMAKE_C_FLAGS_ADD='-O0 -g0' -DCMAKE_CXX_FLAGS_ADD='-O0 -g0' $CMAKE_FLAGS" \
    `# Use all possible contrib libs from system` \
    `# psmisc - killall` \
    EXTRAPACKAGES="psmisc clang-5.0 lld-5.0 liblld-5.0-dev libclang-5.0-dev liblld-5.0 libc++abi-dev libc++-dev libboost-program-options-dev libboost-system-dev libboost-filesystem-dev libboost-thread-dev zlib1g-dev liblz4-dev libdouble-conversion-dev libzookeeper-mt-dev libsparsehash-dev librdkafka-dev libpoco-dev libsparsehash-dev libgoogle-perftools-dev libzstd-dev libre2-dev $EXTRAPACKAGES" \
    $CUR_DIR/../../release $RELEASE_OPT

date
