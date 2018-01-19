#!/bin/sh

# env CXX=clang++-5.0 CC=clang-5.0 DH_VERBOSE=1 utils/travis/pbuilder.sh

set -e
set -x

df -h

env TEST_RUN=1 \
    `# Skip tests:` \
    `# 00281 requires internal compiler` \
    `# 00416 requires patched poco from contrib/` \
    TEST_OPT="--no-long --skip 00281 00416" \
    TEST_TRUE=false \
    `# travisci will not upload ccache cache after timeout (48min), use our less timeout` \
    PBUILDER_OPT="--timeout 35m" \
    `# clang faster than gcc` \
    DEB_CC=$CC DEB_CXX=$CXX \
    CCACHE_SIZE=4G CCACHEDIR=$HOME/.ccache \
    `# Disable all features` \
    CMAKE_FLAGS="-DCMAKE_BUILD_TYPE=Debug -DUNBUNDLED=1 -DENABLE_UNWIND=0 -DENABLE_MYSQL=0 -DENABLE_CAPNP=0 -DENABLE_RDKAFKA=0 -DUSE_EMBEDDED_COMPILER=0 -DCMAKE_C_FLAGS_ADD='-O0 -g0' -DCMAKE_CXX_FLAGS_ADD='-O0 -g0'" \
    `# Use all possible contrib libs from system` \
    `# psmisc - killall` \
    `# gdb - symbol test in pbuilder` \
    EXTRAPACKAGES="psmisc gdb clang-5.0 libc++abi-dev libc++-dev libboost-dev libboost-program-options-dev zlib1g-dev liblz4-dev libdouble-conversion-dev libzookeeper-mt-dev libsparsehash-dev librdkafka-dev libpoco-dev libsparsehash-dev libgoogle-perftools-dev libzstd-dev libre2-dev" \
    ./release --pbuilder
