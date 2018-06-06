#!/bin/bash

# Manual run:
# env CXX=g++-7 CC=gcc-7 utils/travis/normal.sh
# env CXX=clang++-5.0 CC=clang-5.0 utils/travis/normal.sh

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CXX=${CXX=g++}
CC=${CC=gcc}

set -e
set -x

date

# clean not used ~600mb
[ -n "$TRAVIS" ] && rm -rf .git contrib/poco/openssl

ccache -s
ccache -M ${CCACHE_SIZE:=4G}
df -h

date

mkdir -p build
cd build
cmake $CUR_DIR/../.. -DCMAKE_CXX_COMPILER=`which $DEB_CXX $CXX` -DCMAKE_C_COMPILER=`which $DEB_CC $CC` \
    `# Does not optimize to speedup build, skip debug info to use less disk` \
    -DCMAKE_C_FLAGS_ADD="-O0 -g0" -DCMAKE_CXX_FLAGS_ADD="-O0 -g0" \
    `# ignore ccache disabler on trusty` \
    -DCMAKE_C_COMPILER_LAUNCHER=`which ccache` -DCMAKE_CXX_COMPILER_LAUNCHER=`which ccache` \
    `# Use all possible contrib libs from system` \
    -DUNBUNDLED=1 \
    `# Disable all features` \
    -DENABLE_CAPNP=0 -DENABLE_RDKAFKA=0 -DENABLE_EMBEDDED_COMPILER=0 -DENABLE_TCMALLOC=0 -DENABLE_UNWIND=0 -DENABLE_MYSQL=0 -DUSE_INTERNAL_LLVM_LIBRARY=0 $CMAKE_FLAGS \
    && make -j `nproc || grep -c ^processor /proc/cpuinfo || sysctl -n hw.ncpu || echo 4` clickhouse-bundle \
    `# Skip tests:` \
    `# 00281 requires internal compiler` \
    `# 00428 requires sudo (not all vms allow this)` \
    `# 00385 runs infinitly (TODO: fix it)` \
    && ( [ ! ${TEST_RUN=1} ] || ( ( cd $CUR_DIR/../.. && env TEST_OPT="--skip long compile 00428 00385 $TEST_OPT" TEST_PORT_RANDOM= TEST_PERF= bash -x dbms/tests/clickhouse-test-server ) || ${TEST_TRUE=false} ) )

date
