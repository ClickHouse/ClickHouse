#!/bin/sh

# Manual run:
# env CXX=g++-7 CC=gcc-7 utils/travis/normal.sh
# env CXX=clang++-5.0 CC=clang-5.0 utils/travis/normal.sh

set -e
set -x

TEST_TRUE=${TEST_TRUE:=false}

# clean not used ~600mb
[ -n "$TRAVIS" ] && rm -rf .git contrib/poco/openssl

ccache -s
ccache -M 4G
df -h

mkdir -p build
cd build
cmake .. -DCMAKE_CXX_COMPILER=`which $CXX` -DCMAKE_C_COMPILER=`which $CC` \
    `# Does not optimize to speedup build, skip debug info to use less disk` \
    -DCMAKE_C_FLAGS_ADD="-O0 -g0" -DCMAKE_CXX_FLAGS_ADD="-O0 -g0" \
    `# ignore ccache disabler on trusty` \
    -DCMAKE_C_COMPILER_LAUNCHER=`which ccache` -DCMAKE_CXX_COMPILER_LAUNCHER=`which ccache` \
    `# Use all possible contrib libs from system` \
    -DUNBUNDLED=1 \
    `# Disable all features` \
    -DENABLE_CAPNP=0 -DENABLE_RDKAFKA=0 -DUSE_EMBEDDED_COMPILER=0 -DENABLE_TCMALLOC=0 -DENABLE_UNWIND=0 -DENABLE_MYSQL=0 \
    && make -j `nproc || grep -c ^processor /proc/cpuinfo || sysctl -n hw.ncpu || echo 4` clickhouse-bundle \
    `# Skip tests:` \
    `# 00281 requires internal compiler` \
    `# 00428 requires sudo (not all vms allow this)` \
    && ( ( cd .. && env TEST_OPT="--no-long --no-shard --skip 00281 00428" bash -x dbms/tests/server_wrapper.sh ) || $TEST_TRUE )
