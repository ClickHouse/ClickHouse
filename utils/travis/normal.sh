#!/bin/sh

# manual run:
# env CXX=g++-7 CC=gcc-7 utils/travis/normal.sh
# env CXX=clang++-5.0 CC=clang-5.0 utils/travis/normal.sh

set -e
set -x

ccache -s
ccache -M 6G
mkdir -p build
cd build
cmake .. -DCMAKE_CXX_COMPILER=`which $CXX` -DCMAKE_C_COMPILER=`which $CC` -DCMAKE_C_FLAGS_ADD=-O0 -DCMAKE_CXX_FLAGS_ADD=-O0 -DCMAKE_C_COMPILER_LAUNCHER=/usr/bin/ccache -DCMAKE_CXX_COMPILER_LAUNCHER=/usr/bin/ccache -DUNBUNDLED=1 -DENABLE_CAPNP=0 -DENABLE_RDKAFKA=0 -DUSE_EMBEDDED_COMPILER=0 -DENABLE_TCMALLOC=0 -DENABLE_UNWIND=0 -DENABLE_MYSQL=0 && make -j `nproc || grep -c ^processor /proc/cpuinfo` clickhouse-bundle && ( ( cd .. && env TEST_OPT="--no-long" sh -x dbms/tests/server_wrapper.sh ) || true )
