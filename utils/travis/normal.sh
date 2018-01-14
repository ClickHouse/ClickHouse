#!/bin/sh

set -e

ccache -s
ccache -M 6G
mkdir build
cd build
cmake .. -DCMAKE_CXX_COMPILER=`which $CXX` -DCMAKE_C_COMPILER=`which $CC` -DCMAKE_C_FLAGS_ADD=-O0 -DCMAKE_CXX_FLAGS_ADD=-O0 -DCMAKE_C_COMPILER_LAUNCHER=/usr/bin/ccache -DCMAKE_CXX_COMPILER_LAUNCHER=/usr/bin/ccache -DUNBUNDLED=1 -DENABLE_CAPNP=0 -DENABLE_RDKAFKA=0 -DUSE_EMBEDDED_COMPILER=0 -DENABLE_TCMALLOC=0 -DENABLE_UNWIND=0 -DENABLE_MYSQL=0 && make -j `nproc || grep -c ^processor /proc/cpuinfo` clickhouse-bundle && ( ( cd .. && dbms/tests/server_wrapper.sh ) || true )
