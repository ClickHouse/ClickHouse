#!/usr/bin/env bash
set -e -x

# How to run:
# From "ci" directory:
#     jobs/quick-build/run.sh
# or:
#     ./run-with-docker.sh ubuntu:bionic jobs/quick-build/run.sh

cd "$(dirname $0)"/../..

. default-config

SOURCES_METHOD=local
COMPILER=clang
COMPILER_INSTALL_METHOD=packages
COMPILER_PACKAGE_VERSION=6.0
BUILD_METHOD=normal
BUILD_TARGETS=clickhouse
BUILD_TYPE=Debug
ENABLE_EMBEDDED_COMPILER=0

CMAKE_FLAGS="-D CMAKE_C_FLAGS_ADD=-g0 -D CMAKE_CXX_FLAGS_ADD=-g0 -D ENABLE_JEMALLOC=0 -D ENABLE_CAPNP=0 -D ENABLE_RDKAFKA=0 -D ENABLE_UNWIND=0 -D ENABLE_ICU=0 -D ENABLE_POCO_MONGODB=0 -D ENABLE_POCO_REDIS=0 -D ENABLE_POCO_NETSSL=0 -D ENABLE_ODBC=0 -D ENABLE_MYSQL=0 -D ENABLE_SSL=0 -D ENABLE_POCO_NETSSL=0 -D ENABLE_CASSANDRA=0 -D ENABLE_LDAP=0"

[[ $(uname) == "FreeBSD" ]] && COMPILER_PACKAGE_VERSION=devel && export COMPILER_PATH=/usr/local/bin

. get-sources.sh
. prepare-toolchain.sh
. install-libraries.sh
. build-normal.sh
