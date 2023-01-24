#!/usr/bin/env bash
set -e -x

source default-config

[[ -d "${WORKSPACE}/sources" ]] || die "Run get-sources.sh first"

mkdir -p "${WORKSPACE}/build"
pushd "${WORKSPACE}/build"

cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DENABLE_EMBEDDED_COMPILER=${ENABLE_EMBEDDED_COMPILER} $CMAKE_FLAGS ../sources

[[ "$BUILD_TARGETS" != 'all' ]] && BUILD_TARGETS_STRING="--target $BUILD_TARGETS"

cmake --build . $BUILD_TARGETS_STRING -- -j $THREADS

popd
