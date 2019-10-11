#!/usr/bin/env bash
set -e -x

source default-config

./install-os-packages.sh svn
./install-os-packages.sh cmake

mkdir "${WORKSPACE}/llvm"

svn co "http://llvm.org/svn/llvm-project/llvm/${CLANG_SOURCES_BRANCH}" "${WORKSPACE}/llvm/llvm"
svn co "http://llvm.org/svn/llvm-project/cfe/${CLANG_SOURCES_BRANCH}" "${WORKSPACE}/llvm/llvm/tools/clang"
svn co "http://llvm.org/svn/llvm-project/lld/${CLANG_SOURCES_BRANCH}" "${WORKSPACE}/llvm/llvm/tools/lld"
svn co "http://llvm.org/svn/llvm-project/polly/${CLANG_SOURCES_BRANCH}" "${WORKSPACE}/llvm/llvm/tools/polly"
svn co "http://llvm.org/svn/llvm-project/clang-tools-extra/${CLANG_SOURCES_BRANCH}" "${WORKSPACE}/llvm/llvm/tools/clang/tools/extra"
svn co "http://llvm.org/svn/llvm-project/compiler-rt/${CLANG_SOURCES_BRANCH}" "${WORKSPACE}/llvm/llvm/projects/compiler-rt"
svn co "http://llvm.org/svn/llvm-project/libcxx/${CLANG_SOURCES_BRANCH}" "${WORKSPACE}/llvm/llvm/projects/libcxx"
svn co "http://llvm.org/svn/llvm-project/libcxxabi/${CLANG_SOURCES_BRANCH}" "${WORKSPACE}/llvm/llvm/projects/libcxxabi"

mkdir "${WORKSPACE}/llvm/build"
cd "${WORKSPACE}/llvm/build"

# NOTE You must build LLVM with the same ABI as ClickHouse.
# For example, if you compile ClickHouse with libc++, you must add
#  -DLLVM_ENABLE_LIBCXX=1
# to the line below.

cmake -DCMAKE_BUILD_TYPE:STRING=Release -DLLVM_ENABLE_LIBCXX=1 -DLLVM_ENABLE_RTTI=1 ../llvm

make -j $THREADS
$SUDO make install
hash clang

cd ../../..

export CC=clang
export CXX=clang++
