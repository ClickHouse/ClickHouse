#!/usr/bin/env bash

set -e

BRANCH=trunk
#BRANCH=tags/RELEASE_500/final

THREADS=$(grep -c ^processor /proc/cpuinfo)

cd ~
sudo apt-get install -y subversion cmake

mkdir llvm
cd llvm
svn co "http://llvm.org/svn/llvm-project/llvm/${BRANCH}" llvm

cd llvm/tools
svn co "http://llvm.org/svn/llvm-project/cfe/${BRANCH}" clang
svn co "http://llvm.org/svn/llvm-project/lld/${BRANCH}" lld
svn co "http://llvm.org/svn/llvm-project/polly/${BRANCH}" polly

cd clang/tools
svn co "http://llvm.org/svn/llvm-project/clang-tools-extra/${BRANCH}" extra

git clone https://github.com/include-what-you-use/include-what-you-use.git
echo 'add_subdirectory(include-what-you-use)' >> CMakeLists.txt
sudo apt-get install libncurses5-dev

cd ../../../..
cd llvm/projects/
svn co "http://llvm.org/svn/llvm-project/compiler-rt/${BRANCH}" compiler-rt
svn co "http://llvm.org/svn/llvm-project/libcxx/${BRANCH}" libcxx
svn co "http://llvm.org/svn/llvm-project/libcxxabi/${BRANCH}" libcxxabi

cd ../..
mkdir build
cd build/
cmake -D CMAKE_BUILD_TYPE:STRING=Release ../llvm
make -j $THREADS
sudo make install
hash clang
