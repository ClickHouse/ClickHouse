#!/bin/sh
# sudo apt install time
# Small .h isolated compile checker
# Finds missing #include <...>
# prints compile time, number of includes, use with sort: ./check_include.sh 2>&1 | sort -rk3
# use with clang: CXX=`which clang++-7 clang++-7.0 clang++-6.0 clang++-5.0 | head -n1` ./check_include.sh
pwd=`pwd`
BUILD_DIR=${BUILD_DIR:=./build}
inc="-I. \
-I./contrib/libdivide \
-I./contrib/re2 \
-I${BUILD_DIR}/contrib/re2_st \
-I./contrib/libfarmhash \
-I./contrib/libmetrohash/src \
-I./contrib/double-conversion \
-I./contrib/cityhash102/include \
-I./contrib/zookeeper/src/c/include \
-I./contrib/zookeeper/src/c/generated \
-I./contrib/libtcmalloc/include \
-I${BUILD_DIR}/contrib/zlib-ng \
-I./contrib/zlib-ng \
-I./contrib/poco/MongoDB/include \
-I./contrib/poco/XML/include \
-I./contrib/poco/Crypto/include \
-I./contrib/poco/Data/ODBC/include \
-I./contrib/poco/Data/include \
-I./contrib/poco/Net/include \
-I./contrib/poco/Util/include \
-I./contrib/poco/Foundation/include \
-I./contrib/boost/libs/*/include \
-I./contrib/boost \
-I./contrib/llvm/llvm/include \
-I${BUILD_DIR}/contrib/llvm/llvm/include \
-I./contrib/libbtrie/include \
-I./contrib/libpcg-random/include \
-I./libs/libmysqlxx/include \
-I./libs/libcommon/include \
-I${BUILD_DIR}/libs/libcommon/include \
-I./libs/libpocoext/include \
-I./libs/libzkutil/include \
-I./libs/libdaemon/include \
-I./libs/libconsistent-hashing \
-I./dbms/src \
-I${BUILD_DIR}/dbms/src"

if [ -z $1 ]; then
    cd ..
    find dbms libs utils \( -name *.h -and -not -name *.inl.h \) -exec sh $pwd/$0 {} \; ;
else
    echo -n "$1    "
    echo -n `grep "#include" $1| wc -l` "    "
    echo -e "#include <$1> \n int main() {return 0;}" | time --format "%e %M" ${CXX:=g++-7} -c -std=c++1z $inc -x c++ -
fi
