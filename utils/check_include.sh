#!/bin/sh
# Small .h isolated compile checker
# Finds missing #include <...>
# prints compile time, number of includes, use with sort: ./check_include.sh 2>&1 | sort -rk3
pwd=`pwd`
inc="-I. \
-I./contrib/libdivide \
-I./contrib/libre2 \
-I./build/contrib/libre2 \
-I./contrib/libfarmhash \
-I./contrib/libmetrohash/src \
-I./contrib/libdouble-conversion \
-I./contrib/libcityhash/include \
-I./contrib/libzookeeper/include \
-I./contrib/libtcmalloc/include \
-I./build/contrib/libzlib-ng \
-I./contrib/libzlib-ng \
-I./contrib/libpoco/MongoDB/include \
-I./contrib/libpoco/XML/include \
-I./contrib/libpoco/Crypto/include \
-I./contrib/libpoco/Data/ODBC/include \
-I./contrib/libpoco/Data/include \
-I./contrib/libpoco/Net/include \
-I./contrib/libpoco/Util/include \
-I./contrib/libpoco/Foundation/include \
-I./contrib/libboost/boost_1_62_0 \
-I./contrib/libbtrie/include \
-I./libs/libmysqlxx/include \
-I./libs/libcommon/include \
-I./build/libs/libcommon/include \
-I./libs/libpocoext/include \
-I./libs/libzkutil/include \
-I./libs/libdaemon/include \
-I./dbms/src \
-I./build/dbms/src"

if [ -z $1 ]; then
    cd ..
    find dbms libs utils \( -name *.h -and -not -name *.inl.h \) -exec sh $pwd/$0 {} \; ;
else
    echo -n "$1    "
    echo -n `grep "#include" $1| wc -l` "    "
    echo -e "#include <$1> \n int main() {return 0;}" | bash -c "TIMEFORMAT='%3R'; time g++-6 -c -std=gnu++1z $inc -x c++ -"
fi
