#!/bin/sh

#  How to build ClickHouse under freebsd 11+

#  Variant 1: Use pkg:
# pkg install databases/clickhouse

#  Variant 2: Use ports:
# make -C /usr/ports/databases/clickhouse install clean

#  Run server:
# echo clickhouse_enable="YES" >> /etc/rc.conf.local
# service clickhouse restart


#  Variant 3: Manual build:


# pkg install -y curl sudo
# curl https://raw.githubusercontent.com/yandex/ClickHouse/master/utils/build/build_freebsd.sh | sh

#  install compiler and libs
sudo pkg install devel/git devel/cmake devel/ninja shells/bash devel/icu devel/libltdl databases/unixODBC devel/google-perftools devel/libdouble-conversion archivers/zstd archivers/liblz4 devel/sparsehash devel/re2

#  install testing only stuff if you want:
sudo pkg install lang/python devel/py-lxml devel/py-termcolor www/py-requests ftp/curl perl5

#  If you want ODBC support: Check UNIXODBC option:
# make -C /usr/ports/devel/poco config reinstall

BASE_DIR=$(dirname $0) && [ -f "$BASE_DIR/../../CMakeLists.txt" ] && ROOT_DIR=$BASE_DIR/../.. && cd $ROOT_DIR

if [ -z $ROOT_DIR ]; then
    #  Checkout ClickHouse sources
    git clone --recursive https://github.com/yandex/ClickHouse.git
    cd ClickHouse
fi

#  Build!
mkdir -p build
cd build
cmake .. -DUNBUNDLED=1 -DUSE_STATIC_LIBRARIES=0
cmake --build .
cd ..

#  Run server:
# build/programs/clickhouse-server --config-file=ClickHouse/programs/server/config.xml &

#  Run client:
# build/programs/clickhouse-client
