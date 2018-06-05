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
sudo pkg install devel/git devel/cmake shells/bash devel/icu devel/libltdl databases/unixODBC devel/google-perftools devel/libdouble-conversion archivers/zstd archivers/liblz4 devel/sparsehash devel/re2

#  install testing only stuff if you want:
sudo pkg install lang/python devel/py-lxml devel/py-termcolor www/py-requests ftp/curl perl5

#  If you want ODBC support: Check UNIXODBC option:
# make -C /usr/ports/devel/poco config reinstall

#  Checkout ClickHouse sources
git clone --recursive https://github.com/yandex/ClickHouse.git

#  Build!
mkdir -p ClickHouse/build
cd ClickHouse/build
cmake .. -DUNBUNDLED=1 -DUSE_STATIC_LIBRARIES=0
# build with boost 1.64 from ports temporary broken

make clickhouse-bundle -j $(nproc || sysctl -n hw.ncpu || echo 2)
cd ../..

#  Run server:
# ClickHouse/build/dbms/src/Server/clickhouse-server --config-file=ClickHouse/dbms/src/Server/config.xml &

#  Run client:
# ClickHouse/build/dbms/src/Server/clickhouse-client
