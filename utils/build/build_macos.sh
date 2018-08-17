#!/bin/sh

# How to build ClickHouse on Mac OS X
#  Build should work on Mac OS X 10.12. If you're using earlier version, you can try to build ClickHouse using Gentoo Prefix and clang sl in this instruction.
#  With appropriate changes, build should work on any other OS X distribution.

## Install Homebrew

if [ -z `which brew` ]; then
    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
fi

## Install required compilers, tools, libraries

brew install cmake gcc icu4c mariadb-connector-c openssl unixodbc libtool gettext readline

## Checkout ClickHouse sources

#  To get the latest stable version:

git clone -b stable --recursive --depth=10 https://github.com/yandex/ClickHouse.git

cd ClickHouse

#  For development, switch to the `master` branch.
#  For the latest release candidate, switch to the `testing` branch.

## Build ClickHouse

mkdir build
cd build
cmake .. -DCMAKE_CXX_COMPILER=`which g++-8 g++-7` -DCMAKE_C_COMPILER=`which gcc-8 gcc-7`
make -j `sysctl -n hw.ncpu`

cd ../..

#  Run server:
# ClickHouse/build/dbms/programs/clickhouse-server --config-file=ClickHouse/dbms/programs/server/config.xml &

#  Run client:
# ClickHouse/build/dbms/programs/clickhouse-client


## Caveats
#  If you intend to run clickhouse-server, make sure to increase system's maxfiles variable. See [MacOS.md](https://github.com/yandex/ClickHouse/blob/master/MacOS.md) for more details.
