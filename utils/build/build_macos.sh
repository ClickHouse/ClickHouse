#!/bin/sh

# How to build ClickHouse on Mac OS X
#  Build should work on Mac OS X 10.12. If you're using earlier version, you can try to build ClickHouse using Gentoo Prefix and clang sl in this instruction.
#  With appropriate changes, build should work on any other OS X distribution.

## Install Homebrew

if [ -z `which brew` ]; then
    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
fi

## Install required compilers, tools, libraries

brew install cmake ninja gcc icu4c mariadb-connector-c openssl unixodbc libtool gettext readline librdkafka

# If you want to run tests
brew install python
sudo pip install lxml termcolor requests

## Checkout ClickHouse sources

#  To get the latest stable version:

BASE_DIR=$(dirname $0) && [ -f "$BASE_DIR/../../CMakeLists.txt" ] && ROOT_DIR=$BASE_DIR/../.. && cd $ROOT_DIR

if [ -z $ROOT_DIR ]; then
    # Checkout ClickHouse sources
    git clone -b stable --recursive https://github.com/ClickHouse/ClickHouse.git
    cd ClickHouse
fi

#  For development, switch to the `master` branch.
#  For the latest release candidate, switch to the `testing` branch.

## Build ClickHouse

mkdir build
cd build
cmake .. -DCMAKE_CXX_COMPILER=`which g++-9 g++-8` -DCMAKE_C_COMPILER=`which gcc-9 gcc-8`
cmake --build .

cd ..

#  Run server:
# build/programs/clickhouse-server --config-file=ClickHouse/programs/server/config.xml &

#  Run client:
# build/programs/clickhouse-client


## Caveats
#  If you intend to run clickhouse-server, make sure to increase system's maxfiles variable. See [MacOS.md](https://github.com/ClickHouse/ClickHouse/blob/master/MacOS.md) for more details.
