#!/bin/sh

# How to build ClickHouse under debian-based systems (ubuntu)

# apt install -y curl sudo
# curl https://raw.githubusercontent.com/yandex/ClickHouse/master/utils/build/build_debian.sh | sh

# install compiler and libs
sudo apt install -y git bash cmake ninja-build gcc-8 g++-8 libicu-dev libreadline-dev gperf
# for -DUNBUNDLED=1 mode:
#sudo apt install -y libboost-program-options-dev libboost-system-dev libboost-filesystem-dev libboost-thread-dev libboost-regex-dev libboost-iostreams-dev zlib1g-dev liblz4-dev libdouble-conversion-dev libzstd-dev libre2-dev librdkafka-dev libcapnp-dev libpoco-dev libgoogle-perftools-dev libunwind-dev googletest libcctz-dev

# install testing only stuff if you want:
sudo apt install -y expect python3 python3-lxml python3-termcolor python3-requests curl perl sudo openssl netcat-openbsd telnet

BASE_DIR=$(dirname $0) && [ -f "$BASE_DIR/../../CMakeLists.txt" ] && ROOT_DIR=$BASE_DIR/../.. && cd $ROOT_DIR

if [ -z $ROOT_DIR ]; then
    # Checkout ClickHouse sources
    git clone --recursive https://github.com/yandex/ClickHouse.git
    cd ClickHouse
fi

# Build!
mkdir -p build
cd build
cmake .. -DCMAKE_CXX_COMPILER=`which g++-9 g++-8 | head -n1` -DCMAKE_C_COMPILER=`which gcc-9 gcc-8 | head -n1`
cmake --build .
cd ..

#  Run server:
# build/programs/clickhouse-server --config-file=ClickHouse/programs/server/config.xml &

#  Run client:
# build/programs/clickhouse-client
