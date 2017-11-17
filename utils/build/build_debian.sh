#!/bin/sh

# How to build ClickHouse under debian-based systems (ubuntu)

# apt install -y curl sudo
# curl https://raw.githubusercontent.com/yandex/ClickHouse/master/utils/build/build_debian.sh | sh

# install compiler and libs
sudo apt install -y git bash cmake gcc-7 g++-7 libicu-dev libreadline-dev libmysqlclient-dev unixodbc-dev libltdl-dev libssl-dev
# for -DUNBUNDLED=1 mode:
#sudo apt install -y libboost-dev zlib1g-dev liblz4-dev libdouble-conversion-dev libzstd-dev libre2-dev libzookeeper-mt-dev libsparsehash-dev librdkafka-dev libpoco-dev libsparsehash-dev libgoogle-perftools-dev libunwind-dev libzstd-dev

# install testing only stuff if you want:
sudo apt install -y python python-lxml python-termcolor curl perl

# Checkout ClickHouse sources
git clone --recursive https://github.com/yandex/ClickHouse.git

# Build!
mkdir -p ClickHouse/build
cd ClickHouse/build
cmake .. -DCMAKE_CXX_COMPILER=`which g++-7` -DCMAKE_C_COMPILER=`which gcc-7`

make -j $(nproc || sysctl -n hw.ncpu || echo 2)
cd ..
