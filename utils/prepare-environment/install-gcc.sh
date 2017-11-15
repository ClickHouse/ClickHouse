#!/usr/bin/env bash

set -e

sudo apt-get install -y curl

VERSION=$(curl -sSL https://ftpmirror.gnu.org/gcc/ | grep -oE 'gcc-[0-9]+(\.[0-9]+)+' | sort -Vr | head -n1) #'
#VERSION=gcc-7.1.0

VERSION_SHORT=$(echo "$VERSION" | grep -oE '[0-9]' | head -n1)

echo "Will download ${VERSION} (short version: $VERSION_SHORT)."

THREADS=$(grep -c ^processor /proc/cpuinfo)

cd ~
mkdir gcc
cd gcc

wget https://ftpmirror.gnu.org/gcc/${VERSION}/${VERSION}.tar.xz
tar xf ${VERSION}.tar.xz
cd ${VERSION}
./contrib/download_prerequisites
cd ..
mkdir gcc-build
cd gcc-build
../${VERSION}/configure --enable-languages=c,c++ --disable-multilib
make -j $THREADS
sudo make install

sudo ln -sf /usr/local/bin/gcc /usr/local/bin/gcc-${VERSION_SHORT}
sudo ln -sf /usr/local/bin/g++ /usr/local/bin/g++-${VERSION_SHORT}
sudo ln -sf /usr/local/bin/gcc /usr/local/bin/cc
sudo ln -sf /usr/local/bin/g++ /usr/local/bin/c++

echo "/usr/local/lib64" | sudo tee /etc/ld.so.conf.d/10_local-lib64.conf
sudo ldconfig

hash gcc g++
gcc --version
