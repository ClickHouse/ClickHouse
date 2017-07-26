#!/usr/bin/env bash

set -e

sudo apt-get install -y curl

VERSION=$(curl -sSL https://ftpmirror.gnu.org/gcc/ | grep -oE 'gcc-[0-9]+(\.[0-9]+)+' | sort -Vr | head -n1) #'
#VERSION=gcc-7.1.0

VERSION_SHORT=$(echo "$VERSION" | grep -oE '[0-9]')

echo "Will download ${VERSION} (short version: $VERSION_SHORT)."

THREADS=$(grep -c ^processor /proc/cpuinfo)

cd ~

wget https://ftpmirror.gnu.org/gcc/${VERSION}/${VERSION}.tar.bz2
tar xf ${VERSION}.tar.bz2
cd ${VERSION}
./contrib/download_prerequisites
cd ..
mkdir gcc-build
cd gcc-build
../${VERSION}/configure --enable-languages=c,c++
make -j $THREADS
sudo make install

sudo ln -s /usr/local/bin/gcc /usr/local/bin/gcc-${VERSION_SHORT}
sudo ln -s /usr/local/bin/g++ /usr/local/bin/g++-${VERSION_SHORT}
sudo ln -s /usr/local/bin/gcc /usr/local/bin/cc
sudo ln -s /usr/local/bin/g++ /usr/local/bin/c++

hash gcc g++
gcc --version
