#!/bin/sh

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd ../.. && pwd)

# also possible: DIST=bionic DIST=testing
export DIST=${DIST=unstable}

cd $ROOT_DIR
. $ROOT_DIR/debian/.pbuilderrc
if [[ -n "$FORCE_PBUILDER_CREATE" || ! -e "$BASETGZ" ]] ; then
    sudo --preserve-env pbuilder create --configfile $ROOT_DIR/debian/.pbuilderrc $PBUILDER_OPT
fi

env TEST_RUN=1 \
    `# Skip tests:` \
    `# 00281 requires internal compiler` \
    `# 00416 requires patched poco from contrib/` \
    TEST_OPT="--skip long compile 00416 $TEST_OPT" \
    TEST_TRUE=false \
    DH_VERBOSE=1 \
    CMAKE_FLAGS="-DUNBUNDLED=1 $CMAKE_FLAGS" \
    `# Use all possible contrib libs from system` \
    `# psmisc - killall` \
    `# gdb - symbol test in pbuilder` \
    EXTRAPACKAGES="psmisc gdb clang-5.0 libc++abi-dev libc++-dev libboost-program-options-dev libboost-system-dev libboost-filesystem-dev libboost-thread-dev zlib1g-dev liblz4-dev libdouble-conversion-dev libsparsehash-dev librdkafka-dev libpoco-dev libsparsehash-dev libgoogle-perftools-dev libzstd-dev libre2-dev libunwind-dev googletest libcctz-dev $EXTRAPACKAGES" \
    pdebuild --configfile $ROOT_DIR/debian/.pbuilderrc $PDEBUILD_OPT
