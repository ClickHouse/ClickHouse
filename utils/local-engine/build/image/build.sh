mkdir -p /build && cd /build || exit
export CCACHE_DIR=/ccache
export CCACHE_BASEDIR=/build
export CCACHE_NOHASHDIR=true
export CCACHE_COMPILERCHECK=content
export CCACHE_MAXSIZE=15G

cmake -G Ninja  "-DCMAKE_C_COMPILER=$CC" "-DCMAKE_CXX_COMPILER=$CXX" "-DCMAKE_BUILD_TYPE=Release" "-DENABLE_PROTOBUF=1" "-DENABLE_EMBEDDED_COMPILER=$ENABLE_EMBEDDED_COMPILER" "-DENABLE_TESTS=OFF" "-DWERROR=OFF" "-DENABLE_JEMALLOC=1" /clickhouse
ninja ch

cp /build/utils/local-engine/libch.so "/output/libch_$(date +%Y%m%d).so"