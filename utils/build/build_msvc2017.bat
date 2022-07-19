
:: WINDOWS BUILD NOT SUPPORTED!
:: Script only for development

cd ../..
git clone --recursive https://github.com/madler/zlib contrib/zlib
md build
cd build

:: Stage 1: try build client
cmake .. -G "Visual Studio 15 2017 Win64" -DENABLE_CLICKHOUSE_ALL=0 -DENABLE_CLICKHOUSE_CLIENT=1 > cmake.log
cmake --build . --target clickhouse -- /m > build.log
:: Stage 2: try build minimal server
:: Stage 3: enable all possible features (ssl, ...)
