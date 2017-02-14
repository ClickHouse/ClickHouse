
include_directories (BEFORE ${ClickHouse_SOURCE_DIR}/contrib/libzstd/include)

add_executable (clickhouse-compressor main.cpp)
target_link_libraries (clickhouse-compressor dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})

install (TARGETS clickhouse-compressor RUNTIME DESTINATION bin COMPONENT clickhouse-compressor)

add_executable (zstd_test zstd_test.cpp)
target_link_libraries (zstd_test zstd)
