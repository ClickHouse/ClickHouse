clickhouse_add_executable (decompress_perf decompress_perf.cpp)
target_link_libraries(decompress_perf PRIVATE dbms ch_contrib::lz4)
