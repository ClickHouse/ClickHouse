add_executable (mutator mutator.cpp)
target_link_libraries(mutator PRIVATE clickhouse_common_io)

add_executable (decompress_perf decompress_perf.cpp)
target_link_libraries(decompress_perf PRIVATE dbms ${LZ4_LIBRARY})
