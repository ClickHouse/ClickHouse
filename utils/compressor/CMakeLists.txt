find_package (Threads)

add_executable (zstd_test zstd_test.cpp)
target_link_libraries (zstd_test PRIVATE ${ZSTD_LIBRARY} common Threads::Threads)

add_executable (mutator mutator.cpp)
target_link_libraries(mutator PRIVATE clickhouse_common_io)

add_executable (decompress_perf decompress_perf.cpp)
target_link_libraries(decompress_perf PRIVATE clickhouse_common_io clickhouse_compression ${LZ4_LIBRARY})

if (NOT USE_INTERNAL_ZSTD_LIBRARY)
    target_include_directories (zstd_test BEFORE PRIVATE ${ZSTD_INCLUDE_DIR})
endif ()
