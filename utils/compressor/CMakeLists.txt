find_package (Threads)

add_executable (zstd_test zstd_test.cpp)
if(ZSTD_LIBRARY)
    target_link_libraries(zstd_test PRIVATE ${ZSTD_LIBRARY})
endif()
target_link_libraries (zstd_test PRIVATE common)

add_executable (mutator mutator.cpp)
target_link_libraries(mutator PRIVATE clickhouse_common_io)

add_executable (decompress_perf decompress_perf.cpp)
target_link_libraries(decompress_perf PRIVATE dbms ${LZ4_LIBRARY})

if (NOT USE_INTERNAL_ZSTD_LIBRARY AND ZSTD_INCLUDE_DIR)
    target_include_directories (zstd_test BEFORE PRIVATE ${ZSTD_INCLUDE_DIR})
endif ()
