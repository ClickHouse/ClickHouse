find_package (Threads)

add_executable (util-clickhouse-compressor main.cpp)
target_link_libraries (util-clickhouse-compressor clickhouse-compressor-lib)
set_target_properties(util-clickhouse-compressor PROPERTIES OUTPUT_NAME "clickhouse-compressor")

#install (TARGETS util-clickhouse-compressor RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse-compressor)

add_executable (zstd_test zstd_test.cpp)
target_link_libraries (zstd_test ${ZSTD_LIBRARY} common Threads::Threads)

add_executable (mutator mutator.cpp)
target_link_libraries (mutator clickhouse_common_io)

add_executable (decompress_perf decompress_perf.cpp)
target_link_libraries (decompress_perf clickhouse_common_io)

if (NOT USE_INTERNAL_ZSTD_LIBRARY)
    target_include_directories (zstd_test BEFORE PRIVATE ${ZSTD_INCLUDE_DIR})
endif ()
