find_package (Threads)

add_executable (util-clickhouse-compressor main.cpp)
target_link_libraries (util-clickhouse-compressor clickhouse-compressor-lib)
set_target_properties(util-clickhouse-compressor PROPERTIES OUTPUT_NAME "clickhouse-compressor")

#install (TARGETS util-clickhouse-compressor RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse-compressor)

add_executable (zstd_test zstd_test.cpp)
target_link_libraries (zstd_test ${ZSTD_LIBRARY} common Threads::Threads)
if (NOT USE_INTERNAL_ZSTD_LIBRARY)
    target_include_directories (zstd_test BEFORE PRIVATE ${ZSTD_INCLUDE_DIR})
endif ()
