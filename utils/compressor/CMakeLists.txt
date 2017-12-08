find_package (Threads)

add_executable (clickhouse-compressor main.cpp)
target_link_libraries (clickhouse-compressor libclickhouse-compressor)

install (TARGETS clickhouse-compressor RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse-compressor)

add_executable (zstd_test zstd_test.cpp)
target_link_libraries (zstd_test ${ZSTD_LIBRARY} Threads::Threads)
