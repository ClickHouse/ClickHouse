
add_executable (iotest iotest.cpp ${SRCS})
target_link_libraries (iotest clickhouse_common_io)

add_executable (iotest_nonblock iotest_nonblock.cpp ${SRCS})
target_link_libraries (iotest_nonblock clickhouse_common_io)

if (NOT APPLE AND NOT ARCH_FREEBSD)
    add_executable (iotest_aio iotest_aio.cpp ${SRCS})
    target_link_libraries (iotest_aio clickhouse_common_io)
endif()
