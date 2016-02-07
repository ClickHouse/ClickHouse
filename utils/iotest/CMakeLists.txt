add_executable (iotest iotest.cpp ${SRCS})
target_link_libraries (iotest dbms)

add_executable (iotest_nonblock iotest_nonblock.cpp ${SRCS})
target_link_libraries (iotest_nonblock dbms)

add_executable (iotest_aio iotest_aio.cpp ${SRCS})
target_link_libraries (iotest_aio dbms)
