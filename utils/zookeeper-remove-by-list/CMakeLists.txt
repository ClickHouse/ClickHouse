
add_executable (zookeeper-remove-by-list main.cpp ${SRCS})
target_link_libraries (zookeeper-remove-by-list zkutil dbms ${BOOST_PROGRAM_OPTIONS_LIB})
