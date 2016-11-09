add_executable (zookeeper-dump-tree main.cpp ${SRCS})
target_link_libraries (zookeeper-dump-tree zkutil dbms ${BOOST_PROGRAM_OPTIONS_LIB})
