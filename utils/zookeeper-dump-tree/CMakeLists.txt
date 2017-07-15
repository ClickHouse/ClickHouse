add_executable (zookeeper-dump-tree main.cpp ${SRCS})
target_link_libraries (zookeeper-dump-tree dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})
