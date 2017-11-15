add_executable (zookeeper-remove-by-list main.cpp ${SRCS})
target_link_libraries (zookeeper-remove-by-list dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})
