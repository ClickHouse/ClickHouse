add_executable (zookeeper-remove-by-list main.cpp ${SRCS})
target_link_libraries (zookeeper-remove-by-list zkutil dbms libboost_program_options.a)
