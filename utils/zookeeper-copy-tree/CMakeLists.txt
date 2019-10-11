add_executable (zookeeper-copy-tree main.cpp ${SRCS})
target_link_libraries(zookeeper-copy-tree PRIVATE clickhouse_common_zookeeper clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
