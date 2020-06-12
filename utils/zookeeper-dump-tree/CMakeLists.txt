add_executable (zookeeper-dump-tree main.cpp ${SRCS})
target_link_libraries(zookeeper-dump-tree PRIVATE clickhouse_common_zookeeper clickhouse_common_io boost::program_options)
