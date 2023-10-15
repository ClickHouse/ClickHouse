clickhouse_add_executable (zookeeper-remove-by-list main.cpp ${SRCS})
target_link_libraries(zookeeper-remove-by-list PRIVATE clickhouse_common_zookeeper_no_log clickhouse_common_io clickhouse_parsers boost::program_options)
