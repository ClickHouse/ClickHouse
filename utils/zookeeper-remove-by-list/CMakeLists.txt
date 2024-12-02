clickhouse_add_executable (zookeeper-remove-by-list main.cpp ${SRCS})
target_link_libraries(zookeeper-remove-by-list PRIVATE clickhouse_common_zookeeper_no_log boost::program_options)
