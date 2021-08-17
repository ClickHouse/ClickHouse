add_executable(clickhouse-zookeeper-cli zookeeper-cli.cpp)
target_link_libraries(clickhouse-zookeeper-cli PRIVATE clickhouse_common_zookeeper)
target_no_warning(clickhouse-zookeeper-cli reserved-identifier)
