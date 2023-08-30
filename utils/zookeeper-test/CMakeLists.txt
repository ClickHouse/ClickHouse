clickhouse_add_executable(zk-test main.cpp)
target_link_libraries(zk-test PRIVATE clickhouse_common_zookeeper_no_log)
