add_executable(keeper-bench Generator.cpp Runner.cpp Stats.cpp main.cpp)
target_link_libraries(keeper-bench PRIVATE clickhouse_common_zookeeper_no_log)
