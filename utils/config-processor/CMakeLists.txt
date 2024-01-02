clickhouse_add_executable (config-processor config-processor.cpp)
target_link_libraries(config-processor PRIVATE dbms)
