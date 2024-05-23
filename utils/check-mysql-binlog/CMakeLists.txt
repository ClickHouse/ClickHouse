clickhouse_add_executable(check-mysql-binlog main.cpp)
target_link_libraries(check-mysql-binlog PRIVATE dbms clickhouse_functions boost::program_options)
