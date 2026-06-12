clickhouse_add_executable(query-analyzer main.cpp)
target_link_libraries(query-analyzer PRIVATE
    dbms
    clickhouse_table_functions
    clickhouse_aggregate_functions
    clickhouse_functions
    clickhouse_dictionaries
    clickhouse_dictionaries_embedded
    boost::program_options)
