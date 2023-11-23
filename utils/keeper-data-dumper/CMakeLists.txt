clickhouse_add_executable(keeper-data-dumper main.cpp)
target_link_libraries(keeper-data-dumper PRIVATE dbms)
