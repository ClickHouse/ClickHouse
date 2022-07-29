message (WARNING "DEBUG 3 ${ENABLE_UTILS}")
clickhouse_add_executable(keeper-data-dumper main.cpp)
target_link_libraries(keeper-data-dumper PRIVATE dbms)
