clickhouse_add_executable(clickhouse-zookeeper-cli
    zookeeper-cli.cpp
    ${ClickHouse_SOURCE_DIR}/src/Client/LineReader.cpp)
target_link_libraries(clickhouse-zookeeper-cli PRIVATE
    clickhouse_common_zookeeper_no_log
    dbms
    clickhouse_functions
)
