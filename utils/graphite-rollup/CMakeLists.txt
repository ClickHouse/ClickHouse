add_executable(graphite-rollup-bench graphite-rollup-bench.cpp)
target_link_libraries(
    graphite-rollup-bench
    PRIVATE
    clickhouse_storages_system
    clickhouse_aggregate_functions
    clickhouse_common_config
    dbms
)
target_include_directories(
    graphite-rollup-bench
    SYSTEM PRIVATE
    ${ClickHouse_SOURCE_DIR}/src ${CMAKE_BINARY_DIR}/src
    ${ClickHouse_SOURCE_DIR}/base ${ClickHouse_SOURCE_DIR}/base/pcg-random
    ${CMAKE_BINARY_DIR}/src/Core/include
    ${POCO_INCLUDE_DIR}
    ${ClickHouse_SOURCE_DIR}/contrib/double-conversion ${ClickHouse_SOURCE_DIR}/contrib/dragonbox/include
    ${ClickHouse_SOURCE_DIR}/contrib/fmtlib/include
    ${ClickHouse_SOURCE_DIR}/contrib/cityhash102/include
    ${RE2_INCLUDE_DIR} ${CMAKE_BINARY_DIR}/contrib/re2-cmake
)

target_compile_definitions(graphite-rollup-bench PRIVATE RULES_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
