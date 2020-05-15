add_executable (fill-factor main.cpp)
target_link_libraries(fill-factor PRIVATE clickhouse_common_io)
