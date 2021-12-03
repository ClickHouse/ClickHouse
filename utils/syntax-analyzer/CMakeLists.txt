add_executable(syntax-analyzer main.cpp)

target_link_libraries(syntax-analyzer PRIVATE clickhouse_parsers_new dbms)
