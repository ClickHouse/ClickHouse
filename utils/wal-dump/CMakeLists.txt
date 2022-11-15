clickhouse_add_executable (wal-dump main.cpp)
target_link_libraries(wal-dump PRIVATE dbms boost::program_options)
