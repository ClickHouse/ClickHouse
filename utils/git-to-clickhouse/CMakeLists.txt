add_executable (git-to-clickhouse git-to-clickhouse.cpp)
target_link_libraries(git-to-clickhouse PRIVATE dbms boost::program_options)
