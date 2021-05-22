add_executable (query_db_generator query_db_generator.cpp)
target_link_libraries(query_db_generator PRIVATE dbms boost::program_options)
