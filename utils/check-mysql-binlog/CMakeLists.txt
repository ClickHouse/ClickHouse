add_executable(check-mysql-binlog main.cpp)
target_link_libraries(check-mysql-binlog PRIVATE dbms boost::program_options)
