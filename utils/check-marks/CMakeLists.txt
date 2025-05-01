clickhouse_add_executable (check-marks main.cpp)
target_link_libraries(check-marks PRIVATE dbms clickhouse_functions boost::program_options)
