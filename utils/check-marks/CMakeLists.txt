add_executable (check-marks main.cpp)
target_link_libraries(check-marks PRIVATE clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
