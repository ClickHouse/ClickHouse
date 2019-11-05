add_executable (check-marks main.cpp)
target_link_libraries(check-marks PRIVATE dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})
