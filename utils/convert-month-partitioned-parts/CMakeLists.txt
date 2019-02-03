add_executable (convert-month-partitioned-parts main.cpp)
target_link_libraries(convert-month-partitioned-parts PRIVATE dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})
