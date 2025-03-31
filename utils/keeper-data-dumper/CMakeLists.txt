if (NOT TARGET ch_contrib::nuraft)
    message (WARNING "Not building keeper-data-dumper due to nuraft is disabled")
    return ()
endif ()

clickhouse_add_executable(keeper-data-dumper main.cpp)
target_link_libraries(keeper-data-dumper PRIVATE dbms clickhouse_functions)
