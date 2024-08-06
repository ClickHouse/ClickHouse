if (NOT TARGET ch_contrib::rapidjson)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Not building keeper-bench due to rapidjson is disabled")
    return()
endif ()

clickhouse_add_executable(keeper-bench Generator.cpp Runner.cpp Stats.cpp main.cpp)
target_link_libraries(keeper-bench PRIVATE dbms clickhouse_functions ch_contrib::rapidjson)
