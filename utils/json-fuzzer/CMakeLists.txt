add_executable(json-fuzzer main.cpp JSONFuzzer.cpp)

if (TARGET ch_contrib::simdjson)
    target_link_libraries(json-fuzzer PRIVATE ch_contrib::simdjson)
endif()

if (TARGET ch_contrib::rapidjson)
    target_link_libraries(json-fuzzer PRIVATE ch_contrib::rapidjson)
endif()

target_link_libraries(json-fuzzer PRIVATE dbms boost::program_options)
