set(RAPIDJSON_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/rapidjson/include")
function(add_cxx_compile_options option)
    add_compile_options("$<$<STREQUAL:$<TARGET_PROPERTY:LINKER_LANGUAGE>,CXX>:${option}>")
endfunction()
add_cxx_compile_options(-Wzero-as-null-pointer-constant)
add_executable (local_engine local_engine.cpp)
target_include_directories(local_engine PRIVATE ${RAPIDJSON_INCLUDE_DIR})

set (CLICKHOUSE_SERVER_LINK
        PRIVATE
        dbms
        clickhouse_aggregate_functions
        clickhouse_common_io
        clickhouse_functions
        clickhouse_storages_system
        )

target_link_libraries(local_engine ${CLICKHOUSE_SERVER_LINK})

